defmodule SmolNet.Integration.Tls do
  @moduledoc """
  Opens `:ssl` connections over SmolNet or the kernel, by role, as
  `SmolNet.Integration.Network` opens TCP sockets.

  `:ssl` takes its transport through `cb_info`, and `SmolNet.Inet.Tcp` and
  `SmolNet.Inet6.Tcp` are nearly the transport it needs, but not quite
  (ausimian/smolnet#97):

    * every transport call `:ssl` makes passes `{:header, 0}` among its
      options, or asks for `:header`, and SmolNet rejects the option in
      `connect`, `listen`, `setopts` and `getopts`;
    * a TLS server's accept and upgrade call the transport's `port/1`,
      which SmolNet does not export;
    * `:ssl.listen/2` watches its listener with `:inet.monitor/1`, which
      calls `monitor/1` in the socket's own module, which SmolNet does not
      export.

  So a SmolNet socket's `cb_info` is `SmolNet.Integration.Tls.Inet` or
  `SmolNet.Integration.Tls.Inet6`, which drop `:header` (`:ssl` only ever
  sets the default, 0) and answer `port/1` from `sockname/1`. They cannot
  supply `monitor/1`, which `:inet` looks up in the socket's own module, so
  a TLS server over SmolNet accepts with `:gen_tcp` and upgrades the socket
  with `:ssl.handshake/3` rather than using `:ssl.listen/2`.

  With `direct: true` SmolNet's own modules are the `cb_info`, to check a
  fix; `gaps/1` lists what still stands in the way.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak.Context

  @doc """
  Connects `role` to `address` and `port` over TLS, with `tls_options` and
  the transport options for `role`.

  Options are `:timeout`, the connect's (default `:infinity`), `:buffer`,
  the socket buffers of a SmolNet socket (see `buffer_options/3`), and
  `:direct`.
  """
  @spec connect(
          Context.t(),
          Network.role(),
          :inet.ip_address(),
          :inet.port_number(),
          list(),
          keyword()
        ) :: {:ok, :ssl.sslsocket()} | {:error, term()}
  def connect(context, role, address, port, tls_options, options \\ []) do
    family = if tuple_size(address) == 4, do: :inet, else: :inet6
    transport = transport_options(context, role, family, options)

    :ssl.connect(
      address,
      port,
      transport ++ tls_options,
      Keyword.get(options, :timeout, :infinity)
    )
  end

  @doc """
  Runs the server side of a TLS handshake on `socket`, a connected
  `:gen_tcp` socket that `role` accepted.
  """
  @spec upgrade(
          Context.t(),
          Network.role(),
          Network.family(),
          :gen_tcp.socket(),
          list(),
          keyword()
        ) :: {:ok, :ssl.sslsocket()} | {:error, term()}
  def upgrade(context, role, family, socket, tls_options, options \\ []) do
    cb_info =
      if Network.smolnet?(context, role), do: [cb_info: cb_info(family, options)], else: []

    :ssl.handshake(socket, cb_info ++ tls_options, Keyword.get(options, :timeout, :infinity))
  end

  @doc """
  Returns the options that make `:ssl.connect/4` open its transport for
  `role`: the `cb_info` and stack for SmolNet, or only the family for the
  kernel.
  """
  @spec transport_options(Context.t(), Network.role(), Network.family(), keyword()) :: list()
  def transport_options(context, role, family, options \\ []) do
    if Network.smolnet?(context, role) do
      [{:cb_info, cb_info(family, options)}, {:smolnet_stack, context.stack}, family] ++
        buffer_options(context, role, Keyword.get(options, :buffer))
    else
      [family]
    end
  end

  @doc """
  Returns the options that give a SmolNet socket for `role` receive and
  send buffers of `bytes` each, or none for the kernel, whose buffers tune
  themselves, or when `bytes` is `nil`.
  """
  @spec buffer_options(Context.t(), Network.role(), pos_integer() | nil) :: keyword()
  def buffer_options(context, role, bytes) do
    if bytes != nil and Network.smolnet?(context, role),
      do: [recbuf: bytes, sndbuf: bytes],
      else: []
  end

  @doc """
  Lists what stops `:ssl` using `SmolNet.Inet.Tcp` directly as its
  `cb_info`, found by probing the run's stack, or returns `nil` without a
  stack. An empty list means the shim is no longer needed.
  """
  @spec gaps(Context.t()) :: [String.t()] | nil
  def gaps(%Context{stack: nil}), do: nil

  def gaps(context) do
    module = SmolNet.Inet.Tcp
    Code.ensure_loaded!(module)
    base = [:inet, {:smolnet_stack, context.stack}, {:ip, Network.smolnet_address(:inet)}]

    header =
      case module.listen(0, base ++ [{:header, 0}]) do
        {:ok, socket} ->
          module.close(socket)
          []

        {:error, reason} ->
          ["listen/2 rejects {:header, 0}: #{inspect(reason)}"]
      end

    getopts =
      case module.listen(0, base) do
        {:ok, socket} ->
          result = module.getopts(socket, [:header])
          module.close(socket)

          case result do
            {:ok, _values} -> []
            {:error, reason} -> ["getopts/2 rejects :header: #{inspect(reason)}"]
          end

        {:error, reason} ->
          ["listen/2 failed: #{inspect(reason)}"]
      end

    exports =
      for {name, arity} <- [port: 1, monitor: 1], not function_exported?(module, name, arity) do
        "#{inspect(module)} does not export #{name}/#{arity}"
      end

    header ++ getopts ++ exports
  end

  defp cb_info(family, options) do
    module =
      case {family, Keyword.get(options, :direct, false)} do
        {:inet, false} -> SmolNet.Integration.Tls.Inet
        {:inet6, false} -> SmolNet.Integration.Tls.Inet6
        {:inet, true} -> SmolNet.Inet.Tcp
        {:inet6, true} -> SmolNet.Inet6.Tcp
      end

    {module, :tcp, :tcp_closed, :tcp_error}
  end
end

for {shim, target} <- [
      {SmolNet.Integration.Tls.Inet, SmolNet.Inet.Tcp},
      {SmolNet.Integration.Tls.Inet6, SmolNet.Inet6.Tcp}
    ] do
  defmodule shim do
    @moduledoc """
    `#{inspect(target)}` as an `:ssl` transport: drops the `:header` option
    and answers `port/1`. See `SmolNet.Integration.Tls`.
    """

    @target target

    def connect(address, port, options, timeout),
      do: @target.connect(address, port, drop_header(options), timeout)

    def listen(port, options), do: @target.listen(port, drop_header(options))
    def setopts(socket, options), do: @target.setopts(socket, drop_header(options))

    def getopts(socket, names) do
      with {:ok, values} <- @target.getopts(socket, List.delete(names, :header)) do
        {:ok, if(:header in names, do: values ++ [header: 0], else: values)}
      end
    end

    def port(socket) do
      with {:ok, {_address, port}} <- @target.sockname(socket), do: {:ok, port}
    end

    defdelegate accept(socket, timeout), to: @target
    defdelegate send(socket, data), to: @target
    defdelegate recv(socket, length), to: @target
    defdelegate recv(socket, length, timeout), to: @target
    defdelegate shutdown(socket, how), to: @target
    defdelegate close(socket), to: @target
    defdelegate controlling_process(socket, pid), to: @target
    defdelegate sockname(socket), to: @target
    defdelegate peername(socket), to: @target
    defdelegate getstat(socket, names), to: @target

    # `:ssl` sets only the default, and SmolNet has no other.
    defp drop_header(options), do: Enum.reject(options, &match?({:header, _value}, &1))
  end
end
