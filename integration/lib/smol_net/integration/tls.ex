defmodule SmolNet.Integration.Tls do
  @moduledoc """
  Opens `:ssl` connections and listeners over SmolNet or the kernel, by
  role, as `SmolNet.Integration.Network` opens TCP sockets.

  `:ssl` takes its transport through `cb_info`, and for SmolNet that is
  `SmolNet.Inet.Tcp` or `SmolNet.Inet6.Tcp` themselves, as a client and as
  a server behind `:ssl.listen/2`.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak.Context

  @doc """
  Connects `role` to `address` and `port` over TLS, with `tls_options` and
  the transport options for `role`.

  Options are `:timeout`, the connect's (default `:infinity`), and
  `:buffer`, the socket buffers of a SmolNet socket (see
  `buffer_options/3`).
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
    transport = transport_options(context, role, family, Keyword.get(options, :buffer))

    :ssl.connect(
      address,
      port,
      transport ++ tls_options,
      Keyword.get(options, :timeout, :infinity)
    )
  end

  @doc """
  Starts TLS as a client on `socket`, a connected, passive TCP socket of
  `role` for `family`, with `tls_options`, within `timeout` milliseconds.

  Connecting over TCP first, and then upgrading, tells a TCP connect's
  failure from a TLS handshake's, which `connect/6` does not.
  """
  @spec upgrade(Context.t(), Network.role(), term(), Network.family(), list(), timeout()) ::
          {:ok, :ssl.sslsocket()} | {:error, term()}
  def upgrade(context, role, socket, family, tls_options, timeout) do
    transport = if Network.smolnet?(context, role), do: [cb_info: cb_info(family)], else: []
    :ssl.connect(socket, transport ++ tls_options, timeout)
  end

  @doc """
  Listens for TLS on `role`'s address for `family`, on an ephemeral port,
  with `tls_options` and the transport options for `role`.

  Options are `:backlog` (default 16) and `:buffer`, as for `connect/6`.
  """
  @spec listen(Context.t(), Network.role(), Network.family(), list(), keyword()) ::
          {:ok, :ssl.sslsocket()} | {:error, term()}
  def listen(context, role, family, tls_options, options \\ []) do
    transport = transport_options(context, role, family, Keyword.get(options, :buffer))

    address = [
      ip: Network.address(context, role, family),
      backlog: Keyword.get(options, :backlog, 16)
    ]

    :ssl.listen(0, transport ++ address ++ tls_options)
  end

  @doc """
  Returns the options that make `:ssl` open its transport for `role`: the
  `cb_info`, stack and buffers for SmolNet (see `buffer_options/3`), or only
  the family for the kernel.
  """
  @spec transport_options(Context.t(), Network.role(), Network.family(), pos_integer() | nil) ::
          list()
  def transport_options(context, role, family, buffer \\ nil) do
    if Network.smolnet?(context, role) do
      [family, cb_info: cb_info(family), smolnet_stack: context.stack] ++
        buffer_options(context, role, buffer)
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
  Returns TLS options for a local server and its clients, with a
  certificate made for the purpose: `:server` holds the certificate and
  key, and `:client` verifies the server against its root, without a host
  name. Both are passive and binary.
  """
  @spec local_options() :: %{server: list(), client: list()}
  def local_options do
    key = [key: {:namedCurve, :secp256r1}, digest: :sha256]
    chain = %{root: key, intermediates: [], peer: key}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

    %{
      server: [cert: server[:cert], key: server[:key], active: false, mode: :binary],
      client: [
        verify: :verify_peer,
        cacerts: client[:cacerts],
        server_name_indication: :disable,
        active: false,
        mode: :binary
      ]
    }
  end

  defp cb_info(:inet), do: {SmolNet.Inet.Tcp, :tcp, :tcp_closed, :tcp_error}
  defp cb_info(:inet6), do: {SmolNet.Inet6.Tcp, :tcp, :tcp_closed, :tcp_error}
end
