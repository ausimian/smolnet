defmodule SmolNet.Socket do
  @moduledoc """
  A stable, lightweight identity for a socket owned by a `SmolNet` stack.

  Socket values are not processes. Operations are serialized by the stack
  process identified in the `:stack` field. The native socket ID and generation
  together prevent delayed readiness from addressing a later socket.
  """

  alias SmolNet.Stack

  import Bitwise, only: [band: 2]
  import Kernel, except: [send: 2]

  @max_identity 576_460_752_303_423_487
  @max_immediate_retries 16
  @max_timeout 4_294_967_295
  @max_backlog 128

  @enforce_keys [:stack, :id, :generation, :family]
  defstruct [:stack, :id, :generation, :family]

  @type t :: %__MODULE__{
          stack: pid(),
          id: pos_integer(),
          generation: pos_integer(),
          family: :inet | :inet6
        }

  @type ipv4_address :: {0..255, 0..255, 0..255, 0..255}

  @type sockaddr_in :: %{
          required(:family) => :inet,
          required(:addr) => ipv4_address(),
          required(:port) => 0..65_535
        }

  @type ipv6_address ::
          {0..65_535, 0..65_535, 0..65_535, 0..65_535, 0..65_535, 0..65_535, 0..65_535, 0..65_535}

  @type sockaddr_in6 :: %{
          required(:family) => :inet6,
          required(:addr) => ipv6_address(),
          required(:port) => 0..65_535,
          optional(:flowinfo) => 0,
          optional(:scope_id) => 0..4_294_967_295
        }

  @doc false
  @spec open(atom(), atom(), atom(), keyword()) ::
          {:ok, t()}
          | {:error, :unsupported_family | :unsupported_socket | :invalid_options | atom()}
  def open(family, :stream, :tcp, stack: stack) when family in [:inet, :inet6],
    do: Stack.socket_open(stack, family)

  def open(family, :stream, :tcp, _options) when family in [:inet, :inet6],
    do: {:error, :invalid_options}

  def open(family, _type, _protocol, _options) when family in [:inet, :inet6],
    do: {:error, :unsupported_socket}

  def open(_domain, _type, _protocol, options) when is_list(options) do
    {:error, :unsupported_family}
  end

  def open(_domain, _type, _protocol, _options), do: {:error, :invalid_options}

  @doc false
  @spec bind(t(), sockaddr_in() | sockaddr_in6()) :: :ok | {:error, atom()}
  def bind(%__MODULE__{} = socket, address) do
    with true <- valid?(socket),
         {:ok, endpoint} <- encode_endpoint(address, :bind, socket.family) do
      Stack.socket_bind(socket, endpoint)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def bind(_socket, _address), do: {:error, :invalid_socket}

  @doc false
  @spec listen(t(), pos_integer()) :: :ok | {:error, atom()}
  def listen(%__MODULE__{} = socket, backlog)
      when is_integer(backlog) and backlog in 1..@max_backlog do
    if valid?(socket), do: Stack.socket_listen(socket, backlog), else: {:error, :invalid_socket}
  end

  def listen(%__MODULE__{} = socket, _backlog) do
    if valid?(socket), do: {:error, :invalid_backlog}, else: {:error, :invalid_socket}
  end

  def listen(_socket, _backlog), do: {:error, :invalid_socket}

  @doc false
  @spec accept(t()) :: {:ok, t()} | {:error, atom()}
  def accept(socket), do: accept(socket, :infinity)

  @doc false
  @spec accept(t(), :nowait | timeout()) ::
          {:ok, t()} | {:select, :socket.select_info()} | {:error, atom()}
  def accept(%__MODULE__{} = socket, :nowait) do
    if valid?(socket), do: Stack.socket_accept(socket), else: {:error, :invalid_socket}
  end

  def accept(%__MODULE__{} = socket, timeout)
      when timeout == :infinity or
             (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    if valid?(socket) do
      synchronous(socket, timeout, fn deadline, monitor ->
        accept_loop(socket, deadline, monitor, 0)
      end)
    else
      {:error, :invalid_socket}
    end
  end

  def accept(%__MODULE__{} = socket, _timeout) do
    if valid?(socket), do: {:error, :invalid_timeout}, else: {:error, :invalid_socket}
  end

  def accept(_socket, _timeout), do: {:error, :invalid_socket}

  @doc false
  @spec connect(t(), sockaddr_in() | sockaddr_in6()) :: :ok | {:error, atom()}
  def connect(socket, address), do: connect(socket, address, :infinity)

  @doc false
  @spec connect(t(), sockaddr_in() | sockaddr_in6(), :nowait | timeout()) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  def connect(%__MODULE__{} = socket, address, :nowait) do
    with true <- valid?(socket),
         {:ok, endpoint} <- encode_endpoint(address, :remote, socket.family) do
      Stack.socket_connect(socket, endpoint)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def connect(%__MODULE__{} = socket, address, timeout)
      when timeout == :infinity or
             (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    with true <- valid?(socket),
         {:ok, endpoint} <- encode_endpoint(address, :remote, socket.family) do
      synchronous(socket, timeout, fn deadline, monitor ->
        connect_loop(socket, endpoint, deadline, monitor, 0)
      end)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def connect(%__MODULE__{} = socket, _address, _timeout) do
    if valid?(socket), do: {:error, :invalid_timeout}, else: {:error, :invalid_socket}
  end

  def connect(_socket, _address, _timeout), do: {:error, :invalid_socket}

  @doc false
  @spec send(t(), iodata()) :: :ok | {:error, atom() | {atom(), binary()}}
  def send(socket, data), do: send(socket, data, :infinity)

  @doc false
  @spec send(t(), iodata(), :nowait | timeout()) ::
          :ok
          | {:select, {:socket.select_info(), binary()}}
          | {:error, atom() | {atom(), binary()}}
  def send(%__MODULE__{} = socket, data, :nowait) do
    with true <- valid?(socket),
         {:ok, binary} <- encode_data(data) do
      Stack.socket_send(socket, binary)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def send(%__MODULE__{} = socket, data, timeout)
      when timeout == :infinity or
             (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    with true <- valid?(socket),
         {:ok, binary} <- encode_data(data) do
      synchronous(socket, timeout, fn deadline, monitor ->
        send_loop(socket, binary, deadline, monitor, false, 0)
      end)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def send(%__MODULE__{} = socket, _data, _timeout) do
    if valid?(socket), do: {:error, :invalid_timeout}, else: {:error, :invalid_socket}
  end

  def send(_socket, _data, _timeout), do: {:error, :invalid_socket}

  @doc false
  @spec recv(t(), non_neg_integer()) ::
          {:ok, binary()} | {:error, atom() | {atom(), binary()}}
  def recv(socket, length), do: recv(socket, length, :infinity)

  @doc false
  @spec recv(t(), non_neg_integer(), :nowait | timeout()) ::
          {:ok, binary()}
          | {:select, :socket.select_info()}
          | {:select, {:socket.select_info(), binary()}}
          | {:error, atom() | {atom(), binary()}}
  def recv(%__MODULE__{} = socket, length, :nowait) do
    with true <- valid?(socket),
         true <- valid_length?(length) do
      socket
      |> Stack.socket_recv(length)
      |> normalize_nowait_recv()
    else
      false -> invalid_socket_or_length(socket, length)
    end
  end

  def recv(%__MODULE__{} = socket, length, timeout)
      when timeout == :infinity or
             (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    with true <- valid?(socket),
         true <- valid_length?(length) do
      synchronous(socket, timeout, fn deadline, monitor ->
        recv_loop(socket, length, deadline, monitor, [], 0)
      end)
    else
      false -> invalid_socket_or_length(socket, length)
    end
  end

  def recv(%__MODULE__{} = socket, _length, _timeout) do
    if valid?(socket), do: {:error, :invalid_timeout}, else: {:error, :invalid_socket}
  end

  def recv(_socket, _length, _timeout), do: {:error, :invalid_socket}

  @doc false
  @spec shutdown(t(), :read | :write | :read_write) :: :ok | {:error, atom()}
  def shutdown(%__MODULE__{} = socket, how) when how in [:read, :write, :read_write] do
    if valid?(socket), do: Stack.socket_shutdown(socket, how), else: {:error, :invalid_socket}
  end

  def shutdown(%__MODULE__{} = socket, _how) do
    if valid?(socket), do: {:error, :invalid_how}, else: {:error, :invalid_socket}
  end

  def shutdown(_socket, _how), do: {:error, :invalid_socket}

  @doc false
  @spec sockname(t()) :: {:ok, sockaddr_in() | sockaddr_in6()} | {:error, atom()}
  def sockname(%__MODULE__{} = socket) do
    if valid?(socket), do: Stack.socket_sockname(socket), else: {:error, :invalid_socket}
  end

  def sockname(_socket), do: {:error, :invalid_socket}

  @doc false
  @spec peername(t()) :: {:ok, sockaddr_in() | sockaddr_in6()} | {:error, atom()}
  def peername(%__MODULE__{} = socket) do
    if valid?(socket), do: Stack.socket_peername(socket), else: {:error, :invalid_socket}
  end

  def peername(_socket), do: {:error, :invalid_socket}

  @doc false
  @spec close(t()) :: :ok | {:error, atom()}
  def close(%__MODULE__{} = socket) do
    if valid?(socket), do: Stack.socket_close(socket), else: {:error, :invalid_socket}
  end

  def close(_socket), do: {:error, :invalid_socket}

  @doc false
  @spec new(pid(), map(), :inet | :inet6) :: t()
  def new(stack, identity, family \\ :inet6)

  def new(stack, %{id: id, generation: generation}, family)
      when is_pid(stack) and id in 1..@max_identity and generation in 1..@max_identity and
             family in [:inet, :inet6] do
    %__MODULE__{stack: stack, id: id, generation: generation, family: family}
  end

  @doc """
  Cancels the exact pending operation described by `select_info`.

  Returns `:ok` when the waiter was removed, `:already_sent` when its one-shot
  readiness notification won the race, or `:not_found` when the reference does
  not identify the pending operation.
  """
  @spec cancel(t(), :socket.select_info()) ::
          :ok | :already_sent | :not_found | {:error, :closed | :invalid_socket}
  def cancel(
        socket,
        {:select_info, operation, reference} = select_info
      )
      when operation in [:recv, :recvfrom, :accept, :send, :sendto, :connect] and
             is_reference(reference) do
    if valid?(socket) do
      Stack.cancel(socket, select_info)
    else
      {:error, :invalid_socket}
    end
  end

  @doc false
  @spec identity(t()) :: {pos_integer(), pos_integer()}
  def identity(%__MODULE__{id: id, generation: generation}), do: {id, generation}

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{stack: stack, id: id, generation: generation, family: family}) do
    is_pid(stack) and id in 1..@max_identity and generation in 1..@max_identity and
      family in [:inet, :inet6]
  end

  def valid?(_socket), do: false

  @doc false
  @spec endpoint_from_native(map()) :: sockaddr_in() | sockaddr_in6()
  def endpoint_from_native(%{address: [a, b, c, d], port: port, scope_id: 0}) do
    %{family: :inet, addr: {a, b, c, d}, port: port}
  end

  def endpoint_from_native(%{address: bytes, port: port, scope_id: scope_id}) do
    address =
      bytes
      |> Enum.chunk_every(2)
      |> Enum.map(fn [high, low] -> high * 256 + low end)
      |> List.to_tuple()

    %{family: :inet6, addr: address, port: port, flowinfo: 0, scope_id: scope_id}
  end

  defp connect_loop(socket, endpoint, deadline, monitor, retries) do
    case prepare_retry(deadline, retries) do
      :ok ->
        socket
        |> Stack.socket_connect(endpoint)
        |> handle_connect_result(socket, endpoint, deadline, monitor, retries)

      :timeout ->
        {:error, :timeout}
    end
  end

  defp accept_loop(socket, deadline, monitor, retries) do
    case prepare_retry(deadline, retries) do
      :ok ->
        socket
        |> Stack.socket_accept()
        |> handle_accept_result(socket, deadline, monitor, retries)

      :timeout ->
        {:error, :timeout}
    end
  end

  defp handle_accept_result(
         {:select, select_info},
         socket,
         deadline,
         monitor,
         retries
       ) do
    socket
    |> await_select(select_info, deadline, monitor)
    |> continue_accept(socket, deadline, monitor, retries)
  end

  defp handle_accept_result(result, _socket, _deadline, _monitor, _retries), do: result

  defp continue_accept(:ready, socket, deadline, monitor, retries) do
    accept_loop(socket, deadline, monitor, retries + 1)
  end

  defp continue_accept({:error, reason}, _socket, _deadline, _monitor, _retries),
    do: {:error, reason}

  defp handle_connect_result(
         {:select, select_info},
         socket,
         endpoint,
         deadline,
         monitor,
         retries
       ) do
    socket
    |> await_select(select_info, deadline, monitor)
    |> continue_connect(socket, endpoint, deadline, monitor, retries)
  end

  defp handle_connect_result(result, _socket, _endpoint, _deadline, _monitor, _retries),
    do: result

  defp continue_connect(:ready, socket, endpoint, deadline, monitor, retries) do
    connect_loop(socket, endpoint, deadline, monitor, retries + 1)
  end

  defp continue_connect({:error, reason}, _socket, _endpoint, _deadline, _monitor, _retries),
    do: {:error, reason}

  defp send_loop(socket, data, deadline, monitor, sent?, retries) do
    case prepare_retry(deadline, retries) do
      :ok ->
        socket
        |> Stack.socket_send(data)
        |> handle_send_result(socket, data, deadline, monitor, sent?, retries)

      :timeout ->
        send_error(:timeout, data, sent?)
    end
  end

  defp handle_send_result(:ok, _socket, _data, _deadline, _monitor, _sent?, _retries), do: :ok

  defp handle_send_result(
         {:select, {select_info, remainder}},
         socket,
         data,
         deadline,
         monitor,
         sent?,
         retries
       ) do
    sent? = sent? or byte_size(remainder) < byte_size(data)

    socket
    |> await_select(select_info, deadline, monitor)
    |> continue_send(socket, remainder, deadline, monitor, sent?, retries)
  end

  defp handle_send_result(
         {:error, reason},
         _socket,
         data,
         _deadline,
         _monitor,
         sent?,
         _retries
       ),
       do: send_error(reason, data, sent?)

  defp continue_send(:ready, socket, remainder, deadline, monitor, sent?, retries) do
    send_loop(socket, remainder, deadline, monitor, sent?, retries + 1)
  end

  defp continue_send(
         {:error, reason},
         _socket,
         remainder,
         _deadline,
         _monitor,
         sent?,
         _retries
       ),
       do: send_error(reason, remainder, sent?)

  defp recv_loop(socket, length, deadline, monitor, chunks, retries) do
    case prepare_retry(deadline, retries) do
      :ok ->
        socket
        |> Stack.socket_recv(length)
        |> handle_recv_result(socket, length, deadline, monitor, chunks, retries)

      :timeout ->
        recv_error(:timeout, chunks)
    end
  end

  defp handle_recv_result(
         {:ok, data},
         _socket,
         _length,
         _deadline,
         _monitor,
         chunks,
         _retries
       ),
       do: {:ok, chunks_to_binary(chunks, data)}

  defp handle_recv_result(
         {:select, {select_info, data}},
         socket,
         length,
         deadline,
         monitor,
         chunks,
         retries
       ) do
    remaining = if length == 0, do: 0, else: length - byte_size(data)
    chunks = [data | chunks]

    socket
    |> await_select(select_info, deadline, monitor)
    |> continue_recv(socket, remaining, deadline, monitor, chunks, retries)
  end

  defp handle_recv_result(
         {:select, select_info},
         socket,
         length,
         deadline,
         monitor,
         chunks,
         retries
       ) do
    socket
    |> await_select(select_info, deadline, monitor)
    |> continue_recv(socket, length, deadline, monitor, chunks, retries)
  end

  defp handle_recv_result(
         {:error, :end_of_stream},
         _socket,
         _length,
         _deadline,
         _monitor,
         [_chunk | _rest] = chunks,
         _retries
       ),
       do: {:ok, chunks_to_binary(chunks, <<>>)}

  defp handle_recv_result(
         {:error, :end_of_stream},
         _socket,
         _length,
         _deadline,
         _monitor,
         [],
         _retries
       ),
       do: {:error, :closed}

  defp handle_recv_result(
         {:error, reason},
         _socket,
         _length,
         _deadline,
         _monitor,
         chunks,
         _retries
       ),
       do: recv_error(reason, chunks)

  defp continue_recv(:ready, socket, length, deadline, monitor, chunks, retries) do
    recv_loop(socket, length, deadline, monitor, chunks, retries + 1)
  end

  defp continue_recv(
         {:error, reason},
         _socket,
         _length,
         _deadline,
         _monitor,
         chunks,
         _retries
       ),
       do: recv_error(reason, chunks)

  defp synchronous(%__MODULE__{stack: stack}, timeout, operation) do
    monitor = Process.monitor(stack)
    deadline = deadline(timeout)

    try do
      operation.(deadline, monitor)
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  defp await_select(
         %__MODULE__{stack: stack, id: id, generation: generation} = socket,
         {:select_info, _operation, reference} = select_info,
         deadline,
         monitor
       ) do
    receive do
      {:"$smol_socket", {^id, ^generation}, :select, ^reference} ->
        :ready

      {:"$smol_socket", {^id, ^generation}, :abort, ^reference, reason} ->
        {:error, reason}

      {:DOWN, ^monitor, :process, ^stack, _reason} ->
        {:error, :closed}
    after
      remaining_timeout(deadline) ->
        _cancel_result = cancel(socket, select_info)
        drain_select_message(socket, reference)
        {:error, :timeout}
    end
  end

  defp drain_select_message(%__MODULE__{id: id, generation: generation}, reference) do
    receive do
      {:"$smol_socket", {^id, ^generation}, :select, ^reference} -> :ok
      {:"$smol_socket", {^id, ^generation}, :abort, ^reference, _reason} -> :ok
    after
      0 -> :ok
    end
  end

  defp send_error(reason, _remainder, false), do: {:error, reason}
  defp send_error(reason, remainder, true), do: {:error, {reason, remainder}}

  defp recv_error(reason, []), do: {:error, reason}
  defp recv_error(reason, chunks), do: {:error, {reason, chunks_to_binary(chunks, <<>>)}}

  defp normalize_nowait_recv({:error, :end_of_stream}), do: {:error, :closed}
  defp normalize_nowait_recv(result), do: result

  defp chunks_to_binary([], final), do: final
  defp chunks_to_binary(chunks, final), do: IO.iodata_to_binary([Enum.reverse(chunks), final])

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp remaining_timeout(:infinity), do: :infinity

  defp remaining_timeout(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp deadline_reached?(:infinity), do: false
  defp deadline_reached?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  defp prepare_retry(_deadline, 0), do: :ok

  defp prepare_retry(deadline, retries) do
    if deadline_reached?(deadline) do
      :timeout
    else
      yield_if_needed(retries)
    end
  end

  defp yield_if_needed(retries) when rem(retries, @max_immediate_retries) == 0 do
    Process.sleep(0)
  end

  defp yield_if_needed(_retries), do: :ok

  defp encode_data(data) do
    {:ok, IO.iodata_to_binary(data)}
  rescue
    ArgumentError -> {:error, :invalid_data}
  end

  defp valid_length?(length), do: is_integer(length) and length in 0..@max_identity

  defp invalid_socket_or_length(socket, length) do
    if valid?(socket) and not valid_length?(length),
      do: {:error, :invalid_length},
      else: {:error, :invalid_socket}
  end

  defp encode_endpoint(%{family: family}, _usage, expected) when family != expected,
    do: {:error, :invalid_address}

  defp encode_endpoint(%{family: :inet, addr: address, port: port} = sockaddr, usage, :inet) do
    with true <- Enum.all?(Map.keys(sockaddr), &(&1 in [:family, :addr, :port])),
         {:ok, bytes} <- ipv4_bytes(address, usage),
         :ok <- valid_port(port, usage) do
      {:ok, %{address: bytes, port: port, scope_id: 0}}
    else
      {:error, _reason} = error -> error
      false -> {:error, :invalid_address}
    end
  end

  defp encode_endpoint(%{family: :inet6, addr: address, port: port} = sockaddr, usage, :inet6) do
    with true <-
           Enum.all?(Map.keys(sockaddr), &(&1 in [:family, :addr, :port, :flowinfo, :scope_id])),
         true <- Map.get(sockaddr, :flowinfo, 0) === 0,
         {:ok, bytes} <- ipv6_bytes(address, usage),
         :ok <- valid_port(port, usage),
         {:ok, scope_id} <- valid_scope(bytes, Map.get(sockaddr, :scope_id, 0)) do
      {:ok, %{address: bytes, port: port, scope_id: scope_id}}
    else
      {:error, _reason} = error -> error
      false -> {:error, :invalid_address}
    end
  end

  defp encode_endpoint(_address, _usage, _family), do: {:error, :invalid_address}

  defp ipv4_bytes(address, usage) when is_tuple(address) and tuple_size(address) == 4 do
    octets = Tuple.to_list(address)

    cond do
      not Enum.all?(octets, &(is_integer(&1) and &1 in 0..255)) ->
        {:error, :invalid_address}

      hd(octets) in 224..239 ->
        {:error, :invalid_address}

      octets == [255, 255, 255, 255] ->
        {:error, :invalid_address}

      usage == :remote and Enum.all?(octets, &(&1 == 0)) ->
        {:error, :invalid_address}

      true ->
        {:ok, octets}
    end
  end

  defp ipv4_bytes(_address, _usage), do: {:error, :invalid_address}

  defp ipv6_bytes(address, usage) when is_tuple(address) and tuple_size(address) == 8 do
    segments = Tuple.to_list(address)

    if Enum.all?(segments, &(is_integer(&1) and &1 in 0..65_535)) do
      bytes = Enum.flat_map(segments, &[div(&1, 256), rem(&1, 256)])

      cond do
        hd(bytes) == 0xFF -> {:error, :invalid_address}
        ipv4_mapped?(bytes) -> {:error, :invalid_address}
        usage == :remote and Enum.all?(bytes, &(&1 == 0)) -> {:error, :invalid_address}
        true -> {:ok, bytes}
      end
    else
      {:error, :invalid_address}
    end
  end

  defp ipv6_bytes(_address, _usage), do: {:error, :invalid_address}

  defp ipv4_mapped?(bytes) do
    Enum.take(bytes, 10) == List.duplicate(0, 10) and Enum.slice(bytes, 10, 2) == [0xFF, 0xFF]
  end

  defp valid_port(port, :bind) when is_integer(port) and port in 0..65_535, do: :ok
  defp valid_port(port, :remote) when is_integer(port) and port in 1..65_535, do: :ok
  defp valid_port(_port, _usage), do: {:error, :invalid_port}

  defp valid_scope([first, second | _rest], scope_id)
       when first == 0xFE and band(second, 0xC0) == 0x80 do
    cond do
      scope_id === 0 -> {:error, :scope_required}
      is_integer(scope_id) and scope_id in 1..4_294_967_295 -> {:ok, scope_id}
      true -> {:error, :invalid_scope}
    end
  end

  defp valid_scope(_bytes, 0), do: {:ok, 0}
  defp valid_scope(_bytes, _scope_id), do: {:error, :invalid_scope}
end
