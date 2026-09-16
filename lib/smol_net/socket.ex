defmodule SmolNet.Socket do
  @moduledoc """
  A stable, lightweight identity for a socket owned by a `SmolNet` stack.

  Socket values are not processes. Operations are serialized by the stack
  process identified in the `:stack` field. The native socket ID and generation
  together prevent delayed readiness from addressing a later socket.
  """

  alias SmolNet.Stack

  import Bitwise, only: [band: 2]

  @max_identity 576_460_752_303_423_487

  @enforce_keys [:stack, :id, :generation]
  defstruct [:stack, :id, :generation]

  @type t :: %__MODULE__{
          stack: pid(),
          id: pos_integer(),
          generation: pos_integer()
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
  def open(:inet6, :stream, :tcp, stack: stack), do: Stack.socket_open(stack)

  def open(:inet6, :stream, :tcp, _options), do: {:error, :invalid_options}

  def open(:inet, :stream, :tcp, _options), do: {:error, :unsupported_family}

  def open(:inet6, _type, _protocol, _options), do: {:error, :unsupported_socket}

  def open(_domain, _type, _protocol, options) when is_list(options) do
    {:error, :unsupported_family}
  end

  def open(_domain, _type, _protocol, _options), do: {:error, :invalid_options}

  @doc false
  @spec bind(t(), sockaddr_in6()) :: :ok | {:error, atom()}
  def bind(%__MODULE__{} = socket, address) do
    with true <- valid?(socket),
         {:ok, endpoint} <- encode_endpoint(address, :bind) do
      Stack.socket_bind(socket, endpoint)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def bind(_socket, _address), do: {:error, :invalid_socket}

  @doc false
  @spec connect(t(), sockaddr_in6(), :nowait) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  def connect(%__MODULE__{} = socket, address, :nowait) do
    with true <- valid?(socket),
         {:ok, endpoint} <- encode_endpoint(address, :remote) do
      Stack.socket_connect(socket, endpoint)
    else
      false -> {:error, :invalid_socket}
      {:error, _reason} = error -> error
    end
  end

  def connect(%__MODULE__{} = socket, _address, _timeout) do
    if valid?(socket), do: {:error, :unsupported_timeout}, else: {:error, :invalid_socket}
  end

  def connect(_socket, _address, _timeout), do: {:error, :invalid_socket}

  @doc false
  @spec sockname(t()) :: {:ok, sockaddr_in6()} | {:error, atom()}
  def sockname(%__MODULE__{} = socket) do
    if valid?(socket), do: Stack.socket_sockname(socket), else: {:error, :invalid_socket}
  end

  def sockname(_socket), do: {:error, :invalid_socket}

  @doc false
  @spec peername(t()) :: {:ok, sockaddr_in6()} | {:error, atom()}
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
  @spec new(pid(), map()) :: t()
  def new(stack, %{id: id, generation: generation})
      when is_pid(stack) and id in 1..@max_identity and generation in 1..@max_identity do
    %__MODULE__{stack: stack, id: id, generation: generation}
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
  def valid?(%__MODULE__{stack: stack, id: id, generation: generation}) do
    is_pid(stack) and id in 1..@max_identity and generation in 1..@max_identity
  end

  def valid?(_socket), do: false

  @doc false
  @spec endpoint_from_native(map()) :: sockaddr_in6()
  def endpoint_from_native(%{address: bytes, port: port, scope_id: scope_id}) do
    address =
      bytes
      |> Enum.chunk_every(2)
      |> Enum.map(fn [high, low] -> high * 256 + low end)
      |> List.to_tuple()

    %{family: :inet6, addr: address, port: port, flowinfo: 0, scope_id: scope_id}
  end

  defp encode_endpoint(%{family: :inet} = _address, _usage),
    do: {:error, :unsupported_family}

  defp encode_endpoint(%{family: :inet6, addr: address, port: port} = sockaddr, usage) do
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

  defp encode_endpoint(_address, _usage), do: {:error, :invalid_address}

  defp ipv6_bytes(address, usage) when is_tuple(address) and tuple_size(address) == 8 do
    segments = Tuple.to_list(address)

    if Enum.all?(segments, &(is_integer(&1) and &1 in 0..65_535)) do
      bytes = Enum.flat_map(segments, &[div(&1, 256), rem(&1, 256)])

      cond do
        hd(bytes) == 0xFF -> {:error, :invalid_address}
        usage == :remote and Enum.all?(bytes, &(&1 == 0)) -> {:error, :invalid_address}
        true -> {:ok, bytes}
      end
    else
      {:error, :invalid_address}
    end
  end

  defp ipv6_bytes(_address, _usage), do: {:error, :invalid_address}

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
