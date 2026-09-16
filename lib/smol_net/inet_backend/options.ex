defmodule SmolNet.InetBackend.Options do
  @moduledoc false

  alias SmolNet.Stack.Ref

  @default_buffer 65_536
  @max_buffer 1_048_576
  @max_timeout 4_294_967_295
  @default_backlog 5
  @max_backlog 128

  @enforce_keys [:stack]
  defstruct stack: nil,
            active: true,
            mode: :list,
            packet: :raw,
            packet_size: @default_buffer,
            buffer: @default_buffer,
            send_timeout: :infinity,
            send_timeout_close: false,
            bind_address: nil,
            bind_port: 0,
            bind_scope_id: 0,
            backlog: @default_backlog

  @type active :: false | true | :once | 1..32_767
  @type packet :: :raw | :line | 1 | 2 | 4

  @type t :: %__MODULE__{
          stack: Ref.t(),
          active: active(),
          mode: :binary | :list,
          packet: packet(),
          packet_size: pos_integer(),
          buffer: pos_integer(),
          send_timeout: timeout(),
          send_timeout_close: boolean(),
          bind_address: :inet.ip6_address() | nil,
          bind_port: :inet.port_number(),
          bind_scope_id: non_neg_integer(),
          backlog: pos_integer()
        }

  @spec parse(list()) :: {:ok, t()} | {:error, atom()}
  def parse(options) when is_list(options) do
    case fetch_stack(options) do
      {:ok, stack} -> reduce(options, %__MODULE__{stack: stack}, :connect)
      {:error, _reason} = error -> error
    end
  end

  def parse(_options), do: {:error, :einval}

  @spec parse_listen(list(), :inet.port_number()) :: {:ok, t()} | {:error, atom()}
  def parse_listen(options, port)
      when is_list(options) and is_integer(port) and port in 0..65_535 do
    case fetch_stack(options) do
      {:ok, stack} -> reduce(options, %__MODULE__{stack: stack, bind_port: port}, :listen)
      {:error, _reason} = error -> error
    end
  end

  def parse_listen(_options, _port), do: {:error, :einval}

  @spec update(t(), list()) :: {:ok, t()} | {:error, atom()}
  def update(%__MODULE__{} = current, options) when is_list(options) do
    reduce(options, current, :runtime)
  end

  def update(%__MODULE__{}, _options), do: {:error, :einval}

  @spec get(t(), list()) :: {:ok, list()} | {:error, atom()}
  def get(%__MODULE__{} = options, names) when is_list(names) do
    names
    |> Enum.reduce_while([], fn name, values ->
      case option_value(options, name) do
        {:ok, value} -> {:cont, [value | values]}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> {:error, :einval}
      values -> {:ok, Enum.reverse(values)}
    end
  end

  def get(%__MODULE__{}, _names), do: {:error, :einval}

  @spec receive_limit(t()) :: pos_integer()
  def receive_limit(%__MODULE__{buffer: buffer, packet_size: packet_size}) do
    max(buffer, packet_size + 4)
  end

  @spec local_endpoint(t()) :: map() | nil
  def local_endpoint(%__MODULE__{bind_address: nil, bind_port: 0}), do: nil

  def local_endpoint(%__MODULE__{} = options) do
    %{
      family: :inet6,
      addr: options.bind_address || {0, 0, 0, 0, 0, 0, 0, 0},
      port: options.bind_port,
      flowinfo: 0,
      scope_id: options.bind_scope_id
    }
  end

  defp fetch_stack(options) do
    case Keyword.fetch(options, :smolnet_stack) do
      {:ok, %Ref{} = stack} -> {:ok, stack}
      _other -> {:error, :einval}
    end
  end

  defp reduce(options, initial, context) do
    Enum.reduce_while(options, {:ok, initial}, fn option, {:ok, current} ->
      case put_option(current, option, context) do
        {:ok, updated} -> {:cont, {:ok, updated}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp put_option(options, {:smolnet_stack, %Ref{}}, context)
       when context in [:connect, :listen],
       do: {:ok, options}

  defp put_option(_options, {:smolnet_stack, _stack}, context)
       when context in [:connect, :listen],
       do: {:error, :einval}

  defp put_option(options, :inet6, context) when context in [:connect, :listen],
    do: {:ok, options}

  defp put_option(_options, :inet, context) when context in [:connect, :listen],
    do: {:error, :eafnosupport}

  defp put_option(options, :binary, _context), do: {:ok, %{options | mode: :binary}}
  defp put_option(options, :list, _context), do: {:ok, %{options | mode: :list}}
  defp put_option(options, {:mode, mode}, context), do: put_option(options, mode, context)

  defp put_option(options, {:active, active}, _context) do
    case normalize_active(active) do
      {:ok, active} -> {:ok, %{options | active: active}}
      :error -> {:error, :einval}
    end
  end

  defp put_option(options, {:packet, packet}, _context) do
    case normalize_packet(packet) do
      {:ok, packet} -> {:ok, %{options | packet: packet}}
      :error -> {:error, :einval}
    end
  end

  defp put_option(options, {:packet_size, size}, _context)
       when is_integer(size) and size in 0..@max_buffer do
    size = if size == 0, do: @default_buffer, else: size
    {:ok, %{options | packet_size: size}}
  end

  defp put_option(options, {name, size}, _context)
       when name in [:buffer, :recbuf] and is_integer(size) and size in 1..@max_buffer do
    {:ok, %{options | buffer: size}}
  end

  defp put_option(options, {:send_timeout, timeout}, _context)
       when timeout == :infinity or
              (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    {:ok, %{options | send_timeout: timeout}}
  end

  defp put_option(options, {:send_timeout_close, close?}, _context) when is_boolean(close?) do
    {:ok, %{options | send_timeout_close: close?}}
  end

  defp put_option(options, {:ipv6_v6only, true}, _context), do: {:ok, options}

  defp put_option(options, {:ip, address}, context) when context in [:connect, :listen] do
    put_bind_address(options, address)
  end

  defp put_option(options, {:ifaddr, address}, context) when context in [:connect, :listen] do
    put_ifaddr(options, address)
  end

  defp put_option(options, {:port, port}, context)
       when context in [:connect, :listen] and is_integer(port) and port in 0..65_535 do
    {:ok, %{options | bind_port: port}}
  end

  defp put_option(options, {:backlog, backlog}, :listen)
       when is_integer(backlog) and backlog in 1..@max_backlog do
    {:ok, %{options | backlog: backlog}}
  end

  defp put_option(_options, option, :runtime)
       when option in [:inet, :inet6] or
              (is_tuple(option) and
                 tuple_size(option) > 0 and
                 elem(option, 0) in [:smolnet_stack, :ip, :ifaddr, :port, :backlog]) do
    {:error, :einval}
  end

  defp put_option(_options, _option, _context), do: {:error, :einval}

  defp put_ifaddr(options, %{family: :inet6, addr: address} = sockaddr) do
    with {:ok, options} <- put_bind_address(options, address),
         {:ok, port} <- bind_port(Map.get(sockaddr, :port, options.bind_port)),
         {:ok, scope_id} <- scope_id(Map.get(sockaddr, :scope_id, 0)) do
      {:ok, %{options | bind_port: port, bind_scope_id: scope_id}}
    end
  end

  defp put_ifaddr(_options, %{family: :inet}), do: {:error, :eafnosupport}
  defp put_ifaddr(options, address), do: put_bind_address(options, address)

  defp put_bind_address(options, :any) do
    {:ok, %{options | bind_address: {0, 0, 0, 0, 0, 0, 0, 0}}}
  end

  defp put_bind_address(options, :loopback) do
    {:ok, %{options | bind_address: {0, 0, 0, 0, 0, 0, 0, 1}}}
  end

  defp put_bind_address(options, address) when is_tuple(address) and tuple_size(address) == 8 do
    if address |> Tuple.to_list() |> Enum.all?(&(is_integer(&1) and &1 in 0..65_535)) do
      {:ok, %{options | bind_address: address}}
    else
      {:error, :einval}
    end
  end

  defp put_bind_address(_options, address) when is_tuple(address) and tuple_size(address) == 4,
    do: {:error, :eafnosupport}

  defp put_bind_address(_options, _address), do: {:error, :einval}

  defp bind_port(port) when is_integer(port) and port in 0..65_535, do: {:ok, port}
  defp bind_port(_port), do: {:error, :einval}

  defp scope_id(scope_id) when is_integer(scope_id) and scope_id in 0..4_294_967_295,
    do: {:ok, scope_id}

  defp scope_id(_scope_id), do: {:error, :einval}

  defp normalize_active(0), do: {:ok, false}
  defp normalize_active(active) when active in [false, true, :once], do: {:ok, active}
  defp normalize_active(active) when is_integer(active) and active in 1..32_767, do: {:ok, active}
  defp normalize_active(_active), do: :error

  defp normalize_packet(0), do: {:ok, :raw}
  defp normalize_packet(packet) when packet in [:raw, :line, 1, 2, 4], do: {:ok, packet}
  defp normalize_packet(_packet), do: :error

  defp option_value(options, :active), do: {:ok, {:active, options.active}}
  defp option_value(options, :mode), do: {:ok, {:mode, options.mode}}
  defp option_value(options, :packet), do: {:ok, {:packet, options.packet}}
  defp option_value(options, :packet_size), do: {:ok, {:packet_size, options.packet_size}}
  defp option_value(options, :buffer), do: {:ok, {:buffer, options.buffer}}
  defp option_value(options, :recbuf), do: {:ok, {:recbuf, options.buffer}}
  defp option_value(options, :send_timeout), do: {:ok, {:send_timeout, options.send_timeout}}
  defp option_value(options, :backlog), do: {:ok, {:backlog, options.backlog}}

  defp option_value(options, :send_timeout_close),
    do: {:ok, {:send_timeout_close, options.send_timeout_close}}

  defp option_value(_options, _name), do: :error
end
