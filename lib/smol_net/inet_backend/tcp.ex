defmodule SmolNet.InetBackend.Tcp do
  @moduledoc """
  TCP adapter for Erlang `:gen_tcp` and `:inet`.

  Select this callback with the `:tcp_module` option and pass the target stack
  with the `:smolnet_stack` option. Each returned OTP socket is backed by one
  temporary `:gen_statem` child of that stack's inet supervisor.

  This module is the IPv6 callback and shared socket implementation. Use
  `SmolNet.InetBackend.Tcp4` as the callback for IPv4. File descriptors and
  packet modes other than raw, line, 1, 2, and 4 fail explicitly.
  """

  @behaviour :gen_statem

  alias SmolNet.InetBackend.Options
  alias SmolNet.InetBackend.Packet
  alias SmolNet.Socket
  alias SmolNet.Stack
  alias SmolNet.StackSupervisor

  import Kernel, except: [send: 2]

  @max_read_chunks 16
  @max_active_deliveries 16
  @max_timeout 4_294_967_295

  @type socket_term :: {:"$inet", __MODULE__, pid()}

  # OTP TCP callback entry points

  @spec family() :: :inet6
  def family, do: :inet6

  @spec mask(tuple(), tuple()) :: tuple()
  def mask(mask, address), do: :inet6_tcp.mask(mask, address)

  @spec parse_address(charlist()) :: {:ok, :inet.ip6_address()} | {:error, atom()}
  def parse_address(address), do: :inet6_tcp.parse_address(address)

  @spec translate_ip(term()) :: term()
  def translate_ip(address), do: :inet6_tcp.translate_ip(address)

  @spec getserv(term()) :: {:ok, :inet.port_number()} | {:error, atom()}
  def getserv(port), do: :inet6_tcp.getserv(port)

  @spec getaddr(term()) :: {:ok, :inet.ip6_address()} | {:error, atom()}
  def getaddr(address), do: :inet6_tcp.getaddr(address)

  @spec getaddr(term(), term()) :: {:ok, :inet.ip6_address()} | {:error, atom()}
  def getaddr(address, timer), do: :inet6_tcp.getaddr(address, timer)

  @spec getaddrs(term()) :: {:ok, [:inet.ip6_address()]} | {:error, atom()}
  def getaddrs(address), do: :inet6_tcp.getaddrs(address)

  @spec getaddrs(term(), term()) :: {:ok, [:inet.ip6_address()]} | {:error, atom()}
  def getaddrs(address, timer), do: :inet6_tcp.getaddrs(address, timer)

  @spec connect(:inet.ip6_address(), :inet.port_number(), list()) ::
          {:ok, socket_term()} | {:error, atom()}
  def connect(address, port, options) when is_integer(port) and is_list(options),
    do: connect(address, port, options, :infinity)

  @spec connect(map(), list(), timeout()) :: {:ok, socket_term()} | {:error, atom()}
  def connect(%{family: family} = sockaddr, options, timeout)
      when family in [:inet, :inet6] do
    with {:ok, endpoint} <- endpoint(sockaddr),
         {:ok, parsed} <- Options.parse(ensure_family_option(options, family)),
         true <- parsed.family == family do
      start_client(endpoint, parsed, timeout)
    else
      false -> {:error, :eafnosupport}
      {:error, _reason} = error -> error
    end
  end

  def connect(_sockaddr, _options, _timeout), do: {:error, :einval}

  @spec connect(term(), term(), list(), timeout()) ::
          {:ok, socket_term()} | {:error, atom()}
  def connect(address, port, options, timeout)
      when is_tuple(address) and tuple_size(address) in [4, 8] and is_integer(port) and
             port in 1..65_535 do
    family = if tuple_size(address) == 4, do: :inet, else: :inet6
    connect(%{family: family, addr: address, port: port}, options, timeout)
  end

  def connect(_address, _port, _options, _timeout), do: {:error, :einval}

  @spec listen(:inet.port_number(), list()) :: {:ok, socket_term()} | {:error, atom()}
  def listen(port, options) do
    with {:ok, parsed} <- Options.parse_listen(options, port) do
      start_listener(parsed)
    end
  end

  @spec accept(socket_term(), timeout()) :: {:ok, socket_term()} | {:error, atom()}
  def accept(socket, timeout)
      when timeout == :infinity or
             (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    socket_call(socket, {:accept, deadline(timeout)})
  end

  def accept(_socket, _timeout), do: {:error, :einval}

  @spec accept(socket_term()) :: {:ok, socket_term()} | {:error, atom()}
  def accept(socket), do: accept(socket, :infinity)

  @spec fdopen(term(), list()) :: {:error, :enotsup}
  def fdopen(_fd, _options), do: {:error, :enotsup}

  @spec send(socket_term(), iodata()) :: :ok | {:error, term()}
  def send(socket, packet), do: socket_call(socket, {:send, packet})

  @spec send(socket_term(), iodata(), list()) :: :ok | {:error, term()}
  def send(socket, packet, _options), do: send(socket, packet)

  @spec recv(socket_term(), non_neg_integer()) :: {:ok, term()} | {:error, atom()}
  def recv(socket, length), do: recv(socket, length, :infinity)

  @spec recv(socket_term(), non_neg_integer(), timeout()) ::
          {:ok, term()} | {:error, atom()}
  def recv(socket, length, timeout)
      when is_integer(length) and length >= 0 and
             (timeout == :infinity or
                (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout)) do
    socket_call(socket, {:recv, length, deadline(timeout)})
  end

  def recv(_socket, _length, _timeout), do: {:error, :einval}

  @spec unrecv(socket_term(), iodata()) :: :ok | {:error, atom()}
  def unrecv(socket, data), do: socket_call(socket, {:unrecv, data})

  @spec shutdown(socket_term(), :read | :write | :read_write) :: :ok | {:error, atom()}
  def shutdown(socket, how), do: socket_call(socket, {:shutdown, how})

  @spec close(socket_term()) :: :ok
  def close(socket) do
    case socket_pid(socket) do
      pid when is_pid(pid) ->
        try do
          :gen_statem.call(pid, :close, :infinity)
        catch
          :exit, _reason -> :ok
        end

      _other ->
        :ok
    end
  end

  @spec controlling_process(socket_term(), pid()) :: :ok | {:error, atom()}
  def controlling_process(socket, new_owner) when is_pid(new_owner) do
    case socket_call(socket, {:begin_transfer, new_owner}) do
      :unchanged ->
        :ok

      {:ok, token} ->
        messages = take_socket_messages(socket, [])

        case socket_call(socket, {:commit_transfer, token, messages}) do
          :ok ->
            :ok

          {:error, _reason} = error ->
            restore_socket_messages(messages)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  def controlling_process(_socket, _new_owner), do: {:error, :badarg}

  @spec setopts(socket_term(), list()) :: :ok | {:error, atom()}
  def setopts(socket, options), do: socket_call(socket, {:setopts, options})

  @spec getopts(socket_term(), list()) :: {:ok, list()} | {:error, atom()}
  def getopts(socket, names), do: socket_call(socket, {:getopts, names})

  @spec sockname(socket_term()) ::
          {:ok, {:inet.ip6_address(), :inet.port_number()}} | {:error, atom()}
  def sockname(socket), do: socket_call(socket, :sockname)

  @spec peername(socket_term()) ::
          {:ok, {:inet.ip6_address(), :inet.port_number()}} | {:error, atom()}
  def peername(socket), do: socket_call(socket, :peername)

  @spec info(socket_term()) :: map() | {:error, atom()}
  def info(socket), do: socket_call(socket, :info)

  @spec socket_to_list(socket_term()) :: charlist()
  def socket_to_list(socket) do
    case socket_pid(socket) do
      pid when is_pid(pid) -> ~c"#SmolNet.TCP<#{inspect(pid)}>"
      _other -> ~c"#SmolNet.TCP<closed>"
    end
  end

  @spec getstat(socket_term(), list()) :: {:ok, list()} | {:error, atom()}
  def getstat(socket, names), do: socket_call(socket, {:getstat, names})

  # Temporary adapter child / gen_statem callbacks

  @spec child_spec(map()) :: Supervisor.child_spec()
  def child_spec(config) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [config]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  @spec start_link(map()) :: :gen_statem.start_ret()
  def start_link(config), do: :gen_statem.start_link(__MODULE__, config, [])

  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(%{owner: owner, endpoint: endpoint, options: %Options{} = options}) do
    data = base_data(owner, options, :stream, endpoint, nil)

    {:ok, :connecting, data, [{:next_event, :internal, :continue_setup}]}
  end

  def init(%{owner: owner, role: :listener, options: %Options{} = options}) do
    data = base_data(owner, options, :listener, nil, nil)

    with {:ok, socket} <- SmolNet.open(options.family, :stream, :tcp, stack: options.stack),
         :ok <- Stack.socket_watch_owner(socket, self()),
         :ok <- SmolNet.bind(socket, listener_endpoint(options)),
         :ok <- SmolNet.listen(socket, options.backlog) do
      {:ok, :listening, %{data | low_socket: socket}}
    else
      {:error, reason} -> {:stop, translate_reason(reason)}
    end
  end

  def init(%{
        owner: owner,
        accepted_socket: %Socket{} = socket,
        options: %Options{} = options
      }) do
    data = base_data(owner, options, :stream, nil, socket)

    case Stack.socket_watch_owner(socket, self()) do
      :ok -> {:ok, :connected, data, [{:next_event, :internal, :drain_active}]}
      {:error, reason} -> {:stop, translate_reason(reason)}
    end
  end

  @impl true
  def handle_event(:internal, :continue_setup, :connecting, data) do
    data
    |> handle_continue(:open_and_connect)
    |> state_return()
  end

  def handle_event({:call, from}, {:await_connect, deadline}, :connecting, data) do
    cond do
      data.connect_from != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :ealready}}]}

      data.connect_result == :ok ->
        :gen_statem.reply(from, {:ok, data.public_socket})
        connected(data)

      match?({:error, _reason}, data.connect_result) ->
        {:error, reason} = data.connect_result
        :gen_statem.reply(from, {:error, reason})
        {:stop, :normal, data}

      deadline_reached?(deadline) ->
        data = cancel_connect_select(data)
        :gen_statem.reply(from, {:error, :timeout})
        {:stop, :normal, data}

      true ->
        {timer, token} = arm_timer(:connect, deadline)

        {:keep_state,
         %{
           data
           | connect_from: from,
             connect_deadline: deadline,
             connect_timer: {timer, token}
         }}
    end
  end

  def handle_event({:call, from}, request, :connecting, _data) do
    if request == :close do
      {:stop_and_reply, :normal, [{:reply, from, :ok}]}
    else
      {:keep_state_and_data, [{:reply, from, {:error, :ealready}}]}
    end
  end

  def handle_event({:call, from}, {:accept, deadline}, :listening, data) do
    if data.accept do
      {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}
    else
      {timer, token} = arm_timer(:accept, deadline)

      accept = %{
        from: from,
        select: nil,
        deadline: deadline,
        timer: {timer, token}
      }

      data
      |> Map.put(:accept, accept)
      |> drive_accept()
      |> state_return()
    end
  end

  def handle_event({:call, from}, :close, :listening, data) do
    data = fail_accept(data, :closed)
    _result = close_low_socket(data)
    {:stop_and_reply, :normal, [{:reply, from, :ok}], %{data | low_socket: nil}}
  end

  def handle_event({:call, from}, :sockname, :listening, data) do
    {:keep_state_and_data, [{:reply, from, endpoint_result(SmolNet.sockname(data.low_socket))}]}
  end

  def handle_event({:call, from}, :peername, :listening, _data) do
    {:keep_state_and_data, [{:reply, from, {:error, :enotconn}}]}
  end

  def handle_event({:call, from}, {:setopts, options}, :listening, data) do
    case Options.update(data.options, options) do
      {:ok, updated} -> {:keep_state, %{data | options: updated}, [{:reply, from, :ok}]}
      {:error, reason} -> {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
    end
  end

  def handle_event({:call, from}, {:getopts, names}, :listening, data) do
    {:keep_state_and_data, [{:reply, from, Options.get(data.options, names)}]}
  end

  def handle_event({:call, from}, {:begin_transfer, new_owner}, :listening, data) do
    begin_transfer(from, new_owner, data)
  end

  def handle_event({:call, from}, {:commit_transfer, token, messages}, :listening, data) do
    commit_transfer(from, token, messages, data)
  end

  def handle_event({:call, from}, :info, :listening, data) do
    info = %{
      owner: data.owner,
      state: :listening,
      backlog: data.options.backlog,
      accept_pending: data.accept != nil
    }

    {:keep_state_and_data, [{:reply, from, info}]}
  end

  def handle_event({:call, from}, {:getstat, names}, :listening, _data) do
    {:keep_state_and_data, [{:reply, from, getstat_reply(names)}]}
  end

  def handle_event({:call, from}, _request, :listening, _data) do
    {:keep_state_and_data, [{:reply, from, {:error, :enotsup}}]}
  end

  def handle_event({:call, from}, {:send, packet}, :connected, data) do
    cond do
      data.write != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      data.transfer != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      true ->
        case Packet.encode(packet, data.options.packet, data.options.packet_size) do
          {:ok, framed} ->
            write = new_write(from, framed, data.options.send_timeout)
            data = %{data | write: write}
            data |> drive_write() |> state_return()

          {:error, reason} ->
            {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
        end
    end
  end

  def handle_event({:call, from}, {:recv, length, deadline}, :connected, data) do
    cond do
      data.options.active != false ->
        {:keep_state_and_data, [{:reply, from, {:error, :einval}}]}

      data.read != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      length > Options.receive_limit(data.options) ->
        {:keep_state_and_data, [{:reply, from, {:error, :emsgsize}}]}

      true ->
        {timer, token} = arm_timer(:read, deadline)

        read = %{
          kind: :passive,
          from: from,
          length: length,
          select: nil,
          deadline: deadline,
          timer: {timer, token}
        }

        data = %{data | read: read}
        data |> drive_read(@max_read_chunks, @max_active_deliveries) |> state_return()
    end
  end

  def handle_event({:call, from}, {:unrecv, value}, :connected, data) do
    if data.read != nil do
      {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}
    else
      reply_unrecv(from, value, data)
    end
  end

  def handle_event({:call, from}, {:shutdown, how}, :connected, data) do
    case SmolNet.shutdown(data.low_socket, how) do
      :ok ->
        data = if how in [:read, :read_write], do: close_read_direction(data), else: data
        {:keep_state, data, [{:reply, from, :ok}]}

      {:error, reason} ->
        {:keep_state_and_data, [{:reply, from, {:error, translate_reason(reason)}}]}
    end
  end

  def handle_event({:call, from}, :close, :connected, data) do
    _result = close_low_socket(data)
    {:stop_and_reply, :normal, [{:reply, from, :ok}], %{data | low_socket: nil}}
  end

  def handle_event({:call, from}, :sockname, :connected, data) do
    {:keep_state_and_data, [{:reply, from, endpoint_result(SmolNet.sockname(data.low_socket))}]}
  end

  def handle_event({:call, from}, :peername, :connected, data) do
    {:keep_state_and_data, [{:reply, from, endpoint_result(SmolNet.peername(data.low_socket))}]}
  end

  def handle_event({:call, from}, {:setopts, options}, :connected, data) do
    set_options(from, options, data)
  end

  def handle_event({:call, from}, {:getopts, names}, :connected, data) do
    {:keep_state_and_data, [{:reply, from, Options.get(data.options, names)}]}
  end

  def handle_event({:call, from}, {:begin_transfer, new_owner}, :connected, data) do
    begin_transfer(from, new_owner, data)
  end

  def handle_event({:call, from}, {:commit_transfer, token, messages}, :connected, data) do
    commit_transfer(from, token, messages, data)
  end

  def handle_event({:call, from}, :info, :connected, data) do
    info = %{
      owner: data.owner,
      active: data.options.active,
      mode: data.options.mode,
      packet: data.options.packet,
      packet_size: data.options.packet_size,
      read_pending: data.read != nil,
      write_pending: data.write != nil
    }

    {:keep_state_and_data, [{:reply, from, info}]}
  end

  def handle_event({:call, from}, {:getstat, names}, :connected, _data) do
    {:keep_state_and_data, [{:reply, from, getstat_reply(names)}]}
  end

  def handle_event({:call, from}, _request, :connected, _data) do
    {:keep_state_and_data, [{:reply, from, {:error, :enotsup}}]}
  end

  def handle_event(:internal, :drain_active, :connected, data) do
    data = %{data | read_scheduled: false}

    data
    |> ensure_active_read()
    |> drive_read(@max_read_chunks, @max_active_deliveries)
    |> state_return()
  end

  def handle_event(:info, :continue_read, :connected, data) do
    data = %{data | read_scheduled: false}
    data |> drive_read(@max_read_chunks, @max_active_deliveries) |> state_return()
  end

  def handle_event(
        :info,
        {:"$smol_socket", identity, :select, reference},
        state_name,
        %{low_socket: %Socket{} = socket} = data
      ) do
    if Socket.identity(socket) == identity do
      handle_select(reference, state_name, data)
    else
      :keep_state_and_data
    end
  end

  def handle_event(
        :info,
        {:"$smol_socket", identity, :abort, reference, reason},
        state_name,
        %{low_socket: %Socket{} = socket} = data
      ) do
    if Socket.identity(socket) == identity do
      handle_abort(reference, translate_reason(reason), state_name, data)
    else
      :keep_state_and_data
    end
  end

  def handle_event(:info, {:operation_timeout, :connect, token}, :connecting, data) do
    case data.connect_timer do
      {_timer, ^token} ->
        data = data |> cancel_connect_select() |> clear_connect_timer()
        complete_connect({:error, :timeout}, data) |> state_return()

      _other ->
        :keep_state_and_data
    end
  end

  def handle_event(:info, {:operation_timeout, :accept, token}, :listening, data) do
    case data.accept do
      %{timer: {_timer, ^token}} = accept ->
        data = cancel_accept_select(data)
        :gen_statem.reply(accept.from, {:error, :timeout})
        {:keep_state, %{data | accept: nil}}

      _other ->
        :keep_state_and_data
    end
  end

  def handle_event(:info, {:operation_timeout, :read, token}, :connected, data) do
    case data.read do
      %{kind: :passive, timer: {_timer, ^token}} = read ->
        data = cancel_read_select(data)
        :gen_statem.reply(read.from, {:error, :timeout})
        {:keep_state, %{data | read: nil}}

      _other ->
        :keep_state_and_data
    end
  end

  def handle_event(:info, {:operation_timeout, :write, token}, :connected, data) do
    case data.write do
      %{timer: {_timer, ^token}} = write ->
        data = cancel_write_select(data)
        :gen_statem.reply(write.from, write_timeout_reply(write))
        data = %{data | write: nil}

        if data.options.send_timeout_close do
          _result = close_low_socket(data)
          {:stop, :normal, %{data | low_socket: nil}}
        else
          {:keep_state, data}
        end

      _other ->
        :keep_state_and_data
    end
  end

  def handle_event(
        :info,
        {:DOWN, monitor, :process, owner, _reason},
        _state_name,
        %{owner_monitor: monitor, owner: owner} = data
      ) do
    {:stop, :normal, data}
  end

  def handle_event(
        :info,
        {:DOWN, monitor, :process, stack_pid, _reason},
        _state_name,
        %{stack_monitor: monitor, stack_pid: stack_pid} = data
      ) do
    data = fail_all(data, :enetdown)
    {:stop, {:shutdown, :stack_down}, %{data | low_socket: nil}}
  end

  def handle_event(:info, _message, _state_name, _data), do: :keep_state_and_data

  @impl true
  def terminate(_reason, _state_name, data) do
    _result = close_low_socket(data)
    :ok
  end

  defp getstat_reply(names) do
    supported = [
      :recv_avg,
      :recv_cnt,
      :recv_dvi,
      :recv_max,
      :recv_oct,
      :send_avg,
      :send_cnt,
      :send_pend,
      :send_max,
      :send_oct
    ]

    if is_list(names) and Enum.all?(names, &(&1 in supported)) do
      {:ok, Enum.map(names, &{&1, 0})}
    else
      {:error, :einval}
    end
  end

  defp start_client(endpoint, %Options{} = options, timeout)
       when timeout == :infinity or
              (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout) do
    owner = self()
    deadline = deadline(timeout)
    child = child_spec(%{owner: owner, endpoint: endpoint, options: options})

    case StackSupervisor.start_inet_backend(options.stack, child) do
      {:ok, pid} -> pid_call(pid, {:await_connect, deadline})
      {:error, reason} -> {:error, start_error(reason)}
    end
  catch
    :exit, _reason -> {:error, :closed}
  end

  defp start_client(_endpoint, _options, _timeout), do: {:error, :einval}

  defp start_listener(%Options{} = options) do
    owner = self()
    child = child_spec(%{owner: owner, role: :listener, options: options})

    case StackSupervisor.start_inet_backend(options.stack, child) do
      {:ok, pid} -> {:ok, module_socket(pid)}
      {:error, reason} -> {:error, start_error(reason)}
    end
  catch
    :exit, _reason -> {:error, :closed}
  end

  defp start_accepted(socket, %Options{} = options, owner) do
    child = child_spec(%{owner: owner, accepted_socket: socket, options: options})

    case StackSupervisor.start_inet_backend(options.stack, child) do
      {:ok, pid} -> {:ok, module_socket(pid)}
      {:error, reason} -> {:error, start_error(reason)}
    end
  catch
    :exit, _reason -> {:error, :closed}
  end

  defp base_data(owner, options, kind, endpoint, low_socket) do
    Process.flag(:trap_exit, true)
    stack_pid = options.stack.stack

    %{
      kind: kind,
      owner: owner,
      owner_monitor: Process.monitor(owner),
      stack: options.stack,
      stack_pid: stack_pid,
      stack_monitor: Process.monitor(stack_pid),
      options: options,
      endpoint: endpoint,
      public_socket: module_socket(self()),
      low_socket: low_socket,
      connect_result: nil,
      connect_select: nil,
      connect_from: nil,
      connect_deadline: nil,
      connect_timer: nil,
      accept: nil,
      read: nil,
      read_buffer: <<>>,
      read_scheduled: false,
      read_closed: false,
      write: nil,
      transfer: nil
    }
  end

  defp listener_endpoint(options) do
    address =
      options.bind_address ||
        if(options.family == :inet, do: {0, 0, 0, 0}, else: {0, 0, 0, 0, 0, 0, 0, 0})

    endpoint = %{family: options.family, addr: address, port: options.bind_port}

    if options.family == :inet6,
      do: Map.merge(endpoint, %{flowinfo: 0, scope_id: options.bind_scope_id}),
      else: endpoint
  end

  defp handle_continue(data, :open_and_connect) do
    case SmolNet.open(data.options.family, :stream, :tcp, stack: data.stack) do
      {:ok, socket} ->
        data = %{data | low_socket: socket}

        with :ok <- Stack.socket_watch_owner(socket, self()),
             :ok <- maybe_bind(socket, Options.local_endpoint(data.options)) do
          attempt_connect(data)
        else
          {:error, reason} -> complete_connect({:error, translate_reason(reason)}, data)
        end

      {:error, reason} ->
        complete_connect({:error, translate_reason(reason)}, data)
    end
  end

  defp maybe_bind(_socket, nil), do: :ok
  defp maybe_bind(socket, endpoint), do: SmolNet.bind(socket, endpoint)

  defp drive_accept(%{accept: nil} = data), do: {:keep, data}

  defp drive_accept(data) do
    case Stack.socket_accept(data.low_socket, self()) do
      {:ok, child} ->
        accept = cancel_op_timer(data.accept)

        case start_accepted(child, data.options, from_pid(accept.from)) do
          {:ok, socket} ->
            :gen_statem.reply(accept.from, {:ok, socket})
            {:keep, %{data | accept: nil}}

          {:error, reason} ->
            _result = SmolNet.close(child)
            :gen_statem.reply(accept.from, {:error, reason})
            {:keep, %{data | accept: nil}}
        end

      {:select, select_info} ->
        {:keep, put_in(data, [:accept, :select], select_info)}

      {:error, reason} ->
        accept = cancel_op_timer(data.accept)
        :gen_statem.reply(accept.from, {:error, translate_reason(reason)})
        {:keep, %{data | accept: nil}}
    end
  end

  defp attempt_connect(data) do
    case SmolNet.connect(data.low_socket, data.endpoint, :nowait) do
      :ok -> complete_connect(:ok, %{data | connect_select: nil})
      {:select, select_info} -> {:keep, %{data | connect_select: select_info}}
      {:error, reason} -> complete_connect({:error, translate_reason(reason)}, data)
    end
  end

  defp complete_connect(result, data) do
    data = data |> clear_connect_timer() |> Map.put(:connect_result, result)

    case {result, data.connect_from} do
      {:ok, nil} ->
        {:keep, data}

      {:ok, from} ->
        :gen_statem.reply(from, {:ok, data.public_socket})
        {:next, :connected, clear_connect_waiter(data)}

      {{:error, _reason}, nil} ->
        {:keep, data}

      {{:error, reason}, from} ->
        :gen_statem.reply(from, {:error, reason})
        {:stop, :normal, clear_connect_waiter(data)}
    end
  end

  defp connected(data) do
    data = clear_connect_waiter(data)
    data = schedule_active_read(data)
    {:next_state, :connected, data}
  end

  defp clear_connect_waiter(data) do
    %{
      data
      | connect_from: nil,
        connect_deadline: nil,
        connect_timer: nil,
        connect_select: nil,
        connect_result: :ok
    }
  end

  defp clear_connect_timer(%{connect_timer: {timer, _token}} = data) when is_reference(timer) do
    _cancelled = Process.cancel_timer(timer, async: false, info: false)
    %{data | connect_timer: nil}
  end

  defp clear_connect_timer(data), do: %{data | connect_timer: nil}

  defp cancel_connect_select(%{connect_select: nil} = data), do: data

  defp cancel_connect_select(data) do
    _cancelled = SmolNet.cancel(data.low_socket, data.connect_select)
    %{data | connect_select: nil}
  end

  defp handle_select(reference, :connecting, data) do
    if select_reference(data.connect_select) == reference do
      data |> Map.put(:connect_select, nil) |> attempt_connect() |> state_return()
    else
      :keep_state_and_data
    end
  end

  defp handle_select(reference, :listening, data) do
    if accept_reference(data.accept) == reference do
      data |> put_in([:accept, :select], nil) |> drive_accept() |> state_return()
    else
      :keep_state_and_data
    end
  end

  defp handle_select(reference, :connected, data) do
    cond do
      read_reference(data.read) == reference ->
        data
        |> put_in([:read, :select], nil)
        |> drive_read(@max_read_chunks, @max_active_deliveries)
        |> state_return()

      write_reference(data.write) == reference ->
        data |> put_in([:write, :select], nil) |> drive_write() |> state_return()

      true ->
        :keep_state_and_data
    end
  end

  defp handle_abort(reference, reason, :connecting, data) do
    if select_reference(data.connect_select) == reference do
      data = Map.put(data, :connect_select, nil)
      complete_connect({:error, reason}, data) |> state_return()
    else
      :keep_state_and_data
    end
  end

  defp handle_abort(reference, reason, :listening, data) do
    if accept_reference(data.accept) == reference do
      accept = cancel_op_timer(data.accept)
      :gen_statem.reply(accept.from, {:error, reason})
      {:keep_state, %{data | accept: nil}}
    else
      :keep_state_and_data
    end
  end

  defp handle_abort(reference, reason, :connected, data) do
    cond do
      read_reference(data.read) == reference ->
        data
        |> put_in([:read, :select], nil)
        |> read_failure(reason)
        |> state_return()

      write_reference(data.write) == reference ->
        data
        |> put_in([:write, :select], nil)
        |> write_failure(reason)
        |> state_return()

      true ->
        :keep_state_and_data
    end
  end

  defp new_write(from, framed, timeout) do
    deadline = deadline(timeout)
    {timer, token} = arm_timer(:write, deadline)

    %{
      from: from,
      remainder: framed,
      progressed: false,
      select: nil,
      timer: {timer, token}
    }
  end

  defp drive_write(%{write: nil} = data), do: {:keep, data}

  defp drive_write(data) do
    write = data.write

    case SmolNet.send(data.low_socket, write.remainder, :nowait) do
      :ok ->
        _write = cancel_op_timer(write)
        :gen_statem.reply(write.from, :ok)
        {:keep, %{data | write: nil}}

      {:select, {select_info, remainder}} ->
        progressed = write.progressed or byte_size(remainder) < byte_size(write.remainder)
        write = %{write | remainder: remainder, progressed: progressed, select: select_info}
        {:keep, %{data | write: write}}

      {:error, reason} ->
        write_failure(data, translate_reason(reason))
    end
  end

  defp write_failure(%{write: nil} = data, _reason), do: {:keep, data}
  defp write_failure(data, :econnreset), do: terminal_connection_failure(data, :econnreset)

  defp write_failure(data, reason) do
    write = cancel_op_timer(data.write)
    :gen_statem.reply(write.from, write_error_reply(write, reason))
    {:keep, %{data | write: nil}}
  end

  defp write_error_reply(%{progressed: true, remainder: remainder}, reason),
    do: {:error, {reason, remainder}}

  defp write_error_reply(_write, reason), do: {:error, reason}

  defp write_timeout_reply(%{progressed: true, remainder: remainder}),
    do: {:error, {:timeout, remainder}}

  defp write_timeout_reply(_write), do: {:error, :timeout}

  defp cancel_write_select(%{write: %{select: nil}} = data), do: data
  defp cancel_write_select(%{write: nil} = data), do: data

  defp cancel_write_select(data) do
    _cancelled = SmolNet.cancel(data.low_socket, data.write.select)
    put_in(data, [:write, :select], nil)
  end

  defp cancel_accept_select(%{accept: %{select: nil}} = data), do: data

  defp cancel_accept_select(data) do
    _cancelled = SmolNet.cancel(data.low_socket, data.accept.select)
    put_in(data, [:accept, :select], nil)
  end

  defp fail_accept(%{accept: nil} = data, _reason), do: data

  defp fail_accept(data, reason) do
    data = cancel_accept_select(data)
    accept = cancel_op_timer(data.accept)
    :gen_statem.reply(accept.from, {:error, reason})
    %{data | accept: nil}
  end

  defp drive_read(%{read: nil} = data, _chunks, _deliveries), do: {:keep, data}

  defp drive_read(%{transfer: transfer} = data, _chunks, _deliveries)
       when transfer != nil,
       do: {:keep, data}

  defp drive_read(data, chunks, deliveries) when chunks <= 0 or deliveries <= 0 do
    if read_reference(data.read) do
      {:keep, data}
    else
      {:keep, schedule_read_continuation(data)}
    end
  end

  defp drive_read(data, chunks, deliveries) do
    read = data.read

    case Packet.extract(
           data.read_buffer,
           data.options.packet,
           read.length,
           data.options.packet_size
         ) do
      {:ok, packet, rest} ->
        data
        |> cancel_read_select()
        |> Map.put(:read_buffer, rest)
        |> deliver_read_packet(packet, chunks, deliveries)

      :more when data.read_closed ->
        finish_eof(data, chunks, deliveries)

      # A low-level waiter already armed (a partial read handed back its select) is the
      # continuation for the rest: asking the socket again while it stands returns
      # `:busy`, which would fail the caller's read and leave the waiter behind to reject
      # every later read until it fires. Wait for its notification instead.
      :more ->
        if read_reference(read),
          do: {:keep, data},
          else: fetch_read_data(data, chunks, deliveries)

      {:error, reason} ->
        read_failure(data, reason)
    end
  end

  defp fetch_read_data(data, chunks, deliveries) do
    available = receive_limit(data) - byte_size(data.read_buffer)

    if available <= 0 do
      read_failure(data, :emsgsize)
    else
      request = read_request(data, available)

      case SmolNet.recv(data.low_socket, request, :nowait) do
        {:ok, binary} ->
          continue_read_with_data(data, binary, nil, chunks, deliveries)

        {:select, {select_info, binary}} ->
          continue_read_with_data(data, binary, select_info, chunks, deliveries)

        {:select, select_info} ->
          {:keep, put_in(data, [:read, :select], select_info)}

        {:error, :closed} ->
          data
          |> Map.put(:read_closed, true)
          |> drive_read(chunks - 1, deliveries)

        {:error, reason} ->
          read_failure(data, translate_reason(reason))
      end
    end
  end

  defp read_request(
         %{options: %{packet: :raw}, read: %{kind: :passive, length: length}} = data,
         available
       )
       when length > 0 do
    min(length - byte_size(data.read_buffer), available)
  end

  defp read_request(_data, available), do: available

  defp continue_read_with_data(data, binary, select_info, chunks, deliveries) do
    case append_read_data(data, binary) do
      {:ok, data} ->
        data = if select_info, do: put_in(data, [:read, :select], select_info), else: data
        drive_read(data, chunks - 1, deliveries)

      {:error, reason} ->
        read_failure(data, reason)
    end
  end

  defp append_read_data(data, binary) do
    if byte_size(binary) + byte_size(data.read_buffer) <= receive_limit(data) do
      {:ok, %{data | read_buffer: data.read_buffer <> binary}}
    else
      {:error, :emsgsize}
    end
  end

  defp deliver_read_packet(
         %{read: %{kind: :passive} = read} = data,
         packet,
         _chunks,
         _deliveries
       ) do
    read = cancel_op_timer(read)
    :gen_statem.reply(read.from, {:ok, Packet.represent(packet, data.options.mode)})
    {:keep, %{data | read: nil}}
  end

  defp deliver_read_packet(%{read: %{kind: :active}} = data, packet, chunks, deliveries) do
    Kernel.send(
      data.owner,
      {:tcp, data.public_socket, Packet.represent(packet, data.options.mode)}
    )

    {active, passive?} = consume_active(data.options.active)
    options = %{data.options | active: active}

    if passive? do
      Kernel.send(data.owner, {:tcp_passive, data.public_socket})
    end

    data = %{data | options: options}

    if active == false do
      {:keep, %{data | read: nil}}
    else
      drive_read(data, chunks, deliveries - 1)
    end
  end

  defp finish_eof(%{read: %{kind: :passive} = read} = data, _chunks, _deliveries) do
    {reply, buffer} = passive_eof_reply(data)
    read = cancel_op_timer(read)
    :gen_statem.reply(read.from, reply)
    {:keep, %{data | read: nil, read_buffer: buffer}}
  end

  defp finish_eof(%{read: %{kind: :active}} = data, chunks, deliveries) do
    if data.options.packet == :line and data.read_buffer != <<>> do
      packet = data.read_buffer

      data
      |> Map.put(:read_buffer, <<>>)
      |> deliver_read_packet(packet, chunks, deliveries)
      |> case do
        {:keep, %{read: nil} = data} -> {:keep, data}
        {:keep, data} -> finish_eof(data, chunks, deliveries - 1)
        other -> other
      end
    else
      notify_active_terminal(data, :closed)
    end
  end

  defp passive_eof_reply(%{options: %{packet: :raw} = options, read_buffer: buffer})
       when buffer != <<>> do
    {{:ok, Packet.represent(buffer, options.mode)}, <<>>}
  end

  defp passive_eof_reply(%{options: %{packet: :line} = options, read_buffer: buffer})
       when buffer != <<>> do
    {{:ok, Packet.represent(buffer, options.mode)}, <<>>}
  end

  defp passive_eof_reply(_data), do: {{:error, :closed}, <<>>}

  defp read_failure(%{read: nil} = data, _reason), do: {:keep, data}
  defp read_failure(data, :econnreset), do: terminal_connection_failure(data, :econnreset)

  defp read_failure(%{read: %{kind: :passive} = read} = data, reason) do
    read = cancel_op_timer(read)
    :gen_statem.reply(read.from, {:error, reason})

    if reason == :emsgsize do
      _result = close_low_socket(data)
      {:stop, :normal, %{data | read: nil, low_socket: nil}}
    else
      {:keep, %{data | read: nil}}
    end
  end

  defp read_failure(%{read: %{kind: :active}} = data, reason) do
    notify_active_terminal(data, reason)
  end

  defp notify_active_terminal(data, reason) do
    if data.options.active != false do
      if reason != :closed do
        Kernel.send(data.owner, {:tcp_error, data.public_socket, reason})
      end

      Kernel.send(data.owner, {:tcp_closed, data.public_socket})
    end

    options = %{data.options | active: false}

    if reason == :closed do
      {:keep, %{data | read: nil, options: options, read_closed: true}}
    else
      _result = close_low_socket(data)
      {:stop, :normal, %{data | read: nil, options: options, low_socket: nil}}
    end
  end

  defp terminal_connection_failure(data, reason) do
    data = data |> cancel_read_select() |> cancel_write_select()
    reply_terminal_read(data, reason)
    reply_terminal_write(data, reason)
    _result = close_low_socket(data)

    options = %{data.options | active: false}
    {:stop, :normal, %{data | read: nil, write: nil, options: options, low_socket: nil}}
  end

  defp reply_terminal_read(%{read: %{kind: :passive} = read}, reason) do
    read = cancel_op_timer(read)
    :gen_statem.reply(read.from, {:error, reason})
  end

  defp reply_terminal_read(%{read: %{kind: :active}} = data, reason) do
    Kernel.send(data.owner, {:tcp_error, data.public_socket, reason})
    Kernel.send(data.owner, {:tcp_closed, data.public_socket})
  end

  defp reply_terminal_read(_data, _reason), do: :ok

  defp reply_terminal_write(%{write: nil}, _reason), do: :ok

  defp reply_terminal_write(%{write: write}, reason) do
    write = cancel_op_timer(write)
    :gen_statem.reply(write.from, write_error_reply(write, reason))
  end

  defp cancel_read_select(%{read: %{select: nil}} = data), do: data
  defp cancel_read_select(%{read: nil} = data), do: data

  defp cancel_read_select(data) do
    _cancelled = SmolNet.cancel(data.low_socket, data.read.select)
    put_in(data, [:read, :select], nil)
  end

  defp ensure_active_read(%{options: %{active: false}} = data), do: data
  defp ensure_active_read(%{read: nil} = data), do: %{data | read: active_read()}
  defp ensure_active_read(data), do: data

  defp active_read do
    %{kind: :active, from: nil, length: 0, select: nil, deadline: :infinity, timer: nil}
  end

  defp schedule_active_read(%{options: %{active: false}} = data), do: data

  defp schedule_active_read(data) do
    data = ensure_active_read(data)

    case data.read do
      %{select: {:select_info, _operation, _reference}} -> data
      _read -> schedule_read_continuation(data)
    end
  end

  defp schedule_read_continuation(%{read_scheduled: true} = data), do: data

  defp schedule_read_continuation(data) do
    Kernel.send(self(), :continue_read)
    %{data | read_scheduled: true}
  end

  defp close_read_direction(data) do
    data = cancel_read_select(data)

    case data.read do
      %{kind: :passive} = read ->
        read = cancel_op_timer(read)
        :gen_statem.reply(read.from, {:error, :closed})

      %{kind: :active} ->
        if data.options.active != false do
          Kernel.send(data.owner, {:tcp_closed, data.public_socket})
        end

      nil ->
        :ok
    end

    options = %{data.options | active: false}
    %{data | read: nil, read_closed: true, options: options}
  end

  defp set_options(from, options, data) do
    with {:ok, updated} <- Options.update(data.options, options),
         :ok <- option_change_allowed(data, updated) do
      data = apply_active_change(data, updated)
      {:keep_state, data, [{:reply, from, :ok}]}
    else
      {:error, reason} -> {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
    end
  end

  defp option_change_allowed(%{read: %{kind: :passive}} = data, updated) do
    incompatible? =
      updated.active != false or
        updated.mode != data.options.mode or
        updated.packet != data.options.packet or
        updated.packet_size != data.options.packet_size or
        updated.buffer != data.options.buffer

    if incompatible?, do: {:error, :busy}, else: :ok
  end

  defp option_change_allowed(data, updated) do
    changed_framing? =
      data.options.packet != updated.packet or data.options.mode != updated.mode or
        data.options.packet_size != updated.packet_size

    cond do
      changed_framing? and data.read_buffer != <<>> -> {:error, :busy}
      byte_size(data.read_buffer) > Options.receive_limit(updated) -> {:error, :emsgsize}
      true -> :ok
    end
  end

  defp apply_active_change(data, %{active: false} = updated) do
    data = if match?(%{kind: :active}, data.read), do: cancel_read_select(data), else: data
    read = if match?(%{kind: :active}, data.read), do: nil, else: data.read
    %{data | options: updated, read: read}
  end

  defp apply_active_change(data, updated) do
    data
    |> Map.put(:options, updated)
    |> ensure_active_read()
    |> schedule_read_continuation()
  end

  defp begin_transfer(from, new_owner, data) do
    caller = from_pid(from)

    cond do
      caller != data.owner ->
        {:keep_state_and_data, [{:reply, from, {:error, :not_owner}}]}

      data.transfer != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      new_owner == data.owner ->
        {:keep_state_and_data, [{:reply, from, :unchanged}]}

      node(new_owner) != node() or not Process.alive?(new_owner) ->
        {:keep_state_and_data, [{:reply, from, {:error, :badarg}}]}

      true ->
        token = make_ref()
        transfer = %{token: token, old_owner: data.owner, new_owner: new_owner}
        {:keep_state, %{data | transfer: transfer}, [{:reply, from, {:ok, token}}]}
    end
  end

  defp commit_transfer(
         from,
         token,
         messages,
         %{transfer: %{token: token} = transfer} = data
       )
       when is_list(messages) do
    cond do
      from_pid(from) != transfer.old_owner ->
        {:keep_state_and_data, [{:reply, from, {:error, :not_owner}}]}

      not Process.alive?(transfer.new_owner) ->
        data = %{data | transfer: nil}
        data = resume_after_transfer(data)
        {:keep_state, data, [{:reply, from, {:error, :badarg}}]}

      true ->
        Process.demonitor(data.owner_monitor, [:flush])
        monitor = Process.monitor(transfer.new_owner)

        data = %{
          data
          | owner: transfer.new_owner,
            owner_monitor: monitor,
            transfer: nil
        }

        Enum.each(messages, &Kernel.send(transfer.new_owner, &1))
        data = resume_after_transfer(data)
        {:keep_state, data, [{:reply, from, :ok}]}
    end
  end

  defp commit_transfer(from, _token, _messages, data) do
    {:keep_state, data, [{:reply, from, {:error, :einval}}]}
  end

  defp fail_all(data, reason) do
    if data.connect_from do
      :gen_statem.reply(data.connect_from, {:error, reason})
    end

    if data.accept do
      :gen_statem.reply(data.accept.from, {:error, reason})
    end

    if match?(%{kind: :passive}, data.read) do
      :gen_statem.reply(data.read.from, {:error, reason})
    else
      if data.options.active != false do
        Kernel.send(data.owner, {:tcp_error, data.public_socket, reason})
        Kernel.send(data.owner, {:tcp_closed, data.public_socket})
      end
    end

    if data.write do
      :gen_statem.reply(data.write.from, {:error, reason})
    end

    %{data | connect_from: nil, accept: nil, read: nil, write: nil}
  end

  defp endpoint(%{family: family, addr: address, port: port} = sockaddr)
       when family in [:inet, :inet6] and is_tuple(address) and tuple_size(address) in [4, 8] and
              is_integer(port) and
              port in 1..65_535 do
    expected_size = if family == :inet, do: 4, else: 8

    if tuple_size(address) == expected_size do
      endpoint = %{family: family, addr: address, port: port}

      {:ok,
       if(family == :inet6,
         do:
           Map.merge(endpoint, %{
             flowinfo: Map.get(sockaddr, :flowinfo, 0),
             scope_id: Map.get(sockaddr, :scope_id, 0)
           }),
         else: endpoint
       )}
    else
      {:error, :eafnosupport}
    end
  end

  defp endpoint(_sockaddr), do: {:error, :einval}

  defp endpoint_result({:ok, %{addr: address, port: port}}), do: {:ok, {address, port}}
  defp endpoint_result({:error, reason}), do: {:error, translate_reason(reason)}

  defp ensure_family_option(options, family) do
    if family in options, do: options, else: [family | options]
  end

  defp module_socket(pid), do: {:"$inet", __MODULE__, pid}

  defp socket_pid({:"$inet", __MODULE__, pid}) when is_pid(pid), do: pid
  defp socket_pid(_socket), do: nil

  defp socket_call(socket, request) do
    case socket_pid(socket) do
      pid when is_pid(pid) -> pid_call(pid, request)
      _other -> {:error, :closed}
    end
  end

  defp pid_call(pid, request) do
    :gen_statem.call(pid, request, :infinity)
  catch
    :exit, _reason -> {:error, :closed}
  end

  defp take_socket_messages(socket, messages) do
    receive do
      {:tcp, ^socket, _data} = message ->
        take_socket_messages(socket, [message | messages])

      {:tcp_closed, ^socket} = message ->
        take_socket_messages(socket, [message | messages])

      {:tcp_error, ^socket, _reason} = message ->
        take_socket_messages(socket, [message | messages])

      {:tcp_passive, ^socket} = message ->
        take_socket_messages(socket, [message | messages])
    after
      0 -> Enum.reverse(messages)
    end
  end

  defp restore_socket_messages(messages), do: Enum.each(messages, &Kernel.send(self(), &1))

  defp reply_unrecv(from, value, data) do
    with {:ok, binary} <- to_binary(value),
         true <- byte_size(binary) + byte_size(data.read_buffer) <= receive_limit(data) do
      {:keep_state, %{data | read_buffer: binary <> data.read_buffer}, [{:reply, from, :ok}]}
    else
      false -> {:keep_state_and_data, [{:reply, from, {:error, :emsgsize}}]}
      {:error, reason} -> {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
    end
  end

  defp state_return({:keep, data}), do: {:keep_state, data}

  defp state_return({:next, state_name, data}),
    do: {:next_state, state_name, schedule_active_read(data)}

  defp state_return({:stop, reason, data}), do: {:stop, reason, data}

  defp consume_active(true), do: {true, false}
  defp consume_active(:once), do: {false, false}
  defp consume_active(1), do: {false, true}
  defp consume_active(active) when is_integer(active) and active > 1, do: {active - 1, false}

  defp receive_limit(data), do: Options.receive_limit(data.options)

  defp arm_timer(_kind, :infinity), do: {nil, nil}

  defp arm_timer(kind, deadline) do
    token = make_ref()
    timer = Process.send_after(self(), {:operation_timeout, kind, token}, remaining(deadline))
    {timer, token}
  end

  defp cancel_op_timer(%{timer: {timer, _token}} = operation) when is_reference(timer) do
    _cancelled = Process.cancel_timer(timer, async: false, info: false)
    %{operation | timer: nil}
  end

  defp cancel_op_timer(operation), do: operation

  defp select_reference({:select_info, _operation, reference}), do: reference
  defp select_reference(_select), do: nil

  defp read_reference(%{select: select}), do: select_reference(select)
  defp read_reference(_read), do: nil

  defp write_reference(%{select: select}), do: select_reference(select)
  defp write_reference(_write), do: nil

  defp accept_reference(%{select: select}), do: select_reference(select)
  defp accept_reference(_accept), do: nil

  defp resume_after_transfer(%{kind: :listener} = data), do: data
  defp resume_after_transfer(data), do: schedule_active_read(data)

  defp close_low_socket(%{low_socket: %Socket{} = socket}), do: SmolNet.close(socket)
  defp close_low_socket(_data), do: :ok

  defp from_pid({pid, _tag}) when is_pid(pid), do: pid

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp deadline_reached?(:infinity), do: false
  defp deadline_reached?(deadline), do: remaining(deadline) == 0

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp to_binary(value) do
    {:ok, IO.iodata_to_binary(value)}
  rescue
    ArgumentError -> {:error, :einval}
  end

  defp start_error({:already_started, _pid}), do: :ealready
  defp start_error(:max_children), do: :system_limit
  defp start_error({:shutdown, reason}), do: start_error(reason)

  defp start_error(reason)
       when reason in [:eaddrinuse, :eaddrnotavail, :eafnosupport, :einval, :system_limit],
       do: reason

  defp start_error(_reason), do: :closed

  defp translate_reason(:connection_refused), do: :econnrefused
  defp translate_reason(:connection_reset), do: :econnreset
  defp translate_reason(:connection_timeout), do: :etimedout
  defp translate_reason(:network_unreachable), do: :enetunreach
  defp translate_reason(:address_in_use), do: :eaddrinuse
  defp translate_reason(:address_not_available), do: :eaddrnotavail
  defp translate_reason(:ephemeral_ports_exhausted), do: :system_limit
  defp translate_reason(:system_limit), do: :system_limit
  defp translate_reason(:unsupported_family), do: :eafnosupport
  defp translate_reason(:unsupported_socket), do: :enotsup
  defp translate_reason(:unsupported_timeout), do: :einval
  defp translate_reason(:invalid_options), do: :einval
  defp translate_reason(:not_connected), do: :enotconn
  defp translate_reason(:not_bound), do: :einval
  defp translate_reason(:already_connected), do: :eisconn
  defp translate_reason(:invalid_how), do: :einval
  defp translate_reason(:invalid_address), do: :einval
  defp translate_reason(:invalid_port), do: :einval
  defp translate_reason(:invalid_backlog), do: :einval
  defp translate_reason(:scope_required), do: :einval
  defp translate_reason(:invalid_scope), do: :einval
  defp translate_reason(:invalid_socket_state), do: :einval
  defp translate_reason(:invalid_socket), do: :closed
  defp translate_reason(:stack_down), do: :enetdown
  defp translate_reason(:link_down), do: :enetdown
  defp translate_reason(reason), do: reason
end
