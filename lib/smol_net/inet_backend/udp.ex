defmodule SmolNet.InetBackend.Udp do
  @moduledoc """
  IPv6 UDP adapter for Erlang `:gen_udp` and `:inet`.

  Select this callback with `{:udp_module, SmolNet.InetBackend.Udp}` and pass
  the target stack with `{:smolnet_stack, stack}`. Each returned OTP socket is
  backed by one temporary `:gen_statem` child of the stack's inet supervisor.

  Datagram boundaries are preserved. IPv4, ancillary data, multicast, and file
  descriptors are intentionally unsupported in this phase.
  """

  @behaviour :gen_statem

  alias SmolNet.InetBackend.Options
  alias SmolNet.InetBackend.Packet
  alias SmolNet.Socket
  alias SmolNet.Stack
  alias SmolNet.StackSupervisor

  import Kernel, except: [send: 2]

  @max_active_deliveries 16
  @max_timeout 4_294_967_295

  @type socket_term :: {:"$inet", __MODULE__, pid()}

  # OTP UDP callback entry points

  @spec family() :: :inet6
  def family, do: :inet6

  @spec getserv(term()) :: {:ok, :inet.port_number()} | {:error, atom()}
  def getserv(port), do: :inet6_udp.getserv(port)

  @spec getaddr(term()) :: {:ok, :inet.ip6_address()} | {:error, atom()}
  def getaddr(address), do: :inet6_udp.getaddr(address)

  @spec getaddr(term(), term()) :: {:ok, :inet.ip6_address()} | {:error, atom()}
  def getaddr(address, timer), do: :inet6_udp.getaddr(address, timer)

  @spec translate_ip(term()) :: term()
  def translate_ip(address), do: :inet6_udp.translate_ip(address)

  @spec open(:inet.port_number()) :: {:ok, socket_term()} | {:error, atom()}
  def open(port), do: open(port, [])

  @spec open(:inet.port_number(), list()) :: {:ok, socket_term()} | {:error, atom()}
  def open(port, options) do
    with {:ok, parsed} <- Options.parse_udp(options, port),
         true <- parsed.family == :inet6 do
      start_socket(parsed)
    else
      false -> {:error, :eafnosupport}
      {:error, _reason} = error -> error
    end
  end

  @spec fdopen(term(), list()) :: {:error, :enotsup}
  def fdopen(_fd, _options), do: {:error, :enotsup}

  @spec send(socket_term(), iodata()) :: :ok | {:error, atom()}
  def send(socket, packet), do: socket_call(socket, {:send, :connected, packet})

  @spec send(socket_term(), term(), iodata()) :: :ok | {:error, atom()}
  def send(socket, destination, packet) do
    with {:ok, endpoint} <- endpoint(destination) do
      socket_call(socket, {:send, endpoint, packet})
    end
  end

  @spec send(socket_term(), term(), term(), iodata()) :: :ok | {:error, atom()}
  def send(socket, destination, ancillary, packet) when is_list(ancillary) do
    if ancillary == [] do
      send(socket, destination, packet)
    else
      {:error, :einval}
    end
  end

  def send(socket, destination, 0, packet)
      when (is_tuple(destination) and tuple_size(destination) == 2) or is_map(destination),
      do: send(socket, destination, packet)

  def send(socket, address, port, packet) do
    with {:ok, endpoint} <- endpoint(address, port) do
      socket_call(socket, {:send, endpoint, packet})
    end
  end

  @spec send(socket_term(), term(), term(), list(), iodata()) :: :ok | {:error, atom()}
  def send(socket, address, port, ancillary, packet) when is_list(ancillary) do
    if ancillary == [] do
      send(socket, address, port, packet)
    else
      {:error, :einval}
    end
  end

  def send(_socket, _address, _port, _ancillary, _packet), do: {:error, :einval}

  @spec recv(socket_term(), non_neg_integer()) ::
          {:ok, {:inet.ip6_address(), :inet.port_number(), binary() | list()}}
          | {:error, atom()}
  def recv(socket, length), do: recv(socket, length, :infinity)

  @spec recv(socket_term(), non_neg_integer(), timeout()) ::
          {:ok, {:inet.ip6_address(), :inet.port_number(), binary() | list()}}
          | {:error, atom()}
  def recv(socket, length, timeout)
      when is_integer(length) and length >= 0 and
             (timeout == :infinity or
                (is_integer(timeout) and timeout >= 0 and timeout <= @max_timeout)) do
    socket_call(socket, {:recv, length, deadline(timeout)})
  end

  def recv(_socket, _length, _timeout), do: {:error, :einval}

  @spec connect(socket_term(), map()) :: :ok | {:error, atom()}
  def connect(socket, sockaddr) do
    with {:ok, endpoint} <- endpoint(sockaddr) do
      socket_call(socket, {:connect, endpoint})
    end
  end

  @spec connect(socket_term(), term(), term()) :: :ok | {:error, atom()}
  def connect(socket, address, port) do
    with {:ok, endpoint} <- endpoint(address, port) do
      socket_call(socket, {:connect, endpoint})
    end
  end

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
      pid when is_pid(pid) -> ~c"#SmolNet.UDP<#{inspect(pid)}>"
      _other -> ~c"#SmolNet.UDP<closed>"
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
  def init(%{owner: owner, options: %Options{} = options}) do
    Process.flag(:trap_exit, true)
    stack_pid = options.stack.stack

    data = %{
      owner: owner,
      owner_monitor: Process.monitor(owner),
      stack: options.stack,
      stack_pid: stack_pid,
      stack_monitor: Process.monitor(stack_pid),
      options: options,
      public_socket: module_socket(self()),
      low_socket: nil,
      peer: nil,
      open_from: nil,
      open_result: nil,
      read: nil,
      read_scheduled: false,
      write: nil,
      transfer: nil
    }

    {:ok, :opening, data, [{:next_event, :internal, :continue_setup}]}
  end

  @impl true
  def handle_event(:internal, :continue_setup, :opening, data) do
    result =
      with {:ok, socket} <- SmolNet.open(:inet6, :dgram, :udp, stack: data.stack),
           :ok <- Stack.socket_watch_owner(socket, self()),
           :ok <- SmolNet.bind(socket, local_endpoint(data.options)) do
        {:ok, socket}
      end

    complete_open(result, data)
  end

  def handle_event({:call, from}, :await_open, :opening, data) do
    case data.open_result do
      {:ok, socket} ->
        data = %{data | low_socket: socket, open_from: nil}
        :gen_statem.reply(from, {:ok, data.public_socket})
        opened(data)

      {:error, reason} ->
        :gen_statem.reply(from, {:error, reason})
        {:stop, :normal, data}

      nil ->
        {:keep_state, %{data | open_from: from}}
    end
  end

  def handle_event({:call, from}, request, :opening, _data) do
    if request == :close do
      {:stop_and_reply, :normal, [{:reply, from, :ok}]}
    else
      {:keep_state_and_data, [{:reply, from, {:error, :ealready}}]}
    end
  end

  def handle_event({:call, from}, {:send, destination, packet}, :open, data) do
    cond do
      data.write != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      data.transfer != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      destination == :connected and is_nil(data.peer) ->
        {:keep_state_and_data, [{:reply, from, {:error, :enotconn}}]}

      true ->
        endpoint = if destination == :connected, do: data.peer, else: destination

        case to_binary(packet) do
          {:ok, binary} ->
            data = %{data | write: %{from: from, endpoint: endpoint, data: binary, select: nil}}
            data |> drive_write() |> state_return()

          {:error, reason} ->
            {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
        end
    end
  end

  def handle_event({:call, from}, {:recv, length, deadline}, :open, data) do
    cond do
      data.options.active != false ->
        {:keep_state_and_data, [{:reply, from, {:error, :einval}}]}

      data.read != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      data.transfer != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      true ->
        {timer, token} = arm_timer(deadline)

        read = %{
          kind: :passive,
          from: from,
          length: length,
          select: nil,
          timer: {timer, token}
        }

        data |> Map.put(:read, read) |> drive_read(@max_active_deliveries) |> state_return()
    end
  end

  def handle_event({:call, from}, {:connect, endpoint}, :open, data) do
    case SmolNet.connect(data.low_socket, endpoint) do
      :ok ->
        {:keep_state, %{data | peer: endpoint}, [{:reply, from, :ok}]}

      {:error, reason} ->
        {:keep_state_and_data, [{:reply, from, {:error, translate_reason(reason)}}]}
    end
  end

  def handle_event({:call, from}, :close, :open, data) do
    data = fail_operations(data, :closed)
    _result = close_low_socket(data)
    {:stop_and_reply, :normal, [{:reply, from, :ok}], %{data | low_socket: nil}}
  end

  def handle_event({:call, from}, :sockname, :open, data) do
    {:keep_state_and_data, [{:reply, from, endpoint_result(SmolNet.sockname(data.low_socket))}]}
  end

  def handle_event({:call, from}, :peername, :open, data) do
    {:keep_state_and_data, [{:reply, from, endpoint_result(SmolNet.peername(data.low_socket))}]}
  end

  def handle_event({:call, from}, {:setopts, options}, :open, data) do
    set_options(from, options, data)
  end

  def handle_event({:call, from}, {:getopts, names}, :open, data) do
    {:keep_state_and_data, [{:reply, from, Options.get_udp(data.options, names)}]}
  end

  def handle_event({:call, from}, {:begin_transfer, new_owner}, :open, data) do
    begin_transfer(from, new_owner, data)
  end

  def handle_event({:call, from}, {:commit_transfer, token, messages}, :open, data) do
    commit_transfer(from, token, messages, data)
  end

  def handle_event({:call, from}, :info, :open, data) do
    info = %{
      owner: data.owner,
      active: data.options.active,
      mode: data.options.mode,
      connected: data.peer != nil,
      read_pending: data.read != nil,
      write_pending: data.write != nil
    }

    {:keep_state_and_data, [{:reply, from, info}]}
  end

  def handle_event({:call, from}, {:getstat, names}, :open, _data) do
    {:keep_state_and_data, [{:reply, from, getstat_reply(names)}]}
  end

  def handle_event({:call, from}, _request, :open, _data) do
    {:keep_state_and_data, [{:reply, from, {:error, :enotsup}}]}
  end

  def handle_event(:internal, :drain_active, :open, data) do
    data = %{data | read_scheduled: false}
    data |> ensure_active_read() |> drive_read(@max_active_deliveries) |> state_return()
  end

  def handle_event(:info, :continue_read, :open, data) do
    data = %{data | read_scheduled: false}
    data |> drive_read(@max_active_deliveries) |> state_return()
  end

  def handle_event(
        :info,
        {:"$smol_socket", identity, :select, reference},
        :open,
        %{low_socket: %Socket{} = socket} = data
      ) do
    if Socket.identity(socket) == identity do
      handle_select(reference, data)
    else
      :keep_state_and_data
    end
  end

  def handle_event(
        :info,
        {:"$smol_socket", identity, :abort, reference, reason},
        :open,
        %{low_socket: %Socket{} = socket} = data
      ) do
    if Socket.identity(socket) == identity do
      handle_abort(reference, translate_reason(reason), data)
    else
      :keep_state_and_data
    end
  end

  def handle_event(:info, {:operation_timeout, :read, token}, :open, data) do
    case data.read do
      %{kind: :passive, timer: {_timer, ^token}} = read ->
        data = cancel_read_select(data)
        :gen_statem.reply(read.from, {:error, :timeout})
        {:keep_state, %{data | read: nil}}

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
    data = fail_operations(data, :enetdown)
    {:stop, {:shutdown, :stack_down}, %{data | low_socket: nil}}
  end

  def handle_event(:info, _message, _state_name, _data), do: :keep_state_and_data

  @impl true
  def terminate(_reason, _state_name, data) do
    _result = close_low_socket(data)
    :ok
  end

  defp start_socket(%Options{} = options) do
    child = child_spec(%{owner: self(), options: options})

    case StackSupervisor.start_inet_backend(options.stack, child) do
      {:ok, pid} -> pid_call(pid, :await_open)
      {:error, reason} -> {:error, start_error(reason)}
    end
  catch
    :exit, _reason -> {:error, :closed}
  end

  defp complete_open({:ok, socket} = result, data) do
    data = %{data | low_socket: socket, open_result: result}

    case data.open_from do
      nil ->
        {:keep_state, data}

      from ->
        :gen_statem.reply(from, {:ok, data.public_socket})
        opened(%{data | open_from: nil})
    end
  end

  defp complete_open({:error, reason}, data) do
    reason = translate_reason(reason)

    case data.open_from do
      nil ->
        {:keep_state, %{data | open_result: {:error, reason}}}

      from ->
        :gen_statem.reply(from, {:error, reason})
        {:stop, :normal, data}
    end
  end

  defp opened(data) do
    {:next_state, :open, data, [{:next_event, :internal, :drain_active}]}
  end

  defp local_endpoint(options) do
    %{
      family: :inet6,
      addr: options.bind_address || {0, 0, 0, 0, 0, 0, 0, 0},
      port: options.bind_port,
      flowinfo: 0,
      scope_id: options.bind_scope_id
    }
  end

  defp drive_write(%{write: nil} = data), do: {:keep, data}

  defp drive_write(data) do
    write = data.write

    case SmolNet.sendto(data.low_socket, write.data, write.endpoint, :nowait) do
      :ok ->
        :gen_statem.reply(write.from, :ok)
        {:keep, %{data | write: nil}}

      {:select, select_info} ->
        {:keep, %{data | write: %{write | select: select_info}}}

      {:error, reason} ->
        :gen_statem.reply(write.from, {:error, translate_reason(reason)})
        {:keep, %{data | write: nil}}
    end
  end

  defp drive_read(%{read: nil} = data, _deliveries), do: {:keep, data}

  defp drive_read(%{transfer: transfer} = data, _deliveries) when transfer != nil,
    do: {:keep, data}

  defp drive_read(data, deliveries) when deliveries <= 0 do
    if read_reference(data.read) do
      {:keep, data}
    else
      {:keep, schedule_read_continuation(data)}
    end
  end

  defp drive_read(data, deliveries) do
    read = data.read

    case SmolNet.recvfrom(data.low_socket, receive_length(read, data.options), :nowait) do
      {:ok, datagram} ->
        data |> cancel_read_select() |> deliver_datagram(datagram, deliveries)

      {:select, select_info} ->
        {:keep, put_in(data, [:read, :select], select_info)}

      {:error, reason} ->
        read_failure(data, translate_reason(reason))
    end
  end

  defp deliver_datagram(%{read: %{kind: :passive} = read} = data, datagram, _deliveries) do
    _read = cancel_op_timer(read)
    source = datagram.source
    packet = Packet.represent(datagram.data, data.options.mode)
    :gen_statem.reply(read.from, {:ok, {source.addr, source.port, packet}})
    {:keep, %{data | read: nil}}
  end

  defp deliver_datagram(%{read: %{kind: :active}} = data, datagram, deliveries) do
    source = datagram.source
    packet = Packet.represent(datagram.data, data.options.mode)
    Kernel.send(data.owner, {:udp, data.public_socket, source.addr, source.port, packet})

    {active, passive?} = consume_active(data.options.active)
    options = %{data.options | active: active}

    if passive? do
      Kernel.send(data.owner, {:udp_passive, data.public_socket})
    end

    data = %{data | options: options}

    if active == false do
      {:keep, %{data | read: nil}}
    else
      drive_read(data, deliveries - 1)
    end
  end

  defp read_failure(%{read: nil} = data, _reason), do: {:keep, data}

  defp read_failure(%{read: %{kind: :passive} = read} = data, reason) do
    _read = cancel_op_timer(read)
    :gen_statem.reply(read.from, {:error, reason})
    {:keep, %{data | read: nil}}
  end

  defp read_failure(%{read: %{kind: :active}} = data, reason) do
    Kernel.send(data.owner, {:udp_error, data.public_socket, reason})
    {:keep, %{data | read: nil, options: %{data.options | active: false}}}
  end

  defp handle_select(reference, data) do
    cond do
      read_reference(data.read) == reference ->
        data
        |> put_in([:read, :select], nil)
        |> drive_read(@max_active_deliveries)
        |> state_return()

      write_reference(data.write) == reference ->
        data |> put_in([:write, :select], nil) |> drive_write() |> state_return()

      true ->
        :keep_state_and_data
    end
  end

  defp handle_abort(reference, reason, data) do
    cond do
      read_reference(data.read) == reference ->
        data |> put_in([:read, :select], nil) |> read_failure(reason) |> state_return()

      write_reference(data.write) == reference ->
        write = data.write
        :gen_statem.reply(write.from, {:error, reason})
        {:keep_state, %{data | write: nil}}

      true ->
        :keep_state_and_data
    end
  end

  defp set_options(from, options, data) do
    with {:ok, updated} <- Options.update_udp(data.options, options),
         :ok <- option_change_allowed(data, updated) do
      data = apply_active_change(data, updated)
      {:keep_state, data, [{:reply, from, :ok}]}
    else
      {:error, reason} -> {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
    end
  end

  defp option_change_allowed(%{read: %{kind: :passive}} = data, updated) do
    if updated.active != false or updated.mode != data.options.mode or
         updated.buffer != data.options.buffer do
      {:error, :busy}
    else
      :ok
    end
  end

  defp option_change_allowed(_data, _updated), do: :ok

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

  defp ensure_active_read(%{options: %{active: false}} = data), do: data
  defp ensure_active_read(%{read: nil} = data), do: %{data | read: active_read()}
  defp ensure_active_read(data), do: data

  defp active_read, do: %{kind: :active, length: 0, select: nil, timer: nil}

  defp receive_length(%{length: 0}, options), do: options.buffer
  defp receive_length(%{length: length}, options), do: min(length, options.buffer)

  defp schedule_read_continuation(%{options: %{active: false}} = data), do: data
  defp schedule_read_continuation(%{read_scheduled: true} = data), do: data

  defp schedule_read_continuation(data) do
    if read_reference(data.read) do
      data
    else
      Kernel.send(self(), :continue_read)
      %{data | read_scheduled: true}
    end
  end

  defp begin_transfer(from, new_owner, data) do
    caller = from_pid(from)

    cond do
      caller != data.owner ->
        {:keep_state_and_data, [{:reply, from, {:error, :not_owner}}]}

      data.transfer != nil ->
        {:keep_state_and_data, [{:reply, from, {:error, :busy}}]}

      data.read != nil and data.read.kind == :passive ->
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
        data = %{data | transfer: nil} |> schedule_read_continuation()
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
        {:keep_state, schedule_read_continuation(data), [{:reply, from, :ok}]}
    end
  end

  defp commit_transfer(from, _token, _messages, data) do
    {:keep_state, data, [{:reply, from, {:error, :einval}}]}
  end

  defp fail_operations(data, reason) do
    if data.open_from, do: :gen_statem.reply(data.open_from, {:error, reason})

    if match?(%{kind: :passive}, data.read) do
      :gen_statem.reply(data.read.from, {:error, reason})
    else
      if data.options.active != false do
        Kernel.send(data.owner, {:udp_error, data.public_socket, reason})
      end
    end

    if data.write, do: :gen_statem.reply(data.write.from, {:error, reason})
    %{data | open_from: nil, read: nil, write: nil}
  end

  defp cancel_read_select(%{read: %{select: nil}} = data), do: data

  defp cancel_read_select(data) do
    _cancelled = SmolNet.cancel(data.low_socket, data.read.select)
    put_in(data, [:read, :select], nil)
  end

  defp endpoint(address, port) do
    with {:ok, resolved} <- getaddr(address),
         {:ok, resolved_port} <- getserv(port) do
      endpoint(%{family: :inet6, addr: resolved, port: resolved_port})
    else
      {:error, :einval} -> {:error, :einval}
      {:error, _reason} = error -> error
    end
  end

  defp endpoint({:inet6, {address, port}}), do: endpoint(address, port)
  defp endpoint({address, port}), do: endpoint(address, port)

  defp endpoint(%{family: :inet6, addr: address, port: port} = sockaddr)
       when is_tuple(address) and tuple_size(address) == 8 and is_integer(port) and
              port in 1..65_535 do
    if address |> Tuple.to_list() |> Enum.all?(&(is_integer(&1) and &1 in 0..65_535)) do
      {:ok,
       %{
         family: :inet6,
         addr: address,
         port: port,
         flowinfo: Map.get(sockaddr, :flowinfo, 0),
         scope_id: Map.get(sockaddr, :scope_id, 0)
       }}
    else
      {:error, :einval}
    end
  end

  defp endpoint(%{family: family}) when family in [:inet, :inet6],
    do: {:error, :eafnosupport}

  defp endpoint(_destination), do: {:error, :einval}

  defp endpoint_result({:ok, %{addr: address, port: port}}), do: {:ok, {address, port}}
  defp endpoint_result({:error, reason}), do: {:error, translate_reason(reason)}

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
      {:udp, ^socket, _address, _port, _packet} = message ->
        take_socket_messages(socket, [message | messages])

      {:udp_error, ^socket, _reason} = message ->
        take_socket_messages(socket, [message | messages])

      {:udp_passive, ^socket} = message ->
        take_socket_messages(socket, [message | messages])
    after
      0 -> Enum.reverse(messages)
    end
  end

  defp restore_socket_messages(messages), do: Enum.each(messages, &Kernel.send(self(), &1))

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

  defp state_return({:keep, data}), do: {:keep_state, data}

  defp consume_active(true), do: {true, false}
  defp consume_active(:once), do: {false, false}
  defp consume_active(1), do: {false, true}
  defp consume_active(active) when is_integer(active) and active > 1, do: {active - 1, false}

  defp arm_timer(:infinity), do: {nil, nil}

  defp arm_timer(deadline) do
    token = make_ref()
    timer = Process.send_after(self(), {:operation_timeout, :read, token}, remaining(deadline))
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

  defp close_low_socket(%{low_socket: %Socket{} = socket}), do: SmolNet.close(socket)
  defp close_low_socket(_data), do: :ok

  defp from_pid({pid, _tag}) when is_pid(pid), do: pid

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

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

  defp translate_reason(:network_unreachable), do: :enetunreach
  defp translate_reason(:address_in_use), do: :eaddrinuse
  defp translate_reason(:address_not_available), do: :eaddrnotavail
  defp translate_reason(:ephemeral_ports_exhausted), do: :system_limit
  defp translate_reason(:unsupported_family), do: :eafnosupport
  defp translate_reason(:unsupported_socket), do: :enotsup
  defp translate_reason(:invalid_options), do: :einval
  defp translate_reason(:invalid_address), do: :einval
  defp translate_reason(:invalid_port), do: :einval
  defp translate_reason(:scope_required), do: :einval
  defp translate_reason(:invalid_scope), do: :einval
  defp translate_reason(:invalid_socket_state), do: :einval
  defp translate_reason(:not_bound), do: :einval
  defp translate_reason(:not_connected), do: :enotconn
  defp translate_reason(:message_too_large), do: :emsgsize
  defp translate_reason(:invalid_socket), do: :closed
  defp translate_reason(:stack_down), do: :enetdown
  defp translate_reason(:link_down), do: :enetdown
  defp translate_reason(reason), do: reason
end
