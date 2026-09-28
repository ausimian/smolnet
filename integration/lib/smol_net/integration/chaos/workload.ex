defmodule SmolNet.Integration.Chaos.Workload do
  @moduledoc """
  The traffic of one chaos episode (see `SmolNet.Integration.Scenarios.Chaos`):
  servers on the peer, and operations SmolNet runs against them.

  Each operation runs in a process of its own and reports to the episode
  as `{:op, id, event, value, at_ms}`, where `at_ms` is
  `System.monotonic_time(:millisecond)` and `event` is one of:

    * `:ready` - set up, with `%{owner: pid, sockets: [socket]}`, and about
      to block or loop.
    * `:iteration` - a loop finished a round, such as a checked transfer.
    * `:owner` - the stream's socket changed owner, with the same map.
    * `:casualty` - with `tolerate: true` in its settings, a loop's round
      failed, with `%{error: error, started: at_ms}`, and the loop went on.
    * `:done` - the operation ended: `:stopped` for a loop told to stop,
      or what the call that ended it returned.
  """

  alias SmolNet.Integration.Https
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak.Context
  alias SmolNet.Integration.Tls

  @accept_poll 250
  @connect_timeout 10_000
  @small_buffer 4_096
  @churn_pause 100
  @active_n 4
  @probe_timeout 10_000
  @step_timeout 8_000

  @type kind :: :bulk | :churn | :stream | :recv | :send | :accept | :udp | :connect

  @doc "The operations that block for good, until something ends them."
  @spec blocked_kinds() :: [kind()]
  def blocked_kinds, do: [:recv, :send, :accept, :udp, :connect]

  @doc """
  Starts the episode's servers on the peer for each of `families`: HTTPS
  for `:bulk`, and plain TCP for `:churn`, `:stream`, and the blocked
  `:recv` and `:send`, whose connections it holds and never serves.

  `settings` holds `:server_tls` and `:stream_payload`. Returns a map of
  `{server, family}` to `%{pid: pid, port: port}`.
  """
  @spec start_servers(Context.t(), [Network.family()], map()) :: {:ok, map()} | {:error, term()}
  def start_servers(context, families, settings) do
    specs = for family <- families, kind <- [:tls, :churn, :stream, :silent], do: {kind, family}

    Enum.reduce_while(specs, {:ok, %{}}, fn {kind, family} = key, {:ok, servers} ->
      case start_server(context, kind, family, settings) do
        {:ok, server} ->
          {:cont, {:ok, Map.put(servers, key, server)}}

        {:error, reason} ->
          stop_servers(servers)
          {:halt, {:error, {kind, family, reason}}}
      end
    end)
  end

  @doc "Stops the servers, and every connection they hold."
  @spec stop_servers(map()) :: :ok
  def stop_servers(servers), do: Enum.each(servers, fn {_key, server} -> stop_server(server) end)

  @doc """
  Checks that the stack works over `family`: SmolNet sends 16 KiB to the
  peer over a new TCP connection, which echoes it. Returns `:ok` or
  `{:error, reason}` within #{@probe_timeout} ms.
  """
  @spec probe(Context.t(), Network.family()) :: :ok | {:error, term()}
  def probe(context, family) do
    task = Task.async(fn -> echo_probe(context, family) end)

    case Task.yield(task, @probe_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:crashed, reason}}
      nil -> {:error, :timeout}
    end
  end

  defp echo_probe(context, family) do
    payload = :crypto.strong_rand_bytes(16_384)
    size = byte_size(payload)
    address = Network.address(context, :peer, family)
    listen = Network.tcp_options(context, :peer, family) ++ [:binary, active: false, ip: address]

    with {:ok, listener} <- :gen_tcp.listen(0, listen),
         {:ok, {_address, port}} <- :inet.sockname(listener) do
      echoer = Task.async(fn -> echo_back(listener, size) end)

      options = Network.tcp_options(context, :subject, family) ++ [:binary, active: false]

      result =
        with {:ok, socket} <- :gen_tcp.connect(address, port, options, @step_timeout),
             :ok <- :gen_tcp.send(socket, payload),
             {:ok, echoed} <- :gen_tcp.recv(socket, size, @step_timeout),
             :ok <- :gen_tcp.close(socket),
             do: same(echoed, payload)

      Task.shutdown(echoer, :brutal_kill)
      :gen_tcp.close(listener)
      result
    end
  end

  defp echo_back(listener, size) do
    with {:ok, socket} <- :gen_tcp.accept(listener, @step_timeout),
         {:ok, data} <- :gen_tcp.recv(socket, size, @step_timeout),
         do: :gen_tcp.send(socket, data)
  end

  defp same(payload, payload), do: :ok
  defp same(_echoed, _payload), do: {:error, :echo_differs}

  # Servers

  defp start_server(context, kind, family, settings) do
    parent = self()
    pid = spawn(fn -> server(parent, context, kind, family, settings) end)
    monitor = Process.monitor(pid)

    receive do
      {^pid, {:ok, port}} ->
        Process.demonitor(monitor, [:flush])
        {:ok, %{pid: pid, port: port}}

      {^pid, {:error, _reason} = error} ->
        Process.demonitor(monitor, [:flush])
        error

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, reason}
    end
  end

  defp server(parent, context, :tls, family, settings) do
    case Tls.listen(context, :peer, family, abortive(context) ++ settings.server_tls) do
      {:ok, listener} ->
        {:ok, {_address, port}} = :ssl.sockname(listener)
        send(parent, {self(), {:ok, port}})
        accept = fn -> :ssl.transport_accept(listener, @accept_poll) end
        accept_loop(server_state(accept, &:ssl.controlling_process/2, &serve_tls/1))

      {:error, _reason} = error ->
        send(parent, {self(), error})
    end
  end

  defp server(parent, context, kind, family, settings) do
    buffer = if kind == :silent, do: [recbuf: @small_buffer], else: []
    address = [ip: Network.address(context, :peer, family)]
    options = Network.tcp_options(context, :peer, family) ++ [:binary, active: false] ++ address

    case :gen_tcp.listen(0, options ++ buffer ++ abortive(context)) do
      {:ok, listener} ->
        {:ok, {_address, port}} = :inet.sockname(listener)
        send(parent, {self(), {:ok, port}})
        accept = fn -> :gen_tcp.accept(listener, @accept_poll) end
        serve = fn socket -> serve_tcp(kind, settings, socket) end
        accept_loop(server_state(accept, &:gen_tcp.controlling_process/2, serve))

      {:error, _reason} = error ->
        send(parent, {self(), error})
    end
  end

  # A kernel socket closed with data its peer never took keeps its port,
  # and the data, until the kernel gives up on the peer, some 15 minutes
  # on; halting the VM waits for it. The peer here is a stack that may
  # have been stopped, so the kernel's sockets are closed with a reset,
  # and each server waits for its client to close first, so that nothing
  # the client still wants is thrown away.
  defp abortive(context) do
    if Network.smolnet?(context, :peer), do: [], else: [linger: {true, 0}]
  end

  defp server_state(accept, hand, serve),
    do: %{accept: accept, hand: hand, serve: serve, handlers: []}

  defp accept_loop(server) do
    receive do
      :stop -> kill_handlers(server)
    after
      0 ->
        case server.accept.() do
          {:ok, socket} ->
            accept_loop(handle(server, socket))

          {:error, :timeout} ->
            accept_loop(%{server | handlers: Enum.filter(server.handlers, &Process.alive?/1)})

          # The listener is gone, as it is when its stack stops.
          {:error, _reason} ->
            receive do
              :stop -> kill_handlers(server)
            end
        end
    end
  end

  defp handle(server, socket) do
    serve = server.serve

    handler =
      spawn(fn ->
        receive do
          {:socket, socket} -> serve.(socket)
        end
      end)

    case server.hand.(socket, handler) do
      :ok -> send(handler, {:socket, socket})
      {:error, _reason} -> Process.exit(handler, :kill)
    end

    %{server | handlers: [handler | server.handlers]}
  end

  # A handler's connections close with it.
  defp kill_handlers(server), do: Enum.each(server.handlers, &Process.exit(&1, :kill))

  defp stop_server(%{pid: pid}) do
    monitor = Process.monitor(pid)
    send(pid, :stop)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    end
  end

  defp serve_tls(socket) do
    with {:ok, tls} <- :ssl.handshake(socket, 30_000) do
      _served = Https.serve(tls, :passive)
      _closed = :ssl.recv(tls, 0, 30_000)
      :ssl.close(tls)
    end
  end

  defp serve_tcp(:churn, _settings, socket) do
    _received = :gen_tcp.recv(socket, 1, 5_000)
    :gen_tcp.close(socket)
  end

  # Sends the payload, then waits for the reader to close first, so that
  # nothing it has not read is reset away.
  defp serve_tcp(:stream, settings, socket) do
    with {:ok, _request} <- :gen_tcp.recv(socket, 1, 30_000),
         :ok <- :gen_tcp.send(socket, settings.stream_payload),
         :ok <- :gen_tcp.shutdown(socket, :write) do
      _closed = :gen_tcp.recv(socket, 0, 60_000)
    end

    :gen_tcp.close(socket)
  end

  # Holds the connection, never reading or writing, until the server stops.
  defp serve_tcp(:silent, _settings, _socket), do: Process.sleep(:infinity)

  # Operations

  @doc """
  Starts operation `id`, of `kind` over `family`, in a process that reports
  to the caller, and returns its pid.

    * `:bulk` - TLS downloads and uploads of `:bulk_bytes` to the peer's
      HTTPS server, alternately, each body's length and SHA-256 checked.
    * `:churn` - TCP connections to the peer, each sending a byte and
      closing: a simple churn loop, until #88's crawl replaces it.
    * `:stream` - the peer sends `:stream_payload`, which SmolNet reads in
      active mode and checks whole. On `{:swap, n}`, the reader hands the
      socket on with `controlling_process` `n` times mid-stream, each old
      owner exiting abruptly as soon as it has.
    * `:recv`, `:send`, `:accept`, `:udp`, `:connect` - one call each that
      blocks for good (see `blocked_kinds/0`): a receive from a peer that
      never sends, a send to one that never reads, an accept and a UDP
      receive nobody serves, and a connect to an address nobody answers,
      retried whenever it gives up.

  The loops, `:bulk`, `:churn` and `:stream`, end on `:stop` or on their
  first error, or with `tolerate: true` only on `:stop` or a failed
  integrity check. `settings` also holds `:servers` (from
  `start_servers/3`) and `:client_tls`, `:buffer` and `:stream_sha`.
  """
  @spec start_op(Context.t(), term(), kind(), Network.family(), map()) :: pid()
  def start_op(context, id, kind, family, settings) do
    op = %{
      context: context,
      id: id,
      kind: kind,
      family: family,
      settings: settings,
      parent: self()
    }

    spawn(fn -> run_op(op) end)
  end

  defp run_op(%{kind: kind} = op) when kind in [:bulk, :churn, :stream] do
    report(op, :ready, %{owner: self(), sockets: []})
    report(op, :done, loop(op, 0))
  end

  defp run_op(op), do: report(op, :done, blocked(op))

  defp report(op, event, value), do: send(op.parent, {:op, op.id, event, value, now()})

  defp now, do: System.monotonic_time(:millisecond)

  # With `:tolerate`, a round that fails, other than on integrity, is
  # reported as a `:casualty`, with when it began, and the loop goes on.
  defp loop(op, round) do
    receive do
      :stop -> :stopped
    after
      0 ->
        started = now()

        case round(op, round) do
          :ok ->
            report(op, :iteration, round)
            loop(op, round + 1)

          {:error, {:integrity, _detail}} = error ->
            error

          error ->
            if Map.get(op.settings, :tolerate, false) do
              report(op, :casualty, %{error: error, started: started})
              Process.sleep(@churn_pause)
              loop(op, round + 1)
            else
              error
            end
        end
    end
  end

  defp round(%{kind: :bulk} = op, round) do
    %{port: port} = op.settings.servers[{:tls, op.family}]
    address = Network.address(op.context, :peer, op.family)
    options = [timeout: @connect_timeout, buffer: op.settings.buffer]

    case Tls.connect(op.context, :subject, address, port, op.settings.client_tls, options) do
      {:ok, tls} ->
        try do
          if rem(round, 2) == 0,
            do: download(tls, op.settings.bulk_bytes),
            else: upload(tls, op.settings.bulk_bytes)
        after
          :ssl.close(tls)
        end

      {:error, reason} ->
        {:error, {:connect, reason}}
    end
  end

  defp round(%{kind: :churn} = op, _round) do
    result =
      with {:ok, socket} <- connect(op, :churn, small_buffers(op)) do
        sent = :gen_tcp.send(socket, "x")
        :gen_tcp.close(socket)
        sent
      end

    Process.sleep(@churn_pause)
    result
  end

  defp round(%{kind: :stream} = op, _round), do: stream_round(op)

  defp download(tls, bytes) do
    case Https.get(tls, "smolnet.test", "/down?bytes=#{bytes}", :passive) do
      {:ok, %{status: 200, bytes: ^bytes, sha256: sha, headers: %{"x-sha256" => sha}}} -> :ok
      {:ok, response} -> {:error, {:integrity, response}}
      {:error, _reason} = error -> error
    end
  end

  defp upload(tls, bytes) do
    body = :crypto.strong_rand_bytes(bytes)
    sha = Https.sha256(body)
    length = Integer.to_string(bytes)

    case Https.post(tls, "smolnet.test", "/up", body, :passive) do
      {:ok, %{status: 200, headers: %{"x-upload-bytes" => ^length, "x-sha256" => ^sha}}} -> :ok
      {:ok, response} -> {:error, {:integrity, response}}
      {:error, _reason} = error -> error
    end
  end

  defp connect(op, server, extra) do
    %{port: port} = op.settings.servers[{server, op.family}]
    address = Network.address(op.context, :peer, op.family)
    options = Network.tcp_options(op.context, :subject, op.family) ++ [:binary, active: false]

    case :gen_tcp.connect(address, port, options ++ extra, @connect_timeout) do
      {:ok, socket} -> {:ok, socket}
      {:error, reason} -> {:error, {:connect, reason}}
    end
  end

  defp small_buffers(op), do: Tls.buffer_options(op.context, :subject, @small_buffer)

  # The operation's process coordinates; the socket belongs to a reader,
  # and then to each reader it is handed on to.
  # The client asks for the stream, as a real one would, so that it always
  # has something to send: a peer that has lost the connection answers it
  # with a reset, where silence would leave the reader waiting for good.
  defp stream_round(op) do
    with {:ok, socket} <- connect(op, :stream, []),
         :ok <- ask(socket) do
      reader = spawn_reader(self())

      case :gen_tcp.controlling_process(socket, reader) do
        :ok ->
          send(reader, {:take, socket, %{bytes: 0, hash: :crypto.hash_init(:sha256), swaps: 0}})
          report(op, :owner, %{owner: reader, sockets: [socket]})
          op |> await_stream(reader, Process.monitor(reader), socket) |> check_stream(op)

        {:error, reason} ->
          Process.exit(reader, :kill)
          :gen_tcp.close(socket)
          {:error, {:controlling_process, reason}}
      end
    end
  end

  defp ask(socket) do
    case :gen_tcp.send(socket, "g") do
      :ok ->
        :ok

      {:error, _reason} = error ->
        :gen_tcp.close(socket)
        error
    end
  end

  defp await_stream(op, reader, monitor, socket) do
    receive do
      {:swap, count} ->
        send(reader, {:swap, count})
        await_stream(op, reader, monitor, socket)

      {:reader, :owner, next} ->
        Process.demonitor(monitor, [:flush])
        report(op, :owner, %{owner: next, sockets: [socket]})
        await_stream(op, next, Process.monitor(next), socket)

      {:reader, :result, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^reader, reason} ->
        {:error, {:owner_died, reason}}
    end
  end

  defp check_stream({:ok, bytes, sha}, op) do
    expected = byte_size(op.settings.stream_payload)

    if bytes == expected and sha == op.settings.stream_sha,
      do: :ok,
      else: {:error, {:integrity, %{bytes: bytes, expected: expected, sha256: sha}}}
  end

  defp check_stream(error, _op), do: error

  defp spawn_reader(coordinator) do
    spawn(fn ->
      receive do
        {:take, socket, acc} ->
          case :inet.setopts(socket, active: @active_n) do
            :ok -> read_stream(coordinator, socket, acc)
            error -> send(coordinator, {:reader, :result, error})
          end
      end
    end)
  end

  defp read_stream(coordinator, socket, acc) do
    receive do
      {:tcp, ^socket, data} ->
        acc = %{
          acc
          | bytes: acc.bytes + byte_size(data),
            hash: :crypto.hash_update(acc.hash, data)
        }

        if acc.swaps > 0,
          do: hand_on(coordinator, socket, acc),
          else: read_stream(coordinator, socket, acc)

      {:tcp_passive, ^socket} ->
        case :inet.setopts(socket, active: @active_n) do
          :ok -> read_stream(coordinator, socket, acc)
          error -> send(coordinator, {:reader, :result, error})
        end

      {:tcp_closed, ^socket} ->
        :gen_tcp.close(socket)
        sha = acc.hash |> :crypto.hash_final() |> Base.encode16(case: :lower)
        send(coordinator, {:reader, :result, {:ok, acc.bytes, sha}})

      {:tcp_error, ^socket, reason} ->
        send(coordinator, {:reader, :result, {:error, reason}})

      {:swap, count} ->
        read_stream(coordinator, socket, %{acc | swaps: acc.swaps + count})
    end
  end

  # Hands the socket, and what has been read of it, to a new reader, and
  # exits abruptly: the socket must survive its old owner.
  defp hand_on(coordinator, socket, acc) do
    next = spawn_reader(coordinator)

    case :gen_tcp.controlling_process(socket, next) do
      :ok ->
        send(coordinator, {:reader, :owner, next})
        send(next, {:take, socket, %{acc | swaps: acc.swaps - 1}})
        exit(:kill)

      {:error, reason} ->
        Process.exit(next, :kill)
        send(coordinator, {:reader, :result, {:error, {:controlling_process, reason}}})
    end
  end

  # Blocked operations. Each reports :ready just before its call blocks.

  @nowhere %{inet: {10, 77, 0, 99}, inet6: {0xFD00, 0x77, 0, 0, 0, 0, 0, 0x99}}

  defp blocked(%{kind: kind} = op) when kind in [:recv, :send] do
    with {:ok, socket} <- connect(op, :silent, small_buffers(op)) do
      report(op, :ready, %{owner: self(), sockets: [socket]})

      if kind == :recv,
        do: :gen_tcp.recv(socket, 0, :infinity),
        else: :gen_tcp.send(socket, op.settings.stream_payload)
    end
  end

  defp blocked(%{kind: :accept} = op) do
    options = Network.tcp_options(op.context, :subject, op.family) ++ local_options(op)

    with {:ok, listener} <- :gen_tcp.listen(0, options) do
      report(op, :ready, %{owner: self(), sockets: [listener]})
      :gen_tcp.accept(listener, :infinity)
    end
  end

  defp blocked(%{kind: :udp} = op) do
    options = Network.udp_options(op.context, :subject, op.family) ++ local_options(op)

    with {:ok, socket} <- :gen_udp.open(0, options) do
      report(op, :ready, %{owner: self(), sockets: [socket]})
      :gen_udp.recv(socket, 0, :infinity)
    end
  end

  defp blocked(%{kind: :connect} = op) do
    options = Network.tcp_options(op.context, :subject, op.family) ++ [:binary, active: false]
    report(op, :ready, %{owner: self(), sockets: []})
    connect_nowhere(Map.fetch!(@nowhere, op.family), options)
  end

  # Nobody answers, so a connect that gives up is tried again: only
  # something else may end this one.
  defp connect_nowhere(address, options) do
    case :gen_tcp.connect(address, 9, options, :infinity) do
      {:error, reason} when reason in [:etimedout, :timeout] -> connect_nowhere(address, options)
      other -> other
    end
  end

  defp local_options(op),
    do: [:binary, active: false, ip: Network.address(op.context, :subject, op.family)]
end
