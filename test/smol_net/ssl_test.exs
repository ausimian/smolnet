defmodule SmolNet.SslTest do
  # `:ssl` over SmolNet, with `SmolNet.Inet.Tcp` and `SmolNet.Inet6.Tcp` as
  # its `cb_info` transport, and the transport callbacks `:ssl` relies on.
  use ExUnit.Case, async: false

  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @server4 {192, 0, 2, 1}
  @client4 {192, 0, 2, 2}
  @server6 {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client6 {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  # Liveness budgets: how long a healthy run may take, never the property
  # under test.
  @wait_30s Timing.liveness(30_000)

  # Several TLS records each way, and more than one receive window.
  @payload_bytes 200_000

  setup_all do
    {:ok, _apps} = Application.ensure_all_started(:ssl)

    key = [key: {:namedCurve, :secp256r1}, digest: :sha256]
    chain = %{root: key, intermediates: [], peer: key}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

    %{
      server_tls: [cert: server[:cert], key: server[:key], active: false, mode: :binary],
      client_tls: [
        verify: :verify_peer,
        cacerts: client[:cacerts],
        server_name_indication: :disable,
        active: false,
        mode: :binary
      ]
    }
  end

  setup do
    on_exit(&stop_all_stacks/0)
    {server_stack, client_stack} = linked_stacks()
    %{server_stack: server_stack, client_stack: client_stack}
  end

  for family <- [:inet, :inet6] do
    describe "#{family} as :ssl's cb_info" do
      @describetag family: family

      test ":ssl.listen/2, transport_accept/2 and handshake/2 serve :ssl.connect/4", context do
        %{family: family, server_tls: server_tls, client_tls: client_tls} = context

        listen_options =
          [family, cb_info: cb_info(family), smolnet_stack: context.server_stack] ++
            [ip: server(family)] ++ server_tls

        assert {:ok, listener} = :ssl.listen(0, listen_options)
        assert {:ok, {address, port}} = :ssl.sockname(listener)
        assert address == server(family)

        server =
          Task.async(fn ->
            {:ok, transport} = :ssl.transport_accept(listener, @wait_30s)
            {:ok, tls} = :ssl.handshake(transport, @wait_30s)
            echo(tls, :active)
          end)

        connect_options =
          [family, cb_info: cb_info(family), smolnet_stack: context.client_stack] ++ client_tls

        assert {:ok, client} = :ssl.connect(server(family), port, connect_options, @wait_30s)
        assert {:ok, [protocol: :"tlsv1.3"]} = :ssl.connection_information(client, [:protocol])
        assert {:ok, {^address, ^port}} = :ssl.peername(client)

        assert_echo(client, :passive)
        assert :ok = Task.await(server, @wait_30s)
        assert :ok = :ssl.close(listener)
      end

      test ":ssl.handshake/3 and :ssl.connect/3 upgrade connected SmolNet sockets", context do
        %{family: family, server_tls: server_tls, client_tls: client_tls} = context
        module = transport(family)

        assert {:ok, listener} =
                 :gen_tcp.listen(0, tcp_options(family, context.server_stack, ip: server(family)))

        assert {:ok, port} = :inet.port(listener)

        server =
          Task.async(fn ->
            {:ok, socket} = :gen_tcp.accept(listener, @wait_30s)
            options = [cb_info: cb_info(family)] ++ server_tls
            {:ok, tls} = :ssl.handshake(socket, options, @wait_30s)
            echo(tls, :passive)
          end)

        assert {:ok, socket} =
                 :gen_tcp.connect(
                   server(family),
                   port,
                   tcp_options(family, context.client_stack),
                   @wait_30s
                 )

        assert {:ok, ^port} = module.port(listener)

        options = [cb_info: cb_info(family)] ++ client_tls
        assert {:ok, client} = :ssl.connect(socket, options, @wait_30s)

        assert_echo(client, :active)
        assert :ok = Task.await(server, @wait_30s)
        assert :ok = :gen_tcp.close(listener)
      end

      test "connect, listen, setopts and getopts take the :header :ssl sets", context do
        %{family: family} = context
        module = transport(family)

        assert {:ok, listener} =
                 :gen_tcp.listen(0, tcp_options(family, context.server_stack, header: 0))

        assert {:ok, [header: 0]} = :inet.getopts(listener, [:header])
        assert {:ok, port} = module.port(listener)
        test_pid = self()
        accept = Task.async(fn -> accept_for(listener, test_pid) end)

        assert {:ok, client} =
                 :gen_tcp.connect(
                   server(family),
                   port,
                   tcp_options(family, context.client_stack, header: 0),
                   @wait_30s
                 )

        assert {:ok, accepted} = Task.await(accept, @wait_30s)
        assert {:ok, ^port} = module.port(accepted)
        assert {:ok, client_port} = module.port(client)
        assert {:ok, {_address, ^client_port}} = :inet.sockname(client)

        assert :ok = :inet.setopts(client, header: 0, active: false)

        assert {:ok, [packet: :raw, header: 0, mode: :binary]} =
                 :inet.getopts(client, [:packet, :header, :mode])

        # Only the default, 0, is supported.
        assert {:error, :einval} = :inet.setopts(client, header: 2)
        assert {:error, :einval} = :inet.setopts(listener, header: 1)

        assert {:error, :einval} =
                 :gen_tcp.listen(0, tcp_options(family, context.server_stack, header: 1))

        assert :ok = :gen_tcp.send(client, "still usable")
        assert {:ok, "still usable"} = :gen_tcp.recv(accepted, 12, @wait_30s)
        assert :ok = :gen_tcp.close(client)
        assert {:error, :closed} = module.port(client)
      end
    end
  end

  describe "monitor/1" do
    test "a DOWN of type :socket arrives when the socket is closed", context do
      listener = listener(context)
      ref = :inet.monitor(listener)
      assert is_reference(ref)

      assert :ok = :gen_tcp.close(listener)
      assert_receive {:DOWN, ^ref, :socket, ^listener, :closed}, @wait_30s

      # The monitor is gone once it has triggered.
      refute :inet.cancel_monitor(ref)
    end

    test "a socket that is already closed triggers the monitor at once with :nosock",
         context do
      listener = listener(context)
      assert :ok = :gen_tcp.close(listener)

      ref = SmolNet.Inet.Tcp.monitor(listener)
      assert_receive {:DOWN, ^ref, :socket, ^listener, :nosock}, @wait_30s
    end

    test "a cancelled monitor sends no DOWN", context do
      listener = listener(context)
      cancelled = :inet.monitor(listener)
      kept = :inet.monitor(listener)
      refute cancelled == kept

      assert :inet.cancel_monitor(cancelled)
      refute :inet.cancel_monitor(cancelled)

      assert :ok = :gen_tcp.close(listener)
      assert_receive {:DOWN, ^kept, :socket, ^listener, :closed}, @wait_30s
      refute_received {:DOWN, ^cancelled, _type, _object, _info}
    end

    test "the socket's owner exiting, or its process being killed, triggers the monitor",
         context do
      test_pid = self()

      owner =
        spawn(fn ->
          send(test_pid, {:listening, listener(context)})
          Process.sleep(:infinity)
        end)

      assert_receive {:listening, owned}, @wait_30s
      owned_ref = :inet.monitor(owned)
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^owned_ref, :socket, ^owned, :closed}, @wait_30s

      {:"$inet", _module, pid} = killed = listener(context)
      killed_ref = :inet.monitor(killed)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^killed_ref, :socket, ^killed, :closed}, @wait_30s
    end

    test "any process may monitor, and only it may cancel", context do
      listener = listener(context)
      test_pid = self()

      watcher =
        Task.async(fn ->
          ref = :inet.monitor(listener)
          send(test_pid, {:monitoring, ref})

          receive do
            {:DOWN, ^ref, _type, _object, _info} = down -> down
          end
        end)

      assert_receive {:monitoring, ref}, @wait_30s
      refute SmolNet.Inet.Tcp.cancel_monitor(ref)

      assert :ok = :gen_tcp.close(listener)
      assert {:DOWN, ^ref, :socket, ^listener, :closed} = Task.await(watcher, @wait_30s)
    end

    test "a monitor ends with the process that set it", context do
      listener = listener(context)
      test_pid = self()

      watcher =
        spawn(fn ->
          send(test_pid, {:monitoring, :inet.monitor(listener)})
          Process.sleep(:infinity)
        end)

      assert_receive {:monitoring, ref}, @wait_30s
      [{^ref, relay, ^watcher}] = :ets.lookup(SmolNet.InetBackend.Monitor, ref)
      relay_monitor = Process.monitor(relay)

      Process.exit(watcher, :kill)
      assert_receive {:DOWN, ^relay_monitor, :process, ^relay, :normal}, @wait_30s
      assert [] = :ets.lookup(SmolNet.InetBackend.Monitor, ref)
    end

    test "a term that is not a SmolNet socket is rejected" do
      assert_raise ArgumentError, fn -> SmolNet.Inet6.Tcp.monitor(:not_a_socket) end
      assert_raise ArgumentError, fn -> SmolNet.Inet6.Tcp.cancel_monitor(:not_a_ref) end
      refute SmolNet.Inet6.Tcp.cancel_monitor(make_ref())
    end
  end

  # Echoes one payload back to the client, then waits for the client to
  # close.
  defp echo(tls, mode) do
    {:ok, data} = receive_bytes(tls, @payload_bytes, mode, [])
    :ok = :ssl.send(tls, data)
    :ok = await_close(tls, mode)
    :ssl.close(tls)
  end

  defp assert_echo(client, mode) do
    payload = :crypto.strong_rand_bytes(@payload_bytes)
    assert :ok = :ssl.send(client, payload)
    assert {:ok, ^payload} = receive_bytes(client, @payload_bytes, mode, [])
    assert :ok = :ssl.close(client)
  end

  defp receive_bytes(_tls, 0, _mode, acc),
    do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}

  defp receive_bytes(tls, remaining, :passive, acc) do
    with {:ok, data} <- :ssl.recv(tls, 0, @wait_30s) do
      receive_bytes(tls, remaining - byte_size(data), :passive, [data | acc])
    end
  end

  defp receive_bytes(tls, remaining, :active, acc) do
    :ok = :ssl.setopts(tls, active: :once)

    receive do
      {:ssl, ^tls, data} -> receive_bytes(tls, remaining - byte_size(data), :active, [data | acc])
      {:ssl_closed, ^tls} -> {:error, :closed}
      {:ssl_error, ^tls, reason} -> {:error, reason}
    after
      @wait_30s -> {:error, :timeout}
    end
  end

  defp await_close(tls, :passive) do
    case :ssl.recv(tls, 0, @wait_30s) do
      {:error, :closed} -> :ok
      other -> {:unexpected, other}
    end
  end

  defp await_close(tls, :active) do
    :ok = :ssl.setopts(tls, active: :once)

    receive do
      {:ssl_closed, ^tls} -> :ok
    after
      @wait_30s -> {:error, :timeout}
    end
  end

  defp accept_for(listener, owner) do
    with {:ok, socket} <- :gen_tcp.accept(listener, @wait_30s),
         :ok <- :gen_tcp.controlling_process(socket, owner) do
      {:ok, socket}
    end
  end

  defp listener(context) do
    {:ok, listener} = :gen_tcp.listen(0, tcp_options(:inet, context.server_stack))
    listener
  end

  defp tcp_options(family, stack, extra \\ []) do
    [{:tcp_module, transport(family)}, {:smolnet_stack, stack}, family, :binary, active: false] ++
      extra
  end

  defp cb_info(family), do: {transport(family), :tcp, :tcp_closed, :tcp_error}

  defp transport(:inet), do: SmolNet.Inet.Tcp
  defp transport(:inet6), do: SmolNet.Inet6.Tcp

  defp server(:inet), do: @server4
  defp server(:inet6), do: @server6

  defp linked_stacks do
    # The link reports every packet it carries; nothing here reads them.
    sink = spawn_link(fn -> discard() end)
    {:ok, link} = RawIpLink.start_link(sink)

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server4, 24}, {@server6, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client4, 24}, {@client6, 64}])

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)
    {server_stack, client_stack}
  end

  defp discard do
    receive do
      _message -> discard()
    end
  end

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
