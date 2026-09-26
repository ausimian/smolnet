defmodule SmolNet.IPv4UdpTest do
  use ExUnit.Case, async: false

  alias SmolNet.Inet.Udp, as: InetUdp
  alias SmolNet.Inet6.Udp, as: Inet6Udp
  alias SmolNet.Test.IPv6Link
  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  # Liveness budgets: bounds on how long a healthy run may take to make
  # progress, not properties under test, so they scale with the host.
  # Quiescence budgets below bound how long the suite waits to conclude that
  # nothing happened, or assert that an operation times out; their expiry is
  # the assertion, so they never scale. See `SmolNet.Test.Timing`.
  @wait_1s Timing.liveness(1_000)
  @wait_2s Timing.liveness(2_000)
  @wait_3s Timing.liveness(3_000)
  @wait_4s Timing.liveness(4_000)
  @wait_5s Timing.liveness(5_000)
  @wait_6s Timing.liveness(6_000)
  @idle_10ms Timing.quiescence(10)
  @idle_20ms Timing.quiescence(20)

  @server4 {192, 0, 2, 1}
  @client4 {192, 0, 2, 2}
  @server6 {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client6 {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    on_exit(&stop_all_stacks/0)
  end

  for link_module <- [RawIpLink, IPv6Link] do
    @link_module link_module

    test "low-level IPv4 UDP preserves boundaries, metadata, and MTU limits via #{inspect(link_module)}" do
      {server_stack, client_stack, _link} = ipv4_stacks(@link_module)
      port = 43_001

      {:ok, server} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server, endpoint4(@server4, port))
      {:ok, client} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client, endpoint4(@client4, 0))

      assert {:select, select} = SmolNet.recvfrom(server, 0, :nowait)
      assert :ok = SmolNet.cancel(server, select)
      assert {:error, :timeout} = SmolNet.recvfrom(server, 0, @idle_10ms)

      assert {:error, :invalid_address} =
               SmolNet.sendto(client, "wrong family", endpoint6(@server6, port), :nowait)

      assert {:error, :invalid_address} =
               SmolNet.sendto(client, "broadcast", endpoint4({255, 255, 255, 255}, port), :nowait)

      assert {:error, :network_unreachable} =
               SmolNet.sendto(client, "no route", endpoint4({198, 51, 100, 1}, port), :nowait)

      assert :ok = SmolNet.sendto(client, "hello", endpoint4(@server4, port), @wait_1s)

      assert {:ok,
              %{
                source: %{family: :inet, addr: @client4, port: client_port},
                destination: %{family: :inet, addr: @server4, port: ^port},
                data: "hello",
                truncated: false
              }} = SmolNet.recvfrom(server, 0, @wait_1s)

      assert client_port in 49_152..50_175

      assert {:ok, %{family: :inet, addr: @client4, port: ^client_port}} =
               SmolNet.sockname(client)

      assert :ok = SmolNet.sendto(client, "abcdef", endpoint4(@server4, port), @wait_1s)
      assert {:ok, %{data: "abc", truncated: true}} = SmolNet.recvfrom(server, 3, @wait_1s)

      assert :ok = SmolNet.sendto(client, <<>>, endpoint4(@server4, port), @wait_1s)
      assert {:ok, %{data: <<>>, truncated: false}} = SmolNet.recvfrom(server, 0, @wait_1s)

      assert {:error, :message_too_large} =
               SmolNet.sendto(
                 client,
                 :binary.copy("x", 1_473),
                 endpoint4(@server4, port),
                 :nowait
               )

      maximum = :binary.copy("m", 1_472)
      assert :ok = SmolNet.sendto(client, maximum, endpoint4(@server4, port), @wait_1s)
      assert {:ok, %{data: ^maximum, truncated: false}} = SmolNet.recvfrom(server, 0, @wait_1s)

      {:ok, info} = SmolNet.stack_info(server_stack)
      assert info.native.result.udp_ipv4_max_datagram_bytes == 1_472
      assert info.native.result.udp_packet_capacity == 16
      assert info.native.result.udp_payload_bytes == 16_384
    end

    test "IPv4 UDP checksum rules work via #{inspect(link_module)}" do
      {server_stack, client_stack, link} = ipv4_stacks(@link_module)
      port = 43_002

      {:ok, server} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server, endpoint4(@server4, port))
      {:ok, client} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client, endpoint4(@client4, 0))

      :ok = @link_module.fault(link, :drop)
      assert :ok = SmolNet.sendto(client, "checksum", endpoint4(@server4, port), @wait_1s)

      assert_receive {:test_link_egress, :client, <<4::4, 5::4, _rest::binary>> = packet},
                     @wait_1s

      <<prefix::binary-size(26), checksum::16, suffix::binary>> = packet
      refute checksum == 0
      bad_checksum = if checksum == 1, do: 2, else: 1
      corrupted = <<prefix::binary, bad_checksum::16, suffix::binary>>

      assert :ok = SmolNet.ingress(server_stack, corrupted)
      assert {:error, :timeout} = SmolNet.recvfrom(server, 0, @idle_20ms)

      zero_checksum = <<prefix::binary, 0::16, suffix::binary>>
      assert :ok = SmolNet.ingress(server_stack, zero_checksum)
      assert {:ok, %{data: "checksum"}} = SmolNet.recvfrom(server, 0, @wait_1s)

      assert :ok = SmolNet.ingress(server_stack, packet)
      assert {:ok, %{data: "checksum"}} = SmolNet.recvfrom(server, 0, @wait_1s)
    end

    test "IPv4 UDP waiters cancel, retry, and abort via #{inspect(link_module)}" do
      {server_stack, client_stack, _link} = ipv4_stacks(@link_module)
      port = 43_004

      {:ok, server} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server, endpoint4(@server4, port))
      {:ok, client} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client, endpoint4(@client4, 0))

      assert {:select, cancelled} = SmolNet.recvfrom(server, 0, :nowait)
      assert :ok = SmolNet.cancel(server, cancelled)
      refute_receive {:"$smol_socket", _, :select, _}, @idle_20ms

      assert {:select, select} = SmolNet.recvfrom(server, 0, :nowait)
      assert :ok = SmolNet.sendto(client, "once", endpoint4(@server4, port), @wait_1s)
      {:select_info, :recvfrom, reference} = select
      identity = SmolNet.Socket.identity(server)
      assert_receive {:"$smol_socket", ^identity, :select, ^reference}, @wait_1s
      assert {:ok, %{data: "once"}} = SmolNet.recvfrom(server, 0, :nowait)

      assert {:select, closing} = SmolNet.recvfrom(server, 0, :nowait)
      {:select_info, :recvfrom, closing_reference} = closing
      assert :ok = SmolNet.close(server)
      assert_receive {:"$smol_socket", ^identity, :abort, ^closing_reference, :closed}, @wait_1s
      assert {:error, :invalid_socket} = SmolNet.recvfrom(server, 0, :nowait)
    end

    test "a full IPv4 UDP ring retries one datagram via #{inspect(link_module)}" do
      {:ok, link} = @link_module.start_link(self())

      {:ok, server_stack} =
        SmolNet.start_stack(egress: {link, :server}, addresses: [{@server4, 24}])

      # Credit for one datagram: the rest wait in the socket's transmit ring
      # until the test grants more.
      {:ok, client_stack} =
        SmolNet.start_stack(
          egress: {link, :client},
          egress_credit: {1, 1_280},
          mtu: 1_280,
          addresses: [{@client4, 24}],
          limits: %{output_packets: 1}
        )

      on_exit(fn -> resume_if_alive(client_stack.stack) end)
      :ok = @link_module.connect(link, :server, client_stack)
      :ok = @link_module.connect(link, :client, server_stack)
      :ok = @link_module.fault(link, :drop)

      port = 43_005
      {:ok, client} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client, endpoint4(@client4, 0))

      :ok = :sys.suspend(client_stack.stack)

      sends =
        for sequence <- 1..18 do
          Task.async(fn ->
            payload = <<sequence::16, 0::size(998 * 8)>>

            case SmolNet.sendto(client, payload, endpoint4(@server4, port), :nowait) do
              :ok ->
                {sequence, :immediate}

              {:select, {:select_info, :sendto, reference}} ->
                receive do
                  {:"$smol_socket", identity, :select, ^reference} ->
                    assert identity == SmolNet.Socket.identity(client)
                after
                  @wait_5s -> flunk("full IPv4 UDP transmit ring never became writable")
                end

                assert :ok = SmolNet.sendto(client, payload, endpoint4(@server4, port), @wait_5s)
                {sequence, :retried}
            end
          end)
        end

      assert_eventually(fn ->
        {:message_queue_len, queued} = Process.info(client_stack.stack, :message_queue_len)
        queued >= 18
      end)

      :ok = :sys.resume(client_stack.stack)
      # Queued behind the 18 sends, so it arrives once the ring is full.
      :ok = SmolNet.grant_egress(client_stack, 17, 17 * 1_280)
      send_results = Enum.map(sends, &Task.await(&1, @wait_6s))
      transmitted = receive_egress_sequences(:client, 18, @wait_6s)

      assert Enum.count(send_results, fn {_sequence, mode} -> mode == :retried end) == 1
      assert Enum.sort(transmitted) == Enum.to_list(1..18)
      refute_receive {:test_link_egress, :client, _packet}, @idle_20ms
    end

    test "unrelated IPv4 receive preserves egress via #{inspect(link_module)}" do
      {:ok, link} = @link_module.start_link(self())

      {:ok, server_stack} =
        SmolNet.start_stack(egress: {link, :server}, addresses: [{@server4, 24}])

      {:ok, client_stack} =
        SmolNet.start_stack(
          egress: {link, :client},
          mtu: 1_280,
          addresses: [{@client4, 24}],
          limits: %{bytes_copied: 1_280, output_packets: 1}
        )

      on_exit(fn -> resume_if_alive(client_stack.stack) end)
      :ok = @link_module.connect(link, :server, client_stack)
      :ok = @link_module.connect(link, :client, server_stack)

      port = 43_008
      {:ok, server} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server, endpoint4(@server4, port))
      {:ok, sender} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(sender, endpoint4(@client4, 0))
      {:ok, idle} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(idle, endpoint4(@client4, 0))

      :ok = :sys.suspend(client_stack.stack)

      send_task =
        Task.async(fn ->
          SmolNet.sendto(sender, :binary.copy("x", 1_000), endpoint4(@server4, port), @wait_1s)
        end)

      assert_eventually(fn -> call_queued?(client_stack.stack, send_task.pid) end)
      recv_task = Task.async(fn -> SmolNet.recvfrom(idle, 0, :nowait) end)
      assert_eventually(fn -> call_queued?(client_stack.stack, recv_task.pid) end)
      :ok = :sys.resume(client_stack.stack)

      assert :ok = Task.await(send_task, @wait_2s)
      assert {:select, _select_info} = Task.await(recv_task, @wait_2s)
      assert {:ok, %{data: data}} = SmolNet.recvfrom(server, 0, @wait_2s)
      assert data == :binary.copy("x", 1_000)
    end

    test "IPv4 UDP connect preserves a TCP timer via #{inspect(link_module)}" do
      {server_stack, client_stack, link} = ipv4_stacks(@link_module)
      :ok = @link_module.fault(link, :drop)

      {:ok, tcp} = SmolNet.open(:inet, :stream, :tcp, stack: client_stack)

      assert {:select, tcp_select} =
               SmolNet.connect(tcp, endpoint4(@server4, 43_009), :nowait)

      {:ok, before} = SmolNet.stack_info(client_stack)
      assert is_integer(before.poll_at)

      {:ok, udp} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      assert :ok = SmolNet.bind(udp, endpoint4(@client4, 0))
      assert :ok = SmolNet.connect(udp, endpoint4(@server4, 43_009))
      {:ok, after_connect} = SmolNet.stack_info(client_stack)
      assert after_connect.poll_at == before.poll_at

      assert :ok = SmolNet.cancel(tcp, tcp_select)
      assert :ok = SmolNet.close(udp)
      assert :ok = SmolNet.close(tcp)
      assert {:ok, _info} = SmolNet.stack_info(server_stack)
    end

    test "connected IPv4 UDP isolates peers and families via #{inspect(link_module)}" do
      {server_stack, client_stack, _link} = ipv4_stacks(@link_module)
      port = 43_003

      {:ok, server} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server, endpoint4(@server4, port))
      {:ok, expected} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(expected, endpoint4(@client4, 0))
      {:ok, %{port: expected_port}} = SmolNet.sockname(expected)
      {:ok, other} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(other, endpoint4(@client4, 0))

      assert {:error, :invalid_address} =
               SmolNet.connect(server, endpoint6(@client6, expected_port))

      assert :ok = SmolNet.connect(server, endpoint4(@client4, expected_port))
      assert :ok = SmolNet.connect(expected, endpoint4(@server4, port))

      assert :ok = SmolNet.sendto(other, "discard", endpoint4(@server4, port), @wait_1s)
      assert :ok = SmolNet.sendto(expected, "keep", endpoint4(@server4, port), @wait_1s)

      assert {:ok, %{data: "keep", source: %{port: ^expected_port}}} =
               SmolNet.recvfrom(server, 0, @wait_1s)

      assert {:error, :invalid_socket_state} =
               SmolNet.sendto(expected, "wrong peer", endpoint4(@client4, 9), :nowait)

      assert :ok = SmolNet.sendto(expected, "still usable", endpoint4(@server4, port), @wait_1s)
      assert {:ok, %{data: "still usable"}} = SmolNet.recvfrom(server, 0, @wait_1s)
    end

    test "dual-family wildcard UDP stays isolated via #{inspect(link_module)}" do
      for order <- [:ipv4_first, :ipv6_first] do
        {server_stack, client_stack, _link} = dual_stacks(@link_module)
        port = 43_006

        {server4, server6} =
          case order do
            :ipv4_first ->
              {:ok, server4} = :gen_udp.open(port, udp_options(server_stack))
              {:ok, server6} = :gen_udp.open(port, udp6_options(server_stack))
              {server4, server6}

            :ipv6_first ->
              {:ok, server6} = :gen_udp.open(port, udp6_options(server_stack))
              {:ok, server4} = :gen_udp.open(port, udp_options(server_stack))
              {server4, server6}
          end

        {:ok, client4} = :gen_udp.open(0, udp_options(client_stack))
        {:ok, client6} = :gen_udp.open(0, udp6_options(client_stack))
        assert :ok = :gen_udp.send(client4, @server4, port, "four")
        assert :ok = :gen_udp.send(client6, @server6, port, "six")
        assert {:ok, {@client4, _port4, "four"}} = :gen_udp.recv(server4, 0, @wait_1s)
        assert {:ok, {@client6, _port6, "six"}} = :gen_udp.recv(server6, 0, @wait_1s)
        assert {:error, :timeout} = :gen_udp.recv(server4, 0, @idle_10ms)
        assert {:error, :timeout} = :gen_udp.recv(server6, 0, @idle_10ms)
      end
    end

    test "IPv4 UDP wildcard covers every address via #{inspect(link_module)}" do
      alternate = {192, 0, 2, 3}
      {:ok, link} = @link_module.start_link(self())

      {:ok, server_stack} =
        SmolNet.start_stack(
          egress: {link, :server},
          addresses: [{@server4, 24}, {alternate, 24}]
        )

      {:ok, client_stack} =
        SmolNet.start_stack(egress: {link, :client}, addresses: [{@client4, 24}])

      :ok = @link_module.connect(link, :server, client_stack)
      :ok = @link_module.connect(link, :client, server_stack)

      port = 43_007
      {:ok, server} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server, endpoint4({0, 0, 0, 0}, port))
      {:ok, client} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client, endpoint4(@client4, 0))

      assert :ok = SmolNet.sendto(client, "alternate", endpoint4(alternate, port), @wait_1s)

      for sequence <- 1..16 do
        assert :ok =
                 SmolNet.sendto(
                   client,
                   <<sequence::16>>,
                   endpoint4(@server4, port),
                   @wait_1s
                 )
      end

      assert {:ok, %{destination: %{addr: @server4}, data: <<1::16>>}} =
               SmolNet.recvfrom(server, 0, @wait_1s)

      assert {:ok, %{destination: %{addr: ^alternate}, data: "alternate"}} =
               SmolNet.recvfrom(server, 0, @wait_1s)

      assert Enum.map(2..16, fn sequence ->
               assert {:ok, %{destination: %{addr: @server4}, data: <<^sequence::16>>}} =
                        SmolNet.recvfrom(server, 0, @wait_1s)

               sequence
             end) == Enum.to_list(2..16)
    end

    test "UDP wildcard sends choose the source nearest each destination via #{inspect(link_module)}" do
      alternate_server4 = {198, 51, 100, 1}
      alternate_client4 = {198, 51, 100, 2}
      alternate_server6 = {0xFD01, 0, 0, 0, 0, 0, 0, 1}
      alternate_client6 = {0xFD01, 0, 0, 0, 0, 0, 0, 2}
      {:ok, link} = @link_module.start_link(self())

      {:ok, server_stack} =
        SmolNet.start_stack(
          egress: {link, :server},
          addresses: [
            {@server4, 24},
            {alternate_server4, 24},
            {@server6, 64},
            {alternate_server6, 64}
          ]
        )

      {:ok, client_stack} =
        SmolNet.start_stack(
          egress: {link, :client},
          addresses: [
            {@client4, 24},
            {alternate_client4, 24},
            {@client6, 64},
            {alternate_client6, 64}
          ]
        )

      :ok = @link_module.connect(link, :server, client_stack)
      :ok = @link_module.connect(link, :client, server_stack)

      port = 43_008
      {:ok, server4} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server4, endpoint4({0, 0, 0, 0}, port))
      {:ok, client4} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client4, endpoint4({0, 0, 0, 0}, 0))

      {:ok, server6} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
      :ok = SmolNet.bind(server6, endpoint6({0, 0, 0, 0, 0, 0, 0, 0}, port))
      {:ok, client6} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
      :ok = SmolNet.bind(client6, endpoint6({0, 0, 0, 0, 0, 0, 0, 0}, 0))

      assert :ok = SmolNet.sendto(client4, "v4 primary", endpoint4(@server4, port), @wait_1s)

      assert {:ok, %{source: %{addr: @client4}, data: "v4 primary"}} =
               SmolNet.recvfrom(server4, 0, @wait_1s)

      assert :ok =
               SmolNet.sendto(
                 client4,
                 "v4 alternate",
                 endpoint4(alternate_server4, port),
                 @wait_1s
               )

      assert {:ok, %{source: %{addr: ^alternate_client4}, data: "v4 alternate"}} =
               SmolNet.recvfrom(server4, 0, @wait_1s)

      assert :ok = SmolNet.sendto(client6, "v6 primary", endpoint6(@server6, port), @wait_1s)

      assert {:ok, %{source: %{addr: @client6}, data: "v6 primary"}} =
               SmolNet.recvfrom(server6, 0, @wait_1s)

      assert :ok =
               SmolNet.sendto(
                 client6,
                 "v6 alternate",
                 endpoint6(alternate_server6, port),
                 @wait_1s
               )

      assert {:ok, %{source: %{addr: ^alternate_client6}, data: "v6 alternate"}} =
               SmolNet.recvfrom(server6, 0, @wait_1s)
    end

    test "IPv4 gen_udp callback contract works via #{inspect(link_module)}" do
      {server_stack, client_stack, _link} = ipv4_stacks(@link_module)

      assert InetUdp.family() == :inet

      assert {:ok, server = {:"$inet", InetUdp, _server_pid}} =
               :gen_udp.open(0, udp_options(server_stack))

      assert {:ok, {{0, 0, 0, 0}, server_port}} = :inet.sockname(server)

      assert {:ok, client = {:"$inet", InetUdp, _client_pid}} =
               :gen_udp.open(0, udp_options(client_stack))

      assert :ok = :gen_udp.send(client, @server4, server_port, "address")
      assert {:ok, {@client4, client_port, "address"}} = :gen_udp.recv(server, 0, @wait_1s)

      assert :ok = :gen_udp.send(client, {@server4, server_port}, "destination")
      assert {:ok, {@client4, ^client_port, "destination"}} = :gen_udp.recv(server, 0, @wait_1s)

      sockaddr = %{family: :inet, addr: @server4, port: server_port}
      assert :ok = :gen_udp.send(client, sockaddr, [], "sockaddr")
      assert {:ok, {@client4, ^client_port, "sockaddr"}} = :gen_udp.recv(server, 0, @wait_1s)

      assert :ok = :gen_udp.send(client, {@server4, server_port}, 0, "legacy")
      assert {:ok, {@client4, ^client_port, "legacy"}} = :gen_udp.recv(server, 0, @wait_1s)

      assert :ok = :gen_udp.send(client, @server4, server_port, [], "ancillary-free")

      assert {:ok, {@client4, ^client_port, "ancillary-free"}} =
               :gen_udp.recv(server, 0, @wait_1s)

      assert {:error, :einval} = :gen_udp.send(client, %{}, server_port, "bad host")
      assert {:error, :einval} = :gen_udp.send(client, @server4, %{}, "bad port")
      assert {:error, :einval} = InetUdp.connect(client, %{}, server_port)
      assert is_map(InetUdp.info(client))

      assert {:error, :eafnosupport} =
               :gen_udp.send(
                 client,
                 %{family: :inet6, addr: @server6, port: server_port},
                 [],
                 "wrong family"
               )

      assert :ok = :gen_udp.connect(client, @server4, server_port)
      assert :ok = :gen_udp.send(client, "connected")
      assert {:ok, {@client4, ^client_port, "connected"}} = :gen_udp.recv(server, 0, @wait_1s)

      assert :ok = :inet.setopts(server, active: 2)
      assert :ok = :gen_udp.send(client, "counted one")
      assert :ok = :gen_udp.send(client, "counted two")
      assert_receive {:udp, ^server, @client4, ^client_port, "counted one"}, @wait_1s
      assert_receive {:udp, ^server, @client4, ^client_port, "counted two"}, @wait_1s
      assert_receive {:udp_passive, ^server}, @wait_1s

      assert :ok = :inet.setopts(server, active: true)

      for sequence <- 1..20 do
        assert :ok = :gen_udp.send(client, <<sequence>>)
      end

      assert Enum.sort(receive_active(server, 20, [])) == Enum.map(1..20, &<<&1>>)
      assert :ok = :inet.setopts(server, active: false)

      assert {:ok, [active: false, mode: :binary, buffer: 65_536, recbuf: 65_536]} =
               :inet.getopts(server, [:active, :mode, :buffer, :recbuf])

      assert {:error, :einval} = :inet.setopts(server, packet: 2)
      assert {:error, :einval} = :inet.setopts(server, send_timeout: 10)
      assert {:error, :einval} = :inet.setopts(server, ipv6_v6only: true)
      assert {:error, :einval} = :inet.getopts(server, [:packet])

      assert :ok = :gen_udp.send(client, "after errors")
      assert {:ok, {@client4, ^client_port, "after errors"}} = :gen_udp.recv(server, 0, @wait_1s)
      assert :ok = :gen_udp.close(server)
      assert :ok = :gen_udp.close(client)

      assert {:error, :einval} = :gen_udp.open(0, udp_options(server_stack, packet: 2))

      assert {:error, :eafnosupport} =
               InetUdp.open(0, [
                 :inet6,
                 {:smolnet_stack, server_stack},
                 :binary,
                 {:active, false}
               ])
    end

    test "IPv4 UDP ownership transfer works via #{inspect(link_module)}" do
      {server_stack, client_stack, _link} = ipv4_stacks(@link_module)
      parent = self()

      {:ok, server} = :gen_udp.open(0, udp_options(server_stack))
      {:ok, {_any, server_port}} = :inet.sockname(server)
      {:ok, client} = :gen_udp.open(0, udp_options(client_stack))
      assert :ok = :inet.setopts(server, active: true)

      assert :ok = :gen_udp.send(client, @server4, server_port, "queued")
      assert_eventually(fn -> socket_message_queued?(server) end)

      new_owner = spawn(fn -> forward_to_test(parent) end)
      assert :ok = :gen_udp.controlling_process(server, new_owner)
      assert_receive {:new_owner, {:udp, ^server, @client4, _port, "queued"}}, @wait_1s

      assert :ok = :gen_udp.send(client, @server4, server_port, "future")
      assert_receive {:new_owner, {:udp, ^server, @client4, _port, "future"}}, @wait_1s
      refute_receive {:udp, ^server, _address, _port, _packet}, @idle_20ms

      send(new_owner, :stop)
      assert_eventually(fn -> InetUdp.info(server) == {:error, :closed} end)
      assert :ok = :gen_udp.close(client)
    end

    test "IPv4 active once truncates one datagram via #{inspect(link_module)}" do
      {server_stack, client_stack, _link} = ipv4_stacks(@link_module)
      {:ok, server} = :gen_udp.open(0, udp_options(server_stack, buffer: 3))
      {:ok, {_any, server_port}} = :inet.sockname(server)
      {:ok, client} = :gen_udp.open(0, udp_options(client_stack))
      {:ok, {_any, client_port}} = :inet.sockname(client)

      assert :ok = :inet.setopts(server, active: :once)
      assert :ok = :gen_udp.send(client, @server4, server_port, "abcdef")
      assert :ok = :gen_udp.send(client, @server4, server_port, "ghijkl")
      assert_receive {:udp, ^server, @client4, ^client_port, "abc"}, @wait_1s
      refute_receive {:udp, ^server, @client4, ^client_port, "ghi"}, @idle_20ms
      assert {:ok, {@client4, ^client_port, "ghi"}} = :gen_udp.recv(server, 0, @wait_1s)
      assert {:error, :timeout} = :gen_udp.recv(server, 0, @idle_10ms)
    end
  end

  for link_module <- [RawIpLink, IPv6Link] do
    @link_module link_module

    test "complete dual-family TCP/UDP matrix uses #{inspect(link_module)} without cross-talk" do
      exercise_complete_matrix(@link_module)
    end
  end

  defp exercise_complete_matrix(link_module) do
    {server_stack, client_stack, link} = dual_stacks(link_module)
    port = 43_010

    {:ok, tcp_listener4} = SmolNet.open(:inet, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(tcp_listener4, endpoint4(@server4, port))
    :ok = SmolNet.listen(tcp_listener4, 2)
    {:ok, tcp_listener6} = SmolNet.open(:inet6, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(tcp_listener6, endpoint6(@server6, port))
    :ok = SmolNet.listen(tcp_listener6, 2)

    accept4 = Task.async(fn -> SmolNet.accept(tcp_listener4, @wait_2s) end)
    accept6 = Task.async(fn -> SmolNet.accept(tcp_listener6, @wait_2s) end)
    {:ok, tcp_client4} = SmolNet.open(:inet, :stream, :tcp, stack: client_stack)
    {:ok, tcp_client6} = SmolNet.open(:inet6, :stream, :tcp, stack: client_stack)
    assert :ok = SmolNet.connect(tcp_client4, endpoint4(@server4, port), @wait_2s)
    assert :ok = SmolNet.connect(tcp_client6, endpoint6(@server6, port), @wait_2s)
    assert {:ok, tcp_server4} = Task.await(accept4, @wait_3s)
    assert {:ok, tcp_server6} = Task.await(accept6, @wait_3s)

    {:ok, udp_server4} = SmolNet.open(:inet, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(udp_server4, endpoint4(@server4, port))
    {:ok, udp_server6} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(udp_server6, endpoint6(@server6, port))
    {:ok, udp_client4} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(udp_client4, endpoint4(@client4, 0))
    {:ok, udp_client6} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(udp_client6, endpoint6(@client6, 0))

    assert {:error, :invalid_socket_state} =
             SmolNet.sendto(tcp_client4, "udp", endpoint4(@server4, port), :nowait)

    assert {:error, :invalid_socket_state} = SmolNet.send(udp_client4, "tcp", :nowait)

    assert {:error, :invalid_address} =
             SmolNet.sendto(udp_client4, "six", endpoint6(@server6, port), :nowait)

    tcp4_payload = :binary.copy("4", 512)
    tcp6_payload = :binary.copy("6", 512)
    tcp_read4 = Task.async(fn -> SmolNet.recv(tcp_server4, 512, @wait_3s) end)
    tcp_read6 = Task.async(fn -> SmolNet.recv(tcp_server6, 512, @wait_3s) end)

    udp_read4 = Task.async(fn -> receive_datagrams(udp_server4, 20, []) end)
    udp_read6 = Task.async(fn -> receive_datagrams(udp_server6, 20, []) end)

    tcp_send4 = Task.async(fn -> SmolNet.send(tcp_client4, tcp4_payload, @wait_3s) end)
    tcp_send6 = Task.async(fn -> SmolNet.send(tcp_client6, tcp6_payload, @wait_3s) end)

    for sequence <- 1..20 do
      assert :ok =
               SmolNet.sendto(udp_client4, <<sequence::16>>, endpoint4(@server4, port), @wait_3s)

      assert :ok =
               SmolNet.sendto(udp_client6, <<sequence::16>>, endpoint6(@server6, port), @wait_3s)
    end

    assert :ok = Task.await(tcp_send4, @wait_4s)
    assert :ok = Task.await(tcp_send6, @wait_4s)
    assert {:ok, ^tcp4_payload} = Task.await(tcp_read4, @wait_4s)
    assert {:ok, ^tcp6_payload} = Task.await(tcp_read6, @wait_4s)
    assert Enum.sort(Task.await(udp_read4, @wait_4s)) == Enum.to_list(1..20)
    assert Enum.sort(Task.await(udp_read6, @wait_4s)) == Enum.to_list(1..20)

    assert :ok = SmolNet.sendto(udp_client4, "usable", endpoint4(@server4, port), @wait_1s)
    assert {:ok, %{data: "usable"}} = SmolNet.recvfrom(udp_server4, 0, @wait_1s)

    flush_link_egress()
    assert :ok = link_module.fault(link, :drop)
    assert :ok = SmolNet.sendto(udp_client6, "checksum", endpoint6(@server6, port), @wait_1s)
    assert_receive {:test_link_egress, :client, <<6::4, _rest::bitstring>> = packet}, @wait_1s
    <<prefix::binary-size(46), checksum::16, suffix::binary>> = packet
    bad_checksum = if checksum == 1, do: 2, else: 1

    assert :ok =
             SmolNet.ingress(server_stack, <<prefix::binary, bad_checksum::16, suffix::binary>>)

    assert {:error, :timeout} = SmolNet.recvfrom(udp_server6, 0, @idle_20ms)
    assert :ok = SmolNet.ingress(server_stack, packet)
    assert {:ok, %{data: "checksum"}} = SmolNet.recvfrom(udp_server6, 0, @wait_1s)
    assert :ok = link_module.fault(link, :pass)

    for stack <- [server_stack, client_stack] do
      {:ok, info} = SmolNet.stack_info(stack)
      native = info.native.result
      assert native.counters.max_bytes_copied <= native.limits.bytes_copied
      assert native.counters.max_output_packets <= native.limits.output_packets
      assert native.counters.max_ready_events <= native.limits.ready_events
      assert native.counters.max_maintenance_work <= native.limits.maintenance_work
      assert native.ready_count <= native.limits.ready_events
    end
  end

  defp receive_datagrams(_socket, 0, sequences), do: sequences

  defp receive_datagrams(socket, remaining, sequences) do
    {:ok, %{data: <<sequence::16>>, truncated: false}} = SmolNet.recvfrom(socket, 0, @wait_3s)
    receive_datagrams(socket, remaining - 1, [sequence | sequences])
  end

  defp receive_active(_socket, 0, packets), do: packets

  defp receive_active(socket, remaining, packets) do
    receive do
      {:udp, ^socket, @client4, _port, packet} ->
        receive_active(socket, remaining - 1, [packet | packets])
    after
      @wait_1s -> flunk("did not receive every active IPv4 datagram")
    end
  end

  defp ipv4_stacks(link_module) do
    {:ok, link} = link_module.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server4, 24}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client4, 24}])

    :ok = link_module.connect(link, :server, client_stack)
    :ok = link_module.connect(link, :client, server_stack)
    {server_stack, client_stack, link}
  end

  defp dual_stacks(link_module) do
    {:ok, link} = link_module.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(
        egress: {link, :server},
        addresses: [{@server4, 24}, {@server6, 64}]
      )

    {:ok, client_stack} =
      SmolNet.start_stack(
        egress: {link, :client},
        addresses: [{@client4, 24}, {@client6, 64}]
      )

    :ok = link_module.connect(link, :server, client_stack)
    :ok = link_module.connect(link, :client, server_stack)
    {server_stack, client_stack, link}
  end

  defp udp_options(stack, extra \\ []) do
    [
      {:udp_module, InetUdp},
      {:smolnet_stack, stack},
      :inet,
      :binary,
      {:active, false}
      | extra
    ]
  end

  defp udp6_options(stack, extra \\ []) do
    [
      {:udp_module, Inet6Udp},
      {:smolnet_stack, stack},
      :inet6,
      :binary,
      {:active, false}
      | extra
    ]
  end

  defp endpoint4(address, port), do: %{family: :inet, addr: address, port: port}
  defp endpoint6(address, port), do: %{family: :inet6, addr: address, port: port}

  defp socket_message_queued?(socket) do
    {:messages, messages} = Process.info(self(), :messages)

    Enum.any?(messages, fn
      {:udp, ^socket, _address, _port, _packet} -> true
      _message -> false
    end)
  end

  defp receive_egress_sequences(link_ref, count, timeout) do
    Enum.map(1..count, fn _index ->
      packet =
        receive do
          {:test_link_egress, ^link_ref, packet} -> packet
        after
          timeout -> flunk("timed out collecting IPv4 UDP egress")
        end

      payload = binary_part(packet, byte_size(packet) - 1_000, 1_000)
      <<sequence::16, _padding::binary>> = payload
      sequence
    end)
  end

  defp call_queued?(stack, caller) do
    {:messages, messages} = Process.info(stack, :messages)

    Enum.any?(messages, fn
      {:"$gen_call", {^caller, _tag}, _request} -> true
      _message -> false
    end)
  end

  defp flush_link_egress do
    receive do
      {:test_link_egress, _link_ref, _packet} -> flush_link_egress()
    after
      0 -> :ok
    end
  end

  defp forward_to_test(test) do
    receive do
      :stop ->
        :ok

      message ->
        send(test, {:new_owner, message})
        forward_to_test(test)
    end
  end

  defp resume_if_alive(stack) do
    :sys.resume(stack)
  catch
    :exit, _reason -> :ok
  end

  defp assert_eventually(check, timeout \\ @wait_1s) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(check, deadline)
  end

  defp do_assert_eventually(check, deadline) do
    if check.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true")
      else
        Process.sleep(5)
        do_assert_eventually(check, deadline)
      end
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
