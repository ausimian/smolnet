defmodule SmolNet.IPv4TcpTest do
  use ExUnit.Case, async: false

  alias SmolNet.Inet.Tcp, as: InetTcp
  alias SmolNet.Inet6.Tcp, as: Inet6Tcp
  alias SmolNet.Test.RawIpLink

  import Bitwise, only: [band: 2, bnot: 1]

  @server4 {192, 0, 2, 1}
  @client4 {192, 0, 2, 2}
  @server6 {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client6 {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    on_exit(&stop_all_stacks/0)
  end

  test "one stack serves IPv4 and IPv6 on the same port without cross-talk" do
    {server_stack, client_stack, _link} = dual_stacks()
    port = 41_001

    {:ok, listener4} = SmolNet.open(:inet, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(listener4, endpoint4(@server4, port))
    :ok = SmolNet.listen(listener4, 2)

    {:ok, listener6} = SmolNet.open(:inet6, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(listener6, endpoint6(@server6, port))
    :ok = SmolNet.listen(listener6, 2)

    assert {:error, :invalid_address} = SmolNet.bind(listener4, endpoint6(@server6, 0))

    assert {:error, :invalid_address} =
             SmolNet.connect(listener6, endpoint4(@server4, port), :nowait)

    assert {:select, select4} = SmolNet.accept(listener4, :nowait)
    assert :ok = SmolNet.cancel(listener4, select4)

    accept4 = Task.async(fn -> SmolNet.accept(listener4, 1_000) end)
    {:ok, client4} = SmolNet.open(:inet, :stream, :tcp, stack: client_stack)
    assert :ok = SmolNet.connect(client4, endpoint4(@server4, port), 1_000)
    assert {:ok, server4} = Task.await(accept4)
    assert_receive {:test_link_egress, :client, <<4::4, _rest::bitstring>>}, 1_000

    accept6 = Task.async(fn -> SmolNet.accept(listener6, 1_000) end)
    {:ok, client6} = SmolNet.open(:inet6, :stream, :tcp, stack: client_stack)
    assert :ok = SmolNet.connect(client6, endpoint6(@server6, port), 1_000)
    assert {:ok, server6} = Task.await(accept6)
    assert_receive {:test_link_egress, :client, <<6::4, _rest::bitstring>>}, 1_000

    assert {:ok, %{family: :inet, addr: @server4, port: ^port}} = SmolNet.sockname(server4)
    assert {:ok, %{family: :inet, addr: @client4}} = SmolNet.peername(server4)
    assert {:ok, %{family: :inet6, addr: @server6, port: ^port}} = SmolNet.sockname(server6)
    assert {:ok, %{family: :inet6, addr: @client6}} = SmolNet.peername(server6)

    :ok = SmolNet.send(client4, "four", 1_000)
    :ok = SmolNet.send(client6, "six", 1_000)
    assert {:ok, "four"} = SmolNet.recv(server4, 4, 1_000)
    assert {:ok, "six"} = SmolNet.recv(server6, 3, 1_000)

    assert :ok = SmolNet.shutdown(server4, :write)
    assert {:error, :closed} = SmolNet.send(server4, "late", :nowait)
    assert :ok = SmolNet.close(server4)
    assert :ok = SmolNet.close(server6)
    assert :ok = SmolNet.close(client4)
    assert :ok = SmolNet.close(client6)
    assert :ok = SmolNet.close(listener4)
    assert :ok = SmolNet.close(listener6)
  end

  test "the IPv4 gen_tcp callback supports framing, active delivery, and ownership" do
    {server_stack, client_stack, _link} = ipv4_stacks()

    assert {:ok, listener = {:"$inet", InetTcp, _listener_pid}} =
             :gen_tcp.listen(
               0,
               server_options(server_stack, packet: 2, backlog: 2)
             )

    assert {:ok, {{0, 0, 0, 0}, port}} = :inet.sockname(listener)
    accept = accept_for_parent(listener)

    assert {:ok, client = {:"$inet", InetTcp, _client_pid}} =
             :gen_tcp.connect(
               @server4,
               port,
               client_options(client_stack, packet: 2),
               1_000
             )

    assert {:ok, server = {:"$inet", InetTcp, _server_pid}} = Task.await(accept)
    assert {:ok, {@server4, ^port}} = :inet.sockname(server)
    assert {:ok, {@client4, _client_port}} = :inet.peername(server)

    assert :ok = :gen_tcp.send(client, "framed")
    assert {:ok, "framed"} = :gen_tcp.recv(server, 0, 1_000)

    parent = self()

    receiver =
      spawn(fn ->
        receive do
          message -> send(parent, {:new_owner, message})
        end
      end)

    assert :ok = :gen_tcp.controlling_process(client, receiver)
    assert :ok = :inet.setopts(client, active: :once)
    assert :ok = :gen_tcp.send(server, "owned")
    assert_receive {:new_owner, {:tcp, ^client, "owned"}}, 1_000
    refute_receive {:tcp, ^client, _data}, 50

    assert :ok = :gen_tcp.shutdown(server, :write)
    assert {:error, :closed} = :gen_tcp.send(server, "after shutdown")
    assert :ok = :gen_tcp.close(server)
    assert :ok = :gen_tcp.close(client)
    assert :ok = :gen_tcp.close(listener)
  end

  test "dual-family wildcard listeners remain isolated in reverse creation order" do
    {server_stack, client_stack, _link} = dual_stacks()
    port = 41_002

    ipv6_server_options = [
      {:tcp_module, Inet6Tcp},
      {:smolnet_stack, server_stack},
      :inet6,
      :binary,
      {:active, false}
    ]

    assert {:ok, listener6} = :gen_tcp.listen(port, ipv6_server_options)
    assert {:ok, listener4} = :gen_tcp.listen(port, server_options(server_stack))

    accept4 = accept_for_parent(listener4)
    assert {:ok, client4} = :gen_tcp.connect(@server4, port, client_options(client_stack), 1_000)
    assert {:ok, server4} = Task.await(accept4)
    assert {:error, :timeout} = :gen_tcp.accept(listener6, 20)

    ipv6_client_options = [
      {:tcp_module, Inet6Tcp},
      {:smolnet_stack, client_stack},
      :inet6,
      :binary,
      {:active, false}
    ]

    accept6 = accept_for_parent(listener6)
    assert {:ok, client6} = :gen_tcp.connect(@server6, port, ipv6_client_options, 1_000)
    assert {:ok, server6} = Task.await(accept6)
    assert {:error, :timeout} = :gen_tcp.accept(listener4, 20)

    assert :ok = :gen_tcp.send(client4, "v4")
    assert :ok = :gen_tcp.send(client6, "v6")
    assert {:ok, "v4"} = :gen_tcp.recv(server4, 2, 1_000)
    assert {:ok, "v6"} = :gen_tcp.recv(server6, 2, 1_000)
  end

  test "an IPv4 wildcard listener accepts every configured IPv4 address" do
    alternate = {192, 0, 2, 3}
    {:ok, link} = RawIpLink.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(
        egress: {link, :server},
        addresses: [{@server4, 24}, {alternate, 24}]
      )

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client4, 24}])

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(listener, endpoint4({0, 0, 0, 0}, 41_007))
    :ok = SmolNet.listen(listener, 2)

    accept = Task.async(fn -> SmolNet.accept(listener, 1_000) end)
    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: client_stack)
    assert :ok = SmolNet.connect(client, endpoint4(alternate, 41_007), 1_000)
    assert {:ok, child} = Task.await(accept)
    assert {:ok, %{addr: ^alternate, port: 41_007}} = SmolNet.sockname(child)
  end

  test "accepted child family comes from native state" do
    {server_stack, client_stack, _link} = ipv4_stacks()
    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(listener, endpoint4({0, 0, 0, 0}, 41_005))
    :ok = SmolNet.listen(listener, 1)
    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: client_stack)
    connect = Task.async(fn -> SmolNet.connect(client, endpoint4(@server4, 41_005), 1_000) end)

    altered = %{listener | family: :inet6}
    assert {:ok, %{family: :inet} = child} = SmolNet.accept(altered, 1_000)
    assert :ok = Task.await(connect)
    assert :ok = SmolNet.send(client, "native", 1_000)
    assert {:ok, "native"} = SmolNet.recv(child, 6, 1_000)
  end

  test "IPv4 accept and connect timeouts cancel cleanly" do
    {server_stack, client_stack, link} = ipv4_stacks()
    {:ok, listener} = :gen_tcp.listen(41_003, server_options(server_stack))

    assert {:error, :timeout} = :gen_tcp.accept(listener, 10)
    :ok = RawIpLink.fault(link, :drop)

    assert {:error, :timeout} =
             :gen_tcp.connect(@server4, 41_003, client_options(client_stack), 10)

    :ok = RawIpLink.fault(link, :pass)
    accept = accept_for_parent(listener)
    assert {:ok, client} = :gen_tcp.connect(@server4, 41_003, client_options(client_stack), 1_000)
    assert {:ok, child} = Task.await(accept)
    assert :ok = :gen_tcp.send(client, "ok")
    assert {:ok, "ok"} = :gen_tcp.recv(child, 2, 1_000)
  end

  test "IPv4 ingress validates length, checksum, fragmentation, and MTU" do
    sink = spawn(fn -> sink_loop() end)

    {:ok, stack} =
      SmolNet.start_stack(
        egress: {sink, :raw4},
        mtu: 1_280,
        addresses: [{@server4, 24}],
        routes: [{{0, 0, 0, 0}, 0, {192, 0, 2, 254}}]
      )

    valid = ipv4_packet(@client4, @server4, <<>>)
    assert :ok = SmolNet.ingress(stack, valid)

    <<prefix::binary-size(8), ttl, rest::binary>> = valid

    assert {:error, :invalid_packet} =
             SmolNet.ingress(stack, <<prefix::binary, Bitwise.bxor(ttl, 1), rest::binary>>)

    <<version_and_dscp::binary-size(2), _length::16, tail::binary>> = valid

    assert {:error, :invalid_packet} =
             SmolNet.ingress(stack, <<version_and_dscp::binary, 21::16, tail::binary>>)

    fragmented = ipv4_packet(@client4, @server4, <<>>, 0x2000)
    assert {:error, :invalid_packet} = SmolNet.ingress(stack, fragmented)

    non_initial_fragment = ipv4_packet(@client4, @server4, <<>>, 1)
    assert {:error, :invalid_packet} = SmolNet.ingress(stack, non_initial_fragment)

    oversized = ipv4_packet(@client4, @server4, :binary.copy(<<0>>, 1_261))
    assert {:error, :packet_too_large} = SmolNet.ingress(stack, oversized)

    assert SmolNet.start_stack(routes: [{{0, 0, 0, 0}, 0, @server6}]) ==
             {:error, :invalid_routes}

    assert SmolNet.start_stack(addresses: [{{224, 0, 0, 1}, 24}]) ==
             {:error, :invalid_addresses}

    assert SmolNet.start_stack(addresses: [{{255, 255, 255, 255}, 32}]) ==
             {:error, :invalid_addresses}

    assert SmolNet.start_stack(routes: [{{0, 0, 0, 0}, 0, {255, 255, 255, 255}}]) ==
             {:error, :invalid_routes}
  end

  test "IPv4-mapped IPv6 endpoints are rejected explicitly" do
    {:ok, stack} = SmolNet.start_stack(addresses: [{@server6, 64}])
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    mapped = {0, 0, 0, 0, 0, 0xFFFF, 0xC000, 0x0201}

    assert {:error, :invalid_address} = SmolNet.bind(socket, endpoint6(mapped, 41_004))

    assert {:error, :invalid_address} =
             SmolNet.connect(socket, endpoint6(mapped, 41_004), :nowait)
  end

  test "IPv4 broadcast endpoints are rejected explicitly" do
    {:ok, stack} = SmolNet.start_stack(addresses: [{@server4, 24}])
    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    broadcast = endpoint4({255, 255, 255, 255}, 41_006)

    assert {:error, :invalid_address} = SmolNet.bind(socket, broadcast)
    assert {:error, :invalid_address} = SmolNet.connect(socket, broadcast, :nowait)
  end

  defp dual_stacks do
    {:ok, link} = RawIpLink.start_link(self())

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

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)
    {server_stack, client_stack, link}
  end

  defp ipv4_stacks do
    {:ok, link} = RawIpLink.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server4, 24}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client4, 24}])

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)
    {server_stack, client_stack, link}
  end

  defp accept_for_parent(listener) do
    parent = self()

    Task.async(fn ->
      with {:ok, socket} <- :gen_tcp.accept(listener, 1_000),
           :ok <- :gen_tcp.controlling_process(socket, parent) do
        {:ok, socket}
      end
    end)
  end

  defp server_options(stack, extra \\ []) do
    [
      {:tcp_module, InetTcp},
      {:smolnet_stack, stack},
      :inet,
      :binary,
      {:active, false},
      {:backlog, Keyword.get(extra, :backlog, 5)},
      {:packet, Keyword.get(extra, :packet, :raw)}
    ]
  end

  defp client_options(stack, extra \\ []) do
    [
      {:tcp_module, InetTcp},
      {:smolnet_stack, stack},
      :inet,
      :binary,
      {:active, false},
      {:packet, Keyword.get(extra, :packet, :raw)}
    ]
  end

  defp endpoint4(address, port), do: %{family: :inet, addr: address, port: port}
  defp endpoint6(address, port), do: %{family: :inet6, addr: address, port: port}

  defp ipv4_packet(source, destination, payload, flags_fragment \\ 0) do
    total_length = 20 + byte_size(payload)

    header =
      <<0x45, 0, total_length::16, 0::16, flags_fragment::16, 64, 59, 0::16,
        tuple_bytes(source)::binary, tuple_bytes(destination)::binary>>

    checksum = checksum(header)
    <<prefix::binary-size(10), _checksum::16, suffix::binary>> = header
    <<prefix::binary, checksum::16, suffix::binary, payload::binary>>
  end

  defp tuple_bytes(address), do: address |> Tuple.to_list() |> :binary.list_to_bin()

  defp checksum(binary) do
    binary
    |> :binary.bin_to_list()
    |> Enum.chunk_every(2)
    |> Enum.reduce(0, fn [high, low], sum -> sum + high * 256 + low end)
    |> fold_checksum()
    |> bnot()
    |> band(0xFFFF)
  end

  defp fold_checksum(sum) when sum > 0xFFFF,
    do: fold_checksum(band(sum, 0xFFFF) + div(sum, 0x10000))

  defp fold_checksum(sum), do: sum

  defp sink_loop do
    receive do
      _message -> sink_loop()
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
