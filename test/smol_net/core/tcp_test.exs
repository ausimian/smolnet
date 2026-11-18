defmodule SmolNet.Core.TcpTest do
  use ExUnit.Case, async: true

  # The Elixir core's TCP sockets driven segment by segment, for what the
  # black-box suite cannot set up: a peer that reuses a 4-tuple.

  require SmolNet.Core.Wire.Ipv4
  require SmolNet.Core.Wire.Tcp

  alias SmolNet.Conformance.Driver
  alias SmolNet.Core.Iface
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Tcp

  @elixir SmolNet.Stack.Backend.Elixir
  @server <<192, 0, 2, 1>>
  @client <<192, 0, 2, 2>>

  test "a new connection from a dead child's 4-tuple survives the child's close" do
    {:ok, driver} = Driver.start(:server, @elixir, [addresses: [{{192, 0, 2, 1}, 24}]], 0)
    {{:ok, listener}, driver} = Driver.call(driver, :tcp_open, [:inet, 4_096, 4_096], 0)
    endpoint = %{address: [192, 0, 2, 1], port: 80, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_bind, [listener, endpoint], 0)
    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_listen, [listener, 2], 0)

    {first, driver} = handshake(driver, 100, 1)
    {{:ok, {:ok, first_child, :inet}}, driver} = accept(driver, listener, 2)

    # The first connection is reset, and the peer connects again from the
    # same address and port.
    {_emitted, driver} = ingress(driver, segment(:rst, 101, nil, <<>>), 3)
    {second, driver} = handshake(driver, 5_000, 4)

    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_close, [first_child], 5)
    {{:ok, {:ok, second_child, :inet}}, driver} = accept(driver, listener, 6)

    # The second connection still takes its data, without a RST.
    {emitted, driver} = ingress(driver, segment(:psh, 5_001, second + 1, "hello"), 7)
    refute Enum.any?(emitted, &(control(&1) == :rst))
    {ref, driver} = Driver.ref(driver)

    assert {{:ok, {:ok, "hello"}}, _driver} =
             Driver.call(driver, :tcp_recv, [second_child, 0, ref], 8)

    assert first != second
  end

  test "a handshake a RST ends makes way for the next SYN in the same batch" do
    options = [addresses: [{{192, 0, 2, 1}, 24}], limits: %{input_packets: 3}]
    {:ok, driver} = Driver.start(:server, @elixir, options, 0)
    {{:ok, listener}, driver} = Driver.call(driver, :tcp_open, [:inet, 4_096, 4_096], 0)
    endpoint = %{address: [192, 0, 2, 1], port: 80, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_bind, [listener, endpoint], 0)
    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_listen, [listener, 2], 0)

    batch =
      for segment <- [
            segment(:syn, 100, nil, <<>>),
            segment(:rst, 101, nil, <<>>),
            segment(:syn, 300, nil, <<>>)
          ],
          do: Iface.tcp_packet(@client, @server, segment)

    {{:ok, 3}, driver} = Driver.call(driver, :stack_ingress_batch, [batch], 1)
    emitted = driver |> Driver.trace() |> List.last() |> Map.fetch!(:emitted)

    # The SYN-ACKs go out after the batch: the first handshake is over by
    # then, and only the second one's is sent.
    assert [{:syn, 301}] =
             for(packet <- emitted, do: {control(packet), Tcp.tcp(parse(packet), :ack)})

    assert {{:ok, :ok}, _driver} = Driver.call(driver, :stack_poll, [], 1_000)
  end

  test "a wildcard UDP socket's rings count against a TCP socket's room" do
    addresses = [{{192, 0, 2, 1}, 24}, {{198, 51, 100, 1}, 24}]
    options = [addresses: addresses, limits: %{sockets: 2}]
    {:ok, driver} = Driver.start(:server, @elixir, options, 0)
    {{:ok, udp}, driver} = Driver.call(driver, :udp_open, [:inet], 0)
    wildcard = %{address: [0, 0, 0, 0], port: 53, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :udp_bind, [udp, wildcard], 0)

    assert {{:error, :system_limit}, _driver} =
             Driver.call(driver, :tcp_open, [:inet, 4_096, 4_096], 0)
  end

  # A handshake from the client's port 5000 with sequence number `seq`;
  # returns the server's ISN.
  defp handshake(driver, seq, at) do
    {[syn_ack], driver} = ingress(driver, segment(:syn, seq, nil, <<>>), at)
    Tcp.tcp(seq: isn, ack: ack) = parse(syn_ack)
    assert ack == seq + 1
    {_emitted, driver} = ingress(driver, segment(:none, seq + 1, isn + 1, <<>>), at)
    {isn, driver}
  end

  defp accept(driver, listener, at) do
    {ref, driver} = Driver.ref(driver)
    Driver.call(driver, :tcp_accept, [listener, ref], at)
  end

  defp ingress(driver, segment, at) do
    {{:ok, :ok}, driver} =
      Driver.call(driver, :stack_ingress, [Iface.tcp_packet(@client, @server, segment)], at)

    {driver |> Driver.trace() |> List.last() |> Map.fetch!(:emitted), driver}
  end

  defp segment(control, seq, ack, payload) do
    Tcp.tcp(
      src_port: 5_000,
      dst_port: 80,
      control: control,
      seq: seq,
      ack: ack,
      window: 4_096,
      payload: payload
    )
  end

  defp parse(packet) do
    {:ok, Ipv4.ipv4(src: src, dst: dst), payload} = Ipv4.parse(packet)
    {:ok, segment} = Tcp.parse(payload, src, dst)
    segment
  end

  defp control(packet), do: Tcp.tcp(parse(packet), :control)
end
