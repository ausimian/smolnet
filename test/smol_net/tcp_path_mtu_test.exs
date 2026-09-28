defmodule SmolNet.TcpPathMtuTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @server {192, 0, 2, 1}
  @client {192, 0, 2, 2}
  @router {192, 0, 2, 254}
  @port 41_300

  # Three segments at the default MTU of 1500, whose MSS is 1460.
  @write_bytes 4_000
  # A hop narrower than the path, as an ICMP error reports it (#128).
  @hop_mtu 1_000

  @wait_5s Timing.liveness(5_000)

  setup do
    on_exit(&stop_all_stacks/0)
  end

  # The hop refuses the client's full-size segments and says so with ICMP
  # "Fragmentation Needed". The client must send the data again at once, in
  # segments that fit, rather than resend the same segments forever.
  test "an ICMP error that reports a narrower hop lowers the segment size" do
    {server, client, link} = stacks()
    {socket, child} = connection(server, client)

    # The hop drops every segment, as too big for it.
    :ok = RawIpLink.fault(link, :drop)
    flush_egress()
    payload = :crypto.strong_rand_bytes(@write_bytes)
    assert :ok = SmolNet.send(socket, payload, @wait_5s)
    [first | _] = sent = await_client_data([], 0)
    assert byte_size(first) == 1_500
    assert reassemble(sent) == payload

    # An error forged without seeing the connection guesses the sequence
    # number wrong, and changes nothing.
    <<head::binary-size(24), seq::32, tail::binary>> = first
    # A 32-bit segment keeps the low bits, so the sum wraps as sequence numbers do.
    forged = <<head::binary, seq + 100_000::32, tail::binary>>
    assert :ok = SmolNet.ingress(client, frag_needed(forged, @hop_mtu))
    await_path_mtu_counters(client, {1, 1, 0})

    :ok = RawIpLink.fault(link, :hold)
    flush_egress()
    assert :ok = SmolNet.ingress(client, frag_needed(first, @hop_mtu))
    resent = await_client_data([], 0)
    assert Enum.all?(resent, &(byte_size(&1) <= @hop_mtu))
    assert reassemble(resent) == payload

    await_path_mtu_counters(client, {2, 1, 1})

    :ok = RawIpLink.fault(link, :pass)
    :ok = RawIpLink.release(link)
    assert {:ok, ^payload} = SmolNet.recv(child, @write_bytes, @wait_5s)
  end

  defp connection(server, client) do
    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server)
    :ok = SmolNet.bind(listener, endpoint(@server))
    :ok = SmolNet.listen(listener, 1)
    accept = Task.async(fn -> SmolNet.accept(listener, @wait_5s) end)

    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: client)
    :ok = SmolNet.connect(socket, endpoint(@server), @wait_5s)
    {:ok, child} = Task.await(accept, @wait_5s)
    {socket, child}
  end

  # The client's packets that carry data, in the order sent, until they
  # carry a write's worth.
  defp await_client_data(packets, bytes) when bytes >= @write_bytes, do: Enum.reverse(packets)

  defp await_client_data(packets, bytes) do
    receive do
      {:test_link_egress, :client, packet} ->
        case tcp_data(packet) do
          {_sequence, data} -> await_client_data([packet | packets], bytes + byte_size(data))
          nil -> await_client_data(packets, bytes)
        end
    after
      @wait_5s ->
        flunk("the client sent only #{bytes} bytes of data, in #{length(packets)} packets")
    end
  end

  # The data of `packets`, in sequence from the first packet's. A
  # retransmission replaces its original.
  defp reassemble([first | _] = packets) do
    {base, _data} = tcp_data(first)

    packets
    |> Map.new(fn packet ->
      {sequence, data} = tcp_data(packet)
      {sequence - base &&& 0xFFFFFFFF, data}
    end)
    |> Enum.sort()
    |> Enum.map_join(&elem(&1, 1))
  end

  defp tcp_data(<<4::4, ihl::4, _tos, total::16, _rest::binary>> = packet) do
    header_bytes = ihl * 4
    tcp_bytes = total - header_bytes
    <<_ip::binary-size(^header_bytes), tcp::binary-size(^tcp_bytes), _padding::binary>> = packet
    <<_ports::32, sequence::32, _ack::32, offset::4, _bits::12, _tail::binary>> = tcp

    case binary_part(tcp, offset * 4, byte_size(tcp) - offset * 4) do
      <<>> -> nil
      data -> {sequence, data}
    end
  end

  # An ICMP "Fragmentation Needed" from a router on the path to the client,
  # that reports `mtu` and quotes the first 548 bytes of `packet`, as
  # Linux's routers do.
  defp frag_needed(packet, mtu) do
    quoted = binary_part(packet, 0, min(byte_size(packet), 548))
    icmp = <<3, 4, 0::16, 0::16, mtu::16, quoted::binary>>
    <<type_code::binary-size(2), _unset::16, rest::binary>> = icmp
    icmp = <<type_code::binary, checksum(icmp)::16, rest::binary>>

    header = fn sum ->
      <<0x45, 0, 20 + byte_size(icmp)::16, 0::32, 64, 1, sum::16, ip(@router)::binary,
        ip(@client)::binary>>
    end

    header.(checksum(header.(0))) <> icmp
  end

  defp ip({a, b, c, d}), do: <<a, b, c, d>>

  defp checksum(data) do
    padded = if rem(byte_size(data), 2) == 1, do: data <> <<0>>, else: data
    sum = for <<word::16 <- padded>>, reduce: 0, do: (total -> total + word)
    bnot(fold(sum)) &&& 0xFFFF
  end

  defp fold(sum) when sum > 0xFFFF, do: fold((sum &&& 0xFFFF) + (sum >>> 16))
  defp fold(sum), do: sum

  # Waits for the stack to have taken the ICMP errors delivered to it, and
  # for its path MTU counters to be `{received, rejected, reductions}`.
  defp await_path_mtu_counters(stack, expected) do
    deadline = System.monotonic_time(:millisecond) + @wait_5s
    await_path_mtu_counters(stack, expected, deadline)
  end

  defp await_path_mtu_counters(stack, expected, deadline) do
    {:ok, info} = SmolNet.stack_info(stack)
    counters = info.native.result.counters

    actual =
      {counters.icmp_too_big_received, counters.icmp_too_big_rejected,
       counters.path_mtu_reductions}

    cond do
      actual == expected ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("path MTU counters #{inspect(actual)}, expected #{inspect(expected)}")

      true ->
        Process.sleep(5)
        await_path_mtu_counters(stack, expected, deadline)
    end
  end

  defp flush_egress do
    receive do
      {:test_link_egress, _link_ref, _packet} -> flush_egress()
    after
      0 -> :ok
    end
  end

  defp stacks do
    {:ok, link} = RawIpLink.start_link(self())
    {:ok, server} = SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 24}])
    {:ok, client} = SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 24}])
    :ok = RawIpLink.connect(link, :server, client)
    :ok = RawIpLink.connect(link, :client, server)
    {server, client, link}
  end

  defp endpoint(address), do: %{family: :inet, addr: address, port: @port}

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
