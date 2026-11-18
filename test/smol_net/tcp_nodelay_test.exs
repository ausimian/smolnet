defmodule SmolNet.TcpNodelayTest do
  use ExUnit.Case, async: false

  alias SmolNet.Inet.Tcp
  alias SmolNet.Inet.Udp
  alias SmolNet.Test.ManualClock
  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @server {192, 0, 2, 1}
  @client {192, 0, 2, 2}
  @port 41_300

  @first "first"
  @second "second"
  @third "third"

  @wait_5s Timing.liveness(5_000)

  setup do
    {:ok, clock} = ManualClock.start()

    # Callbacks run last first, so the stacks stop before their clock does.
    on_exit(fn ->
      Application.delete_env(:smolnet, :manual_clock)
      Agent.stop(clock)
    end)

    on_exit(&stop_all_stacks/0)
    %{clock: clock}
  end

  # The behavioural tests hold every packet the link carries, so no ACK
  # reaches the sender, and run the sender's stack on a clock that never
  # advances, so it never retransmits. A first small write goes out and stays
  # unacknowledged. With Nagle's algorithm on, a second small write must wait
  # for its ACK; with `nodelay: true` it goes out at once. The assertions are
  # on the segments the sender emits, never on how long anything takes.

  test "by default a second small write waits for the first one's ACK", context do
    %{link: link, client: client, server: server} = connected(context, :client)
    assert {:ok, [nodelay: false]} = :inet.getopts(client, [:nodelay])
    assert {:ok, [nodelay: false]} = :inet.getopts(server, [:nodelay])

    _first = hold_and_write(link, client, @first)
    :ok = :gen_tcp.send(client, @second)
    assert_held(link, client)

    release(link)
    assert_delivered(server, @first <> @second)
  end

  test "connect with nodelay: true sends a second small write at once", context do
    %{link: link, client: client, server: server} =
      connected(context, :client, connect: [nodelay: true])

    assert {:ok, [nodelay: true]} = :inet.getopts(client, [:nodelay])

    first = hold_and_write(link, client, @first)
    :ok = :gen_tcp.send(client, @second)
    assert await_segment(client, next(first, @first)) == @second

    release(link)
    assert_delivered(server, @first <> @second)
  end

  test "an accepted socket takes nodelay from its listener as accept returns it", context do
    %{link: link, listener: listener, client: client, server: server, client_stack: stack} =
      connected(context, :server, listen: [nodelay: true])

    assert {:ok, [nodelay: true]} = :inet.getopts(listener, [:nodelay])
    assert {:ok, [nodelay: true]} = :inet.getopts(server, [:nodelay])
    assert {:ok, [nodelay: false]} = :inet.getopts(client, [:nodelay])

    first = hold_and_write(link, server, @first)
    :ok = :gen_tcp.send(server, @second)
    assert await_segment(server, next(first, @first)) == @second

    release(link)
    assert_delivered(client, @first <> @second)

    # Turning it off on the listener leaves the socket it accepted before alone.
    assert :ok = :inet.setopts(listener, nodelay: false)
    {later_client, later_server} = connect_and_accept(listener, stack, [])
    assert {:ok, [nodelay: false]} = :inet.getopts(later_server, [:nodelay])
    assert {:ok, [nodelay: true]} = :inet.getopts(server, [:nodelay])

    _later = hold_and_write(link, later_server, @first)
    :ok = :gen_tcp.send(later_server, @second)
    assert_held(link, later_server)

    release(link)
    assert_delivered(later_client, @first <> @second)
  end

  test "setopts turns Nagle's algorithm off, sending a held write, and on again", context do
    %{link: link, client: client, server: server} = connected(context, :client)

    first = hold_and_write(link, client, @first)
    :ok = :gen_tcp.send(client, @second)
    assert_held(link, client)

    assert :ok = :inet.setopts(client, nodelay: true)
    assert {:ok, [nodelay: true]} = :inet.getopts(client, [:nodelay])
    assert await_segment(client, next(first, @first)) == @second

    assert :ok = :inet.setopts(client, nodelay: false)
    assert {:ok, [nodelay: false]} = :inet.getopts(client, [:nodelay])
    :ok = :gen_tcp.send(client, @third)
    assert_held(link, client)

    release(link)
    assert_delivered(server, @first <> @second <> @third)
  end

  # Here the clocks run, so the server retransmits the write the link holds.
  test "setting nodelay on a listener leaves its stack's timers running", context do
    %{link: link, listener: listener, client: client, server: server} = connected(context, nil)
    flow = flow(server)

    first = hold_and_write(link, server, @first)
    assert :ok = :inet.setopts(listener, nodelay: true)
    assert {^first, @first} = await_data(flow, "the retransmission")

    release(link)
    assert_delivered(client, @first)
  end

  test "nodelay takes a boolean on TCP sockets only", context do
    %{listener: listener, client: client, client_stack: stack} = connected(context, nil)

    for socket <- [listener, client] do
      assert {:error, :einval} = :inet.setopts(socket, nodelay: opaque(:maybe))
      assert {:ok, [nodelay: false]} = :inet.getopts(socket, [:nodelay])
    end

    options = [{:smolnet_stack, stack}, :inet, :binary, {:active, false}]

    assert {:error, :einval} =
             Tcp.connect(@server, @port, [{:nodelay, opaque(1)} | options], @wait_5s)

    assert {:error, :einval} = Tcp.listen(0, [{:nodelay, opaque(:yes)} | options])
    assert {:error, :einval} = Udp.open(0, [{:nodelay, true} | options])
  end

  test "SmolNet.setopt/3 and getopt/2 set and read nodelay on TCP sockets", context do
    %{server: server_stack, client: client_stack} = stacks(context, nil)

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server_stack)
    assert {:ok, false} = SmolNet.getopt(listener, {:tcp, :nodelay})
    assert :ok = SmolNet.setopt(listener, {:tcp, :nodelay}, true)
    :ok = SmolNet.bind(listener, endpoint(@server))
    :ok = SmolNet.listen(listener, 2)
    assert {:ok, true} = SmolNet.getopt(listener, {:tcp, :nodelay})

    first = connect_low(client_stack)
    {:ok, first_child} = SmolNet.accept(listener, @wait_5s)
    assert {:ok, true} = SmolNet.getopt(first_child, {:tcp, :nodelay})
    assert {:ok, false} = SmolNet.getopt(first, {:tcp, :nodelay})

    # A child takes the listener's setting as accept returns it, even one that
    # connected before the setting changed.
    _second = connect_low(client_stack)
    assert :ok = SmolNet.setopt(listener, {:tcp, :nodelay}, false)
    {:ok, second_child} = SmolNet.accept(listener, @wait_5s)
    assert {:ok, false} = SmolNet.getopt(second_child, {:tcp, :nodelay})
    assert {:ok, true} = SmolNet.getopt(first_child, {:tcp, :nodelay})

    assert :ok = SmolNet.setopt(first_child, {:tcp, :nodelay}, false)
    assert {:ok, false} = SmolNet.getopt(first_child, {:tcp, :nodelay})
    assert :ok = SmolNet.setopt(first, {:tcp, :nodelay}, true)
    assert {:ok, true} = SmolNet.getopt(first, {:tcp, :nodelay})

    assert {:error, :invalid_options} = SmolNet.setopt(first, {:tcp, :nodelay}, opaque(:maybe))
    assert {:error, :invalid_options} = SmolNet.setopt(first, opaque({:tcp, :cork}), true)
    assert {:error, :invalid_options} = SmolNet.getopt(first, opaque({:socket, :nodelay}))

    {:ok, udp} = SmolNet.open(:inet, :dgram, :udp, stack: client_stack)
    assert {:error, :invalid_socket_state} = SmolNet.setopt(udp, {:tcp, :nodelay}, true)
    assert {:error, :invalid_socket_state} = SmolNet.getopt(udp, {:tcp, :nodelay})

    :ok = SmolNet.close(first)
    assert {:error, :invalid_socket} = SmolNet.setopt(first, {:tcp, :nodelay}, true)
    assert {:error, :invalid_socket} = SmolNet.getopt(first, {:tcp, :nodelay})
  end

  # Holds every packet from now on and writes `data`, which goes out at once
  # because nothing is in flight. Returns the segment's sequence number.
  defp hold_and_write(link, socket, data) do
    :ok = RawIpLink.fault(link, :hold)
    flush_egress()
    :ok = :gen_tcp.send(socket, data)
    assert {sequence, ^data} = await_data(flow(socket))
    sequence
  end

  defp await_data(flow, what \\ "the first write") do
    receive do
      {:test_link_egress, _link_ref, packet} ->
        case tcp_data(packet) do
          {^flow, sequence, data} -> {sequence, data}
          _other -> await_data(flow, what)
        end
    after
      @wait_5s -> flunk("#{what} did not go out")
    end
  end

  # Waits for `socket` to send a data segment starting at `sequence`, and
  # returns its payload.
  defp await_segment(socket, sequence) do
    flow = flow(socket)

    receive do
      {:test_link_egress, _link_ref, packet} ->
        case tcp_data(packet) do
          {^flow, ^sequence, data} -> data
          _other -> await_segment(socket, sequence)
        end
    after
      @wait_5s -> flunk("no data segment starting at sequence #{sequence}")
    end
  end

  # Checks that `socket` sent no data since the last segment the test took.
  # The link reports each packet before it handles a later call, and a
  # socket call returns only after the stack has handed the link every
  # packet the call emitted, so after this call to the link every data
  # segment the writes emitted is already in the mailbox.
  defp assert_held(link, socket) do
    :ok = RawIpLink.fault(link, :hold)
    assert data_segments(flow(socket), []) == []
  end

  defp data_segments(flow, segments) do
    receive do
      {:test_link_egress, _link_ref, packet} ->
        case tcp_data(packet) do
          {^flow, sequence, data} -> data_segments(flow, [{sequence, data} | segments])
          _other -> data_segments(flow, segments)
        end
    after
      0 -> Enum.reverse(segments)
    end
  end

  defp release(link) do
    :ok = RawIpLink.fault(link, :pass)
    :ok = RawIpLink.release(link)
  end

  defp assert_delivered(socket, data) do
    assert {:ok, ^data} = :gen_tcp.recv(socket, byte_size(data), @wait_5s)
  end

  defp next(sequence, data), do: rem(sequence + byte_size(data), 0x1_0000_0000)

  defp flow(socket) do
    {:ok, {_address, local}} = :inet.sockname(socket)
    {:ok, {_address, remote}} = :inet.peername(socket)
    {local, remote}
  end

  defp tcp_data(<<4::4, ihl::4, _tos, total::16, _rest::binary>> = packet) do
    header_bytes = ihl * 4
    tcp_bytes = total - header_bytes
    <<_ip::binary-size(^header_bytes), tcp::binary-size(^tcp_bytes), _padding::binary>> = packet

    <<source::16, destination::16, sequence::32, _ack::32, offset::4, _bits::12, _tail::binary>> =
      tcp

    case binary_part(tcp, offset * 4, byte_size(tcp) - offset * 4) do
      <<>> -> nil
      data -> {{source, destination}, sequence, data}
    end
  end

  defp flush_egress do
    receive do
      {:test_link_egress, _link_ref, _packet} -> flush_egress()
    after
      0 -> :ok
    end
  end

  # A listener on the server stack, and a client connected to it and accepted.
  defp connected(context, sender, options \\ []) do
    stacks = stacks(context, sender)

    {:ok, listener} =
      :gen_tcp.listen(@port, tcp_options(stacks.server, Keyword.get(options, :listen, [])))

    {client, server} =
      connect_and_accept(listener, stacks.client, Keyword.get(options, :connect, []))

    %{
      link: stacks.link,
      listener: listener,
      client: client,
      server: server,
      client_stack: stacks.client
    }
  end

  defp connect_and_accept(listener, client_stack, options) do
    {:ok, client} = :gen_tcp.connect(@server, @port, tcp_options(client_stack, options), @wait_5s)
    {:ok, server} = :gen_tcp.accept(listener, @wait_5s)
    {client, server}
  end

  defp connect_low(stack) do
    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.connect(socket, endpoint(@server), @wait_5s)
    socket
  end

  # Two stacks joined by a link that can hold packets. The `sender` stack, if
  # any, runs on a clock that never advances.
  defp stacks(context, sender) do
    {:ok, link} = RawIpLink.start_link(self())

    server =
      start_stack(context, sender == :server, egress: {link, :server}, addresses: [{@server, 24}])

    client =
      start_stack(context, sender == :client, egress: {link, :client}, addresses: [{@client, 24}])

    :ok = RawIpLink.connect(link, :server, client)
    :ok = RawIpLink.connect(link, :client, server)
    %{server: server, client: client, link: link}
  end

  # A stack reads its clock module when it starts.
  defp start_stack(context, frozen?, options) do
    if frozen? do
      Application.put_env(:smolnet, :manual_clock, context.clock)
      Application.put_env(:smolnet, :clock_module, ManualClock)
    end

    try do
      {:ok, stack} = SmolNet.start_stack(options)
      stack
    after
      Application.delete_env(:smolnet, :clock_module)
    end
  end

  defp tcp_options(stack, extra) do
    [{:tcp_module, Tcp}, {:smolnet_stack, stack}, :inet, :binary, active: false] ++ extra
  end

  defp endpoint(address), do: %{family: :inet, addr: address, port: @port}

  # Hides a deliberately invalid value from the type checker.
  defp opaque(term), do: Process.get({__MODULE__, :opaque}, term)

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
