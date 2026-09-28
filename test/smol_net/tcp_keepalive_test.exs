defmodule SmolNet.TcpKeepaliveTest do
  use ExUnit.Case, async: false

  alias SmolNet.Inet.Tcp
  alias SmolNet.Inet.Udp
  alias SmolNet.Test.ClockedPair
  alias SmolNet.Test.ManualClock
  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @port 41_320

  # SmolNet's fixed keep-alive timing, Linux's defaults, from
  # native/smolnet_core/src/tcp.rs.
  @idle 7_200_000
  @interval 75_000
  @probes 9

  @wait_5s Timing.liveness(5_000)

  setup do
    {:ok, clock} = ManualClock.start()

    # Callbacks run last first, so the stacks stop before their clock does.
    on_exit(fn ->
      Application.delete_env(:smolnet, :manual_clock)
      Agent.stop(clock)
    end)

    on_exit(&ClockedPair.stop_all_stacks/0)
    %{clock: clock}
  end

  # One stack runs on a clock the test moves on, hours at a time, and the
  # link drops every packet once the peer has vanished. The assertions are on
  # the segments that stack sends, and when, by its clock.

  test "keepalive defaults to false, and connect, listen and setopts set it", context do
    {_pair, %{listener: listener, client: client, server: server}} = connected(context)

    for socket <- [listener, client, server] do
      assert {:ok, [keepalive: false]} = :inet.getopts(socket, [:keepalive])
      assert :ok = :inet.setopts(socket, keepalive: true)
      assert {:ok, [keepalive: true]} = :inet.getopts(socket, [:keepalive])
      assert :ok = :inet.setopts(socket, keepalive: false)
      assert {:ok, [keepalive: false]} = :inet.getopts(socket, [:keepalive])
    end
  end

  test "a connection probes after 2 h idle, every 75 s, and fails after nine", context do
    {pair, %{client: client}} = connected(context, connect: [keepalive: true])
    assert {:ok, [keepalive: true]} = :inet.getopts(client, [:keepalive])
    vanish(pair)
    reader = Task.async(fn -> :gen_tcp.recv(client, 0) end)
    :ok = ClockedPair.await_waiters(pair.client, read: 1)

    assert sent_after(pair, @idle - 1) == []
    assert [probe] = sent_after(pair, 1)
    assert ClockedPair.probe?(probe)

    for _probe <- 2..@probes do
      assert sent_after(pair, @interval - 1) == []
      assert [probe] = sent_after(pair, 1)
      assert ClockedPair.probe?(probe)
    end

    assert sent_after(pair, @interval - 1) == []
    assert %{read: 1} = ClockedPair.waiters(pair.client)
    assert [reset] = sent_after(pair, 1)
    assert ClockedPair.reset?(reset)
    assert {:error, :etimedout} = Task.await(reader, @wait_5s)
  end

  test "without keepalive an idle connection sends nothing", context do
    {pair, %{client: client}} = connected(context)
    vanish(pair)
    assert sent_after(pair, @idle + @probes * @interval + 1) == []
    assert {:ok, [keepalive: false]} = :inet.getopts(client, [:keepalive])
  end

  test "an accepted socket takes keepalive from its listener as accept returns it", context do
    {pair, %{listener: listener, server: first}} =
      connected(context, clocked: :server, listen: [keepalive: true])

    assert {:ok, [keepalive: true]} = :inet.getopts(first, [:keepalive])

    # Turning it off on the listener leaves the socket it accepted before alone.
    assert :ok = :inet.setopts(listener, keepalive: false)
    options = ClockedPair.tcp_options(pair.client, [])
    {:ok, _client} = :gen_tcp.connect(ClockedPair.server_address(), @port, options, @wait_5s)
    {:ok, second} = :gen_tcp.accept(listener, @wait_5s)
    assert {:ok, [keepalive: false]} = :inet.getopts(second, [:keepalive])

    vanish(pair, :server)
    :ok = ClockedPair.advance(pair, @idle)
    probes = ClockedPair.segments(pair, :server)
    assert [%{port: port} = probe] = probes
    assert ClockedPair.probe?(probe)
    assert {:ok, {_address, ^port}} = :inet.sockname(first)
  end

  test "setopts turns keepalive on, counting from the last packet received", context do
    {pair, %{client: client}} = connected(context)
    vanish(pair)
    assert sent_after(pair, 3_600_000) == []

    assert :ok = :inet.setopts(client, keepalive: true)
    assert sent_after(pair, @idle - 3_600_000 - 1) == []
    assert [probe] = sent_after(pair, 1)
    assert ClockedPair.probe?(probe)
  end

  test "a peer that answers a probe keeps the connection open", context do
    {pair, %{client: client, server: server}} = connected(context, connect: [keepalive: true])
    _handshake = ClockedPair.segments(pair, :client)
    _handshake = ClockedPair.segments(pair, :server)

    assert [probe] = sent_after(pair, @idle)
    assert ClockedPair.probe?(probe)
    # The peer's stack keeps real time and answers at once.
    :ok = await_answer(pair)

    # So the next probe is 2 h after the answer, and nothing fails meanwhile.
    assert sent_after(pair, @probes * @interval + 1) == []
    :ok = :gen_tcp.send(client, "still here")
    assert {:ok, "still here"} = :gen_tcp.recv(server, 10, @wait_5s)
  end

  test "turning keepalive off stops the probes", context do
    {pair, %{client: client}} = connected(context, connect: [keepalive: true])
    vanish(pair)
    assert [probe] = sent_after(pair, @idle)
    assert ClockedPair.probe?(probe)

    assert :ok = :inet.setopts(client, keepalive: false)
    assert sent_after(pair, @idle + @probes * @interval) == []
    assert {:ok, [keepalive: false]} = :inet.getopts(client, [:keepalive])
  end

  test "keepalive takes a boolean on TCP sockets only", context do
    {pair, %{listener: listener, client: client}} = connected(context)

    for socket <- [listener, client] do
      assert {:error, :einval} = :inet.setopts(socket, keepalive: opaque(:maybe))
      assert {:ok, [keepalive: false]} = :inet.getopts(socket, [:keepalive])
    end

    options = [{:smolnet_stack, pair.client}, :inet, :binary, {:active, false}]
    address = ClockedPair.server_address()

    assert {:error, :einval} =
             Tcp.connect(address, @port, [{:keepalive, opaque(1)} | options], @wait_5s)

    assert {:error, :einval} = Tcp.listen(0, [{:keepalive, opaque(:yes)} | options])
    assert {:error, :einval} = Udp.open(0, [{:keepalive, true} | options])
  end

  test "SmolNet.setopt/3 and getopt/2 set and read keepalive on TCP sockets", context do
    pair = ClockedPair.start(context.clock, :client)
    endpoint = %{family: :inet, addr: ClockedPair.server_address(), port: @port}

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: pair.server)
    assert {:ok, false} = SmolNet.getopt(listener, {:socket, :keepalive})
    assert :ok = SmolNet.setopt(listener, {:socket, :keepalive}, true)
    :ok = SmolNet.bind(listener, endpoint)
    :ok = SmolNet.listen(listener, 2)
    assert {:ok, true} = SmolNet.getopt(listener, {:socket, :keepalive})

    first = connect_low(pair.client, endpoint)
    {:ok, first_child} = SmolNet.accept(listener, @wait_5s)
    assert {:ok, true} = SmolNet.getopt(first_child, {:socket, :keepalive})
    assert {:ok, false} = SmolNet.getopt(first, {:socket, :keepalive})

    # A child takes the listener's setting as accept returns it.
    _second = connect_low(pair.client, endpoint)
    assert :ok = SmolNet.setopt(listener, {:socket, :keepalive}, false)
    {:ok, second_child} = SmolNet.accept(listener, @wait_5s)
    assert {:ok, false} = SmolNet.getopt(second_child, {:socket, :keepalive})
    assert {:ok, true} = SmolNet.getopt(first_child, {:socket, :keepalive})

    assert :ok = SmolNet.setopt(first, {:socket, :keepalive}, true)
    assert {:ok, true} = SmolNet.getopt(first, {:socket, :keepalive})
    assert {:error, :invalid_options} = SmolNet.setopt(first, {:socket, :keepalive}, opaque(1))
    assert {:error, :invalid_options} = SmolNet.getopt(first, opaque({:tcp, :keepalive}))

    {:ok, udp} = SmolNet.open(:inet, :dgram, :udp, stack: pair.client)
    assert {:error, :invalid_socket_state} = SmolNet.setopt(udp, {:socket, :keepalive}, true)
    assert {:error, :invalid_socket_state} = SmolNet.getopt(udp, {:socket, :keepalive})

    :ok = SmolNet.close(first)
    assert {:error, :invalid_socket} = SmolNet.setopt(first, {:socket, :keepalive}, true)
    assert {:error, :invalid_socket} = SmolNet.getopt(first, {:socket, :keepalive})
  end

  defp connected(context, options \\ []) do
    {clocked, options} = Keyword.pop(options, :clocked, :client)
    pair = ClockedPair.start(context.clock, clocked)
    {pair, ClockedPair.connect(pair, @port, options)}
  end

  defp connect_low(stack, endpoint) do
    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.connect(socket, endpoint, @wait_5s)
    socket
  end

  # The peer vanishes: nothing either stack sends arrives from now on. What
  # `side` sent before is forgotten.
  defp vanish(pair, side \\ :client) do
    :ok = RawIpLink.fault(pair.link, :drop)
    _before = ClockedPair.segments(pair, side)
    :ok
  end

  # What the clocked client sends as its clock moves on by `milliseconds`.
  defp sent_after(pair, milliseconds) do
    :ok = ClockedPair.advance(pair, milliseconds)
    ClockedPair.segments(pair, :client)
  end

  # Waits for the server's answer to a probe to reach the client's stack.
  defp await_answer(pair) do
    receive do
      {:test_link_egress, :server, _answer} ->
        :ok = ClockedPair.sync_link(pair)
        {:ok, _info} = SmolNet.stack_info(pair.client)
        :ok
    after
      @wait_5s -> flunk("the peer did not answer the probe")
    end
  end

  # Hides a deliberately invalid value from the type checker.
  defp opaque(term), do: Process.get({__MODULE__, :opaque}, term)
end
