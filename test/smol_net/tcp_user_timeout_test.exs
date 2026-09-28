defmodule SmolNet.TcpUserTimeoutTest do
  use ExUnit.Case, async: false

  alias SmolNet.Test.ClockedPair
  alias SmolNet.Test.ManualClock
  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @port 41_310

  # SmolNet's fixed user timeout, from native/smolnet_core/src/tcp.rs.
  @user_timeout 924_600

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

  # The client's stack runs on a clock the test moves on, and once its peer
  # has vanished the link drops every packet, so nothing the client sends is
  # acknowledged. No test waits for real time to pass: the timeout falls due
  # when the clock is moved on to it.

  test "a pending recv fails with etimedout once unacked data goes unanswered", context do
    {pair, %{client: client}} = connected(context)
    vanish(pair)
    :ok = :gen_tcp.send(client, "unanswered")
    reader = Task.async(fn -> :gen_tcp.recv(client, 0) end)
    :ok = ClockedPair.await_waiters(pair.client, read: 1)

    :ok = ClockedPair.advance(pair, @user_timeout - 1)
    assert %{read: 1} = ClockedPair.waiters(pair.client)
    refute Enum.any?(ClockedPair.segments(pair, :client), &ClockedPair.reset?/1)

    :ok = ClockedPair.advance(pair, 1)
    assert {:error, :etimedout} = Task.await(reader, @wait_5s)
    assert Enum.any?(ClockedPair.segments(pair, :client), &ClockedPair.reset?/1)
    assert {:error, :closed} = :gen_tcp.send(client, "later")
  end

  test "a pending send fails with etimedout", context do
    {pair, %{client: client}} = connected(context, connect: [sndbuf: 1_024])
    vanish(pair)
    # This fills the send buffer, so the next send waits for room.
    :ok = :gen_tcp.send(client, :binary.copy("a", 1_024))
    writer = Task.async(fn -> :gen_tcp.send(client, "waits") end)
    :ok = ClockedPair.await_waiters(pair.client, write: 1)

    :ok = ClockedPair.advance(pair, @user_timeout)
    assert {:error, :etimedout} = Task.await(writer, @wait_5s)
  end

  test "an active owner gets tcp_error etimedout, then tcp_closed", context do
    {pair, %{client: client}} = connected(context, connect: [active: true])
    vanish(pair)
    :ok = :gen_tcp.send(client, "unanswered")
    :ok = ClockedPair.await_waiters(pair.client, read: 1)

    :ok = ClockedPair.advance(pair, @user_timeout)
    assert_receive {:tcp_error, ^client, :etimedout}, @wait_5s
    assert_receive {:tcp_closed, ^client}, @wait_5s
  end

  test "an idle connection outlasts the user timeout by far", context do
    {pair, %{client: client, server: server}} = connected(context)
    vanish(pair)

    :ok = ClockedPair.advance(pair, 86_400_000)
    assert ClockedPair.segments(pair, :client) == []

    :ok = RawIpLink.fault(pair.link, :pass)
    :ok = :gen_tcp.send(client, "still here")
    assert {:ok, "still here"} = :gen_tcp.recv(server, 10, @wait_5s)
  end

  test "the low-level API fails the connection with connection_timeout", context do
    pair = ClockedPair.start(context.clock, :client)
    options = ClockedPair.tcp_options(pair.server, [])
    {:ok, listener} = :gen_tcp.listen(@port, options)
    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: pair.client)
    endpoint = %{family: :inet, addr: ClockedPair.server_address(), port: @port}
    :ok = SmolNet.connect(socket, endpoint, @wait_5s)
    {:ok, _server} = :gen_tcp.accept(listener, @wait_5s)

    vanish(pair)
    :ok = SmolNet.send(socket, "unanswered", @wait_5s)
    reader = Task.async(fn -> SmolNet.recv(socket, 0, :infinity) end)
    :ok = ClockedPair.await_waiters(pair.client, read: 1)

    :ok = ClockedPair.advance(pair, @user_timeout)
    assert {:error, :connection_timeout} = Task.await(reader, @wait_5s)
    assert {:error, :connection_timeout} = SmolNet.send(socket, "later", :nowait)
    assert {:error, :connection_timeout} = SmolNet.recv(socket, 0, :nowait)
    assert :ok = SmolNet.close(socket)
  end

  # The integration harness shortens the timers this way, to see a vanished
  # peer detected within a short run.
  test "the test-only tcp timers shorten the user timeout", context do
    timers = [test_tcp_timers: %{user_timeout: 5_000}]
    {pair, %{client: client}} = connected(context, [], timers)
    vanish(pair)
    :ok = :gen_tcp.send(client, "unanswered")
    reader = Task.async(fn -> :gen_tcp.recv(client, 0) end)
    :ok = ClockedPair.await_waiters(pair.client, read: 1)

    :ok = ClockedPair.advance(pair, 4_999)
    assert %{read: 1} = ClockedPair.waiters(pair.client)
    :ok = ClockedPair.advance(pair, 1)
    assert {:error, :etimedout} = Task.await(reader, @wait_5s)
  end

  test "the test-only tcp timers are validated" do
    for timers <- [
          %{user_timeout: 0},
          %{keepalive_idle: 86_400_001},
          %{keepalive_probes: 256},
          %{keepalive_interval: 1.5},
          %{retries: 15},
          :fast
        ] do
      assert {:error, :invalid_options} = SmolNet.start_stack(test_tcp_timers: opaque(timers))
    end
  end

  defp connected(context, options \\ [], stack_options \\ []) do
    pair = ClockedPair.start(context.clock, :client, stack_options)
    {pair, ClockedPair.connect(pair, @port, options)}
  end

  # The peer vanishes: nothing either stack sends arrives from now on. What
  # the client sent before is forgotten.
  defp vanish(pair) do
    :ok = RawIpLink.fault(pair.link, :drop)
    _before = ClockedPair.segments(pair, :client)
    :ok
  end

  # Hides a deliberately invalid value from the type checker.
  defp opaque(term), do: Process.get({__MODULE__, :opaque}, term)
end
