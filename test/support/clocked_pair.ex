defmodule SmolNet.Test.ClockedPair do
  @moduledoc false

  # Two stacks joined by a `SmolNet.Test.RawIpLink`, one of them on a
  # `SmolNet.Test.ManualClock`, so that a test can move that stack's time on
  # by hours at once and see what its TCP timers do, with no real time
  # passing. The other stack keeps real time and answers at once.

  import Bitwise

  alias SmolNet.Inet.Tcp
  alias SmolNet.Test.ManualClock
  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @server {192, 0, 2, 1}
  @client {192, 0, 2, 2}

  def server_address, do: @server

  # Starts the pair, with `clocked` (`:client` or `:server`) on `clock`.
  def start(clock, clocked, stack_options \\ []) do
    {:ok, link} = RawIpLink.start_link(self())
    server_options = [egress: {link, :server}, addresses: [{@server, 24}]]
    client_options = [egress: {link, :client}, addresses: [{@client, 24}]]
    server = start_stack(clock, clocked == :server, server_options ++ stack_options)
    client = start_stack(clock, clocked == :client, client_options ++ stack_options)
    :ok = RawIpLink.connect(link, :server, client)
    :ok = RawIpLink.connect(link, :client, server)
    %{link: link, clock: clock, clocked: clocked, server: server, client: client}
  end

  # A `:gen_tcp` listener on the server stack, and a client connected to it
  # and accepted.
  def connect(pair, port, options \\ []) do
    wait = Timing.liveness(5_000)
    listen = tcp_options(pair.server, Keyword.get(options, :listen, []))
    {:ok, listener} = :gen_tcp.listen(port, listen)
    connect = tcp_options(pair.client, Keyword.get(options, :connect, []))
    {:ok, client} = :gen_tcp.connect(@server, port, connect, wait)
    {:ok, server} = :gen_tcp.accept(listener, wait)
    %{listener: listener, client: client, server: server}
  end

  def tcp_options(stack, extra) do
    [{:tcp_module, Tcp}, {:smolnet_stack, stack}, :inet, :binary, active: false] ++ extra
  end

  # Moves the clocked stack's time on, and returns once that stack has handled
  # the timers that fell due and the link has passed on what they sent: the
  # clock sends their messages from this process, before its call to the stack.
  def advance(pair, milliseconds) do
    :ok = ManualClock.advance(pair.clock, milliseconds)
    {:ok, _info} = SmolNet.stack_info(Map.fetch!(pair, pair.clocked))
    sync_link(pair)
  end

  def waiters(stack) do
    {:ok, %{native: %{result: result}}} = SmolNet.stack_info(stack)
    %{read: result.read_waiter_count, write: result.write_waiter_count}
  end

  # Waits for another process's call to leave a waiter in the stack.
  def await_waiters(stack, expected) do
    deadline = System.monotonic_time(:millisecond) + Timing.liveness(5_000)
    await_waiters(stack, Map.new(expected), deadline)
  end

  defp await_waiters(stack, expected, deadline) do
    waiters = waiters(stack)

    cond do
      Map.take(waiters, Map.keys(expected)) == expected ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        raise "the stack has waiters #{inspect(waiters)}, not #{inspect(expected)}"

      true ->
        Process.sleep(5)
        await_waiters(stack, expected, deadline)
    end
  end

  # The link reports each packet to the test before it handles a later call.
  def sync_link(pair) do
    _state = :sys.get_state(pair.link)
    :ok
  end

  # The TCP segments `side`'s stack sent since the last call, whether the link
  # passed or dropped them, as maps of the source `port`, `flags`, `sequence`
  # and `payload`.
  def segments(pair, side) do
    :ok = sync_link(pair)
    collect(side, [])
  end

  defp collect(side, segments) do
    receive do
      {:test_link_egress, ^side, packet} -> collect(side, [segment(packet) | segments])
    after
      0 -> Enum.reverse(segments)
    end
  end

  defp segment(<<4::4, ihl::4, _tos, total::16, _rest::binary>> = packet) do
    tcp = binary_part(packet, ihl * 4, total - ihl * 4)

    <<source::16, _destination::16, sequence::32, _ack::32, offset::4, _reserved::4, flags,
      _tail::binary>> = tcp

    payload = binary_part(tcp, offset * 4, byte_size(tcp) - offset * 4)
    %{port: source, flags: flags, sequence: sequence, payload: payload}
  end

  # A keep-alive probe: one byte, before the next sequence number, with no
  # SYN, FIN or RST.
  def probe?(%{flags: flags, payload: <<0>>}), do: (flags &&& 0x07) == 0
  def probe?(_segment), do: false

  def reset?(%{flags: flags}), do: (flags &&& 0x04) != 0

  # A stack reads its clock module when it starts.
  defp start_stack(clock, clocked?, options) do
    if clocked? do
      Application.put_env(:smolnet, :manual_clock, clock)
      Application.put_env(:smolnet, :clock_module, ManualClock)
    end

    try do
      {:ok, stack} = SmolNet.start_stack(options)
      stack
    after
      Application.delete_env(:smolnet, :clock_module)
    end
  end

  def stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end

    :ok
  end
end
