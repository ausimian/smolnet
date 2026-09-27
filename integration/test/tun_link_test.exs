defmodule SmolNet.Integration.TunLinkTest do
  # These run the real helper, in its loopback mode: every packet the stack
  # emits crosses the port, is echoed back by the helper, and is acknowledged.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  require Logger

  alias SmolNet.Integration.TunLink

  @address {10, 77, 0, 2}
  @default_credit %{packets: 64, bytes: 131_072}

  test "carries a stack's TCP traffic through the helper and returns all its credit" do
    {link, stack} = start_link()

    assert echo(stack, 1_048_576)

    # The connection's closing segments may still be in flight at first.
    assert_eventually(fn ->
      stats = TunLink.stats(link)

      stats.in_flight_packets == 0 and stats.rx_packets == stats.tx_packets and
        stats.rx_bytes == stats.tx_bytes and egress_credit(stack) == @default_credit
    end)

    stats = TunLink.stats(link)
    assert stats.device == "loopback"
    assert stats.tx_packets > 0
    assert %{tx_dropped: 0, ingress_refused: 0, ingress_dropped: 0} = stats
    assert is_integer(stats.helper_os_pid)
  end

  test "with a small credit the stack waits for the device, and nothing is lost" do
    {link, stack} = start_link(egress_credit: {2, 3_000})

    assert echo(stack, 262_144)
    assert_eventually(fn -> TunLink.stats(link).in_flight_packets == 0 end)
    assert TunLink.stats(link).credit_waits > 0

    # The connection's closing segments may still be in flight.
    assert_eventually(fn -> egress_credit(stack) == %{packets: 2, bytes: 3_000} end)
  end

  # Ingress that the stack holds while it waits for credit must not hold up
  # the acknowledgements that return the credit. A one-packet output queue
  # makes held ingress common.
  test "keeps returning credit while the stack holds an ingress call" do
    {link, stack} = start_link(limits: %{output_packets: 1})

    assert echo(stack, 1_048_576)

    assert_eventually(fn ->
      egress_credit(stack) == @default_credit and TunLink.stats(link).ingress_queue_len == 0
    end)
  end

  # The loopback helper drops, and reports, echoes it has no credit for, as a
  # device leaves them to overflow in the kernel.
  test "never has more device packets on their way than its ingress queue" do
    {link, stack} = start_link(ingress_queue: 2, egress_credit: :infinity)

    # A queue of two loses so much that the transfer need not finish; it only
    # has to overflow the queue. Stopping the stack then ends it.
    capture_log(fn ->
      {transfer, monitor} = spawn_monitor(fn -> echo(stack, 262_144) end)

      assert_eventually(fn -> TunLink.stats(link).ingress_dropped > 0 end)

      for _sample <- 1..20 do
        stats = TunLink.stats(link)
        assert stats.ingress_queue_len + stats.ingress_credit <= 2
      end

      SmolNet.stop_stack(stack)
      assert_receive {:DOWN, ^monitor, :process, ^transfer, _reason}, 5_000
      Logger.flush()
    end)
  end

  test "with unlimited credit the stack sends without waiting" do
    {link, stack} = start_link(egress_credit: :infinity)

    assert echo(stack, 262_144)
    assert egress_credit(stack) == nil
    assert TunLink.stats(link).credit_waits == 0
  end

  test "a helper that dies takes the link down, and the stack stops under :stop" do
    {link, stack} = start_link()
    link_monitor = Process.monitor(link)
    stack_monitor = SmolNet.monitor(stack)

    capture_log(fn ->
      kill_helper(link)
      assert_receive {:DOWN, ^link_monitor, :process, _pid, {:helper_exit, _status}}, 5_000
      assert_receive {:DOWN, ^stack_monitor, :process, _object, _reason}, 5_000
    end)
  end

  test "a helper that dies leaves a :mark_down stack running, marked down" do
    {link, stack} = start_link(link_down: :mark_down)
    link_monitor = Process.monitor(link)

    capture_log(fn ->
      kill_helper(link)
      assert_receive {:DOWN, ^link_monitor, :process, _pid, {:helper_exit, _status}}, 5_000
    end)

    assert_eventually(fn -> match?({:ok, %{link_status: :down}}, SmolNet.stack_info(stack)) end)
    SmolNet.stop_stack(stack)
  end

  test "stopping the stack stops the link" do
    {link, stack} = start_link()
    monitor = Process.monitor(link)

    :ok = SmolNet.stop_stack(stack)
    assert_receive {:DOWN, ^monitor, :process, _pid, :normal}, 5_000
  end

  test "rejects :egress, and needs exactly one of :device and :loopback" do
    assert {:error, :invalid_options} = TunLink.start(loopback: true, egress: {self(), :other})
    assert {:error, :invalid_device} = TunLink.start([])
    assert {:error, :invalid_device} = TunLink.start(device: "tun0", loopback: true)
  end

  test "rejects an MTU too large for the helper's framing" do
    assert {:error, :invalid_mtu} = TunLink.start(loopback: true, mtu: 65_535)
  end

  test "a device the helper cannot open is an error" do
    assert {:error, {:helper_exit, 1}} = TunLink.start(device: "smolnet-name-too-long")
  end

  defp start_link(options \\ []) do
    {:ok, link, stack} = TunLink.start([loopback: true, addresses: [{@address, 24}]] ++ options)

    on_exit(fn ->
      if Process.alive?(link), do: SmolNet.stop_stack(stack)
    end)

    {link, stack}
  end

  # Echoes `bytes` over one TCP connection to the stack's own address, which
  # the helper's loopback carries both ways.
  defp echo(stack, bytes) do
    options = [
      {:tcp_module, SmolNet.Inet.Tcp},
      {:smolnet_stack, stack},
      :inet,
      :binary,
      active: false,
      ip: @address
    ]

    {:ok, listener} = :gen_tcp.listen(0, options)
    {:ok, {_address, port}} = :inet.sockname(listener)
    payload = :crypto.strong_rand_bytes(bytes)

    echoer =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 10_000)
        {:ok, data} = :gen_tcp.recv(socket, bytes, 30_000)
        :ok = :gen_tcp.send(socket, data)
        :gen_tcp.close(socket)
      end)

    {:ok, socket} = :gen_tcp.connect(@address, port, options, 10_000)
    :ok = :gen_tcp.send(socket, payload)
    {:ok, echoed} = :gen_tcp.recv(socket, bytes, 30_000)
    :gen_tcp.close(socket)
    Task.await(echoer, 30_000)
    :gen_tcp.close(listener)
    echoed == payload
  end

  defp egress_credit(stack) do
    {:ok, %{native: %{result: %{egress_credit: credit}}}} = SmolNet.stack_info(stack)
    credit
  end

  defp kill_helper(link) do
    {_output, 0} = System.cmd("kill", ["-KILL", to_string(TunLink.stats(link).helper_os_pid)])
  end

  defp assert_eventually(fun, attempts \\ 100) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition never held")

      true ->
        Process.sleep(50)
        assert_eventually(fun, attempts - 1)
    end
  end
end
