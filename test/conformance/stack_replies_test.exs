defmodule SmolNet.Conformance.StackRepliesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # The Elixir backend against the NIF, live: both are fed the same random
  # packets, chosen to reach the IP layer's corners, and must answer each
  # with the same result and the same bytes, and count the same. Neither
  # stack has a socket, so these are the replies an interface makes by
  # itself. What this finds is kept in the committed `stack_replies`
  # trace, which outlives the NIF.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.Packets
  alias SmolNet.Conformance.StackPackets

  # It needs the NIF, whatever the suite's backend.
  @moduletag :nif

  @native SmolNet.Stack.Backend.Native
  @elixir SmolNet.Stack.Backend.Elixir
  @counters [
    :ingress_packets,
    :rejected_packets,
    :emitted_packets,
    :icmp_too_big_received,
    :icmp_too_big_rejected,
    :path_mtu_reductions
  ]

  property "the Elixir backend answers what a stack is sent as the NIF does" do
    check all(
            packets <- list_of(StackPackets.packet(), min_length: 1, max_length: 6),
            max_runs: 1_000
          ) do
      native = start(@native)
      elixir = start(@elixir)

      {native, elixir} =
        packets
        |> Enum.with_index(1)
        |> Enum.reduce({native, elixir}, fn {packet, at}, {native, elixir} ->
          {expected, native} = Driver.call(native, :stack_ingress, [packet], at)
          {actual, elixir} = Driver.call(elixir, :stack_ingress, [packet], at)
          assert actual == expected, "result for #{Base.encode16(packet)}"

          assert hd(elixir.trace).emitted == hd(native.trace).emitted,
                 "replies to #{Base.encode16(packet)}"

          {native, elixir}
        end)

      assert counters(elixir) == counters(native)
    end
  end

  test "a batch is checked and answered as the NIF does" do
    ping = Packets.icmpv4_echo(1, 1, "batch")
    echo = Packets.ipv4(<<192, 0, 2, 9>>, <<192, 0, 2, 1>>, 1, ping)

    for batch <- [[echo, echo], [echo, <<0x45>>], [], [echo, echo, echo]] do
      native = start(@native, limits: %{input_packets: 2})
      elixir = start(@elixir, limits: %{input_packets: 2})
      {expected, native} = Driver.call(native, :stack_ingress_batch, [batch], 1)
      {actual, elixir} = Driver.call(elixir, :stack_ingress_batch, [batch], 1)
      assert actual == expected
      assert hd(elixir.trace).emitted == hd(native.trace).emitted
      assert counters(elixir) == counters(native)
    end
  end

  defp start(backend, options \\ []) do
    {:ok, driver} = Driver.start(:stack, backend, StackPackets.config() ++ options, 0)
    driver
  end

  defp counters(driver) do
    {{:ok, snapshot}, _driver} = Driver.call(driver, :stack_snapshot, [], 0)
    Map.take(snapshot.counters, @counters)
  end
end
