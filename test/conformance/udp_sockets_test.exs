defmodule SmolNet.Conformance.UdpSocketsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # The Elixir backend's UDP sockets against the NIF, live: both stacks are
  # given the same socket calls and datagrams (`UdpCalls`), and must answer
  # each call with the same result, packets, wakes and aborts, and end with
  # the same counts. The calls reach the error checks in their order,
  # waiters and cancel, ports, binds, peers, truncation and the receive
  # rings. The committed `udp_sockets` trace keeps what this compares for
  # after the NIF is gone.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.UdpCalls

  # It needs the NIF, whatever the suite's backend.
  @moduletag :nif

  @native SmolNet.Stack.Backend.Native
  @elixir SmolNet.Stack.Backend.Elixir

  @counts [
    :socket_count,
    :udp_socket_count,
    :socket_buffer_bytes,
    :waiter_count,
    :read_waiter_count,
    :write_waiter_count,
    :sent_waiter_count,
    :read_sent_waiter_count,
    :write_sent_waiter_count,
    :lifecycle
  ]
  @counters [
    :ingress_packets,
    :rejected_packets,
    :emitted_packets,
    :max_bytes_copied,
    :max_output_packets,
    :notifications_delivered
  ]

  property "UDP sockets behave as the NIF's" do
    check all(
            two_addresses <- boolean(),
            calls <- UdpCalls.sequence(two_addresses),
            max_runs: 500
          ) do
      run_both(two_addresses, calls)
    end
  end

  for {description, stack, calls} <- UdpCalls.scripted() do
    @calls {stack, calls}
    test description do
      {stack, calls} = @calls
      run_both(stack, calls)
    end
  end

  # Both backends make each call in turn, and must answer it alike.
  defp run_both(stack, calls) do
    options = UdpCalls.options(stack)
    {:ok, native} = Driver.start(:stack, @native, options, 0)
    {:ok, elixir} = Driver.start(:stack, @elixir, options, 0)

    {native, elixir, _model} =
      calls
      |> Enum.with_index(1)
      |> Enum.reduce({native, elixir, UdpCalls.new()}, fn {call, at}, acc ->
        step(acc, call, at)
      end)

    assert counts(elixir) == counts(native)
  end

  defp step({native, elixir, model}, call, at) do
    case UdpCalls.prepare(model, call) do
      nil ->
        {native, elixir, model}

      {name, args} ->
        {args, {native, elixir}} = Enum.map_reduce(args, {native, elixir}, &fresh_ref/2)
        {expected, native} = Driver.call(native, name, args, at)
        {actual, elixir} = Driver.call(elixir, name, args, at)
        context = "#{name} #{inspect(args)}: NIF #{inspect(expected)}, Elixir #{inspect(actual)}"
        assert actual == expected, context
        assert entry(elixir) == entry(native), context
        {native, elixir, UdpCalls.observe(model, name, expected)}
    end
  end

  # A fresh reference label, the same on both.
  defp fresh_ref(:fresh_ref, {native, elixir}) do
    {ref, native} = Driver.ref(native)
    {^ref, elixir} = Driver.ref(elixir)
    {ref, {native, elixir}}
  end

  defp fresh_ref(arg, drivers), do: {arg, drivers}

  defp entry(driver), do: driver.trace |> hd() |> Map.take([:result, :emitted, :events, :poll_at])

  defp counts(driver) do
    {{:ok, snapshot}, _driver} = Driver.call(driver, :stack_snapshot, [], 0)
    {Map.take(snapshot, @counts), Map.take(snapshot.counters, @counters)}
  end
end
