defmodule SmolNet.CoreTest do
  use ExUnit.Case, async: true

  # The Elixir backend's stack calls, whatever the suite's backend: its
  # lifecycle, ingress checks, counters and snapshot, against the NIF where
  # the NIF can say what is right. The replies themselves are compared in
  # test/conformance (stack_replies).

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.Packets
  alias SmolNet.Stack.Backend
  alias SmolNet.Stack.Backend.Native
  alias SmolNet.Stack.Options

  @elixir SmolNet.Stack.Backend.Elixir
  @local4 <<192, 0, 2, 1>>
  @peer4 <<192, 0, 2, 9>>
  @options [addresses: [{{192, 0, 2, 1}, 24}]]

  defp start(backend \\ @elixir, options \\ @options) do
    {:ok, driver} = Driver.start(:stack, backend, options, 0)
    driver
  end

  defp echo, do: Packets.ipv4(@peer4, @local4, 1, Packets.icmpv4_echo(1, 1, "ping"))

  defp snapshot(driver) do
    {{:ok, snapshot}, driver} = Driver.call(driver, :stack_snapshot, [], 0)
    {snapshot, driver}
  end

  test "the backend is named :elixir" do
    assert :elixir in Backend.names()
    assert Backend.resolve(:elixir) == {:ok, @elixir}
    assert Backend.name(@elixir) == :elixir
  end

  test "a stack starts through the shell and answers a ping" do
    {:ok, stack} =
      SmolNet.start_stack(
        backend: :elixir,
        egress: {self(), :link},
        addresses: [{{192, 0, 2, 1}, 24}]
      )

    assert :ok = SmolNet.ingress(stack, echo())
    assert_receive {:smol_stack, :link, :egress, [reply]}
    assert <<0x45, _::binary-size(8), 1, _::binary>> = reply

    assert {:ok, %{backend: :elixir, poll_at: nil, native: %{result: %{lifecycle: :running}}}} =
             SmolNet.stack_info(stack)

    assert {:error, :not_implemented} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    assert :ok = SmolNet.stop_stack(stack)
  end

  describe "stack_new" do
    # Compares with the NIF, so it needs it whatever the suite's backend.
    @tag :nif
    test "validates its limits and configuration as the NIF does" do
      {:ok, %{limits: limits, native_config: config}} = Options.parse(@options)

      for {limits, config} <- [
            {Map.put(limits, :sockets, 0), config},
            {Map.delete(limits, :output_packets), config},
            {limits, %{config | mtu: 1_279}},
            {%{limits | bytes_copied: 1_400}, config},
            {limits, %{config | addresses: [%{address: [224, 0, 0, 1], prefix_length: 4}]}},
            {limits, %{config | addresses: [%{address: [192, 0, 2, 1], prefix_length: 33}]}},
            {limits,
             %{
               config
               | routes: [%{destination: [0, 0, 0, 0], prefix_length: 0, gateway: [0, 0, 0, 0]}]
             }},
            {limits,
             %{
               config
               | routes: [
                   %{destination: [224, 0, 0, 0], prefix_length: 4, gateway: [192, 0, 2, 9]}
                 ]
             }},
            {limits,
             %{
               config
               | routes: [
                   %{
                     destination: [0, 0, 0, 0],
                     prefix_length: 0,
                     gateway: List.duplicate(0, 15) ++ [1]
                   }
                 ]
             }},
            {limits,
             %{
               config
               | routes: [
                   %{destination: [0, 0, 0, 0], prefix_length: 0, gateway: [255, 255, 255, 255]}
                 ]
             }},
            {limits,
             %{
               config
               | routes: [
                   %{destination: [0, 0, 0, 0], prefix_length: 33, gateway: [192, 0, 2, 9]}
                 ]
             }},
            {limits,
             %{
               config
               | addresses: [
                   %{
                     address: List.duplicate(0, 10) ++ [255, 255, 192, 0, 2, 1],
                     prefix_length: 64
                   }
                 ]
             }},
            {limits, Map.put(config, :ephemeral_port_start, 50_176)}
          ] do
        {native, _resource, []} = Native.stack_new(limits, config, 0)
        assert {^native, nil, []} = @elixir.stack_new(limits, config, 0)
        assert {:error, _reason} = native
      end
    end

    test "refuses egress credit, which comes in phase 7 of #173" do
      {:ok, %{limits: limits, native_config: config}} =
        Options.parse(@options ++ [egress_credit: {4, 6_000}])

      assert {{:error, :not_implemented}, nil, []} = @elixir.stack_new(limits, config, 0)
    end
  end

  describe "ingress" do
    # Compares with the NIF, so it needs it whatever the suite's backend.
    @tag :nif
    test "refuses what the NIF refuses, before processing any of a batch" do
      fragment = Packets.ipv4(@peer4, @local4, 1, Packets.icmpv4_echo(4, 1, "x"), flags: 0b001)
      <<head::binary-10, _checksum::16, tail::binary>> = echo()
      bad_checksum = <<head::binary, 0::16, tail::binary>>
      too_large = Packets.ipv4(@peer4, @local4, 17, :binary.copy(<<0>>, 1_481))

      for {call, input} <- [
            {:stack_ingress, fragment},
            {:stack_ingress, bad_checksum},
            {:stack_ingress, too_large},
            {:stack_ingress, <<>>},
            {:stack_ingress, <<0x60, 0::312>>},
            {:stack_ingress_batch, [echo(), echo()]},
            {:stack_ingress_batch, [echo(), fragment]}
          ] do
        {expected, native} = Driver.call(start(Native), call, [input], 1)
        {actual, elixir} = Driver.call(start(), call, [input], 1)
        assert actual == expected
        assert hd(elixir.trace).emitted == hd(native.trace).emitted
        {native_snapshot, _native} = snapshot(native)
        {elixir_snapshot, _elixir} = snapshot(elixir)

        assert elixir_snapshot.counters.rejected_packets ==
                 native_snapshot.counters.rejected_packets
      end
    end

    test "counts what it takes and what it emits" do
      driver = start(@elixir, @options ++ [limits: %{input_packets: 4}])

      {{:ok, 3}, driver} =
        Driver.call(driver, :stack_ingress_batch, [[echo(), echo(), echo()]], 1)

      {snapshot, _driver} = snapshot(driver)

      assert %{
               ingress_packets: 3,
               emitted_packets: 3,
               max_input_packets: 3,
               max_output_packets: 3,
               max_bytes_copied: 96,
               native_calls: 1,
               poll_calls: 0
             } = snapshot.counters
    end
  end

  describe "lifecycle" do
    test "after shutdown, ingress and egress grants are closed and polls do nothing" do
      driver = start()
      {{:ok, :ok}, driver} = Driver.call(driver, :stack_shutdown, [:closed], 1)
      assert {{:error, :closed}, driver} = Driver.call(driver, :stack_ingress, [echo()], 2)
      assert {{:error, :closed}, driver} = Driver.call(driver, :stack_grant_egress, [1, 1], 2)
      assert {{:ok, :ok}, driver} = Driver.call(driver, :stack_poll, [], 3)
      {snapshot, _driver} = snapshot(driver)
      assert snapshot.lifecycle == :shutdown
      assert %{native_calls: 4, poll_calls: 1} = snapshot.counters
    end

    test "an egress grant without credit is refused, as the NIF refuses it" do
      assert {{:error, :egress_credit_disabled}, _driver} =
               Driver.call(start(), :stack_grant_egress, [1, 1], 1)
    end

    test "socket calls are not implemented yet, and never time anything" do
      assert {{:error, :not_implemented}, driver} = Driver.call(start(), :udp_open, [:inet], 0)

      assert {{:error, :not_implemented}, driver} =
               Driver.call(driver, :tcp_open, [:inet, 4_096, 4_096], 0)

      assert Driver.timer(driver) == nil
    end
  end

  # Compares with the NIF, so it needs it whatever the suite's backend.
  @tag :nif
  test "the snapshot has the NIF's shape" do
    {native, _driver} = snapshot(start(Native))
    {elixir, _driver} = snapshot(start())
    assert shape(elixir) == shape(native)
    assert elixir.limits == native.limits

    assert Map.take(elixir, [:mtu, :ip_address_count, :lifecycle, :egress_credit]) ==
             Map.take(native, [:mtu, :ip_address_count, :lifecycle, :egress_credit])
  end

  defp shape(map) when is_map(map), do: Map.new(map, fn {key, value} -> {key, shape(value)} end)
  defp shape(_value), do: :value
end
