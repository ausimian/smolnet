defmodule SmolNet.Conformance.IngressPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # What fuzzing the NIF's parsers did, at the backend boundary, so that it
  # holds for any backend: whatever arrives, ingress answers `:ok` or an
  # error, and the stack carries on working.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.Generators
  alias SmolNet.Conformance.Packets
  alias SmolNet.Conformance.StackPackets
  alias SmolNet.Stack.Backend

  @local4 <<192, 0, 2, 1>>
  @peer4 <<192, 0, 2, 9>>
  @local6 <<0xFD00::16, 0::96, 1::16>>
  @peer6 <<0xFD00::16, 0::96, 9::16>>

  # The listener and the UDP socket need the Elixir backend's TCP, which
  # comes in phase 5 of #173.
  @tag :elixir_phase_5
  property "ingress never fails on what a peer sends, and the stack still answers" do
    check all(
            packets <-
              list_of(
                one_of([Generators.packet(@peer4, @local4), Generators.packet(@peer6, @local6)]),
                min_length: 1,
                max_length: 8
              ),
            max_runs: 200
          ) do
      driver = listening_stack()

      driver =
        packets
        |> Enum.with_index(1)
        |> Enum.reduce(driver, fn {packet, at}, driver ->
          {result, driver} = Driver.call(driver, :stack_ingress, [packet], at)

          assert match?({:ok, _result}, result) or
                   match?({:error, reason} when is_atom(reason), result)

          driver
        end)

      # It still answers a ping.
      ping = Packets.icmpv4_echo(9, 9, "still there?")
      echo = Packets.ipv4(@peer4, @local4, 1, ping)
      {{:ok, :ok}, driver} = Driver.call(driver, :stack_ingress, [echo], 100)
      assert [%{emitted: [_reply]} | _rest] = driver.trace
    end
  end

  # The same of the Elixir backend's IP layer alone, whatever the suite's
  # backend, with the packets that reach its corners.
  property "the Elixir backend's ingress never fails, and it still answers" do
    check all(
            packets <- list_of(StackPackets.packet(), min_length: 1, max_length: 8),
            max_runs: 300
          ) do
      {:ok, driver} =
        Driver.start(:stack, SmolNet.Stack.Backend.Elixir, StackPackets.config(), 0)

      driver =
        packets
        |> Enum.with_index(1)
        |> Enum.reduce(driver, fn {packet, at}, driver ->
          {result, driver} = Driver.call(driver, :stack_ingress, [packet], at)
          assert result == {:ok, :ok} or match?({:error, reason} when is_atom(reason), result)
          driver
        end)

      ping = Packets.icmpv4_echo(9, 9, "still there?")
      echo = Packets.ipv4(@peer4, @local4, 1, ping)
      {{:ok, :ok}, driver} = Driver.call(driver, :stack_ingress, [echo], 100)
      assert [%{emitted: [_reply]} | _rest] = driver.trace
    end
  end

  # A stack with a TCP listener on port 80 and a UDP socket on port 7, so
  # that generated segments and datagrams reach sockets as well as the
  # stack's own replies.
  defp listening_stack do
    options = [addresses: [{{192, 0, 2, 1}, 24}, {{0xFD00, 0, 0, 0, 0, 0, 0, 1}, 64}]]
    {:ok, driver} = Driver.start(:stack, Backend.default(), options, 0)

    {{:ok, listener}, driver} = Driver.call(driver, :tcp_open, [:inet, 4_096, 4_096], 0)
    endpoint = %{address: [192, 0, 2, 1], port: 80, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_bind, [listener, endpoint], 0)
    {{:ok, :ok}, driver} = Driver.call(driver, :tcp_listen, [listener, 4], 0)

    {{:ok, udp}, driver} = Driver.call(driver, :udp_open, [:inet], 0)
    endpoint = %{address: [192, 0, 2, 1], port: 7, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :udp_bind, [udp, endpoint], 0)
    driver
  end
end
