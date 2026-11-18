defmodule SmolNet.Conformance.Scenarios do
  @moduledoc false

  # The scenarios whose traces are committed in test/conformance/traces:
  # what the NIF did, call for call, for another backend to be compared
  # against once the NIF is gone. `scripts/backend_traces.exs` records them;
  # each must be deterministic on the NIF, which the differential test
  # checks by replaying them.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.LinkSim
  alias SmolNet.Conformance.Packets
  alias SmolNet.Conformance.Trace

  @transfers [
    {"tcp_clean_inet", "A 32 KiB transfer and close over a clean IPv4 link.",
     [seed: 1, family: :inet]},
    {"tcp_clean_inet6", "A 32 KiB transfer and close over a clean IPv6 link.",
     [seed: 2, family: :inet6]},
    {"tcp_loss_inet", "A 32 KiB transfer over IPv4 losing 5% of packets.",
     [seed: 3, family: :inet, profile: %{loss: 0.05}]},
    {"tcp_reorder_inet6", "A 32 KiB transfer over IPv6 reordering 10% of packets, with jitter.",
     [seed: 4, family: :inet6, profile: %{reorder: 0.1, jitter: 5}]},
    {"tcp_duplicate_inet", "A 32 KiB transfer over IPv4 duplicating 5% of packets.",
     [seed: 5, family: :inet, profile: %{duplicate: 0.05}]},
    {"tcp_impaired_inet",
     "A 32 KiB transfer over IPv4 with 3% loss, 3% duplication, 5% reordering and jitter.",
     [seed: 6, family: :inet, profile: %{loss: 0.03, duplicate: 0.03, reorder: 0.05, jitter: 10}]}
  ]

  @doc "Records every scenario on `backend`, as traces."
  def record(backend) do
    transfers =
      for {name, description, options} <- @transfers do
        result = LinkSim.run([backend: backend] ++ options)

        if result.status != :ok,
          do: raise("scenario #{name} did not complete: #{inspect(result.status)}")

        Trace.new(name, description, result.entries)
      end

    transfers ++ [ingress_replies(backend)]
  end

  @local4 <<192, 0, 2, 1>>
  @peer4 <<192, 0, 2, 9>>
  @local6 <<0xFD00::16, 0::96, 1::16>>
  @peer6 <<0xFD00::16, 0::96, 9::16>>

  @doc """
  What a stack answers by itself: echo replies, Port and Protocol
  Unreachable, Parameter Problem, a RST for a closed port, silence for an
  IPv4 fragment, and a datagram for a bound UDP socket.
  """
  def ingress_replies(backend) do
    options = [
      addresses: [{{192, 0, 2, 1}, 24}, {{0xFD00, 0, 0, 0, 0, 0, 0, 1}, 64}],
      test_determinism: %{random_seed: 1, timestamp_origin: 0, ephemeral_port_start: 50_000}
    ]

    {:ok, driver} = Driver.start(:stack, backend, options, 0)
    {{:ok, udp}, driver} = Driver.call(driver, :udp_open, [:inet], 0)
    bound = %{address: [192, 0, 2, 1], port: 7, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :udp_bind, [udp, bound], 0)

    syn = %{source: 40_000, destination: 81, seq: 1_000, flags: [:syn], window: 1_000}

    packets = [
      Packets.ipv4(@peer4, @local4, 1, Packets.icmpv4_echo(1, 1, "ping")),
      Packets.ipv6(@peer6, @local6, 58, Packets.icmpv6_echo(@peer6, @local6, 1, 1, "ping")),
      Packets.ipv4(@peer4, @local4, 17, Packets.udp(@peer4, @local4, 40_000, 7, "to a socket")),
      Packets.ipv4(@peer4, @local4, 17, Packets.udp(@peer4, @local4, 40_000, 9, "closed")),
      Packets.ipv6(@peer6, @local6, 17, Packets.udp(@peer6, @local6, 40_000, 9, "closed")),
      Packets.ipv4(@peer4, @local4, 6, Packets.tcp(@peer4, @local4, syn)),
      Packets.ipv6(@peer6, @local6, 6, Packets.tcp(@peer6, @local6, syn)),
      Packets.ipv4(@peer4, @local4, 253, "unknown protocol"),
      Packets.ipv6(@peer6, @local6, 253, "unknown protocol"),
      Packets.ipv6(
        @peer6,
        @local6,
        0,
        Packets.ipv6_options(58, Packets.icmpv6_echo(@peer6, @local6, 2, 1, "hop by hop"))
      ),
      Packets.ipv6(
        @peer6,
        @local6,
        60,
        Packets.ipv6_options(58, Packets.icmpv6_echo(@peer6, @local6, 3, 1, "destination"))
      ),
      Packets.ipv4(@peer4, @local4, 1, Packets.icmpv4_echo(4, 1, "fragment"), flags: 0b001)
    ]

    driver =
      packets
      |> Enum.with_index(1)
      |> Enum.reduce(driver, fn {packet, at}, driver ->
        {_result, driver} = Driver.call(driver, :stack_ingress, [packet], at)
        driver
      end)

    {ref, driver} = Driver.ref(driver)

    {{:ok, {:ok, _source, _destination, "to a socket", false}}, driver} =
      Driver.call(driver, :udp_recvfrom, [udp, 100, ref], 20)

    Trace.new(
      "ingress_replies",
      "What a stack answers by itself to IPv4 and IPv6 packets.",
      Driver.trace(driver)
    )
  end
end
