defmodule SmolNet.Conformance.LinkDifferentialTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # The Elixir backend against the NIF over impaired links: the same seeded
  # transfer, NIF to NIF, Elixir to Elixir and mixed, must give the same
  # calls, packets, notices, deadlines and snapshots (see
  # SmolNet.Conformance.LinkDifferential). It needs the NIF, so it runs in
  # the native suite. SMOLNET_DIFFERENTIAL_RUNS sets how many transfers it
  # tries, for longer runs than a commit's.
  @moduletag :nif

  alias SmolNet.Conformance.LinkDifferential
  alias SmolNet.Conformance.LinkSim
  alias SmolNet.Conformance.YieldingNative

  @runs String.to_integer(System.get_env("SMOLNET_DIFFERENTIAL_RUNS", "30"))

  @tag timeout: :infinity
  property "the Elixir backend sends what the NIF sends over any link" do
    check all(transfer <- LinkDifferential.transfer(), max_runs: @runs) do
      case LinkDifferential.check(transfer) do
        {:ok, _run} -> :ok
        {:error, report} -> flunk("the backends departed:\n" <> inspect(report, pretty: true))
      end
    end
  end

  # Transfers on which the backends once departed, kept as regressions.
  @found [
    # The client's 30 s close deadline fell in the same millisecond as a
    # resend: the NIF sends the resend and then its RST, so the Elixir
    # backend aborts closing connections after egress, not before.
    %{
      seed: 1_032_655_350,
      family: :inet,
      sender: :client,
      bytes: 1_639,
      mtu: {1_280, 1_280},
      buffer: 2_048,
      profile: %{loss: 0.32764948630569407, duplicate: 0.0, reorder: 0.0, delay: 85, jitter: 29},
      burst: {:client, 184, 2},
      strip_sack: false
    }
  ]

  test "the transfers it once departed on agree" do
    for transfer <- @found do
      assert {:ok, _run} = LinkDifferential.check(transfer)
    end
  end

  # The NIF can stop any call at its wall-clock budget, as a busy runner
  # makes it do, and the comparison must not depend on where. The hook that
  # makes it yield is a call of its own, which `native_calls` counts.
  @tag :debug_nif
  property "the comparison does not depend on where the NIF's budget cuts a call" do
    check all(transfer <- LinkDifferential.transfer(), max_runs: 20) do
      case LinkDifferential.check(transfer, nif: YieldingNative, ignore: [:native_calls]) do
        {:ok, _run} -> :ok
        {:error, report} -> flunk("the backends departed:\n" <> inspect(report, pretty: true))
      end
    end
  end

  test "a departure is reported where it starts" do
    transfer = %{
      seed: 3,
      family: :inet,
      sender: :client,
      bytes: 8_192,
      mtu: {1_500, 1_500},
      buffer: 65_536,
      profile: %{loss: 0.05, duplicate: 0.0, reorder: 0.0, delay: 5, jitter: 0},
      burst: nil,
      strip_sack: false
    }

    assert {:ok, %{status: :ok}} = LinkDifferential.check(transfer)

    # The server's 4th packet lost on one pairing only: the runs agree up to
    # it, and differ from the call after.
    assert {:error, report} = LinkDifferential.check(transfer, perturb: {:server, 4})
    assert [{_pairing, %{index: index, diff: [_ | _]}} | _rest] = report.departed
    assert index > 0
  end

  test "SACK can be taken out of the handshake" do
    sack_permitted? = fn result ->
      Enum.any?(result.entries, fn entry ->
        entry.stack == :server and Enum.any?(entry.emitted, &sack_permitted_syn_ack?/1)
      end)
    end

    assert sack_permitted?.(LinkSim.run(seed: 1, bytes: 1_024))
    refute sack_permitted?.(LinkSim.run(seed: 1, bytes: 1_024, strip_sack: true))
  end

  # A send the `bytes_copied` limit (64 KiB) cuts short, with room left in
  # the buffer, is writable at once: its wake comes in the same call.
  test "a send longer than one call copies finishes" do
    result = LinkSim.run(seed: 4, bytes: 100_000, buffer: 262_144)
    assert result.status == :ok
    assert result.received == result.sent
  end

  test "either end can send, with MTUs of its own" do
    result = LinkSim.run(seed: 2, bytes: 70_000, sender: :server, mtu: {1_280, 9_000})
    assert result.status == :ok
    assert result.received == result.sent

    # The server's segments are cut to the client's MSS (1280 - 40 - 12
    # bytes of timestamps).
    lengths =
      for %{stack: :server} = entry <- result.entries,
          packet <- entry.emitted,
          do: payload_length(packet)

    assert Enum.max(lengths) == 1_228
  end

  # A SYN-ACK whose options include SACK-permitted (kind 4).
  defp sack_permitted_syn_ack?(<<4::4, ihl::4, _::binary-size(8), 6, _rest::binary>> = packet) do
    {_ip, segment} = :erlang.split_binary(packet, ihl * 4)
    <<_::binary-12, offset::4, _::4, flags, _::binary-6, rest::binary>> = segment

    Bitwise.band(flags, 0x12) == 0x12 and
      sack_permitted?(binary_part(rest, 0, offset * 4 - 20))
  end

  defp sack_permitted_syn_ack?(_packet), do: false

  defp sack_permitted?(<<4, 2, _rest::binary>>), do: true
  defp sack_permitted?(<<kind, rest::binary>>) when kind in [0, 1], do: sack_permitted?(rest)

  defp sack_permitted?(<<_kind, length, rest::binary>>) when length >= 2,
    do: sack_permitted?(binary_part(rest, length - 2, byte_size(rest) - length + 2))

  defp sack_permitted?(_options), do: false

  defp payload_length(
         <<4::4, ihl::4, _::binary-size(1), length::16, _::binary-size(8), _addresses::binary-8,
           _::binary-size(ihl * 4 - 20), _::binary-12, offset::4, _rest::bitstring>>
       ),
       do: length - ihl * 4 - offset * 4
end
