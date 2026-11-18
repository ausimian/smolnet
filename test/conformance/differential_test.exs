defmodule SmolNet.Conformance.DifferentialTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.LinkSim
  alias SmolNet.Conformance.Packets
  alias SmolNet.Conformance.Pcg32
  alias SmolNet.Conformance.Trace
  alias SmolNet.Conformance.YieldingNative
  alias SmolNet.Stack.Backend

  setup_all do
    %{backend: Backend.default()}
  end

  describe "the committed traces" do
    test "cover transfers under every impairment, the stack's own replies and UDP" do
      assert Enum.map(Trace.all(), & &1.name) == [
               "ingress_replies",
               "stack_replies",
               "tcp_clean_inet",
               "tcp_clean_inet6",
               "tcp_cubic_inet",
               "tcp_dsack_inet6",
               "tcp_duplicate_inet",
               "tcp_impaired_inet",
               "tcp_loss_inet",
               "tcp_lossy_probe_inet6",
               "tcp_newreno_inet",
               "tcp_reorder_inet6",
               "tcp_rto_backoff_inet",
               "tcp_small_window_inet",
               "tcp_tail_loss_inet6",
               "udp_sockets"
             ]
    end

    # On the NIF this checks that the traces are current and that it is
    # deterministic; on another backend it is the differential comparison.
    for name <- [
          "ingress_replies",
          "stack_replies",
          "tcp_clean_inet",
          "tcp_clean_inet6",
          "tcp_cubic_inet",
          "tcp_dsack_inet6",
          "tcp_duplicate_inet",
          "tcp_impaired_inet",
          "tcp_loss_inet",
          "tcp_lossy_probe_inet6",
          "tcp_newreno_inet",
          "tcp_reorder_inet6",
          "tcp_rto_backoff_inet",
          "tcp_small_window_inet",
          "tcp_tail_loss_inet6",
          "udp_sockets"
        ] do
      test "#{name} replays call for call on this backend", %{backend: backend} do
        trace = Enum.find(Trace.all(), &(&1.name == unquote(name)))
        assert Trace.replay(trace, backend) == :ok
      end
    end
  end

  # The NIF can stop any call at its wall-clock budget, as a busy runner
  # makes it do, and a trace must not depend on where.
  @tag :debug_nif
  test "the traces do not depend on where the NIF's budget cuts a call" do
    for trace <- Trace.all() do
      assert Trace.replay(trace, YieldingNative) == :ok, trace.name
    end
  end

  test "a replay reports the first call that differs", %{backend: backend} do
    trace = Enum.find(Trace.all(), &(&1.name == "stack_replies"))
    index = Enum.find_index(trace.entries, &(&1.emitted != []))

    altered =
      update_in(trace.entries, fn entries ->
        List.update_at(entries, index, fn entry -> %{entry | emitted: [<<0>>]} end)
      end)

    assert {:error, [mismatch]} = Trace.replay(altered, backend)

    assert %{trace: "stack_replies", index: ^index, field: :emitted, expected: [<<0>>]} =
             mismatch

    assert Trace.replay(altered, backend, ignore: [:emitted]) == :ok
  end

  test "a trace survives its file format", %{backend: backend} do
    dir = Path.join(System.tmp_dir!(), "smolnet-trace-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    result = LinkSim.run(seed: 11, bytes: 4_096, profile: %{loss: 0.05}, backend: backend)
    trace = Trace.new("roundtrip", "A short lossy transfer.", result.entries)
    :ok = Trace.save(trace, dir)
    assert Trace.load(Path.join(dir, "roundtrip.json")) == trace
  end

  describe "the link simulator" do
    test "is a function of its options", %{backend: backend} do
      options = [
        seed: 7,
        bytes: 8_192,
        backend: backend,
        profile: %{loss: 0.03, duplicate: 0.03, reorder: 0.1, jitter: 8}
      ]

      first = LinkSim.run(options)
      assert first.status == :ok
      assert first.received == first.sent
      assert LinkSim.run(options) == first
      refute LinkSim.run(Keyword.put(options, :seed, 8)).entries == first.entries
    end

    test "recognises #177's lost FIN", %{backend: backend} do
      result = LinkSim.run(seed: 2, bytes: 16_384, backend: backend, profile: %{loss: 0.05})
      assert result.received == result.sent
      assert {:failed, {:server, :connection_reset, _at}} = result.status
      assert LinkSim.lost_fin?(result)

      # Codex found a run where the client's reset is lost as well.
      stalled = [seed: 14, family: :inet6, bytes: 24_576, backend: backend]
      profile = %{loss: 0.1, duplicate: 0.1, reorder: 0.2, jitter: 20}
      result = LinkSim.run([profile: profile] ++ stalled)
      assert result.received == result.sent
      assert {:stalled, _at} = result.status
      assert LinkSim.lost_fin?(result)

      refute LinkSim.lost_fin?(LinkSim.run(seed: 1, bytes: 1_024, backend: backend))
    end

    test "reports a transfer that cannot finish", %{backend: backend} do
      lost = [seed: 1, bytes: 4_096, backend: backend, profile: %{loss: 1.0}]
      assert {:timeout, _at} = LinkSim.run([deadline: 1_000] ++ lost).status

      # Given time, the stack gives up first.
      result = LinkSim.run(lost)
      assert {:failed, {:client, :connection_timeout, _at}} = result.status
      assert result.received == ""
    end

    # Integrity and liveness over random links. A stack may lose a FIN for
    # good when a duplicate ACK cancels its retransmission (#177): every
    # byte still arrives, but the close never completes.
    property "delivers the stream intact and finishes, whatever the link", %{backend: backend} do
      check all(
              seed <- integer(1..1_000_000),
              family <- member_of([:inet, :inet6]),
              bytes <- integer(1..24_576),
              loss <- float(min: 0.0, max: 0.1),
              duplicate <- float(min: 0.0, max: 0.1),
              reorder <- float(min: 0.0, max: 0.2),
              jitter <- integer(0..20),
              max_runs: 40
            ) do
        profile = %{loss: loss, duplicate: duplicate, reorder: reorder, jitter: jitter}

        result =
          LinkSim.run(
            seed: seed,
            family: family,
            bytes: bytes,
            profile: profile,
            backend: backend
          )

        assert result.received == result.sent

        assert result.status == :ok or LinkSim.lost_fin?(result)
      end
    end
  end

  describe "the determinism hooks" do
    property "a stack's initial sequence numbers follow smoltcp's PCG from its seed", %{
      backend: backend
    } do
      check all(seed <- integer(0..0xFFFF_FFFF_FFFF_FFFF), max_runs: 25) do
        assert syn_isns(backend, seed, 3) == Pcg32.isns(seed, 3)
      end
    end

    test "a listener's SYN-ACK takes the next one", %{backend: backend} do
      result = LinkSim.run(seed: 21, bytes: 1, backend: backend)
      [server_isn] = Pcg32.isns(21 * 2 + 1, 1)
      [client_isn] = Pcg32.isns(21 * 2, 1)

      assert [{:client, ^client_isn, :syn}, {:server, ^server_isn, :syn_ack} | _rest] =
               for(
                 entry <- result.entries,
                 packet <- entry.emitted,
                 syn = syn(packet),
                 syn != nil,
                 do: {entry.stack, elem(syn, 0), elem(syn, 1)}
               )
    end
  end

  test "the driver refuses a stale backend state", %{backend: backend} do
    {:ok, driver} = Driver.start(:stack, backend, [addresses: [{{192, 0, 2, 1}, 24}]], 0)
    {{:ok, :ok}, _newer} = Driver.call(driver, :stack_poll, [], 0)

    assert_raise RuntimeError, ~r/stale backend state/, fn ->
      Driver.call(driver, :stack_poll, [], 0)
    end
  end

  # The ISNs of `count` SYNs from new connections, one after another.
  defp syn_isns(backend, seed, count) do
    options = [
      addresses: [{{192, 0, 2, 1}, 24}],
      test_determinism: %{random_seed: seed}
    ]

    {:ok, driver} = Driver.start(:stack, backend, options, 0)
    peer = %{address: [192, 0, 2, 2], port: 80, scope_id: 0}

    {isns, _driver} =
      Enum.map_reduce(1..count, driver, fn _n, driver ->
        {{:ok, socket}, driver} = Driver.call(driver, :tcp_open, [:inet, 4_096, 4_096], 0)
        {ref, driver} = Driver.ref(driver)
        {{:ok, _select}, driver} = Driver.call(driver, :tcp_connect, [socket, peer, ref], 0)
        [%{emitted: [packet]} | _rest] = driver.trace
        {elem(syn(packet), 0), driver}
      end)

    isns
  end

  defp syn(<<4::4, ihl::4, _rest::binary>> = packet), do: tcp_syn(packet, ihl * 4)
  defp syn(<<6::4, _rest::bitstring>> = packet), do: tcp_syn(packet, 40)

  defp tcp_syn(packet, header) do
    <<_ports::32, seq::32, _ack::32, _offset::4, _::4, flags, _rest::binary>> =
      binary_part(packet, header, byte_size(packet) - header)

    case Bitwise.band(flags, 0x12) do
      0x02 -> {seq, :syn}
      0x12 -> {seq, :syn_ack}
      _other -> nil
    end
  end

  test "Packets builds checksums a stack accepts" do
    # The checksum of a packet with its checksum in place is zero.
    packet = Packets.ipv4(<<192, 0, 2, 9>>, <<192, 0, 2, 1>>, 1, Packets.icmpv4_echo(1, 2, "x"))
    assert Packets.checksum(binary_part(packet, 0, 20)) == 0
    assert Packets.checksum(binary_part(packet, 20, byte_size(packet) - 20)) == 0
  end
end
