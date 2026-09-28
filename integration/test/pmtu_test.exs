defmodule SmolNet.Integration.PmtuTest do
  # The path MTU scenario's matrix and its expectations. The scenario itself
  # needs a namespace of its own and root in it, so it does not run here.
  use ExUnit.Case, async: true

  alias SmolNet.Integration.Scenarios.Pmtu
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  test "the matrix covers each hop, and IPv6 only where the hop carries it" do
    cases = Pmtu.cases(1500, [1280, 1000], [:inet, :inet6])

    # The control, per family: TCP and UDP each way.
    control = Enum.filter(cases, &(&1.hop == 1500))
    assert length(control) == 2 * 2 * 2

    # Below the MTU: TCP each way with ICMP on and off and three clamps,
    # UDP each way with ICMP on and off, and for IPv4 the peer without DF,
    # sending TCP with and without a clamp, and UDP.
    assert Enum.count(cases, &(&1.hop == 1280 and &1.family == :inet)) == 12 + 4 + 3
    assert Enum.count(cases, &(&1.hop == 1280 and &1.family == :inet6)) == 12 + 4
    assert Enum.count(cases, &(&1.hop == 1000 and &1.family == :inet)) == 12 + 4 + 3
    refute Enum.any?(cases, &(&1.hop == 1000 and &1.family == :inet6))

    ids = Enum.map(cases, & &1.id)
    assert ids == Enum.uniq(ids)
    assert "inet-1000-tcp-out-icmp-off-clamp-rt" in ids
    assert "inet-1000-udp-in-icmp-on-peer-df-off" in ids
  end

  test "SmolNet's own sends adapt to ICMP errors, and without them only to a clamp" do
    expect = fn overrides ->
      [:inet]
      |> then(&Pmtu.cases(1500, [1000], &1))
      |> Enum.find(&Map.equal?(Map.take(&1, Map.keys(overrides)), overrides))
      |> Pmtu.expect(1500)
    end

    out = %{hop: 1000, transport: :tcp, direction: :out}
    assert expect.(Map.merge(out, %{icmp: :on, clamp: :none})) == {:adapts, :pmtud}
    assert expect.(Map.merge(out, %{icmp: :off, clamp: :none})) == {:stalls, :icmp_black_hole}
    assert expect.(Map.merge(out, %{icmp: :on, clamp: :rt})) == {:adapts, :clamped}
    assert expect.(Map.merge(out, %{icmp: :off, clamp: :fixed})) == {:adapts, :clamped}

    down = %{hop: 1000, transport: :tcp, direction: :in, clamp: :none, peer_df: :on}
    assert expect.(Map.put(down, :icmp, :on)) == {:adapts, :peer_pmtud}
    assert expect.(Map.put(down, :icmp, :off)) == {:stalls, :peer_black_hole}
    assert expect.(%{down | peer_df: :off}) == {:stalls, :fragments}
    assert expect.(%{down | peer_df: :off, clamp: :rt}) == {:adapts, :clamped}

    assert expect.(%{hop: 1000, transport: :udp, direction: :out, icmp: :on}) ==
             {:stalls, :no_fragmentation}

    assert expect.(%{hop: 1500, transport: :tcp, direction: :out}) == {:adapts, :fits}
  end

  test "a case fails the run only when it should adapt and does not" do
    assert Pmtu.judge(:adapts, :adapts) == :ok
    assert Pmtu.judge(:stalls, :stalls) == :ok
    assert Pmtu.judge(:adapts, :stalls) == :regression
    assert Pmtu.judge(:adapts, :fails) == :regression
    assert Pmtu.judge(:stalls, :adapts) == :changed
    assert Pmtu.judge(:stalls, :fails) == :changed
  end

  test "every expectation is explained" do
    for test_case <- Pmtu.cases(1500, [1280, 1000, 576], [:inet, :inet6]) do
      {_outcome, reason} = Pmtu.expect(test_case, 1500)
      assert Pmtu.explain(reason) =~ ~r/\w/
    end
  end

  test "a run over the helper's loopback is refused as an environment error", %{tmp_dir: dir} do
    config = Keyword.put(Pmtu.config(), :quiet, true)
    argv = ~w(--self-check --duration 0 --out #{dir})

    assert {:error, verdict} = Soak.run(argv, config, &Pmtu.run/1)
    assert verdict.error =~ "no --baseline or --self-check mode"
  end

  test "bad hops are a usage failure", %{tmp_dir: dir} do
    config = Keyword.put(Pmtu.config(), :quiet, true)

    assert {:help, usage} = Soak.run(~w(--help), config, &Pmtu.run/1)
    assert usage =~ "--hops LIST"

    argv = ~w(--self-check --duration 0 --hops 1000,500 --out #{dir})
    assert {:fail, verdict} = Soak.run(argv, config, &Pmtu.run/1)
    assert [%{kind: :usage, summary: summary}] = verdict.failures
    assert summary =~ "--hops must be MTUs of at least 576"
  end
end
