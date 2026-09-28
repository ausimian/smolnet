defmodule SmolNet.Integration.NetemMatrixTest do
  # The netem matrix's plan, stall limits and summary. The scenario itself
  # needs a namespace of its own and root in it, so it does not run here.
  use ExUnit.Case, async: true

  alias SmolNet.Integration.Scenarios.NetemMatrix
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  test "interleaving runs the whole matrix once per repeat" do
    plan = NetemMatrix.plan(["none", "delay"], [:send, :kernel], [1, 4], [:inet], 2, :interleaved)

    assert length(plan) == 2 * 2 * 2 * 2
    assert Enum.map(Enum.take(plan, 8), & &1.repeat) == List.duplicate(1, 8)

    assert Enum.map(Enum.take(plan, 4), &{&1.profile, &1.flow, &1.streams}) == [
             {"none", :send, 1},
             {"none", :send, 4},
             {"none", :kernel, 1},
             {"none", :kernel, 4}
           ]

    assert hd(plan).id == "none-send-x1-inet"
  end

  test "grouping runs a case's repeats together" do
    plan = NetemMatrix.plan(["delay"], [:receive], [1], [:inet, :inet6], 3, :grouped)

    assert Enum.map(plan, &{&1.family, &1.repeat}) ==
             [inet: 1, inet: 2, inet: 3, inet6: 1, inet6: 2, inet6: 3]
  end

  test "a stall is retransmission timeouts in a row, each twice the last" do
    assert NetemMatrix.stall_limit("none", 1) == 200
    assert NetemMatrix.stall_limit("loss-burst", 5) == 200 * 31
    assert NetemMatrix.stall_limit("delay", 5) == 400 * 31
    assert NetemMatrix.stall_limit("bufferbloat", 2) == 1_240 * 3
  end

  test "medians" do
    assert NetemMatrix.median([]) == nil
    assert NetemMatrix.median([3, 1, 2]) == 2
    assert NetemMatrix.median([4, 1, 3, 2]) == 2.5
  end

  test "a row compares SmolNet's median with the kernel's, and triages a gap" do
    transfers =
      for {flow, rates} <- [kernel: [100.0, 80.0, 90.0], send: [30.0, 40.0, nil], receive: [85.0]],
          rate <- rates do
        transfer("loss-burst", flow, rate)
      end

    [kernel, send, receive] = NetemMatrix.matrix(transfers, 0.5)

    assert %{flow: :kernel, median_mbit_s: 90.0, ratio_to_kernel: nil, gap: false} = kernel
    assert %{runs: 3, intact: 2, stalls: 1, median_mbit_s: 35.0, ratio_to_kernel: 0.39} = send
    assert send.gap
    assert [%{issue: 138} | _more] = send.triage
    assert %{ratio_to_kernel: 0.94, gap: false, triage: []} = receive
  end

  test "a gap nothing explains is untriaged, and a cut transfer does not count" do
    transfers = [
      transfer("duplicate", :kernel, 100.0),
      transfer("duplicate", :send, 10.0),
      %{transfer("duplicate", :send, nil) | outcome: :cut, stalled: false}
    ]

    [_kernel, send] = NetemMatrix.matrix(transfers, 0.5)
    assert %{runs: 1, gap: true, triage: []} = send
    assert NetemMatrix.triage("duplicate", :send) == []
  end

  test "a gap without delay that is no wider than the unimpaired one is the CPU's" do
    transfers =
      for {profile, send} <- [{"none", 30.0}, {"corrupt", 28.0}, {"duplicate", 10.0}],
          {flow, rate} <- [kernel: 100.0, send: send],
          do: transfer(profile, flow, rate)

    [_, none, _, corrupt, _, duplicate] = NetemMatrix.matrix(transfers, 0.5)
    assert [%{issue: nil, why: why}] = none.triage
    assert why =~ "no delay on the path"
    assert [%{issue: nil}] = corrupt.triage
    assert %{gap: true, triage: []} = duplicate
  end

  test "netem is the matrix's own to apply", %{tmp_dir: dir} do
    config = Keyword.put(NetemMatrix.config(), :quiet, true)

    assert {:help, usage} = Soak.run(~w(--help), config, &NetemMatrix.run/1)
    assert usage =~ "--profiles LIST"

    argv = ~w(--self-check --duration 0 --out #{dir})
    assert {:error, verdict} = Soak.run(argv, config, &NetemMatrix.run/1)
    assert verdict.error =~ "no --baseline or --self-check mode"

    argv = ~w(--self-check --duration 0 --profiles delay,hurricane --out #{dir})
    assert {:fail, verdict} = Soak.run(argv, config, &NetemMatrix.run/1)
    assert [%{kind: :usage, summary: summary}] = verdict.failures
    assert summary =~ "--profiles must name none or"
  end

  # A transfer of 8 MiB that completed at `rate`, or stalled for nil.
  defp transfer(profile, flow, rate) do
    %{
      id: "#{profile}-#{flow}-x1-inet",
      profile: profile,
      flow: flow,
      streams: 1,
      family: :inet,
      repeat: 1,
      outcome: if(rate, do: :complete, else: :timeout),
      intact: rate != nil,
      stalled: rate == nil,
      mbit_s: rate,
      completion_ms: if(rate, do: round(8 * 8_388.608 / rate), else: 0),
      longest_wait_ms: 50
    }
  end
end
