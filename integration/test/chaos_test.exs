defmodule SmolNet.Integration.ChaosTest do
  # The chaos scenario's schedule, and short runs over the helper's
  # loopback, which need no device and no root. Its device faults need
  # both, so they do not run here.
  use ExUnit.Case, async: false

  alias SmolNet.Integration.Scenarios.Chaos
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  test "a cycle runs each kill under each policy, and every other fault once" do
    cells = Chaos.cells([:helper_kill, :stop_stack, :credit_stop], [:stop, :notify])

    assert cells == [
             {:helper_kill, :stop},
             {:helper_kill, :notify},
             {:stop_stack, :any},
             {:credit_stop, :any}
           ]
  end

  test "the same seed plans the same episodes, and another seed others" do
    settings = %{policies: [:stop, :mark_down, :notify], max_hold: 4_000}
    faults = [:helper_kill, :route_flap, :credit_trickle, :owner_kill, :owner_swap]

    plans = fn seed ->
      faults
      |> Enum.with_index(1)
      |> Enum.map_reduce(:rand.seed_s(:exsss, seed), fn {fault, n}, rand ->
        Chaos.plan(settings, n, fault, :any, rand)
      end)
      |> elem(0)
    end

    assert plans.(42) == plans.(42)
    assert plans.(42) != plans.(43)

    for plan <- plans.(42) do
      assert plan.policy in settings.policies
      assert plan.delay_ms in 300..2_000
      assert plan.hold_ms in 500..4_000
    end

    [_kill, route, trickle, owners, swap] = plans.(42)
    assert route.params.route in [:blackhole, :unreachable, :prohibit]
    assert %{starve: :trickle, ms: ms} = trickle.params
    assert ms in 1..20
    assert owners.params.targets != []
    assert swap.params.swaps in 1..4
  end

  test "a short run over the helper's loopback passes, and records its schedule", %{tmp_dir: dir} do
    argv =
      ~w(--self-check --seed 7 --episodes 3 --faults stop_stack,link_kill,credit_stop
         --policies mark_down --observe 500 --max-hold 500 --concurrency 1 --churn 1
         --bulk-bytes 65536 --out #{dir})

    assert {:pass, verdict} = run(argv)
    assert verdict.counters.episodes == 3
    assert verdict.counters.injections == 3
    assert verdict.results.seed == 7

    episodes = verdict.results.episodes

    assert episodes |> Enum.map(& &1.fault) |> Enum.sort() == [
             :credit_stop,
             :link_kill,
             :stop_stack
           ]

    assert Enum.all?(episodes, &(&1.outcome == :pass and is_integer(&1.fresh_ms)))

    # Under :mark_down, the link's death is a finding for #43.
    assert [%{fault: :link_kill, policy: :mark_down, calls_alive: 5}] =
             verdict.results.link_restart

    events = verdict.results.events
    assert Enum.any?(events, &(&1.event == :inject))
    assert Enum.all?(events, &is_integer(&1.t_ms))

    assert %{"results" => %{"seed" => 7}} =
             dir |> Path.join("verdict.json") |> File.read!() |> JSON.decode!()

    assert File.read!(Path.join(dir, "run.log")) =~ "rerun with --seed 7"
  end

  test "a baseline run is refused as an environment error", %{tmp_dir: dir} do
    assert {:error, verdict} = run(~w(--baseline --duration 0 --out #{dir}))
    assert verdict.error =~ "no --baseline mode"
  end

  test "a device fault named over the loopback abandons the run", %{tmp_dir: dir} do
    assert {:error, verdict} = run(~w(--self-check --faults device_flap --out #{dir}))
    assert verdict.error =~ "cannot inject device_flap"
  end

  test "an unknown fault is a usage failure", %{tmp_dir: dir} do
    assert {:fail, verdict} = run(~w(--self-check --faults nope --out #{dir}))
    assert [%{kind: :usage, summary: summary}] = verdict.failures
    assert summary =~ "--faults must name some of"
  end

  defp run(argv), do: Soak.run(argv, Keyword.put(Chaos.config(), :quiet, true), &Chaos.run/1)
end
