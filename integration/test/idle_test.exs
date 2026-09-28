defmodule SmolNet.Integration.IdleTest do
  # The idle scenario's working connections over the helper's loopback and
  # the kernel's, and the ruleset it vanishes peers with. Needs no device
  # and no root: without root, no peer vanishes.
  use ExUnit.Case, async: false

  alias SmolNet.Integration.Blackhole
  alias SmolNet.Integration.Scenarios.Idle
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  @short ~w(--duration 12s --quiet 1s --idle-max 2s --trickle-max 1s --burst-bytes 50000
            --outage-max 1s)

  test "holds TCP and TLS connections over the helper's loopback", %{tmp_dir: dir} do
    assert {:pass, verdict} = run(~w(--self-check --connections 16 --out #{dir}) ++ @short)

    assert verdict.counters.connections == 16
    refute Map.has_key?(verdict.counters, :vanished)
    assert Enum.any?(verdict.notes, &(&1 =~ "no peer vanishes over the helper's loopback"))

    # Every behaviour, and every one ends with an exchange.
    working = verdict.results.working
    assert Enum.map(working, & &1.role) == [:burst, :echo, :stall, :trickle]
    assert Enum.all?(working, &(&1.connections == 4 and &1.exchanges >= 4))
    assert verdict.counters.bytes_echoed == Enum.sum(Enum.map(working, & &1.bytes))

    # Little or nothing polls while every connection is idle.
    assert %{poll_calls_per_s: rate, seconds: seconds} = verdict.results.quiet
    assert rate <= 1.0 and seconds >= 1.0

    [header | _rows] = dir |> Path.join("metrics.csv") |> File.read!() |> String.split("\n")
    assert header =~ "poll_calls,native_calls,timer_generation"
  end

  test "runs over the kernel's loopback in baseline mode", %{tmp_dir: dir} do
    argv = ~w(--baseline --family inet --connections 8 --vanish 0 --out #{dir}) ++ @short
    assert {:pass, verdict} = run(argv)

    assert verdict.counters.connections == 8
    assert Enum.all?(verdict.results.working, &(&1.exchanges >= 2))
  end

  test "rejects idle bounds out of order", %{tmp_dir: dir} do
    argv = ~w(--self-check --duration 0 --idle-min 2m --idle-max 1m --out #{dir})
    assert {:fail, verdict} = run(argv)
    assert [%{kind: :usage, summary: "--idle-min must not exceed --idle-max"}] = verdict.failures
  end

  test "runs with keepalive and SmolNet's timers shortened", %{tmp_dir: dir} do
    timers = ~w(--keepalive --keepalive-idle 5s --keepalive-interval 1s --user-timeout 5s)
    argv = ~w(--self-check --connections 8 --out #{dir}) ++ timers ++ @short
    assert {:pass, verdict} = run(argv)
    assert verdict.counters.connections == 8
    assert Enum.all?(verdict.results.working, &(&1.exchanges >= 2))
  end

  test "shortens SmolNet's timers only through their test-only stack option" do
    stack = Keyword.fetch!(Idle.config(), :stack)
    unset = %{user_timeout: nil, keepalive_idle: nil, keepalive_interval: nil}
    unset = Map.put(unset, :keepalive_probes, nil)

    assert stack.(unset) == [limits: %{sockets: 512}]

    assert stack.(%{unset | user_timeout: "45s", keepalive_probes: 3}) ==
             [
               limits: %{sockets: 512},
               test_tcp_timers: %{user_timeout: 45_000, keepalive_probes: 3}
             ]
  end

  test "rejects a timer the stack cannot take", %{tmp_dir: dir} do
    argv = ~w(--self-check --duration 0 --quiet 0 --user-timeout 2d --out #{dir})
    assert {:fail, verdict} = run(argv)
    assert [%{kind: :usage, summary: "--user-timeout must be at most a day"}] = verdict.failures
  end

  test "blackholes a connection both ways on its interface" do
    ruleset = Blackhole.ruleset("tun0")

    assert ruleset =~ "table inet smolnet_blackhole_tun0 {"
    assert ruleset =~ ~s(iifname "tun0" tcp sport . tcp dport @pairs drop)
    assert ruleset =~ ~s(oifname "tun0" tcp sport . tcp dport @pairs drop)
    assert Blackhole.table("tun-0.x") == "smolnet_blackhole_tun_0_x"
  end

  defp run(argv), do: Soak.run(argv, Keyword.put(Idle.config(), :quiet, true), &Idle.run/1)
end
