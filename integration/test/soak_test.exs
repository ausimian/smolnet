defmodule SmolNet.Integration.SoakTest do
  # Whole runs, in self-check mode (the helper's loopback) or baseline mode,
  # so that they need no device and no root.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Scenarios.Smoke
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  test "the smoke scenario passes over the helper's loopback", %{tmp_dir: dir} do
    assert {:pass, verdict} = run(~w(--self-check --duration 1s --bytes 65536 --out #{dir}))

    assert verdict.counters.rounds >= 1
    assert verdict.counters.bytes_echoed >= 4 * 2 * 65_536
    assert verdict.failures == []

    assert %{"verdict" => "pass"} =
             dir |> Path.join("verdict.json") |> File.read!() |> JSON.decode!()

    [header | rows] =
      dir |> Path.join("metrics.csv") |> File.read!() |> String.split("\n", trim: true)

    assert header =~ "socket_count"
    assert header =~ "rounds,bytes_echoed"
    assert rows != []
    refute File.exists?(Path.join(dir, "failures"))
  end

  test "a stalled operation fails the run with its diagnostics", %{tmp_dir: dir} do
    argv = ~w(--self-check --duration 0 --bytes 1024 --family inet --stall --stall-timeout 500)
    assert {:fail, verdict} = run(argv ++ ["--out", dir])

    assert [%{kind: :deadline, summary: summary, file: file}] = verdict.failures
    assert summary == ":stalled_recv did not finish within 500 ms"

    diagnostics = File.read!(Path.join(dir, file))

    for section <- [
          "operation",
          "caller",
          "processes the caller is waiting on",
          "stack_info",
          "link"
        ] do
      assert diagnostics =~ "## #{section}\n"
    end

    assert diagnostics =~ "current_stacktrace"
    assert diagnostics =~ "native_socket_count"
    assert File.read!(Path.join(dir, "stack_info.txt")) =~ "native_socket_count"
  end

  test "the smoke scenario passes over the kernel's loopback in baseline mode", %{tmp_dir: dir} do
    assert {:pass, verdict} = run(~w(--baseline --duration 0 --bytes 65536 --out #{dir}))
    assert verdict.counters.rounds == 1
  end

  test "an oracle the workload checks can fail the run", %{tmp_dir: dir} do
    workload = fn context -> Soak.fail(context, :integrity, "the bytes differ", [{"at", 7}]) end

    assert {:fail, verdict} =
             run(~w(--self-check --duration 0 --out #{dir}), [name: "oracle"], workload)

    assert [%{kind: :integrity, summary: "the bytes differ", file: file}] = verdict.failures
    assert File.read!(Path.join(dir, file)) =~ "## at\n\n7"
  end

  test "a run that reuses an output directory leaves no artifacts of the last", %{tmp_dir: dir} do
    failing = fn context -> Soak.fail(context, :integrity, "the bytes differ") end
    argv = ~w(--baseline --duration 0 --out #{dir})
    assert {:fail, _verdict} = run(argv, [name: "reuse"], failing)
    assert File.exists?(Path.join(dir, "stack_info.txt"))

    assert {:pass, _verdict} = run(argv, [name: "reuse"], fn _context -> :ok end)
    refute File.exists?(Path.join(dir, "failures"))
    refute File.exists?(Path.join(dir, "stack_info.txt"))
    refute File.read!(Path.join(dir, "run.log")) =~ "the bytes differ"
  end

  test "a workload that crashes fails the run", %{tmp_dir: dir} do
    workload = fn _context -> raise "boom" end

    capture_log(fn ->
      assert {:fail, verdict} =
               run(~w(--baseline --duration 0 --out #{dir}), [name: "crash"], workload)

      assert [%{kind: :workload_crashed, summary: summary}] = verdict.failures
      assert summary == "the workload crashed: ** (RuntimeError) boom"
    end)
  end

  test "an environment that cannot be set up is an error, not a failure", %{tmp_dir: dir} do
    argv = ~w(--device smolnet-name-too-long --no-pcap --duration 0 --out #{dir})
    assert {:error, verdict} = run(argv, [name: "setup"], fn _context -> :ok end)

    assert verdict.error =~ "could not start the smolnet network"
    assert verdict.failure_count == 0
  end

  test "counters, notes and the script's switches reach the workload and verdict", %{tmp_dir: dir} do
    config = [
      name: "counting",
      switches: [widgets: :integer],
      defaults: [widgets: 1],
      counters: [:widgets]
    ]

    workload = fn context ->
      Soak.count(context, :widgets, context.extra.widgets)
      Soak.note(context, "counted")
    end

    assert {:pass, verdict} =
             run(~w(--baseline --duration 0 --widgets 3 --out #{dir}), config, workload)

    assert verdict.counters == %{widgets: 3}
    assert "counted" in verdict.notes
    assert dir |> Path.join("metrics.csv") |> File.read!() =~ ~r/,widgets\n/
  end

  test "a workload that abandons the run makes it an error, not a failure", %{tmp_dir: dir} do
    parent = self()

    workload = fn context ->
      Soak.abandon(context, "no route to the target")
      send(parent, {:running, Soak.running?(context)})
    end

    assert {:error, verdict} =
             run(~w(--baseline --duration 1h --out #{dir}), [name: "abandon"], workload)

    assert_received {:running, false}
    assert verdict.error == "no route to the target"
    assert verdict.failure_count == 0
  end

  test "a workload's results reach the verdict, the last of each name", %{tmp_dir: dir} do
    workload = fn context ->
      Soak.record(context, :speed, %{mbit_s: 1.5})
      Soak.record(context, :speed, %{mbit_s: 2.5})
    end

    assert {:pass, verdict} =
             run(~w(--baseline --duration 0 --out #{dir}), [name: "results"], workload)

    assert verdict.results == %{speed: %{mbit_s: 2.5}}

    assert %{"results" => %{"speed" => %{"mbit_s" => 2.5}}} =
             dir |> Path.join("verdict.json") |> File.read!() |> JSON.decode!()
  end

  test "a counter may not shadow a built-in metrics column", %{tmp_dir: dir} do
    config = [name: "shadow", counters: [:rounds, :process_count]]

    assert_raise ArgumentError, ~r/:process_count/, fn ->
      run(~w(--baseline --duration 0 --out #{dir}), config, fn _context -> :ok end)
    end
  end

  test "baseline mode runs the workload over the kernel, with no stack", %{tmp_dir: dir} do
    parent = self()

    workload = fn context ->
      send(parent, {:options, context.stack, Network.tcp_options(context, :subject, :inet6)})
    end

    assert {:pass, _verdict} =
             run(~w(--baseline --duration 0 --out #{dir}), [name: "baseline"], workload)

    assert_received {:options, nil, [:inet6]}
  end

  test "workers run concurrently until the run ends", %{tmp_dir: dir} do
    workload = fn context ->
      Soak.workers(context, fn index -> Soak.count(context, :"worker_#{index}") end)
    end

    argv = ~w(--baseline --concurrency 3 --duration 200ms --out #{dir})
    assert {:pass, verdict} = run(argv, [name: "workers"], workload)
    assert verdict.counters |> Map.keys() |> Enum.sort() == [:worker_1, :worker_2, :worker_3]
  end

  defp run(argv, config \\ Smoke.config(), workload \\ &Smoke.run/1) do
    Soak.run(argv, Keyword.put(config, :quiet, true), workload)
  end
end
