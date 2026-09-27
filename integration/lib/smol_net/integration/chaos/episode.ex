defmodule SmolNet.Integration.Chaos.Episode do
  @moduledoc """
  One episode of the chaos scenario (see `SmolNet.Integration.Scenarios.Chaos`):
  starts a stack and its traffic, injects the plan's fault, checks the
  oracles, tears everything down, and checks that a fresh stack works.

  Messages from the episode's operations, monitors and `{:notify, pid}`
  policy all arrive at the process running it, which folds them into its
  state as they come, timing each.
  """

  alias SmolNet.Integration.Chaos.Workload
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Soak.Pcap
  alias SmolNet.Integration.TunLink
  alias SmolNet.Stack.Ref

  # Between every operation being ready and the plan's delay, so that each
  # blocking call has blocked.
  @settle 200
  @ready_timeout 15_000
  @credit_timeout 5_000
  @process_timeout 10_000
  # Processes an episode may leave that are not its own: :ssl's caches,
  # started on first use, and the like.
  @process_slack 50
  @poll 50

  @doc "Runs `ip` with `arguments`, returning `:ok` or `{:error, output}`."
  @spec ip([String.t()]) :: :ok | {:error, String.t()}
  def ip(arguments) do
    case System.find_executable("ip") || Enum.find(["/usr/sbin/ip", "/sbin/ip"], &File.exists?/1) do
      nil ->
        {:error, "ip not found"}

      ip ->
        case System.cmd(ip, arguments, stderr_to_stdout: true) do
          {_output, 0} -> :ok
          {output, _status} -> {:error, String.trim(output)}
        end
    end
  end

  @doc """
  Runs the episode `plan` describes, and returns its record, for
  `verdict.json`, and its events.
  """
  @spec run(Soak.Context.t(), map(), map()) :: {map(), [map()]}
  def run(context, settings, plan) do
    ep = new(context, settings, plan)
    ep = event(ep, :begin, "#{plan.fault} under #{plan.policy}, #{inspect(plan)}")

    ep =
      case start_network(ep, plan.policy) do
        {:ok, net} -> drive(%{ep | net: net, ctx: %{context | stack: net.stack, link: net.link}})
        {:error, reason} -> fail(ep, :fresh_stack, "could not start a stack: #{inspect(reason)}")
      end

    ep |> fresh_check() |> finish()
  end

  defp new(context, settings, plan) do
    %{
      context: context,
      settings: settings,
      plan: plan,
      baseline: %{bundles: bundles(), processes: Process.list()},
      net: nil,
      ctx: nil,
      servers: nil,
      ops: %{},
      capture: nil,
      events: [],
      failures: [],
      active: nil,
      injected_at: nil,
      lifted_at: nil,
      stopped_at: nil,
      stop_cause: nil,
      stack_down_at: nil,
      link_down_at: nil,
      link_reason: nil,
      notified: nil,
      adapters: %{},
      before: nil,
      last_info: nil,
      finding: nil,
      returns: %{},
      slowest_return_ms: nil,
      down_ms: nil,
      fresh_ms: nil,
      transfers: 0,
      connections: 0,
      casualties: 0
    }
  end

  # A stack and link, with monitors on both that are known to be in place
  # before anything can stop either (#109): the reply to a call made after
  # a monitor shows that the monitor arrived first.
  defp start_network(ep, policy) do
    link_down = if policy == :notify, do: {:notify, self()}, else: policy

    target =
      if ep.context.mode == :self_check, do: [loopback: true], else: [device: ep.context.device]

    started = now()

    with {:ok, link, stack} <-
           TunLink.start(target ++ ep.settings.stack_options ++ [link_down: link_down]) do
      pids = Ref.pids(stack)
      bundle_monitor = SmolNet.monitor(stack)
      _state = :sys.get_state(pids.bundle)
      link_monitor = Process.monitor(link)
      stats = TunLink.stats(link)

      {:ok,
       %{
         link: link,
         stack: stack,
         pids: pids,
         bundle_monitor: bundle_monitor,
         link_monitor: link_monitor,
         helper: stats.helper_os_pid,
         started_ms: now() - started
       }}
    end
  end

  defp drive(ep) do
    ep = ep |> start_capture() |> probe(:fresh_stack)
    ep = if ok?(ep), do: start_traffic(ep), else: ep
    ep = if ok?(ep), do: inject(ep), else: ep
    teardown(ep)
  end

  defp ok?(ep), do: ep.failures == []

  defp probe(ep, kind) do
    Enum.reduce(ep.context.families, ep, fn family, ep ->
      case Workload.probe(ep.ctx, family) do
        :ok ->
          ep

        {:error, reason} ->
          fail(ep, kind, "a new stack could not echo over #{family}: #{inspect(reason)}")
      end
    end)
  end

  defp start_traffic(ep) do
    families = ep.context.families

    case Workload.start_servers(ep.ctx, families, ep.settings) do
      {:ok, servers} ->
        # A flap may cost the connections it catches mid-handshake or
        # mid-transfer, if the host's end gives up on them; the loops go on.
        tolerate = ep.plan.fault in [:device_flap, :route_flap]
        settings = Map.merge(ep.settings, %{servers: servers, tolerate: tolerate})
        ops = start_ops(ep, families, settings)
        ep = pump_until(%{ep | servers: servers, ops: ops}, &all_ready?/1, now() + @ready_timeout)

        if all_ready?(ep) do
          ep
          |> event(:traffic, "#{map_size(ops)} operations: #{describe_ops(ops)}")
          |> pump_for(@settle + ep.plan.delay_ms)
        else
          fail(ep, :setup, "operations that never started: #{waiting(ep)}")
        end

      {:error, reason} ->
        fail(ep, :setup, "could not start the peer's servers: #{inspect(reason)}")
    end
  end

  defp start_ops(ep, families, settings) do
    kinds =
      List.duplicate(:bulk, ep.settings.bulk) ++
        List.duplicate(:churn, ep.settings.churn) ++ [:stream | Workload.blocked_kinds()]

    kinds
    |> Enum.with_index(1)
    |> Map.new(fn {kind, index} ->
      # Unique across episodes, so that a late message from an earlier
      # episode's operation is never taken for one of these.
      id = ep.plan.n * 1_000 + index
      family = Enum.at(families, rem(index, length(families)))
      pid = Workload.start_op(ep.ctx, id, kind, family, settings)

      {id,
       %{
         id: id,
         kind: kind,
         family: family,
         pid: pid,
         monitor: Process.monitor(pid),
         ready: nil,
         owner: pid,
         sockets: [],
         done: nil,
         iterations: 0,
         last_round_at: nil,
         handoffs: 0,
         casualties: [],
         killed: false,
         victim: false
       }}
    end)
  end

  defp waiting(ep) do
    Enum.map_join(for({_id, %{ready: nil, done: nil} = op} <- ep.ops, do: op), ", ", &label/1)
  end

  defp all_ready?(ep),
    do: Enum.all?(ep.ops, fn {_id, op} -> op.ready != nil or op.done != nil end)

  defp describe_ops(ops) do
    ops
    |> Map.values()
    |> Enum.frequencies_by(& &1.kind)
    |> Enum.map_join(", ", fn {kind, count} -> "#{kind} x#{count}" end)
  end

  defp label(op), do: "#{op.kind} ##{op.id} over #{op.family}"

  # Faults

  defp inject(%{plan: %{fault: fault}} = ep) when fault in [:helper_kill, :link_kill] do
    ep =
      case fault do
        :helper_kill ->
          ep = mark_injected(ep, "kill -KILL #{ep.net.helper}, the TUN helper")
          _output = System.cmd("kill", ["-KILL", Integer.to_string(ep.net.helper)])
          ep

        :link_kill ->
          ep = mark_injected(ep, "Process.exit(link, :kill)")
          Process.exit(ep.net.link, :kill)
          ep
      end

    ep =
      ep
      |> pump_until(&(&1.link_down_at != nil), now() + ep.settings.stop_deadline)
      |> check_link_exit()

    cond do
      ep.link_down_at == nil ->
        fail(ep, :hang, "the link still ran #{ep.settings.stop_deadline} ms after its #{fault}")

      ep.plan.policy == :stop ->
        after_stop(%{ep | stopped_at: ep.link_down_at, stop_cause: :link_lost})

      true ->
        observe_marked_down(ep)
    end
  end

  defp inject(%{plan: %{fault: :stop_stack}} = ep) do
    ep |> mark_injected("SmolNet.stop_stack/1") |> stop_stack("the fault")
  end

  defp inject(ep), do: ep |> disrupt() |> recover()

  # TunLink documents the reason it exits with when its helper does.
  defp check_link_exit(%{link_down_at: nil} = ep), do: ep
  defp check_link_exit(%{plan: %{fault: :link_kill}, link_reason: :killed} = ep), do: ep

  defp check_link_exit(
         %{plan: %{fault: :helper_kill}, link_reason: {:helper_exit, _status}} = ep
       ),
       do: ep

  defp check_link_exit(ep) do
    fail(ep, :link_exit, "after #{ep.plan.fault}, the link exited with #{short(ep.link_reason)}")
  end

  # A link that died under :mark_down or :notify: the stack must run on,
  # marked down, and what it still holds is what a link that could be
  # replaced would have saved (#43). No link can replace this one, so the
  # stack is then stopped, and a new one started.
  defp observe_marked_down(ep) do
    policy = ep.plan.policy
    deadline = now() + ep.settings.stop_deadline

    ep =
      if policy == :notify,
        do: ep |> pump_until(&(&1.notified != nil), deadline) |> check_notified(),
        else: ep

    ep = pump_for(ep, ep.settings.observe)
    info = stack_info(ep)
    ep = remember(ep, info)

    ep =
      cond do
        ep.stack_down_at != nil ->
          fail(ep, :link_down_policy, "the stack stopped under #{policy} when its link died")

        not match?({:ok, %{link_status: :down}}, info) ->
          fail(
            ep,
            :link_down_policy,
            "under #{policy}, the stack reports #{inspect(info, limit: 5)}"
          )

        true ->
          ep
      end

    %{ep | finding: finding(ep, info)} |> stop_stack("its link is gone for good (#43)")
  end

  defp check_notified(%{notified: nil} = ep) do
    fail(ep, :no_notify, "{:notify, pid} sent nothing within #{ep.settings.stop_deadline} ms")
  end

  defp check_notified(ep), do: ep

  defp finding(ep, info) do
    since = ep.link_down_at

    %{
      episode: ep.plan.n,
      fault: ep.plan.fault,
      policy: ep.plan.policy,
      link_down_ms: ep.link_down_at - ep.injected_at,
      sockets_before: ep.before.sockets,
      sockets_alive: sockets(info) || 0,
      calls_before: ep.before.calls,
      calls_alive: pending_calls(ep),
      loops_alive: live_loops(ep),
      ended_while_down:
        for(
          {_id, %{done: {result, at}} = op} <- ep.ops,
          at >= since,
          do: "#{label(op)}: #{short(result)}"
        ),
      dropped_egress: (dropped(info) || 0) - (ep.before.dropped_egress || 0),
      restart_ms: nil
    }
  end

  defp mark_injected(ep, detail) do
    info = stack_info(ep)

    before = %{
      sockets: sockets(info),
      dropped_egress: dropped(info),
      calls: pending_calls(ep),
      loops: live_loops(ep)
    }

    %{remember(ep, info) | before: before, injected_at: now()} |> event(:inject, detail)
  end

  defp stop_stack(ep, why) do
    ep = event(ep, :stop_stack, "SmolNet.stop_stack/1: #{why}")
    started = now()
    stack = ep.net.stack
    ep = %{ep | stopped_at: started, stop_cause: :stopped}
    deadline = ep.settings.stop_deadline

    ep =
      case timed(fn -> SmolNet.stop_stack(stack) end, deadline) do
        {:ok, :ok} ->
          event(ep, :stopped, "stop_stack returned :ok in #{now() - started} ms")

        {:ok, other} ->
          fail(ep, :stop_stack, "stop_stack returned #{inspect(other)}")

        {:exit, reason} ->
          fail(ep, :stop_stack, "stop_stack exited: #{inspect(reason)}")

        {:timeout, info} ->
          fail(ep, :hang, "stop_stack did not return within #{deadline} ms", [
            {"its caller", info}
          ])
      end

    after_stop(ep)
  end

  # Calls `fun` in a process of its own, for at most `timeout` ms.
  defp timed(fun, timeout) do
    parent = self()
    {pid, monitor} = spawn_monitor(fn -> send(parent, {self(), :timed, fun.()}) end)

    receive do
      {^pid, :timed, result} ->
        Process.demonitor(monitor, [:flush])
        {:ok, result}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:exit, reason}
    after
      timeout ->
        info = Process.info(pid, [:current_stacktrace, :status, :messages])
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])
        {:timeout, info}
    end
  end

  # Faults that leave the stack running.

  defp disrupt(%{plan: %{fault: :owner_kill}} = ep) do
    targets = ep.plan.params.targets
    ep = mark_injected(ep, "killing the owners of the #{Enum.join(targets, ", ")} sockets")
    victims = for {_id, op} <- ep.ops, op.kind in targets, op.done == nil, do: op
    %{kill_owners(ep, victims) | lifted_at: now()}
  end

  defp disrupt(%{plan: %{fault: :owner_swap}} = ep) do
    swaps = ep.plan.params.swaps

    case Enum.find(Map.values(ep.ops), &(&1.kind == :stream and &1.done == nil)) do
      nil ->
        fail(ep, :setup, "the stream had ended before it could be handed on")

      stream ->
        ep = mark_injected(ep, "handing the stream's socket on #{swaps} times")
        send(stream.pid, {:swap, swaps})
        target = stream.handoffs + swaps
        ep = await_swaps(ep, stream.id, target, now() + ep.settings.transfer_timeout)
        %{ep | lifted_at: now()}
    end
  end

  defp disrupt(ep) do
    {apply, lift, detail} = fault_actions(ep)
    ep = %{mark_injected(ep, detail) | active: lift}

    case apply.() do
      :ok -> ep |> pump_for(ep.plan.hold_ms) |> lift()
      {:error, reason} -> ep |> fail(:fault_injection, "could not inject: #{reason}") |> lift()
    end
  end

  defp lift(%{active: nil} = ep), do: ep

  defp lift(%{active: lift} = ep) do
    ep = %{ep | active: nil, lifted_at: now()}

    case lift.() do
      :ok -> event(ep, :lift, "#{ep.plan.fault} lifted")
      {:error, reason} -> fail(ep, :fault_injection, "could not lift #{ep.plan.fault}: #{reason}")
    end
  end

  # Linux drops a device's IPv6 addresses when it goes down, and with them
  # the host's end of a handshake in flight, which SmolNet, without
  # keepalive (#133), then waits on for good. keep_addr_on_down keeps them,
  # as the IPv4 ones are kept, so that a flap is only a flap.
  defp fault_actions(%{plan: %{fault: :device_flap}} = ep) do
    device = ep.context.device
    keep = "/proc/sys/net/ipv6/conf/#{device}/keep_addr_on_down"

    kept =
      case File.read(keep) do
        {:ok, value} -> String.trim(value)
        {:error, _no_ipv6} -> nil
      end

    down = fn ->
      with :ok <- write_sysctl(keep, kept && "1"), do: ip(["link", "set", "dev", device, "down"])
    end

    up = fn ->
      with :ok <- ip(["link", "set", "dev", device, "up"]),
           :ok <- restore_address(device),
           do: write_sysctl(keep, kept)
    end

    {down, up, "ip link set dev #{device} down, for #{ep.plan.hold_ms} ms"}
  end

  defp fault_actions(%{plan: %{fault: :route_flap, params: %{route: type}}} = ep) do
    routes = Enum.map(ep.context.families, &host_route/1)

    change = fn verb ->
      all_ok(routes, fn {prefix, to} -> ip(prefix ++ [verb, Atom.to_string(type), to]) end)
    end

    {fn -> change.("add") end, fn -> change.("del") end,
     "#{type} routes to #{Enum.map_join(routes, " and ", &elem(&1, 1))}, for #{ep.plan.hold_ms} ms"}
  end

  defp fault_actions(%{plan: %{params: %{starve: starve} = params}} = ep) do
    mode = if starve == :stop, do: :stop, else: {starve, params.ms}
    link = ep.net.link

    {fn -> starve(link, mode) end, fn -> starve(link, :off) end,
     "TunLink.starve(link, #{inspect(mode)}), for #{ep.plan.hold_ms} ms"}
  end

  defp starve(link, mode) do
    case TunLink.starve(link, mode) do
      :ok -> :ok
      {:error, reason} -> {:error, inspect(reason)}
    end
  catch
    :exit, reason -> {:error, "the link is gone: #{inspect(reason, limit: 5)}"}
  end

  defp host_route(:inet), do: {["route"], "10.77.0.2/32"}
  defp host_route(:inet6), do: {["-6", "route"], "fd00:77::2/128"}

  defp write_sysctl(_path, nil), do: :ok

  defp write_sysctl(path, value) do
    case File.write(path, value) do
      :ok -> :ok
      {:error, reason} -> {:error, "cannot write #{path}: #{:file.format_error(reason)}"}
    end
  end

  # Should the address have gone all the same, it is put back.
  defp restore_address(device) do
    if File.exists?("/proc/sys/net/ipv6") do
      address = :inet6 |> Network.host_address() |> :inet.ntoa() |> to_string()
      ip(["-6", "addr", "replace", "#{address}/64", "dev", device, "nodad"])
    else
      :ok
    end
  end

  # Runs `fun` on every item, and returns the first error, if any.
  defp all_ok(items, fun), do: items |> Enum.map(fun) |> Enum.find(:ok, &(&1 != :ok))

  # Kills each victim's socket owner, and checks that the adapter of every
  # socket it owned goes with it.
  defp kill_owners(ep, victims) do
    adapters =
      for op <- victims, adapter <- adapters(op.sockets), into: %{} do
        monitor = Process.monitor(adapter)
        in_place(adapter)
        {monitor, %{op: op.id, pid: adapter, down_at: nil}}
      end

    ep =
      Enum.reduce(victims, %{ep | adapters: Map.merge(ep.adapters, adapters)}, fn op, ep ->
        Process.exit(op.owner, :kill)
        # A blocked call's owner is the operation itself. The stream's is its
        # reader, which may already have finished its round, so the stream
        # may run on and is still judged; it may also end for its owner.
        if op.kind == :stream,
          do: put_op(ep, op.id, victim: true),
          else: put_op(ep, op.id, killed: true)
      end)

    names = Enum.map_join(victims, ", ", &label/1)
    ep = event(ep, :killed, "the owners of #{names}, with #{map_size(adapters)} adapters")
    deadline = ep.settings.stop_deadline
    ep = pump_until(ep, &adapters_down?/1, now() + deadline)

    case for {_monitor, %{down_at: nil} = adapter} <- ep.adapters, do: adapter do
      [] ->
        ep

      alive ->
        info = Enum.map(alive, &{&1.pid, Process.info(&1.pid, [:current_stacktrace, :status])})
        summary = "#{length(alive)} adapters outlived their socket's owner by #{deadline} ms"
        fail(ep, :orphan, summary, [{"adapters", info}])
    end
  end

  defp adapters(sockets),
    do: for({:"$inet", _module, adapter} when is_pid(adapter) <- sockets, do: adapter)

  defp adapters_down?(ep),
    do: Enum.all?(ep.adapters, fn {_monitor, adapter} -> adapter.down_at end)

  # A monitor is in place once a call made after it has been answered.
  defp in_place(pid) do
    _state = :sys.get_state(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end

  # A swap asked for after the reader has read everything is lost with it,
  # so each round that ends short of the target asks again.
  defp await_swaps(ep, id, target, deadline) do
    op = ep.ops[id]

    cond do
      op.handoffs >= target ->
        event(ep, :swapped, "the stream's socket was handed on #{op.handoffs} times")

      op.done != nil or now() >= deadline ->
        fail(
          ep,
          :no_recovery,
          "the stream's socket was handed on #{op.handoffs} of #{target} times"
        )

      true ->
        rounds = op.iterations

        ep =
          pump_until(
            ep,
            fn ep ->
              op = ep.ops[id]
              op.handoffs >= target or op.iterations > rounds or op.done != nil
            end,
            deadline
          )

        op = ep.ops[id]

        if op.handoffs < target and op.done == nil,
          do: send(op.pid, {:swap, target - op.handoffs})

        await_swaps(ep, id, target, deadline)
    end
  end

  # After a fault that leaves the stack running: it must still run, every
  # loop must complete a round, the credit must all come back, and then the
  # stack is stopped, its blocked calls blocked still or their owners gone.
  defp recover(ep) do
    ep
    |> check_running()
    |> await_rounds()
    |> quiesce()
    |> check_credit()
    |> release()
    |> stop_stack("the end of the episode")
  end

  defp check_running(ep) do
    if ep.stack_down_at == nil and ep.link_down_at == nil,
      do: ep,
      else: fail(ep, :unexpected_stop, "the stack or its link stopped during #{ep.plan.fault}")
  end

  defp await_rounds(%{lifted_at: nil} = ep), do: ep

  defp await_rounds(ep) do
    since = ep.lifted_at
    deadline = since + ep.settings.transfer_timeout
    ep = pump_until(ep, &(stale_loops(&1, since) == []), deadline)

    case stale_loops(ep, since) do
      [] ->
        event(ep, :recovered, "every loop completed a round in #{now() - since} ms")

      stale ->
        summary =
          "#{Enum.map_join(stale, ", ", &label/1)} completed no round in the " <>
            "#{ep.settings.transfer_timeout} ms after #{ep.plan.fault} was lifted"

        fail(ep, :no_recovery, summary, [{"operations", Enum.map(stale, &op_info/1)}])
    end
  end

  defp stale_loops(ep, since) do
    for {_id, op} <- ep.ops,
        loop?(op),
        op.done == nil,
        not op.killed,
        op.last_round_at == nil or op.last_round_at < since,
        do: op
  end

  # Stops the loops, each at the end of its round.
  defp quiesce(ep) do
    loops = for {_id, op} <- ep.ops, loop?(op), op.done == nil, not op.killed, do: op
    Enum.each(loops, &send(&1.pid, :stop))
    done? = fn ep -> Enum.all?(loops, &(ep.ops[&1.id].done != nil)) end
    ep = pump_until(ep, done?, now() + ep.settings.transfer_timeout)

    case Enum.filter(loops, &(ep.ops[&1.id].done == nil)) do
      [] ->
        ep

      stuck ->
        summary = "#{Enum.map_join(stuck, ", ", &label/1)} did not stop at the end of a round"
        fail(ep, :hang, summary, [{"operations", Enum.map(stuck, &op_info/1)}])
    end
  end

  # Once the traffic has stopped, the credit must all come back: none held
  # back by the link, none in flight, and the stack's whole again.
  defp check_credit(%{settings: %{credit: :infinity}} = ep), do: ep

  defp check_credit(ep) do
    {packets, bytes} = ep.settings.credit
    whole = %{credit: %{packets: packets, bytes: bytes}, in_flight: 0, withheld: 0}

    case poll(fn -> credit_state(ep) end, &(&1 == whole), now() + @credit_timeout) do
      {:ok, _state} ->
        ep

      {:timeout, state} ->
        summary =
          "#{@credit_timeout} ms after the traffic stopped, the credit was #{inspect(state)}"

        fail(ep, :credit_leak, summary)
    end
  end

  defp credit_state(ep) do
    stats = link_stats(ep)

    credit =
      case stack_info(ep) do
        {:ok, %{native: %{result: %{egress_credit: credit}}}} -> credit
        other -> other
      end

    %{
      credit: credit,
      in_flight: stats[:in_flight_packets],
      withheld: stats[:credit_withheld_packets]
    }
  end

  # Under the :release teardown, the blocked calls' owners are killed and
  # the servers stopped before the stack is: every socket must then close.
  defp release(%{plan: %{teardown: :release}} = ep) do
    # The blocked calls, and any loop that did not stop, already a failure.
    live = for {_id, op} <- ep.ops, op.done == nil, not op.killed, do: op
    ep = ep |> kill_owners(live) |> stop_servers()
    deadline = ep.settings.stop_deadline

    case poll(fn -> open_sockets(ep) end, &(&1 == 0), now() + deadline) do
      {:ok, _open} ->
        event(ep, :released, "every socket closed once its owner was gone")

      {:timeout, open} ->
        summary =
          "#{inspect(open)} sockets were still open #{deadline} ms after every owner was gone"

        fail(ep, :socket_leak, summary)
    end
  end

  defp release(ep), do: ep

  defp open_sockets(ep) do
    case stack_info(ep) do
      {:ok, %{native: %{result: result}}} ->
        result.native_socket_count - result.closing_tcp_socket_count

      other ->
        other
    end
  end

  defp poll(fun, ok?, deadline) do
    value = fun.()

    cond do
      ok?.(value) ->
        {:ok, value}

      now() >= deadline ->
        {:timeout, value}

      true ->
        Process.sleep(@poll)
        poll(fun, ok?, deadline)
    end
  end

  # Judging

  # The stack stopped, or was told to, at stopped_at: its DOWN must
  # arrive, and every call still blocked return, within the stop deadline.
  defp after_stop(ep) do
    deadline = ep.stopped_at + ep.settings.stop_deadline
    ep = pump_until(ep, &(&1.stack_down_at != nil and live_ops(&1) == []), deadline)
    ep |> check_down() |> judge_ops()
  end

  defp live_ops(ep), do: for({_id, %{done: nil, killed: false} = op} <- ep.ops, do: op)

  defp check_down(%{stack_down_at: nil} = ep) do
    summary =
      "SmolNet.monitor/1 delivered no DOWN within #{ep.settings.stop_deadline} ms of the " <>
        "stack stopping"

    fail(ep, :missed_down, summary, [{"bundle", Process.info(ep.net.pids.bundle)}])
  end

  defp check_down(ep), do: %{ep | down_ms: max(0, ep.stack_down_at - ep.stopped_at)}

  defp judge_ops(ep) do
    ops = ep.ops |> Map.values() |> Enum.sort_by(& &1.id)
    ep = Enum.reduce(ops, ep, &judge_casualties/2)
    Enum.reduce(ops, ep, &judge/2)
  end

  # A flap may cost the rounds it catches, but not one begun once it was
  # lifted.
  defp judge_casualties(op, ep) do
    lifted = ep.lifted_at
    {late, caught} = Enum.split_with(op.casualties, &(lifted != nil and &1.started >= lifted))
    ep = %{ep | casualties: ep.casualties + length(caught)}

    case late do
      [] ->
        ep

      [casualty | _more] ->
        summary =
          "#{label(op)}: a round begun #{casualty.started - lifted} ms after the flap was " <>
            "lifted failed: #{short(casualty.error)}"

        fail(ep, :unexpected_error, summary, [{label(op), op_info(op)}])
    end
  end

  defp judge(%{done: nil, killed: true}, ep), do: ep

  defp judge(%{done: nil} = op, ep) do
    summary =
      "#{label(op)} was still blocked #{ep.settings.stop_deadline} ms after the stack stopped"

    fail(ep, :hang, summary, [{label(op), op_info(op)}])
  end

  defp judge(%{done: {{:crashed, reason}, _at}} = op, ep) do
    fail(ep, :op_crashed, "#{label(op)} crashed: #{short(reason)}", [{label(op), reason}])
  end

  # A stack that stops ends an active socket with a bare tcp_closed, as if
  # the peer had closed it, so a stream it cuts short reads as complete but
  # short. After stop_stack/1 that is the contract; after the link dies
  # under :stop the owner should see tcp_error :enetdown first (#136).
  # Either way it is recorded, not failed.
  defp judge(%{done: {{:error, {:integrity, detail}}, at}, kind: :stream} = op, ep)
       when at >= ep.stopped_at do
    ep =
      event(
        ep,
        :cut,
        "#{label(op)}: tcp_closed after #{detail.bytes} of #{detail.expected} bytes"
      )

    %{ep | returns: Map.put(ep.returns, op.kind, "tcp_closed mid-stream, as if at its end")}
  end

  defp judge(%{done: {{:error, {:integrity, detail}}, _at}} = op, ep) do
    fail(ep, :integrity, "#{label(op)}: a body's length or SHA-256 was wrong", [
      {"detail", detail}
    ])
  end

  defp judge(%{done: {result, at}} = op, ep) do
    cond do
      at >= ep.stopped_at -> judge_return(op, result, at, ep)
      at < healthy_until(ep) -> judge_early(op, result, ep)
      # It ended while the link was down; the finding records it.
      true -> ep
    end
  end

  @stopping [:helper_kill, :link_kill, :stop_stack]

  defp healthy_until(%{injected_at: injected, plan: %{fault: fault}} = ep)
       when fault in @stopping and injected != nil,
       do: min(injected, ep.stopped_at)

  defp healthy_until(ep), do: ep.stopped_at

  defp judge_early(_op, :stopped, ep), do: ep
  defp judge_early(%{victim: true, kind: :stream}, {:error, {:owner_died, _reason}}, ep), do: ep

  defp judge_early(op, result, ep) do
    summary =
      "#{label(op)} ended with #{short(result)} while the stack should have run on " <>
        "(#{ep.plan.fault})"

    fail(ep, :unexpected_error, summary, [{label(op), op_info(op)}])
  end

  defp judge_return(op, result, at, ep) do
    latency = at - ep.stopped_at
    ep = %{ep | slowest_return_ms: max(ep.slowest_return_ms || 0, latency)}

    cond do
      loop?(op) ->
        ep

      stopped_error?(result) ->
        %{ep | returns: Map.put(ep.returns, op.kind, describe_return(result))}

      true ->
        ep = %{ep | returns: Map.put(ep.returns, op.kind, describe_return(result))}

        summary =
          "#{label(op)} returned #{short(result)} when the stack stopped, not :closed or :enetdown"

        fail(ep, :wrong_error, summary, [{label(op), op_info(op)}])
    end
  end

  # A send that had made progress reports what it did not send, as
  # {reason, remainder}.
  defp stopped_error?({:error, {reason, rest}}) when is_binary(rest),
    do: stopped_error?({:error, reason})

  defp stopped_error?({:error, reason}), do: reason in [:closed, :enetdown]
  defp stopped_error?(_result), do: false

  defp describe_return({:error, {reason, rest}}) when is_binary(rest),
    do: "{:error, {#{inspect(reason)}, <#{byte_size(rest)} bytes unsent>}}"

  defp describe_return(result), do: short(result)

  # Teardown, whatever happened before it.

  defp teardown(ep) do
    ep = lift(ep)
    ep = if ep.stopped_at == nil, do: stop_stack(ep, "teardown"), else: ep

    ep
    |> kill_leftovers()
    |> stop_servers()
    |> check_orphans()
    |> clean_up()
    |> stop_capture()
  end

  defp kill_leftovers(ep) do
    for {_id, op} <- ep.ops, pid <- Enum.uniq([op.pid, op.owner]), do: Process.exit(pid, :kill)
    ep
  end

  defp stop_servers(%{servers: nil} = ep), do: ep

  defp stop_servers(ep) do
    Workload.stop_servers(ep.servers)
    %{ep | servers: nil}
  end

  # Once the stack has stopped, nothing of it may be left: its bundle,
  # stack and adapters, the link and its helper, and no more processes
  # than there were before the episode, give or take :ssl's own.
  defp check_orphans(ep) do
    deadline = ep.settings.stop_deadline

    ep =
      case poll(fn -> survivors(ep) end, &(&1 == []), now() + deadline) do
        {:ok, []} ->
          ep

        {:timeout, alive} ->
          names = Enum.map_join(alive, ", ", fn {name, _pid} -> to_string(name) end)
          info = for {name, pid} <- alive, is_pid(pid), do: {name, Process.info(pid)}

          fail(ep, :orphan, "#{names} still running #{deadline} ms after the stack stopped", [
            {"survivors", info}
          ])
      end

    extra = fn -> length(Process.list() -- ep.baseline.processes) end

    case poll(extra, &(&1 <= @process_slack), now() + @process_timeout) do
      {:ok, _count} ->
        ep

      {:timeout, count} ->
        new = Enum.take(Process.list() -- ep.baseline.processes, 20)

        info =
          Enum.map(new, &{&1, Process.info(&1, [:initial_call, :current_function, :dictionary])})

        summary =
          "#{count} more processes than before the episode, #{@process_timeout} ms after it"

        fail(ep, :orphan, summary, [{"the first new processes", info}])
    end
  end

  defp survivors(%{net: nil}), do: []

  defp survivors(%{net: net} = ep) do
    adapters = for {_id, op} <- ep.ops, adapter <- adapters(op.sockets), do: {:adapter, adapter}

    processes =
      [
        bundle: net.pids.bundle,
        stack: net.pids.stack,
        inet_backends: net.pids.inet_backends,
        link: net.link
      ] ++ adapters

    alive = for {name, pid} <- processes, Process.alive?(pid), do: {name, pid}
    helper = if os_alive?(net.helper), do: [helper: net.helper], else: []
    stacks = if bundles() > ep.baseline.bundles, do: [stacks: bundles()], else: []
    alive ++ helper ++ stacks
  end

  defp os_alive?(nil), do: false
  defp os_alive?(pid), do: File.exists?("/proc/#{pid}")

  defp bundles, do: DynamicSupervisor.count_children(SmolNet.Supervisor).active

  # Whatever the checks found, the next episode starts clean.
  defp clean_up(%{net: nil} = ep), do: ep

  defp clean_up(%{net: net} = ep) do
    if Process.alive?(net.pids.bundle), do: timed(fn -> SmolNet.stop_stack(net.stack) end, 10_000)
    if Process.alive?(net.link), do: Process.exit(net.link, :kill)

    if os_alive?(net.helper),
      do: System.cmd("kill", ["-KILL", Integer.to_string(net.helper)], stderr_to_stdout: true)

    ep
  end

  # The device's capture runs in a process of its own, which owns its port.
  defp start_capture(%{context: %{mode: :smolnet}} = ep) do
    parent = self()
    options = [snaplen: ep.settings.snaplen, immediate: true]
    device = ep.context.device
    directory = capture_dir(ep)
    pid = spawn(fn -> capture(parent, device, directory, options) end)

    receive do
      {^pid, :capturing} -> %{ep | capture: pid}
      {^pid, {:error, message}} -> event(ep, :capture, "no capture: #{message}")
    after
      10_000 ->
        Process.exit(pid, :kill)
        event(ep, :capture, "no capture: tcpdump did not start")
    end
  end

  defp start_capture(ep), do: ep

  defp capture(parent, device, directory, options) do
    case Pcap.start(device, directory, options) do
      {:ok, pcap} ->
        send(parent, {self(), :capturing})

        receive do
          {:stop, from} ->
            Pcap.stop(pcap)
            send(from, {self(), :stopped})
        end

      {:error, message} ->
        send(parent, {self(), {:error, message}})
    end
  end

  defp stop_capture(%{capture: nil} = ep), do: ep

  defp stop_capture(%{capture: pid} = ep) do
    send(pid, {:stop, self()})

    receive do
      {^pid, :stopped} -> :ok
    after
      15_000 -> Process.exit(pid, :kill)
    end

    %{ep | capture: nil}
  end

  defp capture_dir(ep), do: Path.join([ep.context.out_dir, "captures", number(ep.plan.n)])

  defp number(n), do: n |> Integer.to_string() |> String.pad_leading(2, "0")

  # A fresh stack, started straight after, must work. How long that takes
  # is what replacing a stack costs, next to re-attaching a link (#43).
  defp fresh_check(ep) do
    started = now()
    failures = length(ep.failures)

    case start_network(ep, :stop) do
      {:ok, net} ->
        ep = probe(%{ep | ctx: %{ep.context | stack: net.stack, link: net.link}}, :fresh_stack)
        ms = now() - started
        _stopped = timed(fn -> SmolNet.stop_stack(net.stack) end, ep.settings.stop_deadline)
        monitor = net.bundle_monitor

        ep =
          receive do
            {:DOWN, ^monitor, :process, _object, _reason} -> ep
          after
            ep.settings.stop_deadline ->
              fail(ep, :missed_down, "a fresh stack, stopped, delivered no DOWN")
          end

        finding = ep.finding && %{ep.finding | restart_ms: ms}
        ep = %{ep | fresh_ms: ms, finding: finding}

        if length(ep.failures) == failures,
          do: event(ep, :fresh, "a fresh stack worked #{ms} ms after it was started"),
          else: ep

      {:error, reason} ->
        fail(ep, :fresh_stack, "could not start a fresh stack: #{inspect(reason)}")
    end
  end

  defp finish(ep) do
    failed? = ep.failures != []
    file = if failed?, do: write_report(ep)
    unless failed?, do: File.rm_rf(capture_dir(ep))

    kinds = ep.failures |> Enum.map(& &1.kind) |> Enum.uniq() |> Enum.join(", ")
    ep = event(ep, :end, if(failed?, do: "failed: #{kinds}", else: "passed"))

    record = %{
      n: ep.plan.n,
      fault: ep.plan.fault,
      policy: ep.plan.policy,
      plan: ep.plan,
      outcome: if(failed?, do: :fail, else: :pass),
      failures: ep.failures |> Enum.reverse() |> Enum.map(&Map.take(&1, [:kind, :summary])),
      file: file,
      injected: ep.injected_at != nil,
      returns: ep.returns,
      slowest_return_ms: ep.slowest_return_ms,
      down_ms: ep.down_ms,
      fresh_ms: ep.fresh_ms,
      transfers: ep.transfers,
      connections: ep.connections,
      casualties: ep.casualties,
      finding: ep.finding
    }

    {record, Enum.reverse(ep.events)}
  end

  defp write_report(ep) do
    file = Path.join("episodes", "#{number(ep.plan.n)}-#{ep.plan.fault}-#{ep.plan.policy}.txt")
    path = Path.join(ep.context.out_dir, file)
    File.mkdir_p!(Path.dirname(path))

    failures =
      for failure <- Enum.reverse(ep.failures) do
        ["### #{failure.kind}: #{failure.summary}\n\n" | Enum.map(failure.details, &section/1)]
      end

    sections = [
      {"plan", ep.plan},
      {"timeline", Enum.reverse(ep.events)},
      {"operations", ep.ops |> Map.values() |> Enum.sort_by(& &1.id)},
      {"the last stack_info", ep.last_info}
    ]

    header = "episode #{ep.plan.n}: #{ep.plan.fault} under #{ep.plan.policy}\n\n"
    File.write!(path, [header, "## failures\n\n", failures | Enum.map(sections, &section/1)])
    file
  end

  defp section({title, term}) do
    [
      "## ",
      title,
      "\n\n",
      inspect(term, pretty: true, limit: :infinity, printable_limit: 4_096),
      "\n\n"
    ]
  end

  # Messages

  defp pump_until(ep, done?, deadline) do
    wait = deadline - now()

    cond do
      done?.(ep) ->
        ep

      wait <= 0 ->
        ep

      true ->
        receive do
          message -> ep |> absorb(message) |> pump_until(done?, deadline)
        after
          wait -> ep
        end
    end
  end

  defp pump_for(ep, ms), do: pump_until(ep, fn _ep -> false end, now() + ms)

  defp absorb(ep, {:op, id, kind, value, at}) do
    case ep.ops do
      %{^id => op} -> op_event(ep, op, kind, value, at)
      _other -> ep
    end
  end

  defp absorb(%{net: %{bundle_monitor: monitor}} = ep, {:DOWN, monitor, _type, _object, reason}) do
    %{ep | stack_down_at: now()}
    |> event(:stack_down, "DOWN from SmolNet.monitor/1: #{short(reason)}")
  end

  defp absorb(%{net: %{link_monitor: monitor}} = ep, {:DOWN, monitor, _type, _object, reason}) do
    %{ep | link_down_at: now(), link_reason: reason}
    |> event(:link_down, "the link exited: #{short(reason)}")
  end

  defp absorb(ep, {:DOWN, monitor, :process, _pid, reason}) do
    cond do
      Map.has_key?(ep.adapters, monitor) ->
        put_in(ep.adapters[monitor].down_at, now())

      op = Enum.find(Map.values(ep.ops), &(&1.monitor == monitor and &1.done == nil)) ->
        if op.killed, do: ep, else: put_op(ep, op.id, done: {{:crashed, reason}, now()})

      true ->
        ep
    end
  end

  defp absorb(ep, {:smol_stack, _link_ref, :link_down, reason}) do
    %{ep | notified: now()} |> event(:notified, "{:notify, pid} sent link_down: #{short(reason)}")
  end

  defp absorb(ep, _message), do: ep

  defp op_event(ep, op, :ready, value, at),
    do: put_op(ep, op.id, ready: at, owner: value.owner, sockets: value.sockets)

  defp op_event(ep, op, :owner, value, _at) do
    handoffs = if value.sockets == op.sockets, do: op.handoffs + 1, else: op.handoffs
    put_op(ep, op.id, owner: value.owner, sockets: value.sockets, handoffs: handoffs)
  end

  defp op_event(ep, op, :iteration, _round, at) do
    ep = put_op(ep, op.id, iterations: op.iterations + 1, last_round_at: at)

    case op.kind do
      :churn -> %{ep | connections: ep.connections + 1}
      _transfer -> %{ep | transfers: ep.transfers + 1}
    end
  end

  defp op_event(ep, op, :casualty, casualty, at) do
    detail =
      "#{label(op)}: a round begun #{at - casualty.started} ms before failed: #{short(casualty.error)}"

    ep |> put_op(op.id, casualties: [casualty | op.casualties]) |> event(:casualty, detail)
  end

  defp op_event(ep, op, :done, result, at) do
    ep |> put_op(op.id, done: {result, at}) |> event(:returned, "#{label(op)}: #{short(result)}")
  end

  # State

  defp event(ep, name, detail) do
    Soak.log(ep.context, "episode #{ep.plan.n}: #{name}: #{detail}")
    at = now() - ep.context.started_at
    entry = %{t_ms: at, episode: ep.plan.n, event: name, detail: detail}
    %{ep | events: [entry | ep.events]}
  end

  # A failure's details are taken now, while the state they describe holds.
  defp fail(ep, kind, summary, details \\ []) do
    ep = event(ep, :failure, "#{kind}: #{summary}")
    snapshot = [{"stack_info", ep.net && stack_info(ep)}, {"link", ep.net && link_stats(ep)}]
    failure = %{kind: kind, summary: summary, details: details ++ snapshot}
    %{ep | failures: [failure | ep.failures]}
  end

  defp stack_info(%{net: nil}), do: {:error, :no_stack}

  defp stack_info(ep) do
    SmolNet.stack_info(ep.net.stack)
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp remember(ep, {:ok, _info} = info), do: %{ep | last_info: info}
  defp remember(ep, _unavailable), do: ep

  defp link_stats(ep) do
    TunLink.stats(ep.net.link)
  catch
    :exit, _reason -> %{}
  end

  defp sockets({:ok, %{native: %{result: %{native_socket_count: count}}}}), do: count
  defp sockets(_unavailable), do: nil

  defp dropped({:ok, %{dropped_egress: dropped}}), do: dropped
  defp dropped(_unavailable), do: nil

  defp pending_calls(ep),
    do: Enum.count(ep.ops, fn {_id, op} -> not loop?(op) and op.done == nil and not op.killed end)

  defp live_loops(ep), do: Enum.count(ep.ops, fn {_id, op} -> loop?(op) and op.done == nil end)

  defp loop?(op), do: op.kind in [:bulk, :churn, :stream]

  defp put_op(ep, id, changes), do: update_in(ep.ops[id], &Map.merge(&1, Map.new(changes)))

  defp op_info(op) do
    keys = [:current_stacktrace, :status, :message_queue_len]

    %{
      op: Map.drop(op, [:monitor]),
      process: Process.info(op.pid, keys),
      owner: Process.info(op.owner, keys)
    }
  end

  defp short(term), do: inspect(term, limit: 8, printable_limit: 160)

  defp now, do: System.monotonic_time(:millisecond)
end
