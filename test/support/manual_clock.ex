defmodule SmolNet.Test.ManualClock do
  @moduledoc false

  @behaviour SmolNet.Stack.Clock

  def start(now \\ 0) do
    Agent.start(fn -> %{now: now, timers: %{}} end)
  end

  def advance(clock, milliseconds) do
    due =
      Agent.get_and_update(clock, fn state ->
        now = state.now + milliseconds

        {due, pending} =
          Enum.split_with(state.timers, fn {_ref, timer} -> timer.deadline <= now end)

        {due, %{state | now: now, timers: Map.new(pending)}}
      end)

    due
    |> Enum.sort_by(fn {_ref, timer} -> timer.deadline end)
    |> Enum.each(fn {_ref, timer} -> send(timer.pid, timer.message) end)

    :ok
  end

  @impl true
  def now do
    Agent.get(clock(), & &1.now)
  end

  @impl true
  def send_after(pid, message, delay) do
    reference = make_ref()

    Agent.update(clock(), fn state ->
      timer = %{deadline: state.now + delay, pid: pid, message: message}
      %{state | timers: Map.put(state.timers, reference, timer)}
    end)

    reference
  end

  @impl true
  def cancel_timer(reference) do
    Agent.get_and_update(clock(), fn state ->
      case Map.pop(state.timers, reference) do
        {nil, _timers} -> {false, state}
        {_timer, timers} -> {false, %{state | timers: timers}}
      end
    end)
  end

  defp clock, do: Application.fetch_env!(:smolnet, :manual_clock)
end
