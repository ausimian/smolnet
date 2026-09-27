defmodule SmolNet.Test.Monitoring do
  @moduledoc false

  @doc """
  Monitors `pid`, an OTP process, and returns once the monitor is in place.

  Use it where a test monitors one process and then signals another whose
  exit stops the first, and asserts the first's exit reason. The VM orders
  signals only between a pair of processes, and it may hold a new process
  monitor back until the caller next signals the same process or is
  scheduled out. A kill sent meanwhile to another process can then stop the
  monitored one before the monitor arrives, and the monitor reports
  `:noproc` rather than the exit reason (#109). The reply to a call made
  after the monitor shows that the monitor arrived first.
  """
  @spec monitor_in_place(pid()) :: reference()
  def monitor_in_place(pid) do
    monitor = Process.monitor(pid)
    _state = :sys.get_state(pid)
    monitor
  end
end
