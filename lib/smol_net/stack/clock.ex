defmodule SmolNet.Stack.Clock do
  @moduledoc false

  @callback now() :: integer()
  @callback send_after(pid(), term(), non_neg_integer()) :: reference()
  @callback cancel_timer(reference()) :: boolean() | non_neg_integer()
end

defmodule SmolNet.Stack.Clock.System do
  @moduledoc false

  @behaviour SmolNet.Stack.Clock

  # Milliseconds since the VM started, not the raw monotonic clock. The value becomes
  # smoltcp's `Instant`, and the raw BEAM monotonic clock is negative (it starts near
  # -2^49 ms): smoltcp compares instants against timers initialised to time zero — the
  # challenge-ACK rate limiter is one — so with a negative "now" those gates never open,
  # and a TCP keep-alive probe from the peer is never answered.
  @impl true
  def now do
    System.convert_time_unit(
      :erlang.monotonic_time() - :erlang.system_info(:start_time),
      :native,
      :millisecond
    )
  end

  @impl true
  def send_after(pid, message, delay), do: Process.send_after(pid, message, delay)

  @impl true
  def cancel_timer(reference), do: Process.cancel_timer(reference)
end
