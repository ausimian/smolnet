defmodule SmolNet.Stack.Clock do
  @moduledoc false

  @callback now() :: integer()
  @callback send_after(pid(), term(), non_neg_integer()) :: reference()
  @callback cancel_timer(reference()) :: boolean() | non_neg_integer()
end

defmodule SmolNet.Stack.Clock.System do
  @moduledoc false

  @behaviour SmolNet.Stack.Clock

  @impl true
  def now, do: System.monotonic_time(:millisecond)

  @impl true
  def send_after(pid, message, delay), do: Process.send_after(pid, message, delay)

  @impl true
  def cancel_timer(reference), do: Process.cancel_timer(reference)
end
