defmodule SmolNet.Test.FixtureAdapter do
  @moduledoc false

  use GenServer, restart: :temporary, significant: false

  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @impl true
  def init(options) do
    Process.flag(:trap_exit, true)
    send(Keyword.fetch!(options, :test), {:adapter_initialized, self()})
    {:ok, options}
  end

  @impl true
  def terminate(reason, options) do
    stack = Keyword.fetch!(options, :stack)
    send(Keyword.fetch!(options, :test), {:adapter_terminated, self(), reason, alive?(stack)})
  end

  defp alive?(pid), do: is_pid(pid) and Process.alive?(pid)
end
