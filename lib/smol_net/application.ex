defmodule SmolNet.Application do
  @moduledoc false

  use Application

  alias SmolNet.InetBackend.Monitor

  @impl true
  def start(_type, _args) do
    # Owned by the application's master, so it lives as long as the application.
    Monitor.create_table()

    DynamicSupervisor.start_link(
      strategy: :one_for_one,
      name: SmolNet.Supervisor
    )
  end
end
