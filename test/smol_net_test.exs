defmodule SmolNetTest do
  use ExUnit.Case, async: false

  test "the application starts one empty named DynamicSupervisor" do
    supervisor = Process.whereis(SmolNet.Supervisor)

    assert is_pid(supervisor)
    assert Process.alive?(supervisor)
    assert DynamicSupervisor.which_children(SmolNet.Supervisor) == []
  end

  test "the native library loads and responds" do
    assert SmolNet.Native.health() == :ok
  end
end
