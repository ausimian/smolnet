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

  test "validates stack options before starting a bundle" do
    assert SmolNet.start_stack(limits: %{bytes_copied: 0}) == {:error, :invalid_limits}

    assert SmolNet.start_stack(limits: %{bytes_copied: 16 * 1024 * 1024 + 1}) ==
             {:error, :invalid_limits}

    assert SmolNet.start_stack(limits: :invalid) == {:error, :invalid_limits}
    assert SmolNet.start_stack(:invalid) == {:error, :invalid_options}
    assert SmolNet.start_stack([:bad]) == {:error, :invalid_options}
    assert SmolNet.start_stack(limtis: %{}) == {:error, :invalid_options}

    assert SmolNet.start_stack(limits: %{}, limits: %{}) ==
             {:error, :invalid_options}

    assert DynamicSupervisor.which_children(SmolNet.Supervisor) == []
  end
end
