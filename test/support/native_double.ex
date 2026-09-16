defmodule SmolNet.Test.NativeDouble do
  @moduledoc false

  def stack_new(_limits, _config, _now) do
    test = Application.fetch_env!(:smolnet, :native_test_process)
    send(test, {:native_stack_new, self()})

    case Application.fetch_env!(:smolnet, :native_test_result) do
      :wait ->
        receive do
          {:native_test_reply, reply} -> reply
        end

      reply ->
        reply
    end
  end

  def stack_ingress(_resource, packet, _now) do
    test = Application.fetch_env!(:smolnet, :native_test_process)
    send(test, {:native_stack_ingress, self(), packet})

    case Application.get_env(:smolnet, :native_ingress_result, :default) do
      :default ->
        empty_effects()

      :wait ->
        receive do
          {:native_ingress_reply, reply} -> reply
        end

      reply ->
        reply
    end
  end

  def stack_poll(_resource, now) do
    test = Application.fetch_env!(:smolnet, :native_test_process)
    send(test, {:native_stack_poll, self(), now})

    case Application.get_env(:smolnet, :native_poll_result, empty_effects()) do
      :wait ->
        receive do
          {:native_poll_reply, reply} -> reply
        end

      reply ->
        reply
    end
  end

  def stack_snapshot(_resource) do
    {:ok, %{result: %{test_double: true}, output: [], poll_at: nil, more: false}}
  end

  def test_contention(_resource), do: {:error, :ownership_invariant_violation}

  def test_bounded_work(_resource, requested) do
    {:ok, %{result: requested, output: [], poll_at: nil, more: false}}
  end

  defp empty_effects do
    {:ok, %{result: :ok, output: [], poll_at: nil, more: false}}
  end
end
