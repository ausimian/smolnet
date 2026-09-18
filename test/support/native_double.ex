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
    result = Application.get_env(:smolnet, :native_poll_result, empty_effects())

    unless match?({:countdown, _counter, :silent}, result) do
      send(test, {:native_stack_poll, self(), now})
    end

    case result do
      :wait ->
        receive do
          {:native_poll_reply, reply} -> reply
        end

      {:countdown, counter, :silent} ->
        more =
          Agent.get_and_update(counter, fn remaining ->
            {remaining > 1, max(remaining - 1, 0)}
          end)

        empty_effects(more: more)

      reply ->
        reply
    end
  end

  def stack_shutdown(_resource) do
    case Application.get_env(:smolnet, :native_shutdown_result, empty_effects()) do
      {:counted, counter, result} ->
        :atomics.add(counter, 1, 1)
        result

      {:fail_once, counter, first_result, later_result} ->
        case :atomics.add_get(counter, 1, 1) do
          1 -> first_result
          _later_call -> later_result
        end

      result ->
        result
    end
  end

  def socket_cancel(_resource, _identity, _operation, _reference), do: empty_effects()

  def tcp_shutdown(_resource, _identity, _how, _now), do: empty_effects()

  def stack_snapshot(_resource) do
    {:ok, %{result: %{test_double: true}, output: [], poll_at: nil, more: false}}
  end

  def test_contention(_resource), do: {:error, :ownership_invariant_violation}

  def test_bounded_work(_resource, requested) do
    {:ok, %{result: requested, output: [], poll_at: nil, more: false}}
  end

  defp empty_effects(options \\ []) do
    {:ok, %{result: :ok, output: [], poll_at: nil, more: Keyword.get(options, :more, false)}}
  end
end
