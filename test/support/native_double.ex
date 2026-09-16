defmodule SmolNet.Test.NativeDouble do
  @moduledoc false

  def stack_new(_limits, _now) do
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
end
