defmodule SmolNet.Test.Readiness do
  @moduledoc false

  alias SmolNet.Socket
  alias SmolNet.Stack
  alias SmolNet.Stack.Ref

  def open(%Ref{} = stack, internal_handle \\ 0) do
    Stack.test_socket_open(stack, internal_handle)
  end

  def wait(%Socket{} = socket, direction, operation, options \\ []) do
    Stack.test_socket_wait(
      socket,
      direction,
      operation,
      Keyword.get(options, :wake, :none),
      Keyword.get(options, :wake_count, 1),
      Keyword.get(options, :completed, false)
    )
  end

  def ready(%Socket{} = socket, direction) do
    ready_many([{socket, direction}])
  end

  def ready_many(keys), do: Stack.test_socket_ready(keys)

  def close(%Socket{} = socket, options \\ []) do
    Stack.test_socket_close(socket, Keyword.get(options, :wake))
  end
end
