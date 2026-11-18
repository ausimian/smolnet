defmodule SmolNet.Test.Snapshot do
  @moduledoc false

  # `stack_info/1` reports the backend's snapshot as `native.result`. Every
  # backend fills the keys decision 4 of #173 keeps: the socket, listener,
  # waiter and accepted-queue counts, `closing_tcp_socket_count`,
  # `tcp_buffer_bytes`, `egress_credit`, `lifecycle`, `limits` and the
  # protocol counters. The NIF also reports its own machinery, such as the
  # smoltcp sockets behind the public ones (`native_socket_count`) and its
  # listeners' accept pools (`listener_pool_*`). A test checks those through
  # `nif/2`, so that it still catches a NIF leak on the NIF backend and
  # asserts only kept keys on any other.

  @doc """
  Applies `check` to the snapshot when the stack runs the NIF backend, and
  is true otherwise.
  """
  @spec nif(map(), (map() -> boolean())) :: boolean()
  def nif(%{backend: :native, native: %{result: result}}, check), do: check.(result)
  def nif(%{backend: _other}, _check), do: true
end
