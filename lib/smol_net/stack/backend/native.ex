defmodule SmolNet.Stack.Backend.Native do
  @moduledoc false

  # The smoltcp NIF as a `SmolNet.Stack.Backend`. Its state is the NIF's
  # stack resource, which the NIF mutates in place, so every call returns
  # the state it was given. The NIF's envelope carries the packets to emit
  # as `output`; they become `{:emit, packet}` effects, and the rest of the
  # envelope is the reply. The NIF sends readiness and abort messages
  # itself, so there are never wake or abort effects.

  @behaviour SmolNet.Stack.Backend

  alias SmolNet.Native

  @impl true
  def stack_new(limits, config, now) do
    case Native.stack_new(limits, config, now) do
      {:ok, %{result: resource}} = created -> outcome(created, resource)
      other -> outcome(other, nil)
    end
  end

  @impl true
  def stack_ingress(resource, packet, now),
    do: resource |> Native.stack_ingress(packet, now) |> outcome(resource)

  @impl true
  def stack_ingress_batch(resource, packets, now),
    do: resource |> Native.stack_ingress_batch(packets, now) |> outcome(resource)

  @impl true
  def stack_poll(resource, now), do: resource |> Native.stack_poll(now) |> outcome(resource)

  @impl true
  def stack_grant_egress(resource, packets, bytes, now),
    do: resource |> Native.stack_grant_egress(packets, bytes, now) |> outcome(resource)

  @impl true
  def stack_shutdown(resource, reason),
    do: resource |> Native.stack_shutdown(reason) |> outcome(resource)

  @impl true
  def stack_snapshot(resource), do: resource |> Native.stack_snapshot() |> outcome(resource)

  @impl true
  def socket_cancel(resource, identity, operation, reference),
    do: resource |> Native.socket_cancel(identity, operation, reference) |> outcome(resource)

  @impl true
  def socket_validate(resource, identity),
    do: resource |> Native.socket_validate(identity) |> outcome(resource)

  @impl true
  def tcp_open(resource, family, rcvbuf, sndbuf),
    do: resource |> Native.tcp_open(family, rcvbuf, sndbuf) |> outcome(resource)

  @impl true
  def tcp_bind(resource, identity, endpoint),
    do: resource |> Native.tcp_bind(identity, endpoint) |> outcome(resource)

  @impl true
  def tcp_listen(resource, identity, backlog, now),
    do: resource |> Native.tcp_listen(identity, backlog, now) |> outcome(resource)

  @impl true
  def tcp_accept(resource, identity, pid, reference, now),
    do: resource |> Native.tcp_accept(identity, pid, reference, now) |> outcome(resource)

  @impl true
  def tcp_connect(resource, identity, endpoint, pid, reference, now),
    do:
      resource
      |> Native.tcp_connect(identity, endpoint, pid, reference, now)
      |> outcome(resource)

  @impl true
  def tcp_send(resource, identity, data, pid, reference, now),
    do: resource |> Native.tcp_send(identity, data, pid, reference, now) |> outcome(resource)

  @impl true
  def tcp_recv(resource, identity, length, pid, reference, now),
    do: resource |> Native.tcp_recv(identity, length, pid, reference, now) |> outcome(resource)

  @impl true
  def tcp_shutdown(resource, identity, how, now),
    do: resource |> Native.tcp_shutdown(identity, how, now) |> outcome(resource)

  @impl true
  def tcp_sockname(resource, identity),
    do: resource |> Native.tcp_sockname(identity) |> outcome(resource)

  @impl true
  def tcp_peername(resource, identity),
    do: resource |> Native.tcp_peername(identity) |> outcome(resource)

  @impl true
  def tcp_set_nodelay(resource, identity, nodelay, now),
    do: resource |> Native.tcp_set_nodelay(identity, nodelay, now) |> outcome(resource)

  @impl true
  def tcp_nodelay(resource, identity),
    do: resource |> Native.tcp_nodelay(identity) |> outcome(resource)

  @impl true
  def tcp_set_keepalive(resource, identity, keepalive, now),
    do: resource |> Native.tcp_set_keepalive(identity, keepalive, now) |> outcome(resource)

  @impl true
  def tcp_keepalive(resource, identity),
    do: resource |> Native.tcp_keepalive(identity) |> outcome(resource)

  @impl true
  def tcp_close(resource, identity, now),
    do: resource |> Native.tcp_close(identity, now) |> outcome(resource)

  @impl true
  def udp_open(resource, family), do: resource |> Native.udp_open(family) |> outcome(resource)

  @impl true
  def udp_bind(resource, identity, endpoint),
    do: resource |> Native.udp_bind(identity, endpoint) |> outcome(resource)

  @impl true
  def udp_connect(resource, identity, endpoint),
    do: resource |> Native.udp_connect(identity, endpoint) |> outcome(resource)

  @impl true
  def udp_sendto(resource, identity, endpoint, data, pid, reference, now),
    do:
      resource
      |> Native.udp_sendto(identity, endpoint, data, pid, reference, now)
      |> outcome(resource)

  @impl true
  def udp_recvfrom(resource, identity, length, pid, reference, now),
    do:
      resource
      |> Native.udp_recvfrom(identity, length, pid, reference, now)
      |> outcome(resource)

  @impl true
  def udp_sockname(resource, identity),
    do: resource |> Native.udp_sockname(identity) |> outcome(resource)

  @impl true
  def udp_peername(resource, identity),
    do: resource |> Native.udp_peername(identity) |> outcome(resource)

  @impl true
  def udp_close(resource, identity, now),
    do: resource |> Native.udp_close(identity, now) |> outcome(resource)

  @impl true
  def test_contention(resource), do: {Native.test_contention(resource), resource, []}

  @impl true
  def test_bounded_work(resource, requested),
    do: {Native.test_bounded_work(resource, requested), resource, []}

  @impl true
  def test_socket_open(resource, internal_handle),
    do: resource |> Native.test_socket_open(internal_handle) |> outcome(resource)

  @impl true
  def test_socket_wait(resource, identity, wait),
    do: resource |> Native.test_socket_wait(identity, wait) |> outcome(resource)

  @impl true
  def test_socket_ready(resource, keys),
    do: resource |> Native.test_socket_ready(keys) |> outcome(resource)

  @impl true
  def test_socket_close(resource, identity, wake_direction),
    do: resource |> Native.test_socket_close(identity, wake_direction) |> outcome(resource)

  defp outcome({:ok, %{result: result, output: output, poll_at: poll_at, more: more}}, resource) do
    {{:ok, %{result: result, poll_at: poll_at, more: more}}, resource, emits(output)}
  end

  defp outcome({:error, _reason} = error, resource), do: {error, resource, []}

  defp emits([]), do: []
  defp emits(packets), do: Enum.map(packets, &{:emit, &1})
end
