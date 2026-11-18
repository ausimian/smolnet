defmodule SmolNet.Stack.Backend.Elixir do
  @moduledoc false

  # The pure-Elixir protocol engine, `SmolNet.Core`, as a
  # `SmolNet.Stack.Backend`: the backend name `:elixir`. Its state is the
  # core value. Each callback calls the core and wraps its `{:ok, result}`
  # in the envelope the shell takes, with the core's next deadline as
  # `poll_at` and `more` always false: the core finishes every call within
  # it, so it never asks for a continuation.
  #
  # The module is named after its backend, like `Backend.Native`; it is
  # always referred to in full, never aliased, since an alias named
  # `Elixir` would shadow the root namespace.

  @behaviour SmolNet.Stack.Backend

  alias SmolNet.Core

  @impl true
  def stack_new(limits, config, now) do
    case Core.new(limits, config, now) do
      {:ok, core} -> {{:ok, envelope(core, core)}, core, []}
      {:error, _reason} = error -> {error, nil, []}
    end
  end

  @impl true
  def stack_ingress(core, packet, now), do: core |> Core.ingress(packet, now) |> outcome()

  @impl true
  def stack_ingress_batch(core, packets, now),
    do: core |> Core.ingress_batch(packets, now) |> outcome()

  @impl true
  def stack_poll(core, now), do: core |> Core.poll(now) |> outcome()

  @impl true
  def stack_grant_egress(core, packets, bytes, now),
    do: core |> Core.grant_egress(packets, bytes, now) |> outcome()

  @impl true
  def stack_shutdown(core, reason), do: core |> Core.shutdown(reason) |> outcome()

  @impl true
  def stack_snapshot(core), do: core |> Core.snapshot() |> outcome()

  @impl true
  def socket_cancel(core, identity, operation, reference),
    do: core |> Core.socket_cancel(identity, operation, reference) |> outcome()

  @impl true
  def socket_validate(core, identity), do: core |> Core.socket_validate(identity) |> outcome()

  # TCP comes in a later phase of #173.
  @impl true
  def tcp_open(core, _family, _rcvbuf, _sndbuf), do: not_implemented(core)
  @impl true
  def tcp_bind(core, _identity, _endpoint), do: not_implemented(core)
  @impl true
  def tcp_listen(core, _identity, _backlog, _now), do: not_implemented(core)
  @impl true
  def tcp_accept(core, _identity, _pid, _reference, _now), do: not_implemented(core)
  @impl true
  def tcp_connect(core, _identity, _endpoint, _pid, _reference, _now), do: not_implemented(core)
  @impl true
  def tcp_send(core, _identity, _data, _pid, _reference, _now), do: not_implemented(core)
  @impl true
  def tcp_recv(core, _identity, _length, _pid, _reference, _now), do: not_implemented(core)
  @impl true
  def tcp_shutdown(core, _identity, _how, _now), do: not_implemented(core)
  @impl true
  def tcp_sockname(core, _identity), do: not_implemented(core)
  @impl true
  def tcp_peername(core, _identity), do: not_implemented(core)
  @impl true
  def tcp_set_nodelay(core, _identity, _nodelay, _now), do: not_implemented(core)
  @impl true
  def tcp_nodelay(core, _identity), do: not_implemented(core)
  @impl true
  def tcp_set_keepalive(core, _identity, _keepalive, _now), do: not_implemented(core)
  @impl true
  def tcp_keepalive(core, _identity), do: not_implemented(core)
  @impl true
  def tcp_close(core, _identity, _now), do: not_implemented(core)
  @impl true
  def udp_open(core, family), do: core |> Core.udp_open(family) |> outcome()

  @impl true
  def udp_bind(core, identity, endpoint),
    do: core |> Core.udp_bind(identity, endpoint) |> outcome()

  @impl true
  def udp_connect(core, identity, endpoint),
    do: core |> Core.udp_connect(identity, endpoint) |> outcome()

  @impl true
  def udp_sendto(core, identity, endpoint, data, pid, reference, now),
    do: core |> Core.udp_sendto(identity, endpoint, data, pid, reference, now) |> outcome()

  @impl true
  def udp_recvfrom(core, identity, length, pid, reference, now),
    do: core |> Core.udp_recvfrom(identity, length, pid, reference, now) |> outcome()

  @impl true
  def udp_sockname(core, identity), do: core |> Core.udp_sockname(identity) |> outcome()

  @impl true
  def udp_peername(core, identity), do: core |> Core.udp_peername(identity) |> outcome()

  @impl true
  def udp_close(core, identity, now), do: core |> Core.udp_close(identity, now) |> outcome()

  defp not_implemented(core), do: core |> Core.not_implemented() |> outcome()

  defp outcome({{:ok, result}, core, effects}), do: {{:ok, envelope(result, core)}, core, effects}
  defp outcome({{:error, _reason} = error, core, effects}), do: {error, core, effects}

  defp envelope(result, core), do: %{result: result, poll_at: Core.deadline(core), more: false}
end
