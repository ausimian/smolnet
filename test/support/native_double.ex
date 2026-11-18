defmodule SmolNet.Test.NativeDouble do
  @moduledoc false

  # A `SmolNet.Stack.Backend` that tests the Stack shell without a protocol
  # engine. Tests select it through the application environment's
  # `:backend` and script its answers there:
  #
  #   * `:native_test_process` receives a message for each scripted call;
  #   * `:native_test_result`, `:native_ingress_result`, `:native_poll_result`
  #     and `:native_shutdown_result` hold the answers, built with `ok/1` and
  #     `error/1`, or `:wait` to block the stack until the test sends one.
  #
  # Every other call answers `ok/1` with no effects.

  @behaviour SmolNet.Stack.Backend

  @doc """
  A successful answer. Options: `:result` (default `:ok`), `:poll_at`,
  `:more`, `:output` (packets to emit), and `:effects` (wake and abort
  effects, delivered before the packets).
  """
  def ok(options \\ []) do
    envelope = %{
      result: Keyword.get(options, :result, :ok),
      poll_at: Keyword.get(options, :poll_at),
      more: Keyword.get(options, :more, false)
    }

    emits = options |> Keyword.get(:output, []) |> Enum.map(&{:emit, &1})
    {{:ok, envelope}, Keyword.get(options, :effects, []) ++ emits}
  end

  @doc "A failed answer."
  def error(reason), do: {{:error, reason}, []}

  @impl true
  def stack_new(_limits, _config, _now) do
    send(test_process(), {:native_stack_new, self()})

    case Application.fetch_env!(:smolnet, :native_test_result) do
      :wait ->
        receive do
          {:native_test_reply, reply} -> created(reply)
        end

      reply ->
        created(reply)
    end
  end

  @impl true
  def stack_ingress(state, packet, _now) do
    send(test_process(), {:native_stack_ingress, self(), packet})
    ingress_answer(state)
  end

  @impl true
  def stack_ingress_batch(state, packets, _now) do
    send(test_process(), {:native_stack_ingress_batch, self(), packets})
    ingress_answer(state)
  end

  @impl true
  def stack_poll(state, now) do
    result = Application.get_env(:smolnet, :native_poll_result, ok())

    unless match?({:countdown, _counter, :silent}, result) do
      send(test_process(), {:native_stack_poll, self(), now})
    end

    case result do
      :wait ->
        receive do
          {:native_poll_reply, reply} -> answer(state, reply)
        end

      {:countdown, counter, :silent} ->
        more =
          Agent.get_and_update(counter, fn remaining ->
            {remaining > 1, max(remaining - 1, 0)}
          end)

        answer(state, ok(more: more))

      reply ->
        answer(state, reply)
    end
  end

  @impl true
  def stack_shutdown(state, _reason) do
    reply =
      case Application.get_env(:smolnet, :native_shutdown_result, ok()) do
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

    answer(state, reply)
  end

  @impl true
  def stack_snapshot(state), do: answer(state, ok(result: %{test_double: true}))

  @impl true
  def stack_grant_egress(state, _packets, _bytes, _now), do: answer(state, ok())

  @impl true
  def socket_cancel(state, _identity, _operation, _reference), do: answer(state, ok())

  @impl true
  def socket_validate(state, _identity), do: answer(state, ok(result: :tcp))

  @impl true
  def tcp_open(state, _family, _rcvbuf, _sndbuf), do: answer(state, ok())

  @impl true
  def tcp_bind(state, _identity, _endpoint), do: answer(state, ok())

  @impl true
  def tcp_listen(state, _identity, _backlog, _now), do: answer(state, ok())

  @impl true
  def tcp_accept(state, _identity, _pid, _reference, _now), do: answer(state, ok())

  @impl true
  def tcp_connect(state, _identity, _endpoint, _pid, _reference, _now), do: answer(state, ok())

  # Answers from `:native_send_result` when a test scripts one, so that a
  # test can have a call deliver wake or abort effects.
  @impl true
  def tcp_send(state, _identity, _data, _pid, _reference, _now),
    do: answer(state, Application.get_env(:smolnet, :native_send_result, ok()))

  @impl true
  def tcp_recv(state, _identity, _length, _pid, _reference, _now), do: answer(state, ok())

  @impl true
  def tcp_shutdown(state, _identity, _how, _now), do: answer(state, ok())

  @impl true
  def tcp_sockname(state, _identity), do: answer(state, ok())

  @impl true
  def tcp_peername(state, _identity), do: answer(state, ok())

  @impl true
  def tcp_set_nodelay(state, _identity, _nodelay, _now), do: answer(state, ok())

  @impl true
  def tcp_nodelay(state, _identity), do: answer(state, ok(result: false))

  @impl true
  def tcp_set_keepalive(state, _identity, _keepalive, _now), do: answer(state, ok())

  @impl true
  def tcp_keepalive(state, _identity), do: answer(state, ok(result: false))

  @impl true
  def tcp_close(state, _identity, _now), do: answer(state, ok())

  @impl true
  def udp_open(state, _family), do: answer(state, ok())

  @impl true
  def udp_bind(state, _identity, _endpoint), do: answer(state, ok())

  @impl true
  def udp_connect(state, _identity, _endpoint), do: answer(state, ok())

  @impl true
  def udp_sendto(state, _identity, _endpoint, _data, _pid, _reference, _now),
    do: answer(state, ok())

  @impl true
  def udp_recvfrom(state, _identity, _length, _pid, _reference, _now), do: answer(state, ok())

  @impl true
  def udp_sockname(state, _identity), do: answer(state, ok())

  @impl true
  def udp_peername(state, _identity), do: answer(state, ok())

  @impl true
  def udp_close(state, _identity, _now), do: answer(state, ok())

  @impl true
  def test_contention(state), do: {{:error, :ownership_invariant_violation}, state, []}

  @impl true
  def test_bounded_work(state, requested),
    do: {{:ok, %{result: requested, output: [], poll_at: nil, more: false}}, state, []}

  defp test_process, do: Application.fetch_env!(:smolnet, :native_test_process)

  # The double's state is the result of its stack_new answer.
  defp created({{:ok, %{result: state}}, _effects} = reply), do: answer(state, reply)
  defp created(reply), do: answer(nil, reply)

  defp ingress_answer(state) do
    case Application.get_env(:smolnet, :native_ingress_result, :default) do
      :default ->
        answer(state, ok())

      :wait ->
        receive do
          {:native_ingress_reply, reply} -> answer(state, reply)
        end

      reply ->
        answer(state, reply)
    end
  end

  defp answer(state, {reply, effects}), do: {reply, state, effects}
end
