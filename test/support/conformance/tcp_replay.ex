defmodule SmolNet.Conformance.TcpReplay do
  @moduledoc false

  # Replays the TCP test vectors (SmolNet.Conformance.TcpVector) against a
  # TCP implementation, a subject, and reports where it departs from what
  # smoltcp did. Each socket of a vector is replayed step by step:
  #
  #   * `set`: the test changed the socket outside the recorded operations,
  #     by writing its fields or making a call the harness does not record.
  #     The subject takes the changed fields.
  #   * `input`: a segment arrives; the subject's immediate reply, if any,
  #     must match.
  #   * `dispatch`: the socket is asked for one segment to send; what it
  #     sends, or that it sends nothing, must match. Where the test refused
  #     the segment, as a full device would, the subject is told to expect
  #     that, and must report the refusal as an error.
  #   * `call`: a socket call such as `send_slice` or `close`; its result
  #     must match.
  #
  # After each step the fields the subject chooses to compare must equal
  # the vector's state. A socket's replay stops at its first mismatch, since
  # every later step depends on it.
  #
  # Segments compare whole, except that a dispatched segment's PSH flag is
  # ignored where the test ignored it (`psh_compared: false`), as most do.
  # The tests compare immediate replies exactly.

  alias SmolNet.Conformance.TcpVector

  @typedoc "A segment as `SmolNet.Conformance.TcpVector.segment/1` decodes it."
  @type segment :: map()
  @typedoc "Socket fields under their vector names, with JSON-shaped values."
  @type state :: %{String.t() => term()}
  @type subject :: term()

  @doc """
  Makes the subject for one socket from the vector's build context (MTU,
  assembler capacity, initial sequence number) and the socket's initial
  state. `args` are the `:subject_args` given to `replay/3`.
  """
  @callback new(context :: map(), initial :: state(), args :: term()) :: subject()

  @doc "Takes the fields a `set` step changed; `state` is the full state after it."
  @callback set(subject(), changed :: state(), state :: state()) :: subject()

  @doc """
  A segment arrives at `at`, in an IP packet from `ip.src_addr` to
  `ip.dst_addr` (address strings); returns the immediate reply, or nil.
  """
  @callback input(
              subject(),
              segment(),
              ip :: %{src_addr: String.t(), dst_addr: String.t()},
              at :: integer()
            ) :: {segment() | nil, subject()}

  @doc """
  Asks for one segment to send at `at`, to a device that accepts it or, for
  `:refuse`, refuses it. Returns `{:segment, segment}`, optionally with the
  IP hop limit as a third element, `:nothing`, or `:error` when the device
  refused what the socket tried to send.
  """
  @callback dispatch(subject(), at :: integer(), device :: :accept | :refuse) ::
              {{:segment, segment()} | {:segment, segment(), 0..255} | :nothing | :error,
               subject()}

  @doc """
  A socket call, by its smoltcp name, with the vector's arguments. The
  result must be JSON-shaped as the vector records it.
  """
  @callback call(subject(), name :: String.t(), args :: map(), at :: integer()) ::
              {result :: term(), subject()}

  @doc "The subject's fields under their vector names, for those it compares."
  @callback snapshot(subject()) :: state()

  @doc "The state fields compared after every step."
  @callback compared_fields() :: [String.t()]

  @type mismatch :: %{
          test: String.t(),
          socket: non_neg_integer(),
          step: non_neg_integer(),
          op: atom(),
          what: term(),
          expected: term(),
          actual: term()
        }

  @doc """
  Replays every socket of `vector` against `module`. Returns `:ok`, or
  `{:error, mismatches}` with each socket's first mismatch.
  """
  @spec replay(map(), module(), keyword()) :: :ok | {:error, [mismatch()]}
  def replay(vector, module, options \\ []) do
    vector.sockets
    |> Enum.flat_map(&replay_socket(vector, &1, module, options))
    |> case do
      [] -> :ok
      mismatches -> {:error, mismatches}
    end
  end

  @doc "Replays many vectors; returns every mismatch found."
  @spec replay_all([map()], module(), keyword()) :: [mismatch()]
  def replay_all(vectors, module, options \\ []) do
    Enum.flat_map(vectors, fn vector ->
      case replay(vector, module, options) do
        :ok -> []
        {:error, mismatches} -> mismatches
      end
    end)
  end

  defp replay_socket(vector, socket, module, options) do
    args =
      case Keyword.get(options, :subject_args) do
        fun when is_function(fun, 2) -> fun.(vector, socket)
        args -> args
      end

    subject = module.new(vector.context, socket.initial, args)
    compared = module.compared_fields()

    socket
    |> TcpVector.states()
    |> Enum.with_index()
    |> Enum.reduce_while(subject, fn {{step, _before, state}, index}, subject ->
      where = %{test: vector.name, socket: socket.index, step: index, op: step.op}
      {mismatch, subject} = run(module, subject, step, state)

      case mismatch || compare_fields(module.snapshot(subject), state, compared) do
        nil ->
          {:cont, subject}

        {what, expected, actual} ->
          {:halt, Map.merge(where, %{what: what, expected: expected, actual: actual})}
      end
    end)
    |> case do
      %{test: _} = mismatch -> [mismatch]
      _subject -> []
    end
  end

  defp run(module, subject, %{op: :set} = step, state) do
    {nil, module.set(subject, step.state, state)}
  end

  defp run(module, subject, %{op: :input} = step, _state) do
    ip = %{src_addr: step.src_addr, dst_addr: step.dst_addr}
    {reply, subject} = module.input(subject, step.segment, ip, step.at)
    {compare(:reply, step.reply, reply), subject}
  end

  defp run(module, subject, %{op: :dispatch} = step, _state) do
    device = if step.refused, do: :refuse, else: :accept
    {sent, subject} = module.dispatch(subject, step.at, device)
    {compare(:dispatch, expected_dispatch(step), actual_dispatch(sent, step)), subject}
  end

  defp run(module, subject, %{op: :call} = step, _state) do
    {result, subject} = module.call(subject, step.name, step.args, step.at)
    {compare({:call, step.name}, step.result, result), subject}
  end

  defp expected_dispatch(%{result: :segment} = step) do
    {:segment, if(step.psh_compared, do: step.segment, else: quash(step.segment)), step.hop_limit}
  end

  defp expected_dispatch(step), do: step.result

  # The hop limit is compared only when the subject reports one, and PSH
  # only where the test compared it. A segment where none was expected is
  # compared whole.
  defp actual_dispatch({:segment, segment}, %{result: :segment} = step) do
    actual_dispatch({:segment, segment, step.hop_limit}, step)
  end

  defp actual_dispatch({:segment, segment, hop_limit}, %{result: :segment} = step) do
    {:segment, if(step.psh_compared, do: segment, else: quash(segment)), hop_limit}
  end

  defp actual_dispatch(other, _step), do: other

  defp quash(%{control: :psh} = segment), do: %{segment | control: :none}
  defp quash(segment), do: segment

  defp compare(_what, same, same), do: nil
  defp compare(what, expected, actual), do: {what, expected, actual}

  defp compare_fields(snapshot, state, fields) do
    Enum.find_value(fields, fn field ->
      compare({:field, field}, Map.fetch!(state, field), Map.get(snapshot, field))
    end)
  end

  @doc """
  Problems with a vector that any replay would trip over: unknown fields,
  states that contradict themselves, segments from the wrong port. Returns
  a list of `{socket, step, problem}`, empty for a sound vector.
  """
  @spec validate(map()) :: [{non_neg_integer(), non_neg_integer() | :initial, String.t()}]
  def validate(vector) do
    Enum.flat_map(vector.sockets, fn socket ->
      initial =
        if Enum.sort(Map.keys(socket.initial)) == Enum.sort(TcpVector.state_fields()),
          do: [],
          else: [{socket.index, :initial, "the initial state lacks or adds fields"}]

      steps =
        socket
        |> TcpVector.states()
        |> Enum.with_index()
        |> Enum.flat_map(fn {{step, before, state}, index} ->
          Enum.map(step_problems(step, before, state), &{socket.index, index, &1})
        end)

      initial ++ steps
    end)
  end

  defp step_problems(step, before, state) do
    [
      unknown_fields(step),
      empty_set(step),
      queue_mismatch(state, "send_queue", "tx_buffer"),
      queue_mismatch(state, "recv_queue", "rx_buffer"),
      wrong_port(step, before)
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp unknown_fields(step) do
    case Map.keys(step.state) -- TcpVector.state_fields() do
      [] -> nil
      unknown -> "unknown fields #{inspect(unknown)}"
    end
  end

  defp empty_set(%{op: :set, state: state}) when map_size(state) == 0, do: "an empty set step"
  defp empty_set(_step), do: nil

  defp queue_mismatch(state, observed, buffer) do
    %{"capacity" => capacity, "data" => data} = state[buffer]
    queued = div(byte_size(data), 2)

    cond do
      queued > capacity -> "#{buffer} holds more than its capacity"
      state["observed"][observed] != queued -> "#{observed} disagrees with #{buffer}"
      true -> nil
    end
  end

  defp wrong_port(%{op: :dispatch, result: :segment, segment: segment}, %{"tuple" => tuple})
       when is_map(tuple) do
    [_match, port] = Regex.run(~r/:(\d+)$/, tuple["local"])

    if String.to_integer(port) == segment.src_port,
      do: nil,
      else: "a segment sent from another port"
  end

  defp wrong_port(_step, _state), do: nil
end
