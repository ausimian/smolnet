defmodule SmolNet.Conformance.TcpVectorsTest do
  use ExUnit.Case, async: true

  alias SmolNet.Conformance.TcpReplay
  alias SmolNet.Conformance.TcpReplay.Connection
  alias SmolNet.Conformance.TcpReplay.Recorded
  alias SmolNet.Conformance.TcpVector

  setup_all do
    %{vectors: TcpVector.all()}
  end

  test "the index lists every socket test and a vector for each one that ran", %{
    vectors: vectors
  } do
    index = TcpVector.index()
    assert length(index) == 238

    assert index |> Enum.map(& &1["origin"]) |> Enum.frequencies() ==
             %{"upstream" => 190, "upstream_modified" => 5, "smolnet" => 43}

    with_vector = for %{"vector" => true, "name" => name} <- index, do: name
    assert Enum.map(vectors, & &1.name) == with_vector
    assert length(vectors) == 230

    # The rest are compiled out of SmolNet's feature set, or build no
    # socket, and each says which.
    assert for(%{"vector" => true, "note" => _note, "name" => name} <- index, do: name) ==
             ["test_buffer_wraparound_tx"]

    for %{"vector" => false} = test <- index do
      assert test["note"] =~
               ~r/^(compiled out: #\[cfg\(feature = "[a-z-]+"\)\]|builds no test socket)$/
    end
  end

  test "every vector is sound", %{vectors: vectors} do
    panics = for %{"panics" => true, "name" => name} <- TcpVector.index(), do: name
    assert panics == ["test_set_hop_limit_zero"]

    # It only asserts an internal predicate, which a vector cannot record.
    assert for(vector <- vectors, Enum.all?(vector.sockets, &(&1.steps == [])), do: vector.name) ==
             ["test_dsack_reply_is_read_as_dsack"]

    for vector <- vectors do
      assert vector.panicked == vector.name in panics, vector.name
      assert vector.context["initial_seq_no"] == 10_000
      assert TcpReplay.validate(vector) == [], vector.name
    end
  end

  test "the vectors exercise the harness's operations", %{vectors: vectors} do
    steps = for vector <- vectors, socket <- vector.sockets, step <- socket.steps, do: step
    ops = Enum.frequencies_by(steps, &op/1)

    for op <- [:set, :input, {:dispatch, :segment}, {:dispatch, :nothing}, :call] do
      assert Map.get(ops, op, 0) > 0, "no #{inspect(op)} step"
    end

    calls = for %{op: :call, name: name} <- steps, uniq: true, do: name
    assert Enum.sort(calls) -- TcpVector.calls() == []
    assert "send_slice" in calls and "connect" in calls and "process_path_mtu" in calls
  end

  test "a vector's segments decode and encode again", %{vectors: vectors} do
    for vector <- vectors,
        socket <- vector.sockets,
        step <- socket.steps,
        segment <- segments(step) do
      assert segment |> TcpVector.segment_json() |> TcpVector.segment() == segment
    end
  end

  test "the recorded subject replays every vector", %{vectors: vectors} do
    assert TcpReplay.replay_all(vectors, Recorded, subject_args: &Recorded.args/2) == []
  end

  # Every field of every state, smoltcp's own bookkeeping included (the
  # RACK send log, the assembler's layout, CUBIC's floats), so the
  # connection keeps what smoltcp keeps, not just what it sends.
  test "the Elixir connection replays every vector, field for field", %{vectors: vectors} do
    assert Connection.compared_fields() == TcpVector.state_fields()

    upstream = Enum.filter(vectors, &(&1.origin == "upstream"))
    assert length(upstream) == 183
    assert TcpReplay.replay_all(upstream, Connection) == []

    assert TcpReplay.replay_all(vectors -- upstream, Connection) == []
  end

  describe "the runner reports" do
    setup do
      %{vector: TcpVector.load("test_established_fin")}
    end

    test "a different reply", %{vector: vector} do
      [%{steps: steps} | _] = vector.sockets
      index = Enum.find_index(steps, &(&1.op == :input and &1.reply == nil))

      assert {:error, [mismatch]} =
               replay(vector, index, fn nil -> %{List.first(segments_of(steps)) | seq: 1} end)

      assert %{test: "test_established_fin", step: ^index, op: :input, what: :reply} = mismatch
    end

    test "a different segment, but not a different PSH flag the test ignored", %{
      vector: vector
    } do
      [%{steps: steps} | _] = vector.sockets

      index =
        Enum.find_index(
          steps,
          &match?(%{op: :dispatch, result: :segment, psh_compared: false}, &1)
        )

      flip = fn {:segment, segment, hop} -> {:segment, %{segment | control: :psh}, hop} end
      assert replay(vector, index, flip) == :ok

      shift = fn {:segment, segment, hop} ->
        {:segment, %{segment | ack: segment.ack + 1}, hop}
      end

      assert {:error, [%{step: ^index, what: :dispatch}]} = replay(vector, index, shift)
    end

    test "a reply with a different PSH flag, which the tests compare" do
      vector = TcpVector.load("test_established_bad_seq")
      [%{steps: steps} | _] = vector.sockets
      index = Enum.find_index(steps, &match?(%{op: :input, reply: %{control: :none}}, &1))

      assert {:error, [%{step: ^index, what: :reply}]} =
               replay(vector, index, &%{&1 | control: :psh})
    end

    test "a segment where nothing was sent" do
      vector = TcpVector.load("test_fast_retransmit_survives_refused_emit")
      [%{steps: steps} | _] = vector.sockets
      index = Enum.find_index(steps, &match?(%{op: :dispatch, result: :nothing}, &1))
      segment = steps |> segments_of() |> List.first()

      assert {:error, [%{step: ^index, what: :dispatch, expected: :nothing}]} =
               replay(vector, index, fn :nothing -> {:segment, segment} end)
    end

    test "a different call result" do
      vector = TcpVector.load("test_closed_close")

      assert {:error, [%{what: {:call, "close"}, expected: nil}]} =
               replay(vector, 0, fn nil -> %{"ok" => 1} end)
    end

    test "a different field", %{vector: vector} do
      defmodule Stale do
        # Never moves past its initial state.
        @behaviour TcpReplay
        defdelegate new(context, initial, args), to: Recorded
        defdelegate set(subject, changed, state), to: Recorded
        defdelegate input(subject, segment, ip, at), to: Recorded
        defdelegate dispatch(subject, at, device), to: Recorded
        defdelegate call(subject, name, args, at), to: Recorded
        defdelegate compared_fields, to: Recorded
        def snapshot(_subject), do: %{"state" => "closed"}
      end

      assert {:error, [mismatch]} =
               TcpReplay.replay(vector, Stale, subject_args: &Recorded.args/2)

      # The first step's `set` makes the socket established.
      assert %{step: 0, what: {:field, "state"}, expected: "established", actual: "closed"} =
               mismatch
    end
  end

  defp replay(vector, index, fun) do
    TcpReplay.replay(vector, Recorded,
      subject_args: fn vector, socket ->
        vector |> Recorded.args(socket) |> Map.put(:perturb, {index, fun})
      end
    )
  end

  defp op(%{op: :dispatch, result: result}), do: {:dispatch, result}
  defp op(%{op: op}), do: op

  defp segments(%{op: :input, segment: segment, reply: nil}), do: [segment]
  defp segments(%{op: :input, segment: segment, reply: reply}), do: [segment, reply]
  defp segments(%{op: :dispatch, result: :segment, segment: segment}), do: [segment]
  defp segments(_step), do: []

  defp segments_of(steps), do: Enum.flat_map(steps, &segments/1)
end
