defmodule SmolNet.Conformance.LinkDifferential do
  @moduledoc false

  # The differential run under impairment: one seeded link-simulator
  # transfer made on every pairing of the NIF and the Elixir backend, NIF to
  # NIF, Elixir to Elixir and each mixed, with every call's result, every
  # packet byte for byte, every wake and abort, every deadline and both
  # stacks' snapshots compared. The link draws from its own seed, so as long
  # as the stacks emit the same packets every pairing sees the same losses,
  # duplicates and delays, and the four runs are one run.
  #
  # A snapshot compares on the keys every backend fills (decision 4 of
  # #173). Left out are identities, which number every stack and socket
  # the VM makes (the snapshot's `id`, and those of its sockets' buffer
  # sizes), and the NIF's own machinery: its call budgets, its ready queue,
  # its smoltcp sockets and listener pools, and the counters of its budget
  # and its ready queue (`deadline_yields`, `timeslice_exhaustions`,
  # `max_native_work_nanoseconds`, `max_ready_events`,
  # `max_maintenance_work`, `readiness_dropped`). So are the most packets
  # and bytes one call put out: the NIF holds a call to its
  # `output_packets` and `bytes_copied` limits and finishes in continuation
  # polls, as many as its wall-clock budget makes, where the Elixir backend
  # does not hold its egress to them yet (phase 7). `native_calls` and
  # `poll_calls` compare less the continuation polls the driver made, which
  # the NIF counts and the Elixir backend never needs.

  import ExUnitProperties, only: [gen: 2]
  import StreamData

  alias SmolNet.Conformance.LinkSim
  alias SmolNet.Conformance.Trace
  alias SmolNet.Stack.Backend.Elixir, as: ElixirBackend
  alias SmolNet.Stack.Backend.Native

  @machinery [
    :id,
    :call_target_nanoseconds,
    :work_budget_nanoseconds,
    :encoding_headroom_nanoseconds,
    :readiness,
    :native_socket_count,
    :listener_pool_socket_count,
    :listener_pool_target_count
  ]

  @budget_counters [
    :deadline_yields,
    :timeslice_exhaustions,
    :max_native_work_nanoseconds,
    :max_ready_events,
    :max_maintenance_work,
    :readiness_dropped,
    :max_output_packets,
    :max_bytes_copied
  ]

  @doc """
  Transfers to run: either end sending, IPv4 or IPv6, from a byte to
  384 KiB, MTUs (so MSSs) chosen apart for each end, socket buffers from
  2 KiB to 256 KiB, and links that lose, duplicate, reorder and delay
  packets in any combination, some losing a burst of one end's packets,
  some with SACK taken out of the handshake.
  """
  def transfer do
    gen all(
          seed <- integer(1..0x7FFF_FFFF),
          family <- member_of([:inet, :inet6]),
          sender <- member_of([:client, :server]),
          bytes <-
            frequency([
              {3, integer(1..4_096)},
              {5, integer(4_097..65_536)},
              {2, integer(65_537..393_216)}
            ]),
          mtu <- tuple({mtu(), mtu()}),
          buffer <- member_of([2_048, 8_192, 16_384, 65_536, 262_144]),
          profile <- profile(),
          burst <- one_of([constant(nil), burst()]),
          strip_sack <- frequency([{4, constant(false)}, {1, constant(true)}])
        ) do
      %{
        seed: seed,
        family: family,
        sender: sender,
        bytes: bytes,
        mtu: mtu,
        buffer: buffer,
        profile: profile,
        burst: burst,
        strip_sack: strip_sack
      }
    end
  end

  defp mtu, do: one_of([member_of([1_280, 1_500, 9_000]), integer(1_280..9_000)])

  defp profile do
    gen all(
          loss <-
            frequency([
              {2, constant(0.0)},
              {3, float(min: 0.0, max: 0.1)},
              {1, float(min: 0.1, max: 0.4)}
            ]),
          duplicate <-
            frequency([
              {2, constant(0.0)},
              {2, float(min: 0.0, max: 0.1)},
              {1, float(min: 0.1, max: 0.3)}
            ]),
          reorder <- frequency([{2, constant(0.0)}, {3, float(min: 0.0, max: 0.3)}]),
          delay <-
            frequency([
              {3, integer(1..10)},
              {2, integer(11..100)},
              {1, integer(101..300)}
            ]),
          jitter <- frequency([{2, constant(0)}, {3, integer(0..30)}])
        ) do
      %{loss: loss, duplicate: duplicate, reorder: reorder, delay: delay, jitter: jitter}
    end
  end

  # One end loses `length` packets in a row from its `first`th.
  defp burst do
    tuple({member_of([:client, :server]), integer(1..300), integer(1..8)})
  end

  @doc "The link simulator's options for a transfer, less the backend."
  def options(transfer) do
    [
      seed: transfer.seed,
      family: transfer.family,
      sender: transfer.sender,
      bytes: transfer.bytes,
      mtu: transfer.mtu,
      buffer: transfer.buffer,
      profile: transfer.profile,
      drop: drop(transfer.burst),
      strip_sack: transfer.strip_sack,
      snapshot: true
    ]
  end

  defp drop(nil), do: nil

  defp drop({side, first, length}),
    do: fn from, n, _packet -> from == side and n >= first and n < first + length end

  @doc """
  Runs `transfer` on every pairing. Returns `{:ok, run}`, the NIF-to-NIF
  run, if all four agree, or else `{:error, report}` naming each pairing
  that departed from it, where it did, and the first call at which the
  Elixir backend, replaying the NIF's run, does otherwise.

  `perturb: {side, n}` also drops the `n`th packet `side` sends on the
  Elixir-to-Elixir pairing alone, to check the report. `nif:` names the
  NIF backend module (`Backend.Native` by default), and `ignore:` lists
  snapshot counters not to compare.
  """
  def check(transfer, check_options \\ []) do
    options = options(transfer)
    nif = Keyword.get(check_options, :nif, Native)
    ignore = Keyword.get(check_options, :ignore, [])

    [reference | others] =
      for pairing <- [
            {nif, nif},
            {ElixirBackend, ElixirBackend},
            {nif, ElixirBackend},
            {ElixirBackend, nif}
          ] do
        {pairing, run(perturb(options, pairing, check_options[:perturb]), pairing, ignore)}
      end

    {_pairing, {expected, nif_run}} = reference

    case for({pairing, {actual, _run}} <- others, actual != expected, do: {pairing, actual}) do
      [] ->
        {:ok, nif_run}

      departed ->
        {:error,
         %{
           transfer: transfer,
           status: nif_run.status,
           departed:
             for({pairing, actual} <- departed, do: {pairing, first_difference(expected, actual)}),
           replay: replay(nif_run)
         }}
    end
  end

  defp perturb(options, {ElixirBackend, ElixirBackend}, {side, n}) do
    drop = options[:drop] || fn _from, _n, _packet -> false end

    Keyword.put(options, :drop, fn from, m, packet ->
      {from, m} == {side, n} or drop.(from, m, packet)
    end)
  end

  defp perturb(options, _pairing, _perturb), do: options

  defp run(options, pairing, ignore) do
    result = LinkSim.run([backend: pairing] ++ options)
    {comparable(result, ignore), result}
  end

  # What must agree between two runs: the outcome, what arrived, and every
  # entry, the snapshots taken on their kept keys.
  defp comparable(result, ignore) do
    {entries, []} =
      Enum.map_reduce(result.entries, snapshot_rounds(result), fn
        %{call: :stack_snapshot} = entry, [round | _rest] = rounds ->
          continuations = Map.fetch!(round, entry.stack)
          entry = %{entry | result: kept(entry.result, continuations, ignore)}
          {entry, next(rounds, entry.stack)}

        entry, rounds ->
          {entry, rounds}
      end)

    %{status: result.status, received: result.received, entries: entries}
  end

  # The continuation counts by snapshot round, each round's client and
  # server entries in the order LinkSim makes them.
  defp snapshot_rounds(result), do: result.continuations

  defp next([_round | rest], :server), do: rest
  defp next(rounds, :client), do: rounds

  defp kept({:ok, snapshot}, continuations, ignore) do
    counters =
      snapshot.counters
      |> Map.drop(@budget_counters ++ ignore)
      |> Map.replace_lazy(:native_calls, &(&1 - continuations))
      |> Map.replace_lazy(:poll_calls, &(&1 - continuations))

    sockets =
      for socket <- snapshot.tcp_buffer_bytes.sockets, do: Map.drop(socket, [:id, :generation])

    {:ok,
     snapshot
     |> Map.drop(@machinery)
     |> Map.put(:counters, counters)
     |> put_in([:tcp_buffer_bytes, :sockets], sockets)}
  end

  defp kept(error, _continuations, _ignore), do: error

  defp first_difference(%{entries: same} = expected, %{entries: same} = actual) do
    %{
      status: {expected.status, actual.status},
      received: {byte_size(expected.received), byte_size(actual.received)}
    }
  end

  defp first_difference(expected, actual) do
    index =
      expected.entries
      |> Enum.zip(actual.entries)
      |> Enum.find_index(fn {left, right} -> left != right end) ||
        min(length(expected.entries), length(actual.entries))

    left = Enum.at(expected.entries, index)
    right = Enum.at(actual.entries, index)

    %{
      index: index,
      call: left && Map.take(left, [:stack, :at, :call]),
      diff: for({path, l, r} <- diff(left, right), do: {path, summary(l), summary(r)})
    }
  end

  # TCP packets in a term as their headers, to read a report by.
  defp summary(packets) when is_list(packets), do: Enum.map(packets, &summary/1)

  defp summary(<<4::4, ihl::4, _::binary-size(8), 6, _::binary>> = packet),
    do: tcp_summary(binary_part(packet, ihl * 4, byte_size(packet) - ihl * 4))

  defp summary(<<6::4, _::28, _length::16, 6, _hop, _addresses::binary-32, tcp::binary>>),
    do: tcp_summary(tcp)

  defp summary(other), do: other

  defp tcp_summary(
         <<_ports::32, seq::32, ack::32, offset::4, _::4, flags, window::16, _::binary>> = tcp
       ) do
    %{seq: seq, ack: ack, flags: flags, window: window, length: byte_size(tcp) - offset * 4}
  end

  defp tcp_summary(tcp), do: tcp

  # Where two terms differ, as `{path, expected, actual}`.
  defp diff(left, right, path \\ [])
  defp diff(same, same, _path), do: []

  defp diff(%{} = left, %{} = right, path) do
    keys = Enum.uniq(Map.keys(left) ++ Map.keys(right))
    Enum.flat_map(keys, &diff(Map.get(left, &1), Map.get(right, &1), path ++ [&1]))
  end

  defp diff(left, right, path) when is_tuple(left) and is_tuple(right),
    do: diff(Tuple.to_list(left), Tuple.to_list(right), path)

  defp diff(left, right, path)
       when is_list(left) and is_list(right) and length(left) == length(right) do
    [left, right]
    |> Enum.zip()
    |> Enum.with_index()
    |> Enum.flat_map(fn {{l, r}, i} -> diff(l, r, path ++ [i]) end)
  end

  defp diff(left, right, path), do: [{path, left, right}]

  # The NIF's run replayed on the Elixir backend, stack by stack, its
  # snapshots aside: the first call each stack makes differently.
  defp replay(nif_run) do
    entries = Enum.reject(nif_run.entries, &(&1.call == :stack_snapshot))

    case Trace.replay(Trace.new("differential", "", entries), ElixirBackend) do
      :ok ->
        :ok

      {:error, mismatches} ->
        {:error,
         for(m <- mismatches, do: %{m | expected: summary(m.expected), actual: summary(m.actual)})}
    end
  end
end
