defmodule SmolNet.Conformance.TcpVector do
  @moduledoc false

  # The TCP test vectors: what smoltcp's socket tests fed a socket, and what
  # it did, dumped from the vendored crate by scripts/tcp_vectors.exs. The
  # format is documented in test/conformance/README.md.
  #
  # A vector decodes to a map with the test's `:name`, its `:origin` from the
  # index, the build `:context` and its `:sockets`. Each socket has an
  # `:initial` state and `:steps`; a state is a map of the socket's fields
  # under their JSON names, and a step's `:state` holds the fields it
  # changed. Segments decode to maps with atom keys, payloads to binaries.

  @dir Path.expand("../../conformance/tcp_vectors", __DIR__)
  @format "smolnet.tcp_vector"
  @version 1

  @state_fields ~w(state timer rtte assembler rx_buffer rx_fin_received tx_buffer timeout
                   keep_alive keep_alive_probes timeout_abort hop_limit listen_endpoint tuple
                   local_seq_no remote_seq_no remote_last_seq remote_last_ack remote_last_win
                   remote_win_shift remote_win_len remote_win_scale remote_has_sack remote_mss
                   path_mtu remote_last_ts local_rx_last_seq local_rx_last_ack local_rx_dup_acks
                   pending_fast_retransmit recover scoreboard recovery_rxt_end rack ack_delay
                   ack_delay_timer challenge_ack_timer nagle nagle_small_segment_end congestion
                   tsval last_remote_tsval interface_addresses observed)

  @calls ~w(accepts send_slice recv recv_slice peek_slice listen connect close abort reset set_timeout
            set_ack_delay set_nagle_enabled set_keep_alive set_keep_alive_config set_hop_limit
            set_congestion_control set_tsval_generator process_path_mtu)

  @controls %{
    "none" => :none,
    "psh" => :psh,
    "syn" => :syn,
    "fin" => :fin,
    "rst" => :rst
  }

  def dir, do: @dir

  @doc "Every socket field a state lists, in the vectors' order."
  def state_fields, do: @state_fields

  @doc "The socket calls a `call` step may name."
  def calls, do: @calls

  @doc "The index: each socket test, its origin, and whether it has a vector."
  def index do
    %{"format" => "smolnet.tcp_vector_index", "version" => 1, "tests" => tests} =
      @dir |> Path.join("index.json") |> File.read!() |> JSON.decode!()

    tests
  end

  @doc """
  The vectors, decoded, in index order. `:origin` keeps those of the given
  origins (`"upstream"`, `"upstream_modified"`, `"smolnet"`).
  """
  def all(options \\ []) do
    origins = Keyword.get(options, :origin, ["upstream", "upstream_modified", "smolnet"])

    for %{"vector" => true, "origin" => origin, "name" => name} <- index(),
        origin in origins do
      load(name, origin)
    end
  end

  def load(name) do
    load(name, Enum.find_value(index(), fn test -> test["name"] == name && test["origin"] end))
  end

  defp load(name, origin) do
    @dir |> Path.join(name <> ".json") |> File.read!() |> JSON.decode!() |> decode(origin)
  end

  defp decode(%{"format" => @format, "version" => @version} = vector, origin) do
    %{
      name: vector["test"],
      origin: origin,
      context: vector["context"],
      panicked: vector["panicked"],
      sockets: Enum.map(vector["sockets"], &decode_socket/1)
    }
  end

  defp decode_socket(socket) do
    %{
      index: socket["index"],
      initial: socket["initial"],
      steps: Enum.map(socket["steps"], &decode_step/1)
    }
  end

  defp decode_step(%{"op" => "set", "state" => state}), do: %{op: :set, state: state}

  defp decode_step(%{"op" => "input"} = step) do
    %{
      op: :input,
      at: step["at"],
      src_addr: step["src_addr"],
      dst_addr: step["dst_addr"],
      segment: segment(step["segment"]),
      reply: step["reply"] && segment(step["reply"]),
      state: step["state"]
    }
  end

  defp decode_step(%{"op" => "dispatch", "result" => "segment"} = step) do
    %{
      op: :dispatch,
      at: step["at"],
      result: :segment,
      segment: segment(step["segment"]),
      hop_limit: step["hop_limit"],
      psh_compared: step["psh_compared"],
      refused: false,
      state: step["state"]
    }
  end

  defp decode_step(%{"op" => "dispatch", "result" => "nothing"} = step) do
    %{op: :dispatch, at: step["at"], result: :nothing, refused: false, state: step["state"]}
  end

  # The device refused the segment the socket tried to send.
  defp decode_step(%{"op" => "dispatch", "result" => "error", "refused" => true} = step) do
    %{
      op: :dispatch,
      at: step["at"],
      result: :error,
      segment: segment(step["segment"]),
      hop_limit: step["hop_limit"],
      refused: true,
      state: step["state"]
    }
  end

  defp decode_step(%{"op" => "call", "name" => name} = step) when name in @calls do
    %{
      op: :call,
      at: step["at"],
      name: name,
      args: step["args"],
      result: step["result"],
      state: step["state"]
    }
  end

  @doc "A segment from its JSON form."
  def segment(json) do
    %{
      src_port: json["src_port"],
      dst_port: json["dst_port"],
      control: Map.fetch!(@controls, json["control"]),
      seq: json["seq"],
      ack: json["ack"],
      window_len: json["window_len"],
      window_scale: json["window_scale"],
      max_seg_size: json["max_seg_size"],
      sack_permitted: json["sack_permitted"],
      sack_ranges: Enum.map(json["sack_ranges"], fn [left, right] -> {left, right} end),
      timestamp: json["timestamp"] && {json["timestamp"]["tsval"], json["timestamp"]["tsecr"]},
      payload: Base.decode16!(json["payload"], case: :lower)
    }
  end

  @doc "A segment in its JSON form, the inverse of `segment/1`."
  def segment_json(segment) do
    control = Enum.find_value(@controls, fn {name, atom} -> atom == segment.control && name end)

    %{
      "src_port" => segment.src_port,
      "dst_port" => segment.dst_port,
      "control" => control,
      "seq" => segment.seq,
      "ack" => segment.ack,
      "window_len" => segment.window_len,
      "window_scale" => segment.window_scale,
      "max_seg_size" => segment.max_seg_size,
      "sack_permitted" => segment.sack_permitted,
      "sack_ranges" => Enum.map(segment.sack_ranges, &Tuple.to_list/1),
      "timestamp" =>
        case segment.timestamp do
          {tsval, tsecr} -> %{"tsval" => tsval, "tsecr" => tsecr}
          nil -> nil
        end,
      "payload" => Base.encode16(segment.payload, case: :lower)
    }
  end

  @doc """
  The full state after each step of a socket, as `[{step, state_before,
  state_after}]`: the initial state with every step's changes applied in
  turn.
  """
  def states(socket) do
    {states, _state} =
      Enum.map_reduce(socket.steps, socket.initial, fn step, before ->
        after_step = Map.merge(before, step.state)
        {{step, before, after_step}, after_step}
      end)

    states
  end
end
