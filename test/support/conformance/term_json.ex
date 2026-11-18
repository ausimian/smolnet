defmodule SmolNet.Conformance.TermJson do
  @moduledoc false

  # Erlang terms as JSON, for the backend traces, so that a trace reads as
  # text and a regenerated one diffs line by line. Terms map one to one:
  #
  #   nil, true, false    null, true, false
  #   other atoms         strings: "ok"
  #   integers            numbers
  #   binaries            {"hex": "0a1b"}
  #   lists               arrays
  #   tuples              {"tuple": [...]}
  #   maps                {"map": [["key", value], ...]}, atom keys only,
  #                       sorted, so that the text is stable too
  #
  # Traces hold no strings, floats, pids or references: the driver replaces
  # pids, references and socket identities with labels first.

  @spec encode(term()) :: iodata()
  def encode(term), do: term |> to_json() |> JSON.encode_to_iodata!()

  @spec decode(binary()) :: term()
  def decode(json), do: json |> JSON.decode!() |> from_json()

  @spec to_json(term()) :: term()
  def to_json(term) when term in [nil, true, false], do: term
  def to_json(term) when is_atom(term), do: Atom.to_string(term)
  def to_json(term) when is_integer(term), do: term
  def to_json(term) when is_binary(term), do: %{"hex" => Base.encode16(term, case: :lower)}
  def to_json(term) when is_list(term), do: Enum.map(term, &to_json/1)
  def to_json(term) when is_tuple(term), do: %{"tuple" => term |> Tuple.to_list() |> to_json()}

  def to_json(term) when is_map(term) do
    pairs =
      term
      |> Enum.sort()
      |> Enum.map(fn {key, value} when is_atom(key) -> [Atom.to_string(key), to_json(value)] end)

    %{"map" => pairs}
  end

  @spec from_json(term()) :: term()
  def from_json(json) when json in [nil, true, false], do: json
  def from_json(json) when is_binary(json), do: String.to_atom(json)
  def from_json(json) when is_integer(json), do: json
  def from_json(%{"hex" => hex}), do: Base.decode16!(hex, case: :lower)
  def from_json(json) when is_list(json), do: Enum.map(json, &from_json/1)
  def from_json(%{"tuple" => elements}), do: elements |> from_json() |> List.to_tuple()

  def from_json(%{"map" => pairs}) do
    Map.new(pairs, fn [key, value] -> {String.to_atom(key), from_json(value)} end)
  end
end
