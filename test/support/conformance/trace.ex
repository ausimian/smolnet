defmodule SmolNet.Conformance.Trace do
  @moduledoc false

  # Backend traces: every call a scenario made of its stacks' backends, as
  # SmolNet.Conformance.Driver records them, with the results, packets and
  # notices each call gave. A trace recorded from the NIF is what another
  # backend is compared against once the NIF is gone: `replay/3` makes each
  # stack's calls again, at the same times and with the same packets, on a
  # new backend, and reports the first call whose outcome differs. Each
  # stack replays alone, fed the packets its peer sent in the recording, so
  # a difference shows where it starts rather than everywhere after it.
  #
  # The file format is documented in test/conformance/README.md.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.TermJson

  @format "smolnet.backend_trace"
  @version 1
  @dir Path.expand("../../conformance/traces", __DIR__)

  # What a replay compares, by default all of it. `:poll_at` is the backend's
  # next deadline; `:events` the readiness and abort notices.
  @fields [:result, :emitted, :events, :poll_at]

  def dir, do: @dir

  @type t :: %{name: String.t(), description: String.t(), entries: [Driver.entry()]}

  @spec new(String.t(), String.t(), [Driver.entry()]) :: t()
  def new(name, description, entries),
    do: %{name: name, description: description, entries: entries}

  @doc "The committed traces, by name."
  @spec all() :: [t()]
  def all do
    @dir
    |> Path.join("*.json")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.map(&load/1)
  end

  @spec load(Path.t()) :: t()
  def load(path) do
    [header | entries] = path |> File.read!() |> String.split("\n", trim: true)

    %{"format" => @format, "version" => @version} = header = JSON.decode!(header)

    %{
      name: header["name"],
      description: header["description"],
      entries: Enum.map(entries, &decode_entry/1)
    }
  end

  @doc """
  Writes a trace as a header line and one line per call, so that a
  regenerated trace diffs call by call.
  """
  @spec save(t(), Path.t()) :: :ok
  def save(trace, dir \\ @dir) do
    header =
      for(
        {key, value} <- [
          format: @format,
          version: @version,
          name: trace.name,
          description: trace.description
        ]
      ) do
        [JSON.encode!(Atom.to_string(key)), ?:, JSON.encode!(value)]
      end
      |> Enum.intersperse(?,)
      |> then(&[?{, &1, ?}])

    lines = Enum.map(trace.entries, &[encode_entry(&1), ?\n])
    File.write!(Path.join(dir, trace.name <> ".json"), [header, ?\n | lines])
  end

  # One entry's fields in a fixed order.
  defp encode_entry(entry) do
    fields =
      for key <- [:stack, :at, :call, :args, :result, :poll_at, :emitted, :events] do
        [JSON.encode!(Atom.to_string(key)), ?:, TermJson.encode(Map.fetch!(entry, key))]
      end

    [?{, Enum.intersperse(fields, ?,), ?}]
  end

  defp decode_entry(line) do
    line
    |> JSON.decode!()
    |> Map.new(fn {key, value} -> {String.to_atom(key), TermJson.from_json(value)} end)
  end

  @type mismatch :: %{
          trace: String.t(),
          stack: atom(),
          index: non_neg_integer(),
          call: atom(),
          at: integer(),
          field: atom(),
          expected: term(),
          actual: term()
        }

  @doc """
  Replays every stack of `trace` on `backend` and returns `:ok`, or
  `{:error, mismatch}` for each stack whose replay departed from the
  recording, at the first call that did. `:ignore` lists fields not to
  compare (#{inspect(@fields)}).
  """
  @spec replay(t(), module(), keyword()) :: :ok | {:error, [mismatch()]}
  def replay(trace, backend, options \\ []) do
    fields = @fields -- Keyword.get(options, :ignore, [])

    trace.entries
    |> Enum.with_index()
    |> Enum.group_by(fn {entry, _index} -> entry.stack end)
    |> Enum.flat_map(fn {_stack, entries} -> replay_stack(trace, entries, backend, fields) end)
    |> case do
      [] -> :ok
      mismatches -> {:error, mismatches}
    end
  end

  defp replay_stack(trace, [{%{call: :stack_new} = first, index} | entries], backend, fields) do
    [options] = first.args

    case Driver.start(first.stack, backend, options, first.at) do
      {:ok, driver} ->
        case compare(trace, first, index, driver.trace, fields) do
          nil -> replay_calls(trace, driver, entries, fields)
          mismatch -> [mismatch]
        end

      {error, _driver} ->
        [mismatch(trace, first, index, :result, {:ok, :resource}, error)]
    end
  end

  defp replay_calls(trace, driver, entries, fields) do
    Enum.reduce_while(entries, driver, fn {entry, index}, driver ->
      driver = make_refs(driver, entry.args)
      {_result, driver} = Driver.call(driver, entry.call, entry.args, entry.at)

      case compare(trace, entry, index, driver.trace, fields) do
        nil -> {:cont, driver}
        mismatch -> {:halt, mismatch}
      end
    end)
    |> case do
      %{trace: _trace} = mismatch when not is_struct(mismatch) -> [mismatch]
      _driver -> []
    end
  end

  # The first field in which the latest call differs from the recorded one.
  defp compare(trace, entry, index, [actual | _earlier], fields) do
    Enum.find_value(fields, fn field ->
      expected = Map.fetch!(entry, field)
      got = Map.fetch!(actual, field)
      if expected != got, do: mismatch(trace, entry, index, field, expected, got)
    end)
  end

  # A reference label first used in this call stands for a new reference.
  defp make_refs(driver, args) do
    args
    |> List.flatten()
    |> Enum.reduce(driver, fn
      {:ref, n} = label, driver when not is_map_key(driver.refs, label) ->
        {^label, driver} = Driver.ref(driver)
        if map_size(driver.refs) != n, do: raise("reference labels out of order: #{n}")
        driver

      _arg, driver ->
        driver
    end)
  end

  defp mismatch(trace, entry, index, field, expected, actual) do
    %{
      trace: trace.name,
      stack: entry.stack,
      index: index,
      call: entry.call,
      at: entry.at,
      field: field,
      expected: expected,
      actual: actual
    }
  end
end
