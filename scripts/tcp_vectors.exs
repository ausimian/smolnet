# Regenerates the TCP test vectors in test/conformance/tcp_vectors from the
# vendored smoltcp's socket tests, or checks that the committed ones are
# current. See test/conformance/README.md for the format.
#
#   elixir scripts/tcp_vectors.exs           # regenerate
#   elixir scripts/tcp_vectors.exs --check   # fail if anything would change
#
# The index records each test's origin, which needs the pristine smoltcp
# import (db218b3) in the git history. In a shallow clone, --check
# keeps the committed origins and checks everything else.

defmodule TcpVectors do
  @root Path.expand("..", __DIR__)
  @out Path.join(@root, "test/conformance/tcp_vectors")
  @crate "native/vendor/smoltcp"
  @source "native/vendor/smoltcp/src/socket/tcp.rs"
  @pristine "db218b35e4178982beb1f24d4a7678ecab4100ac"
  @features ["socket-tcp-cubic"]

  # Vectors that depend on something they cannot record.
  @caveats %{
    "test_buffer_wraparound_tx" =>
      "depends on where the send ring buffer wraps, which a state does not record: " <>
        "smoltcp ends a segment at the wrap"
  }

  def main(args) do
    check? = args == ["--check"]
    if args not in [[], ["--check"]], do: abort("usage: elixir scripts/tcp_vectors.exs [--check]")

    # Unique across concurrent runs, in other worktrees too.
    unique = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    tmp = Path.join(System.tmp_dir!(), "smolnet-tcp-vectors-#{System.pid()}-#{unique}")
    File.mkdir!(tmp)

    try do
      dump(tmp)
      File.write!(Path.join(tmp, "index.json"), index(tmp, check?))
      if check?, do: check(tmp), else: install(tmp)
    after
      File.rm_rf!(tmp)
    end
  end

  defp cargo(args, env \\ []) do
    {output, status} =
      System.cmd(
        "cargo",
        ["test", "--manifest-path", Path.join(@crate, "Cargo.toml"), "--lib"] ++
          ["--features", Enum.join(@features, ","), "--target-dir", "native/target/vendor"] ++
          ["socket::tcp::test", "--" | args],
        cd: @root,
        env: env,
        stderr_to_stdout: true
      )

    if status != 0, do: abort(output)
    output
  end

  defp dump(dir), do: cargo(["--quiet"], [{"SMOLTCP_VECTOR_DIR", dir}])

  # Every #[test] in the socket's test module: whether it ran here, whether
  # it left a vector, and where it came from.
  defp index(dir, check?) do
    compiled =
      cargo(["--list"])
      |> String.split("\n")
      |> Enum.flat_map(fn line ->
        case Regex.run(~r/::(\w+): test$/, line) do
          [_, name] -> [name]
          nil -> []
        end
      end)
      |> MapSet.new()

    current = tests(File.read!(Path.join(@root, @source)))
    origins = origins(current, check?)

    entries =
      Enum.map(current, fn {name, test} ->
        vector? = File.exists?(Path.join(dir, name <> ".json"))

        note =
          cond do
            not MapSet.member?(compiled, name) -> "compiled out: #{Enum.join(test.cfg, " ")}"
            not vector? -> "builds no test socket"
            true -> Map.get(@caveats, name)
          end

        %{"name" => name, "origin" => Map.fetch!(origins, name), "vector" => vector?}
        |> then(&if(test.panics?, do: Map.put(&1, "panics", true), else: &1))
        |> then(&if(note, do: Map.put(&1, "note", note), else: &1))
      end)

    header =
      ~s({"format":"smolnet.tcp_vector_index","version":1,"source":#{JSON.encode!(@source)},) <>
        ~s("pristine_commit":#{JSON.encode!(@pristine)},"features":#{JSON.encode!(@features)},"tests":[\n)

    lines = Enum.map_join(entries, ",\n", &encode_entry/1)
    header <> lines <> "\n]}\n"
  end

  defp encode_entry(entry) do
    fields =
      for key <- ["name", "origin", "vector", "panics", "note"], Map.has_key?(entry, key) do
        "#{JSON.encode!(key)}:#{JSON.encode!(entry[key])}"
      end

    "{" <> Enum.join(fields, ",") <> "}"
  end

  # `upstream`: in smoltcp 0.14.0 and unchanged since; `upstream_modified`:
  # in 0.14.0 but changed by a SmolNet patch; `smolnet`: added by one.
  defp origins(current, check?) do
    case System.cmd("git", ["show", "#{@pristine}:#{@source}"], cd: @root, stderr_to_stdout: true) do
      {pristine, 0} ->
        pristine = Map.new(tests(pristine), fn {name, test} -> {name, normal(test.body)} end)

        Map.new(current, fn {name, test} ->
          body = normal(test.body)

          origin =
            case Map.fetch(pristine, name) do
              {:ok, ^body} -> "upstream"
              {:ok, _changed} -> "upstream_modified"
              :error -> "smolnet"
            end

          {name, origin}
        end)

      {_output, _status} when check? ->
        IO.puts(
          "The pristine smoltcp import is not in this clone; keeping the committed origins."
        )

        Path.join(@out, "index.json")
        |> File.read!()
        |> JSON.decode!()
        |> Map.fetch!("tests")
        |> Map.new(&{&1["name"], &1["origin"]})

      {output, _status} ->
        abort("cannot read the pristine smoltcp import: #{output}")
    end
  end

  # A test body as far as behaviour goes: without layout, and with the
  # socket calls the vector harness records written as the harness has
  # them (`s.connect(...)` for `s.socket.connect(&mut s.cx, ...)`).
  defp normal(body) do
    body
    |> String.replace(~r/\s+/, "")
    |> String.replace(~r/\.socket\.(connect|dispatch|accepts)\(&muts\.cx,/, ".\\1(")
    |> String.replace(".socket.close()", ".close()")
  end

  # The test functions of the socket's test module, in source order, each
  # with the attributes above it and its body.
  defp tests(source) do
    lines = String.split(source, "\n")

    lines
    |> Enum.with_index()
    |> Enum.filter(fn {line, _index} -> String.trim(line) == "#[test]" end)
    |> Enum.map(fn {_line, index} ->
      {attributes, [signature | rest]} =
        lines
        |> Enum.drop(index + 1)
        |> Enum.split_while(&(not String.starts_with?(&1, "    fn ")))

      [_, name] = Regex.run(~r/fn (\w+)/, signature)
      body = Enum.take_while(rest, &(&1 != "    }"))

      above =
        lines
        |> Enum.take(index)
        |> Enum.reverse()
        |> Enum.take_while(&String.starts_with?(&1, "    #["))

      cfg =
        (above ++ attributes)
        |> Enum.filter(&String.contains?(&1, "cfg"))
        |> Enum.map(&String.trim/1)

      panics? = Enum.any?(above ++ attributes, &String.contains?(&1, "#[should_panic"))
      {name, %{cfg: cfg, panics?: panics?, body: Enum.join([signature | body], "\n")}}
    end)
  end

  defp check(dir) do
    case System.cmd("diff", ["-r", "-q", @out, dir], stderr_to_stdout: true) do
      {_output, 0} ->
        IO.puts("TCP test vectors are current.")

      {output, _status} ->
        abort(
          "TCP test vectors are stale; run `elixir scripts/tcp_vectors.exs` and commit:\n" <>
            output
        )
    end
  end

  defp install(dir) do
    File.rm_rf!(@out)
    File.mkdir_p!(@out)
    for file <- File.ls!(dir), do: File.cp!(Path.join(dir, file), Path.join(@out, file))

    IO.puts(
      "Wrote #{length(File.ls!(@out)) - 1} vectors and the index to #{Path.relative_to(@out, @root)}."
    )
  end

  defp abort(message) do
    IO.puts(:stderr, message)
    System.halt(1)
  end
end

TcpVectors.main(System.argv())
