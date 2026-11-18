defmodule SmolNet.SslGuideTest do
  # Runs the code in `ssl.md`, so that the guide's examples keep working as
  # written.
  use ExUnit.Case, async: false

  alias SmolNet.Test.Timing

  @moduletag :capture_log

  @guide Path.expand("../../ssl.md", __DIR__)
  @external_resource @guide

  @example "A complete loopback example"
  @stop_stack ":ok = SmolNet.stop_stack(stack)\n"

  # Sections whose code needs a real network, so the test cannot run it.
  @needs_network ["Verifying the server"]

  # The guide's 5 s timeouts suit a reader. A liveness budget replaces them
  # here, so a slow runner does not fail a healthy run.
  @timeout Integer.to_string(Timing.liveness(30_000))

  setup do
    on_exit(&stop_all_stacks/0)
  end

  test "the complete example runs as written" do
    [{line, code}] = blocks(@example)
    run(code, line, [])
  end

  test "the later sections run on from the example" do
    [{line, code}] = blocks(@example)

    # They continue as if the example's last line had not run.
    assert String.ends_with?(code, @stop_stack)
    binding = run(String.replace_suffix(code, @stop_stack, ""), line, [])

    later =
      sections()
      |> Enum.drop_while(fn {title, _blocks} -> title != @example end)
      |> tl()
      |> Enum.reject(fn {title, _blocks} -> title in @needs_network end)

    assert length(later) > 1

    Enum.reduce(later, binding, fn {_title, blocks}, binding ->
      Enum.reduce(blocks, binding, fn {line, code}, binding -> run(code, line, binding) end)
    end)
  end

  test "only sections after the example have code" do
    titles = for {title, [_ | _]} <- sections(), do: title
    assert hd(titles) == @example
    assert Enum.all?(@needs_network, &(&1 in titles))
  end

  defp run(code, line, binding) do
    code = String.replace(code, "5_000", @timeout)
    {_result, binding} = Code.eval_string(code, binding, file: @guide, line: line)
    binding
  end

  defp blocks(title) do
    {^title, blocks} = List.keyfind(sections(), title, 0)
    blocks
  end

  # The guide's `##` sections, in order, each with its Elixir code blocks
  # and the line on which each block starts.
  defp sections do
    @guide
    |> File.read!()
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.reduce({[], nil}, &section_line/2)
    |> elem(0)
    |> Enum.map(fn {title, blocks} -> {title, Enum.reverse(blocks)} end)
    |> Enum.reverse()
  end

  defp section_line({"```elixir", line}, {sections, nil}), do: {sections, {line + 1, []}}

  defp section_line({"```", _line}, {[{title, blocks} | sections], {start, lines}}) do
    code = lines |> Enum.reverse() |> Enum.map_join(&(&1 <> "\n"))
    {[{title, [{start, code} | blocks]} | sections], nil}
  end

  defp section_line({text, _line}, {sections, {start, lines}}),
    do: {sections, {start, [text | lines]}}

  defp section_line({"## " <> title, _line}, {sections, nil}),
    do: {[{title, []} | sections], nil}

  defp section_line(_line, state), do: state

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
