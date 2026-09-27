# Compiles the integration harness under integration/lib into the running VM.
#
# The harness is not part of the smolnet application: it is kept out of
# `lib/` so it never ships in the Hex package and never runs under `mix test`
# or `mix precommit`. Every integration script starts by requiring this file:
#
#     Code.require_file("support/load.exs", __DIR__)
#
# A warning fails the load, as `mix compile --warnings-as-errors` would.

# Mix prunes the code path to the project's declared applications; the
# harness also uses these.
Enum.each([:crypto, :ex_unit], &Mix.ensure_application!/1)

unless Code.ensure_loaded?(SmolNet.Integration.Soak) do
  files =
    [__DIR__, "..", "lib", "**", "*.ex"]
    |> Path.join()
    |> Path.expand()
    |> Path.wildcard()
    |> Enum.sort()

  case Kernel.ParallelCompiler.compile(files, return_diagnostics: true) do
    {:ok, _modules, %{compile_warnings: [], runtime_warnings: []}} ->
      :ok

    {:ok, _modules, _warnings} ->
      raise "the integration harness compiled with warnings"

    {:error, _errors, _warnings} ->
      raise "the integration harness failed to compile"
  end
end
