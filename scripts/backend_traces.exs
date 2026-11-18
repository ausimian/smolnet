# Records the NIF's backend traces in test/conformance/traces, from the
# scenarios in SmolNet.Conformance.Scenarios. See test/conformance/README.md.
#
#   MIX_ENV=test mix run scripts/backend_traces.exs
#
# The differential test replays the committed traces, so it fails when the
# NIF's behaviour changes until they are recorded again and committed.

alias SmolNet.Conformance.Scenarios
alias SmolNet.Conformance.Trace

if Mix.env() != :test do
  Mix.raise("run with MIX_ENV=test: the scenarios live in test/support")
end

File.rm_rf!(Trace.dir())
File.mkdir_p!(Trace.dir())

traces = Scenarios.record(SmolNet.Stack.Backend.Native)
Enum.each(traces, &Trace.save/1)
IO.puts("Recorded #{length(traces)} traces in #{Path.relative_to_cwd(Trace.dir())}.")
