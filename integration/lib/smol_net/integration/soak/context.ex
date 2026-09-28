defmodule SmolNet.Integration.Soak.Context do
  @moduledoc """
  What a soak workload is handed: the run's options, and the network it runs
  over.

  `:extra` holds the values of the script's own switches, and
  `:egress_credit` the `--egress-credit` the runner's link starts its stack
  with, for a scenario that starts stacks of its own. `:stack` and
  `:link` are `nil` in baseline mode, where there is no SmolNet stack. The
  remaining fields belong to `SmolNet.Integration.Soak`; pass the context to
  its functions rather than reading them.
  """

  @enforce_keys [:script, :mode, :families, :concurrency, :duration_ms, :out_dir]
  defstruct [
    :script,
    :mode,
    :families,
    :concurrency,
    :duration_ms,
    :out_dir,
    :device,
    :netem,
    :egress_credit,
    :stack,
    :link,
    :server,
    :table,
    :started_at,
    :ends_at,
    extra: %{},
    quiet: false
  ]

  @type t :: %__MODULE__{
          script: String.t(),
          mode: SmolNet.Integration.Soak.Options.mode(),
          families: [:inet | :inet6],
          concurrency: pos_integer(),
          duration_ms: non_neg_integer(),
          out_dir: Path.t(),
          device: String.t() | nil,
          netem: String.t() | nil,
          egress_credit: {non_neg_integer(), non_neg_integer()} | :infinity | nil,
          stack: SmolNet.Stack.Ref.t() | nil,
          link: pid() | nil,
          server: pid() | nil,
          table: :ets.tid() | nil,
          started_at: integer() | nil,
          ends_at: integer() | nil,
          extra: map(),
          quiet: boolean()
        }
end
