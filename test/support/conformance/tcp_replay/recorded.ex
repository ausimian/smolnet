defmodule SmolNet.Conformance.TcpReplay.Recorded do
  @moduledoc false

  # A replay subject that answers from the vector itself: smoltcp as it was
  # when the vectors were dumped. It exercises the replay runner end to end
  # until there is a TCP implementation in Elixir to replay against, and it
  # is what a subject's mismatches are measured from. Its `args` are the
  # socket's steps (pass `subject_args: &Recorded.args/2`).
  #
  # `:perturb` in the args, `{step_index, fun}`, applies `fun` to that
  # step's recorded output, so tests can check that the runner notices.

  @behaviour SmolNet.Conformance.TcpReplay

  alias SmolNet.Conformance.TcpVector

  def args(_vector, socket), do: %{steps: socket.steps}

  @impl true
  def new(_context, initial, args) do
    %{state: initial, steps: args.steps, index: 0, perturb: Map.get(args, :perturb)}
  end

  @impl true
  def set(subject, changed, _state) do
    {_step, subject} = next(subject)
    advance(subject, changed)
  end

  @impl true
  def input(subject, _segment, _ip, _at) do
    {step, subject} = next(subject)
    {perturb(subject, step.reply), advance(subject, step.state)}
  end

  @impl true
  def dispatch(subject, _at, _device) do
    {step, subject} = next(subject)

    sent =
      case step.result do
        :segment -> {:segment, step.segment, step.hop_limit}
        other -> other
      end

    {perturb(subject, sent), advance(subject, step.state)}
  end

  @impl true
  def call(subject, _name, _args, _at) do
    {step, subject} = next(subject)
    {perturb(subject, step.result), advance(subject, step.state)}
  end

  @impl true
  def snapshot(subject), do: subject.state

  @impl true
  def compared_fields, do: TcpVector.state_fields()

  defp next(%{steps: [step | steps]} = subject), do: {step, %{subject | steps: steps}}

  defp advance(subject, changed) do
    %{subject | state: Map.merge(subject.state, changed), index: subject.index + 1}
  end

  defp perturb(%{perturb: {index, fun}, index: index}, output), do: fun.(output)
  defp perturb(_subject, output), do: output
end
