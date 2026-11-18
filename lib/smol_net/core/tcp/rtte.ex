defmodule SmolNet.Core.Tcp.Rtte do
  @moduledoc false

  # The round-trip time estimator of RFC 6298, as smoltcp's `RttEstimator`:
  # SRTT, RTTVAR and RTO in milliseconds, one segment timed at a time, and
  # the backed-off RTO after timeouts. SmolNet departs from the RFC's 1 s
  # floor, as Linux does: the RTO is at least 200 ms. The initial RTO is
  # 1 s and the most it backs off to 60 s.
  #
  # `sample` is the segment being timed, `{sent_at, seq}` with the time in
  # microseconds, as every instant in `SmolNet.Core.Tcp.Connection` is.

  alias SmolNet.Core.Tcp.Seq

  @initial_rto 1_000
  @min_margin 5
  @k 4
  @min_rto 200
  @max_rto 60_000

  defstruct have_measurement: false,
            srtt: 0,
            rttvar: 0,
            rto: @initial_rto,
            sample: nil,
            max_seq_sent: nil,
            rto_count: 0

  @type t :: %__MODULE__{}

  def new, do: %__MODULE__{}

  def max_rto, do: @max_rto

  @doc "The retransmission timeout, in microseconds."
  def retransmission_timeout(rtte), do: rtte.rto * 1_000

  @doc "SRTT in microseconds, once a round trip has been measured."
  def srtt(%__MODULE__{have_measurement: true, srtt: srtt}), do: srtt * 1_000
  def srtt(_rtte), do: nil

  @doc "Whether a round trip is being timed."
  def sampling?(rtte), do: rtte.sample != nil

  @doc "SRTT in milliseconds, or 0 before a measurement, as CUBIC reads it."
  def smoothed_rtt(%__MODULE__{have_measurement: true, srtt: srtt}), do: srtt
  def smoothed_rtt(_rtte), do: 0

  @doc "Takes a round-trip sample of `rtt` milliseconds."
  def sample(%__MODULE__{have_measurement: true} = rtte, rtt) do
    diff = abs(rtte.srtt - rtt)
    rttvar = div_ceil(rtte.rttvar * 3 + diff, 4)
    srtt = div_ceil(rtte.srtt * 7 + rtt, 8)
    finish(%{rtte | rttvar: rttvar, srtt: srtt})
  end

  def sample(rtte, rtt),
    do: finish(%{rtte | have_measurement: true, srtt: rtt, rttvar: div(rtt, 2)})

  defp finish(rtte) do
    margin = max(@min_margin, rtte.rttvar * @k)
    rto = (rtte.srtt + margin) |> max(@min_rto) |> min(@max_rto)
    %{rtte | rto: rto, rto_count: 0}
  end

  defp div_ceil(a, b), do: div(a + b - 1, b)

  @doc "A segment ending at `seq` was sent at `now`."
  def on_send(rtte, now, seq) do
    if rtte.max_seq_sent == nil or Seq.gt?(seq, rtte.max_seq_sent) do
      sample = rtte.sample || {now, seq}
      %{rtte | max_seq_sent: seq, sample: sample}
    else
      rtte
    end
  end

  @doc "An ACK of `seq` arrived at `now`."
  def on_ack(%__MODULE__{sample: {sent_at, sent_seq}} = rtte, now, seq) do
    if Seq.ge?(seq, sent_seq),
      do: sample(%{rtte | sample: nil}, div(now - sent_at, 1_000)),
      else: rtte
  end

  def on_ack(rtte, _now, _seq), do: rtte

  @doc "The retransmission timer expired: back off, and forget SRTT after three."
  def on_rto(rtte) do
    rto = min(rtte.rto * 2, @max_rto)
    count = rtte.rto_count + 1

    if count >= 3,
      do: %{rtte | rto: rto, rto_count: 0, have_measurement: false},
      else: %{rtte | rto: rto, rto_count: count}
  end

  @doc "Something was resent, so the segment being timed gives no sample (Karn)."
  def on_retransmit(rtte), do: %{rtte | sample: nil}
end
