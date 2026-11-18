defmodule SmolNet.Core.Tcp.Cubic do
  @moduledoc false

  # CUBIC congestion control (RFC 9438) as SmolNet's smoltcp has it, with
  # its patches: RFC 6928's initial window of ten segments once the MSS is
  # known, RFC 3465 byte counting in slow start and congestion avoidance,
  # one MSS per ACK in the slow start after a timeout, and no HyStart.
  # Instants are microseconds. Floating point follows smoltcp's operation
  # for operation, its fused multiply-add included, so that the window
  # comes out the same to the byte.
  #
  # `ssthresh` starts at `usize::MAX`, `usize_max/0`, so that arithmetic on
  # it matches smoltcp's.

  import Bitwise

  @beta 0.7
  @c 0.4
  @alpha 3.0 * (1.0 - @beta) / (1.0 + @beta)
  @default_mss 1024
  @usize_max 0xFFFF_FFFF_FFFF_FFFF

  defstruct w_max: @default_mss * 2,
            cwnd: @default_mss * 2,
            mss: @default_mss,
            ssthresh: @usize_max,
            rwnd: 64 * @default_mss,
            k: 0.0,
            w_est: @default_mss * 2 * 1.0,
            cwnd_prior: @default_mss * 2,
            recovery_start: nil,
            in_fast_recovery: false,
            in_rto_recovery: false,
            rto_slow_start: false,
            idle_start: nil

  @type t :: %__MODULE__{}

  def usize_max, do: @usize_max

  @spec new() :: t()
  def new, do: recompute_k(%__MODULE__{})

  def window(cubic), do: cubic.cwnd

  defp recompute_k(cubic) do
    c_as_bytes = @c * cubic.mss
    k3 = cubic.w_max * (1.0 - @beta) / c_as_bytes
    %{cubic | k: cube_root(k3)}
  end

  # RFC 9438 4.2: the idle period does not count towards `t`.
  defp absorb_idle(cubic, now) do
    recovery_start =
      case cubic do
        %{idle_start: idle, recovery_start: start}
        when idle != nil and start != nil and now >= idle ->
          start + (now - idle)

        _ ->
          cubic.recovery_start
      end

    %{cubic | recovery_start: recovery_start, idle_start: nil}
  end

  def on_ack(cubic, now, len, in_flight, smoothed_rtt) do
    cubic = absorb_idle(cubic, now)
    cubic = if in_flight == 0, do: %{cubic | idle_start: now}, else: cubic

    cond do
      len == 0 ->
        cubic

      cubic.in_fast_recovery ->
        %{
          cubic
          | in_rto_recovery: false,
            in_fast_recovery: false,
            cwnd: cubic.ssthresh,
            w_est: cubic.ssthresh * 1.0
        }

      cubic.cwnd < cubic.ssthresh ->
        cubic = %{cubic | in_rto_recovery: false}
        counted = if cubic.rto_slow_start, do: min(len, cubic.mss), else: len
        grow = min(counted, cubic.ssthresh - cubic.cwnd)
        cwnd = (cubic.cwnd + grow) |> min(cubic.rwnd) |> max(cubic.mss)
        cubic = %{cubic | cwnd: cwnd}
        if cwnd >= cubic.ssthresh, do: %{cubic | rto_slow_start: false}, else: cubic

      true ->
        avoid_congestion(%{cubic | in_rto_recovery: false}, now, len, smoothed_rtt)
    end
  end

  defp avoid_congestion(cubic, now, len, smoothed_rtt) do
    {cubic, recovery_start} =
      case cubic.recovery_start do
        nil ->
          {%{cubic | w_max: cubic.cwnd, k: 0.0, w_est: cubic.cwnd * 1.0, recovery_start: now},
           now}

        start ->
          {cubic, start}
      end

    t = now - recovery_start

    if t < 0 do
      cubic
    else
      c_as_bytes = @c * cubic.mss
      w_cubic = c_as_bytes * cube(t / 1_000_000.0 - cubic.k) + cubic.w_max

      alpha = if cubic.w_est >= cubic.cwnd_prior, do: 1.0, else: @alpha
      w_est = cubic.w_est + alpha * cubic.mss * len / cubic.cwnd
      cubic = %{cubic | w_est: w_est}

      if w_cubic < w_est do
        %{cubic | cwnd: w_est |> to_usize() |> min(cubic.rwnd) |> max(cubic.mss)}
      else
        srtt = max(smoothed_rtt * 1_000, 1_000)
        t_ahead = (t + srtt) / 1_000_000.0
        raw = c_as_bytes * cube(t_ahead - cubic.k) + cubic.w_max
        target = min(raw, 1.5 * cubic.cwnd)
        gap = max(to_usize(target) - cubic.cwnd, 0)
        increment = div(gap * len, cubic.cwnd)
        %{cubic | cwnd: (cubic.cwnd + increment) |> min(cubic.rwnd) |> max(cubic.mss)}
      end
    end
  end

  # `powi(3)`.
  defp cube(x), do: x * x * x

  # `f64 as usize`: truncates, saturating at zero.
  defp to_usize(x) when x <= 0, do: 0
  defp to_usize(x), do: trunc(x)

  def on_dup_ack(%__MODULE__{in_fast_recovery: true} = cubic, len),
    do: %{cubic | cwnd: (cubic.cwnd + len) |> min(cubic.rwnd) |> max(cubic.mss)}

  def on_dup_ack(cubic, _len), do: cubic

  def post_transmit(cubic, now), do: absorb_idle(cubic, now)

  def on_loss(cubic, now, in_flight) do
    cubic = %{cubic | idle_start: nil, rto_slow_start: false}

    if cubic.in_fast_recovery do
      cubic
    else
      w_max =
        if cubic.cwnd < cubic.w_max,
          do: to_usize(cubic.cwnd * (1.0 + @beta) / 2.0),
          else: cubic.cwnd

      ssthresh = max(to_usize(in_flight * @beta), 2 * cubic.mss)

      recompute_k(%{
        cubic
        | cwnd_prior: cubic.cwnd,
          w_max: w_max,
          ssthresh: ssthresh,
          cwnd: min(ssthresh, cubic.rwnd) + 3 * cubic.mss,
          recovery_start: now,
          in_fast_recovery: true
      })
    end
  end

  def on_rto(cubic, in_flight) do
    cubic =
      if cubic.in_rto_recovery,
        do: cubic,
        else: %{
          cubic
          | ssthresh: max(to_usize(in_flight * @beta), 2 * cubic.mss),
            in_rto_recovery: true
        }

    %{
      cubic
      | cwnd: cubic.mss,
        cwnd_prior: in_flight,
        rto_slow_start: true,
        recovery_start: nil,
        in_fast_recovery: false,
        idle_start: nil
    }
  end

  def set_mss(cubic, mss) do
    cubic =
      if cubic.ssthresh == @usize_max and cubic.recovery_start == nil and
           not cubic.in_fast_recovery and not cubic.in_rto_recovery do
        window = initial_window(mss)

        if cubic.cwnd < window,
          do: %{cubic | cwnd: window, w_max: window, w_est: window * 1.0, cwnd_prior: window},
          else: cubic
      else
        cubic
      end

    recompute_k(%{cubic | mss: mss})
  end

  def set_remote_window(cubic, remote_window) do
    if cubic.rwnd < remote_window, do: %{cubic | rwnd: remote_window}, else: cubic
  end

  # RFC 6928 (2).
  defp initial_window(mss), do: min(10 * mss, max(2 * mss, 14_600))

  @doc false
  # smoltcp's `cube_root`: a first guess from the float's bits, then one
  # Newton-Raphson step.
  def cube_root(a) when not is_float(a) or a < 2.2250738585072014e-308, do: 0.0

  def cube_root(a) do
    <<bits::64>> = <<a::float-64>>
    <<m::float-64>> = <<bor(band(bits, 0x000F_FFFF_FFFF_FFFF), 0x3FF0_0000_0000_0000)::64>>
    cbrt_m = fma(m, 0.2599, 0.7401)
    e = band(bits >>> 52, 0x7FF) - 1023
    q = Integer.floor_div(e, 3)
    r = Integer.mod(e, 3)
    <<pow2q::float-64>> = <<(q + 1023) <<< 52::64>>
    x = cbrt_m * pow2q * elem({1.0, 1.2599210498948732, 1.5874010519681994}, r)
    (2.0 * x + a / (x * x)) / 3.0
  end

  # `a * b + c` rounded once, for positive normal doubles, as `f64::mul_add`.
  defp fma(a, b, c) do
    {ma, ea} = decode(a)
    {mb, eb} = decode(b)
    {mc, ec} = decode(c)
    e = min(ea + eb, ec)
    n = ((ma * mb) <<< (ea + eb - e)) + (mc <<< (ec - e))
    round_to_float(n, e)
  end

  defp decode(x) do
    <<0::1, exp::11, frac::52>> = <<x::float-64>>
    {frac + (1 <<< 52), exp - 1075}
  end

  defp round_to_float(n, e) do
    shift = bit_length(n) - 53

    {q, e} =
      if shift <= 0 do
        {n <<< -shift, e + shift}
      else
        q = n >>> shift
        r = n - (q <<< shift)
        half = 1 <<< (shift - 1)
        q = if r > half or (r == half and band(q, 1) == 1), do: q + 1, else: q
        if q == 1 <<< 53, do: {q >>> 1, e + shift + 1}, else: {q, e + shift}
      end

    # q * 2^e with q in [2^52, 2^53): a biased exponent of e + 1075.
    <<value::float-64>> = <<0::1, e + 1075::11, q - (1 <<< 52)::52>>
    value
  end

  defp bit_length(0), do: 0
  defp bit_length(n), do: length(Integer.digits(n, 2))
end
