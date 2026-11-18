defmodule SmolNet.Core.Tcp.Timer do
  @moduledoc false

  # A connection's one timer, as smoltcp's `Timer`, in microseconds:
  #
  #   * `{:idle, keep_alive_at | nil}`: nothing outstanding; the next
  #     keep-alive probe, if keep-alive is on;
  #   * `{:retransmit, expires_at}`: data or a control flag is in flight;
  #   * `:fast_retransmit`: three duplicate ACKs or RACK want a resend now;
  #   * `{:zero_window_probe, expires_at, delay}`: data waits on a zero
  #     window, and `delay` doubles with each probe;
  #   * `{:close, expires_at}`: TIME-WAIT, 10 s.

  alias SmolNet.Core.Tcp.Rtte

  @close_delay 10_000_000

  @type t ::
          {:idle, integer() | nil}
          | {:retransmit, integer()}
          | :fast_retransmit
          | {:zero_window_probe, integer(), non_neg_integer()}
          | {:close, integer()}

  def new, do: {:idle, nil}

  def should_keep_alive?({:idle, at}, now) when at != nil, do: now >= at
  def should_keep_alive?(_timer, _now), do: false

  def should_retransmit?({:retransmit, at}, now), do: now >= at
  def should_retransmit?(:fast_retransmit, _now), do: true
  def should_retransmit?(_timer, _now), do: false

  def should_close?({:close, at}, now), do: now >= at
  def should_close?(_timer, _now), do: false

  def should_zero_window_probe?({:zero_window_probe, at, _delay}, now), do: now >= at
  def should_zero_window_probe?(_timer, _now), do: false

  @doc "When the timer needs a poll: `:now`, `{:time, at}` or `:ingress`."
  def poll_at({:idle, nil}), do: :ingress
  def poll_at({:idle, at}), do: {:time, at}
  def poll_at({:zero_window_probe, at, _delay}), do: {:time, at}
  def poll_at({:retransmit, at}), do: {:time, at}
  def poll_at(:fast_retransmit), do: :now
  def poll_at({:close, at}), do: {:time, at}

  def set_for_idle(_timer, now, keep_alive), do: {:idle, keep_alive && now + keep_alive.idle}

  @doc "Winds up an idle timer with no probe due, as smoltcp's `set_keep_alive`."
  def set_keep_alive({:idle, nil}), do: {:idle, 0}
  def set_keep_alive(timer), do: timer

  def rewind_keep_alive({:idle, _at}, now, keep_alive),
    do: {:idle, keep_alive && now + keep_alive.interval}

  def rewind_keep_alive(timer, _now, _keep_alive), do: timer

  def arm_keep_alive({:idle, _at}, at), do: {:idle, at}
  def arm_keep_alive(timer, _at), do: timer

  def set_for_retransmit({:close, _at} = timer, _now, _delay), do: timer
  def set_for_retransmit(_timer, now, delay), do: {:retransmit, now + delay}

  def set_for_fast_retransmit(_timer), do: :fast_retransmit

  def set_for_close(_timer, now), do: {:close, now + @close_delay}

  def set_for_zero_window_probe(_timer, now, delay), do: {:zero_window_probe, now + delay, delay}

  def rewind_zero_window_probe({:zero_window_probe, _at, delay}, now) do
    delay = min(delay * 2, Rtte.max_rto() * 1_000)
    {:zero_window_probe, now + delay, delay}
  end

  def rewind_zero_window_probe(timer, _now), do: timer

  def idle?({:idle, _at}), do: true
  def idle?(_timer), do: false

  def zero_window_probe?({:zero_window_probe, _at, _delay}), do: true
  def zero_window_probe?(_timer), do: false

  def retransmit?({:retransmit, _at}), do: true
  def retransmit?(:fast_retransmit), do: true
  def retransmit?(_timer), do: false
end
