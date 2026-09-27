defmodule SmolNet.Integration.TunLink do
  @moduledoc """
  A link that carries a stack's packets to and from a host TUN device.

  The device is opened by a small helper program (see
  `SmolNet.Integration.TunHelper`) that the link runs as a port, so no NIF is
  involved. Packets cross the port framed as `{:packet, 2}`.

      {:ok, link, stack} =
        SmolNet.Integration.TunLink.start_link(
          device: "tun0",
          addresses: [{{10, 77, 0, 2}, 24}],
          routes: [{{0, 0, 0, 0}, 0, {10, 77, 0, 1}}]
        )

  Like `SmolNet.Loopback`, the link starts and owns its stack: the options are
  those of `SmolNet.start_stack/1`, less `:egress`, plus the link's own:

    * `:device` - the TUN device to attach to. The device must exist and be
      usable by this user; `integration/setup.sh` creates one.
    * `:loopback` - `true` to open no device and have the helper echo every
      packet back instead. Needs no privileges, so the port protocol and the
      credit loop can be exercised anywhere. Exactly one of `:device` and
      `:loopback` is required.
    * `:helper` - the helper binary; built on demand by default.
    * `:ready_timeout` - how long to wait for the helper to open the device,
      in milliseconds. Defaults to 5 000.
    * `:ingress_queue` - how many device packets may wait for the stack.
      Defaults to 1 024; see below.
    * `:name` - a name to register the link process under.

  ## Backpressure

  `:egress_credit` defaults to `{64, 131_072}` rather than `:infinity`. The
  helper acknowledges each packet once `write(2)` on the device has returned,
  and the link grants exactly the acknowledged packets and bytes back to the
  stack. The stack therefore never has more egress outstanding than its
  credit, and a device or pipe that stops draining stops the stack's senders.
  With `egress_credit: :infinity` the stack sends every batch at once.

  A stack that is waiting for credit can hold an `SmolNet.ingress/2` call
  until credit arrives, so the link does not feed the stack itself: a
  separate feeder process does, and a held ingress call never delays the
  acknowledgements that return the credit.

  Device packets are bounded too. The link grants the helper credit to read
  `:ingress_queue` packets from the device, and more as the feeder hands
  them to the stack, so no more than that many are ever on their way to the
  stack. While the helper has no credit it leaves packets in the kernel's
  queue for the device, which drops what overflows it, as a real network
  interface does.

  ## Credit starvation

  `starve/2` makes the link hold back the egress credit it would grant:
  late, one packet at a time, or not at all, as a link whose transport
  has stalled does. The stack then sees the device as slow or stopped,
  without a packet being lost. `integration/chaos.exs` uses it.

  ## Failure

  If the helper exits, the link exits with `{:helper_exit, status}`, where
  `status` is the helper's exit status or, if the link wrote to the helper
  after it had gone, the reason its port closed, such as `:epipe`. The
  stack applies its `:link_down` policy as for any other link that dies. The
  link watches its stack with `SmolNet.monitor/1` and stops normally when the
  stack stops. Stopping the link closes the port; the helper then sees end of
  file on its input and exits.
  """

  use GenServer, restart: :temporary

  alias SmolNet.Integration.TunHelper
  alias SmolNet.Stack.Ref

  @link_ref :tun_link
  @default_credit {64, 131_072}
  @link_options [:device, :loopback, :helper, :ready_timeout, :ingress_queue]
  @default_ingress_queue 1_024
  # The largest {packet, 2} frame; every frame spends a byte on its type.
  @max_frame 65_535

  # Indexes into the feeder's :counters.
  @rx_packets 1
  @rx_bytes 2
  @ingress_refused 3
  @ingress_queued 4

  @type stats :: %{
          rx_packets: non_neg_integer(),
          rx_bytes: non_neg_integer(),
          ingress_refused: non_neg_integer(),
          tx_packets: non_neg_integer(),
          tx_bytes: non_neg_integer(),
          tx_dropped: non_neg_integer(),
          in_flight_packets: non_neg_integer(),
          in_flight_bytes: non_neg_integer(),
          credit_waits: non_neg_integer(),
          ingress_queue_len: non_neg_integer(),
          ingress_credit: non_neg_integer(),
          ingress_dropped: non_neg_integer(),
          device: String.t(),
          helper_os_pid: non_neg_integer() | nil,
          starve: starve_mode(),
          credit_withheld_packets: non_neg_integer(),
          credit_withheld_bytes: non_neg_integer()
        }

  @type starve_mode :: :off | :stop | {:delay, pos_integer()} | {:trickle, pos_integer()}

  @doc """
  Starts a link linked to the caller, and the stack it carries.

  Returns `{:ok, link, stack}`.
  """
  @spec start_link(keyword()) :: {:ok, pid(), Ref.t()} | {:error, term()}
  def start_link(options) when is_list(options) do
    {name, options} = Keyword.pop(options, :name)
    started(GenServer.start_link(__MODULE__, options, server_options(name)))
  end

  @doc """
  Starts a link, as `start_link/1` does, without linking it to the caller.

  A stack `:mtu` above 65 534 is rejected with `:invalid_mtu`: a packet
  that large cannot cross the helper's framing.
  """
  @spec start(keyword()) :: {:ok, pid(), Ref.t()} | {:error, term()}
  def start(options) when is_list(options) do
    {name, options} = Keyword.pop(options, :name)
    started(GenServer.start(__MODULE__, options, server_options(name)))
  end

  @doc "Returns the stack the link carries."
  @spec stack(GenServer.server()) :: Ref.t()
  def stack(link), do: GenServer.call(link, :stack)

  @doc """
  Returns the link's counters.

  `tx_*` count packets the stack handed the link, `tx_dropped` the ones the
  device refused, and `in_flight_*` the ones the helper has not acknowledged
  yet. `credit_waits` counts egress batches that left the stack's credit
  exhausted, so that the stack had to wait for the device before sending
  more. `ingress_refused` counts device packets the stack would not accept,
  `ingress_queue_len` those waiting for the feeder to hand them over, and
  `ingress_credit` how many more the helper may read. `ingress_dropped`
  counts echoes the loopback helper had no credit for; a device's own
  drops show in its kernel counters (`ip -s link show`).
  """
  @spec stats(GenServer.server()) :: stats()
  def stats(link), do: GenServer.call(link, :stats)

  @doc """
  Starves the stack of the egress credit the link grants it, or stops.

  From now on, credit the helper returns is granted by `mode`:

    * `:off` - at once, as by default.
    * `{:delay, ms}` - `ms` milliseconds late.
    * `{:trickle, ms}` - held back, and granted one packet every `ms`
      milliseconds, each with an even share of the bytes held.
    * `:stop` - held back.

  Credit held back is granted when the mode becomes `:off` or a delay,
  and a delayed grant that falls due under `:stop` or a trickle is held
  back in turn, so none is lost. `stats/1` reports the mode as `starve`,
  and the credit held back or on its way as `credit_withheld_packets` and
  `credit_withheld_bytes`. Returns `{:error, :no_credit}` if the stack has
  unlimited credit.
  """
  @spec starve(GenServer.server(), starve_mode()) :: :ok | {:error, :no_credit}
  def starve(link, mode) when mode in [:off, :stop], do: GenServer.call(link, {:starve, mode})

  def starve(link, {kind, ms} = mode)
      when kind in [:delay, :trickle] and is_integer(ms) and ms > 0,
      do: GenServer.call(link, {:starve, mode})

  defp server_options(nil), do: []
  defp server_options(name), do: [name: name]

  defp started({:ok, link}), do: {:ok, link, stack(link)}
  defp started({:error, _reason} = error), do: error

  @impl true
  def init(options) do
    # A port closed by a write to a helper already gone exits, rather than
    # reporting the helper's exit status.
    Process.flag(:trap_exit, true)
    {link_options, stack_options} = Keyword.split(options, @link_options)
    stack_options = Keyword.put_new(stack_options, :egress_credit, @default_credit)

    with :ok <- reject_egress(stack_options),
         :ok <- check_mtu(Keyword.get(stack_options, :mtu, 1_500)),
         {:ok, args} <- helper_args(link_options),
         {:ok, port, device} <- open_helper(link_options, args) do
      queue = Keyword.get(link_options, :ingress_queue, @default_ingress_queue)

      with {:ok, state} <- start_stack(port, device, queue, stack_options) do
        {:ok, grant_ingress(state)}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:stack, _from, state), do: {:reply, state.stack, state}

  def handle_call(:stats, _from, state) do
    stats =
      Map.merge(state.stats, %{
        rx_packets: :counters.get(state.ingress, @rx_packets),
        rx_bytes: :counters.get(state.ingress, @rx_bytes),
        ingress_refused: :counters.get(state.ingress, @ingress_refused),
        ingress_queue_len: :counters.get(state.ingress, @ingress_queued),
        ingress_credit: state.rx_granted,
        helper_os_pid: os_pid(state.port),
        starve: state.starve,
        credit_withheld_packets: elem(state.withheld, 0) + elem(state.delayed, 0),
        credit_withheld_bytes: elem(state.withheld, 1) + elem(state.delayed, 1)
      })

    {:reply, stats, state}
  end

  def handle_call({:starve, _mode}, _from, %{credit: nil} = state),
    do: {:reply, {:error, :no_credit}, state}

  def handle_call({:starve, mode}, _from, state) do
    state = %{state | starve: mode}

    result =
      case mode do
        :stop -> {:ok, state}
        {:trickle, _ms} -> {:ok, arm_trickle(state)}
        _off_or_delay -> release(state)
      end

    case result do
      {:ok, state} -> {:reply, :ok, state}
      {:closed, state} -> {:stop, :normal, :ok, state}
    end
  end

  @impl true
  def handle_info({:smol_stack, @link_ref, :egress, packets}, state) do
    state = Enum.reduce(packets, state, &transmit/2)
    {:noreply, note_credit_wait(state)}
  end

  # Every device packet was read against credit, so the feeder's queue never
  # holds more than :ingress_queue.
  def handle_info({port, {:data, <<0, packet::binary>>}}, %{port: port} = state) do
    :counters.add(state.ingress, @ingress_queued, 1)
    send(state.feeder, {:ingress, packet})
    {:noreply, grant_ingress(%{state | rx_granted: state.rx_granted - 1})}
  end

  def handle_info(
        {port, {:data, <<1, packets::32, bytes::32, dropped::32, rx_dropped::32>>}},
        %{port: port} = state
      ) do
    state =
      count(state,
        tx_dropped: dropped,
        in_flight_packets: -packets,
        in_flight_bytes: -bytes,
        ingress_dropped: rx_dropped
      )

    grant(state, packets, bytes)
  end

  def handle_info(:ingress_consumed, state), do: {:noreply, grant_ingress(state)}

  # A delayed grant falls due; under a delay it is granted, and under
  # :stop or a trickle it is held back with the rest.
  def handle_info({:delayed_grant, packets, bytes}, state) do
    state = %{state | delayed: add(state.delayed, -packets, -bytes)}

    case state.starve do
      {:delay, _ms} -> state |> grant_now(packets, bytes) |> noreply()
      _other -> state |> pass_on(packets, bytes) |> noreply()
    end
  end

  def handle_info(:trickle, %{starve: {:trickle, _ms}, withheld: {packets, bytes}} = state)
      when packets > 0 do
    share = if packets == 1, do: bytes, else: div(bytes, packets)
    state = %{state | trickle: nil, withheld: {packets - 1, bytes - share}}
    {result, state} = grant_now(state, 1, share)
    noreply({result, arm_trickle(state)})
  end

  def handle_info(:trickle, state), do: {:noreply, %{state | trickle: nil}}

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    {:stop, {:helper_exit, status}, %{state | port: nil}}
  end

  def handle_info({:EXIT, port, reason}, %{port: port} = state) do
    receive do
      {^port, {:exit_status, status}} -> {:stop, {:helper_exit, status}, %{state | port: nil}}
    after
      0 -> {:stop, {:helper_exit, reason}, %{state | port: nil}}
    end
  end

  # The feeder returns once its stack has gone; any other exit is a crash.
  def handle_info({:EXIT, feeder, reason}, %{feeder: feeder} = state) do
    if reason == :normal, do: {:noreply, state}, else: {:stop, reason, state}
  end

  # The stack was stopped from elsewhere, so the device it fed has no purpose.
  def handle_info({:DOWN, monitor, :process, _object, _reason}, %{monitor: monitor} = state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # A normal exit does not end a linked process, so stop the feeder here.
    Process.unlink(state.feeder)
    Process.exit(state.feeder, :kill)
    close(state.port)
  end

  defp close(nil), do: :ok

  defp close(port) do
    Port.close(port)
  catch
    :error, :badarg -> :ok
  end

  # Hands device packets to the stack, one at a time, for as long as it runs,
  # and tells the link every `batch` packets, and whenever the queue empties,
  # so that it can grant the helper more credit.
  defp feed(feeder) do
    receive do
      {:ingress, packet} ->
        result = SmolNet.ingress(feeder.stack, packet)
        :counters.sub(feeder.counters, @ingress_queued, 1)
        fed = feeder.fed + 1

        if rem(fed, feeder.batch) == 0 or :counters.get(feeder.counters, @ingress_queued) == 0 do
          send(feeder.link, :ingress_consumed)
        end

        case result do
          :ok ->
            :counters.add(feeder.counters, @rx_packets, 1)
            :counters.add(feeder.counters, @rx_bytes, byte_size(packet))
            feed(%{feeder | fed: fed})

          # The stack is gone; the link's monitor stops the link.
          {:error, :closed} ->
            :ok

          {:error, _refused} ->
            :counters.add(feeder.counters, @ingress_refused, 1)
            feed(%{feeder | fed: fed})
        end
    end
  end

  # Grants the helper whatever room the queue has, once there is enough of it
  # to be worth a frame. The helper always holds credit or the feeder has
  # packets to report, so the room is never left ungranted for good.
  defp grant_ingress(state) do
    room = state.ingress_queue - state.rx_granted - :counters.get(state.ingress, @ingress_queued)

    if room >= grant_batch(state.ingress_queue) and command(state.port, <<1, room::32>>) do
      %{state | rx_granted: state.rx_granted + room}
    else
      state
    end
  end

  # A helper that exits closes its port, possibly before the link has seen
  # the exit status that stops it, and a command to a closed port raises.
  # Whatever the link sends meanwhile is lost with the helper.
  defp command(port, data) do
    Port.command(port, data)
  catch
    :error, :badarg -> false
  end

  defp grant_batch(queue), do: max(1, div(queue, 4))

  # Each inbound frame spends a byte of the {packet, 2} limit on its type.
  defp check_mtu(mtu) when is_integer(mtu) and mtu > @max_frame - 1, do: {:error, :invalid_mtu}
  defp check_mtu(_mtu), do: :ok

  defp reject_egress(stack_options) do
    if Keyword.has_key?(stack_options, :egress), do: {:error, :invalid_options}, else: :ok
  end

  defp helper_args(options) do
    case {Keyword.get(options, :device), Keyword.get(options, :loopback, false)} do
      {device, false} when is_binary(device) -> {:ok, ["--device", device]}
      {nil, true} -> {:ok, ["--loopback"]}
      _other -> {:error, :invalid_device}
    end
  end

  defp open_helper(options, args) do
    helper = Keyword.get_lazy(options, :helper, &TunHelper.ensure_built!/0)
    timeout = Keyword.get(options, :ready_timeout, 5_000)

    port =
      Port.open({:spawn_executable, helper}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:packet, 2},
        args: args
      ])

    receive do
      {^port, {:data, <<2, device::binary>>}} -> {:ok, port, device}
      {^port, {:exit_status, status}} -> {:error, {:helper_exit, status}}
    after
      timeout ->
        Port.close(port)
        {:error, :helper_timeout}
    end
  end

  defp start_stack(port, device, queue, stack_options) do
    case SmolNet.start_stack([{:egress, {self(), @link_ref}} | stack_options]) do
      {:ok, stack} ->
        ingress = :counters.new(4, [])

        feeder = %{
          stack: stack,
          counters: ingress,
          link: self(),
          batch: grant_batch(queue),
          fed: 0
        }

        {:ok,
         %{
           port: port,
           stack: stack,
           ingress: ingress,
           ingress_queue: queue,
           rx_granted: 0,
           feeder: spawn_link(fn -> feed(feeder) end),
           monitor: SmolNet.monitor(stack),
           credit: credit(Keyword.fetch!(stack_options, :egress_credit)),
           starve: :off,
           # Credit held back by starve/2, and credit on a delayed grant's
           # timer, as {packets, bytes}.
           withheld: {0, 0},
           delayed: {0, 0},
           trickle: nil,
           mtu: Keyword.get(stack_options, :mtu, 1_500),
           stats: %{
             tx_packets: 0,
             tx_bytes: 0,
             tx_dropped: 0,
             in_flight_packets: 0,
             in_flight_bytes: 0,
             credit_waits: 0,
             ingress_dropped: 0,
             device: device
           }
         }}

      {:error, reason} ->
        Port.close(port)
        {:stop, reason}
    end
  end

  defp credit(:infinity), do: nil
  defp credit({packets, bytes}), do: %{packets: packets, bytes: bytes}

  # A packet too large for a port frame never reaches the helper, so the
  # link drops it and returns its credit itself. A stack MTU at or below the
  # device's keeps every packet well inside the limit.
  defp transmit(packet, state) when byte_size(packet) > @max_frame - 1 do
    state = count(state, tx_packets: 1, tx_bytes: byte_size(packet), tx_dropped: 1)
    _granted = if state.credit, do: SmolNet.grant_egress(state.stack, 1, byte_size(packet))
    state
  end

  defp transmit(packet, state) do
    if command(state.port, [0, packet]) do
      count(state,
        tx_packets: 1,
        tx_bytes: byte_size(packet),
        in_flight_packets: 1,
        in_flight_bytes: byte_size(packet)
      )
    else
      count(state, tx_packets: 1, tx_bytes: byte_size(packet), tx_dropped: 1)
    end
  end

  defp note_credit_wait(%{credit: nil} = state), do: state

  defp note_credit_wait(%{credit: credit, stats: stats} = state) do
    if stats.in_flight_packets >= credit.packets or
         credit.bytes - stats.in_flight_bytes < state.mtu do
      count(state, credit_waits: 1)
    else
      state
    end
  end

  defp grant(%{credit: nil} = state, _packets, _bytes), do: {:noreply, state}
  defp grant(state, 0, _bytes), do: {:noreply, state}

  defp grant(state, packets, bytes), do: state |> pass_on(packets, bytes) |> noreply()

  # Every grant passes through here, where starve/2 applies.
  defp pass_on(%{starve: :off} = state, packets, bytes), do: grant_now(state, packets, bytes)

  defp pass_on(%{starve: {:delay, ms}} = state, packets, bytes) do
    Process.send_after(self(), {:delayed_grant, packets, bytes}, ms)
    {:ok, %{state | delayed: add(state.delayed, packets, bytes)}}
  end

  defp pass_on(state, packets, bytes) do
    {:ok, arm_trickle(%{state | withheld: add(state.withheld, packets, bytes)})}
  end

  # Passes on the credit held back, when starving stops or turns to a delay.
  defp release(%{withheld: {0, _bytes}} = state), do: {:ok, state}

  defp release(%{withheld: {packets, bytes}} = state),
    do: pass_on(%{state | withheld: {0, 0}}, packets, bytes)

  defp arm_trickle(%{starve: {:trickle, ms}, trickle: nil, withheld: {packets, _bytes}} = state)
       when packets > 0,
       do: %{state | trickle: Process.send_after(self(), :trickle, ms)}

  defp arm_trickle(state), do: state

  defp grant_now(state, packets, bytes) do
    case SmolNet.grant_egress(state.stack, packets, bytes) do
      :ok -> {:ok, state}
      {:error, :closed} -> {:closed, state}
    end
  end

  defp noreply({:ok, state}), do: {:noreply, state}
  defp noreply({:closed, state}), do: {:stop, :normal, state}

  defp add({packets, bytes}, more_packets, more_bytes),
    do: {packets + more_packets, bytes + more_bytes}

  defp count(state, increments) do
    stats =
      Enum.reduce(increments, state.stats, fn {key, amount}, stats ->
        Map.update!(stats, key, &(&1 + amount))
      end)

    %{state | stats: stats}
  end

  defp os_pid(nil), do: nil

  defp os_pid(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} -> pid
      nil -> nil
    end
  end
end
