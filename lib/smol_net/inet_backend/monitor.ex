defmodule SmolNet.InetBackend.Monitor do
  @moduledoc false

  # `:inet.monitor/1` for a socket backed by a process.
  #
  # `:inet` expects `{:DOWN, ref, :socket, socket, info}`, as OTP's
  # `gen_tcp_socket` sends through `socket_registry`, not the `:process`
  # message a monitor of the socket's process would deliver. So each monitor
  # is a relay process that monitors the socket's process and the monitoring
  # process, and turns the first's exit into the socket's DOWN. The relay, not
  # the socket, sends it, so that a socket killed without running `terminate`
  # still triggers its monitors.
  #
  # `cancel/1` finds the relay through a table keyed by the monitor's
  # reference. The relay decides whether a cancel or the socket's exit comes
  # first, so a cancel that returns `true` is never followed by a DOWN.

  @table __MODULE__

  @doc "Creates the table of live monitors, owned by the calling process."
  @spec create_table() :: atom()
  def create_table do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
  end

  @doc """
  Monitors `pid`, the process behind `socket`, for the calling process.

  Returns once the monitor is in place. When `pid` exits, the caller gets
  `{:DOWN, ref, :socket, socket, :closed}`, or `:nosock` in place of
  `:closed` if `pid` was not alive when the monitor was set.
  """
  @spec monitor(term(), pid()) :: reference()
  def monitor(socket, pid) when is_pid(pid) do
    owner = self()
    ref = make_ref()
    {relay, relay_monitor} = spawn_monitor(fn -> relay(owner, socket, pid, ref) end)

    receive do
      {^ref, :ready} ->
        Process.demonitor(relay_monitor, [:flush])
        ref

      {:DOWN, ^relay_monitor, :process, ^relay, reason} ->
        :erlang.error({:invalid, reason})
    end
  end

  @doc """
  Cancels a monitor that the calling process set with `monitor/2`.

  Returns `true` if it was removed before it triggered, after which no DOWN
  for it arrives, and `false` if it is unknown, belongs to another process,
  or has already triggered, in which case its DOWN is already in the
  caller's mailbox.
  """
  @spec cancel(reference()) :: boolean()
  def cancel(ref) when is_reference(ref) do
    owner = self()

    case lookup(ref) do
      {:ok, relay, ^owner} ->
        relay_monitor = Process.monitor(relay)
        send(relay, {:cancel, ref, relay_monitor})

        receive do
          {^relay_monitor, :cancelled} ->
            Process.demonitor(relay_monitor, [:flush])
            true

          {:DOWN, ^relay_monitor, :process, ^relay, _reason} ->
            false
        end

      _other ->
        false
    end
  end

  defp lookup(ref) do
    case :ets.lookup(@table, ref) do
      [{^ref, relay, owner}] -> {:ok, relay, owner}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp relay(owner, socket, pid, ref) do
    socket_monitor = Process.monitor(pid)
    owner_monitor = Process.monitor(owner)
    true = :ets.insert_new(@table, {ref, self(), owner})
    send(owner, {ref, :ready})

    receive do
      {:DOWN, ^socket_monitor, :process, ^pid, reason} ->
        forget(ref)
        info = if reason == :noproc, do: :nosock, else: :closed
        send(owner, {:DOWN, ref, :socket, socket, info})

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        forget(ref)

      {:cancel, ^ref, tag} ->
        forget(ref)
        send(owner, {tag, :cancelled})
    end
  end

  defp forget(ref) do
    :ets.delete(@table, ref)
  rescue
    ArgumentError -> true
  end
end
