defmodule SmolNet.Integration.Soak.Pcap do
  @moduledoc """
  A rolling packet capture on the TUN device.

  `tcpdump -C 100 -W 10` keeps the most recent ten files of 100 MB in
  `pcap/`, so a long run holds a bounded window of traffic ending at the
  moment it stopped. The runner stops the capture as soon as a run fails,
  keeping that window, and deletes it when a run passes.

  tcpdump needs root or `CAP_NET_RAW`. It runs under a small shell wrapper
  that terminates it when the runner asks, or when the BEAM exits and the
  wrapper's input closes, so no capture outlives its run.
  """

  # Runs tcpdump and terminates it on a line or end of file on stdin, then
  # exits with its status, so a tcpdump that fails to start ends the port.
  #
  # A non-interactive shell starts background commands with stdin from
  # /dev/null and SIGINT ignored, so the watcher reads the port's stdin
  # through a descriptor of its own and sends SIGTERM. It keeps no hold on
  # stdout, so the port sees the wrapper's exit.
  @wrapper """
  exec 3<&0
  "$@" </dev/null &
  child=$!
  echo "pid $child"
  ( read -r _line <&3; kill -TERM "$child" 2>/dev/null ) >/dev/null 2>&1 &
  watcher=$!
  wait "$child"
  status=$?
  kill "$watcher" 2>/dev/null
  echo "tcpdump exited with status $status"
  exit "$status"
  """

  @start_timeout 5_000
  @stop_timeout 10_000

  @opaque t :: %{port: port(), pid: String.t() | nil}

  @doc """
  Starts capturing `device` into `directory`. Returns once tcpdump is
  listening, or `{:error, output}` if it could not start.

  `:tcpdump` names the program to run, `tcpdump` on the path by default.
  `:netns` runs it in that named network namespace (`ip netns exec`), and
  `:snaplen` keeps that many bytes of each packet rather than all of it.
  `immediate: true` has tcpdump take each packet as it arrives, rather
  than in batches, so that a capture stopped straight after a short
  exchange still holds it; it costs a wakeup per packet.
  """
  @spec start(String.t(), Path.t(), keyword()) :: {:ok, t()} | {:error, String.t()}
  def start(device, directory, options \\ []) do
    case Keyword.get_lazy(options, :tcpdump, fn -> System.find_executable("tcpdump") end) do
      nil ->
        {:error, "tcpdump not found"}

      tcpdump ->
        File.mkdir_p!(directory)
        {user, 0} = System.cmd("id", ["-un"])

        snaplen = options |> Keyword.get(:snaplen, 0) |> Integer.to_string()
        immediate = if Keyword.get(options, :immediate, false), do: ["--immediate-mode"], else: []

        arguments =
          in_netns(Keyword.get(options, :netns)) ++
            [tcpdump, "-i", device, "-n", "-s", snaplen, "-U", "-C", "100", "-W", "10"] ++
            immediate ++
            ["-Z", String.trim(user), "-w", Path.join(directory, "#{device}.pcap")]

        port =
          Port.open({:spawn_executable, "/bin/sh"}, [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: ["-c", @wrapper, "pcap" | arguments]
          ])

        await_listening(%{port: port, pid: nil}, "")
    end
  end

  @doc """
  Stops the capture and waits for tcpdump to flush its files. A tcpdump
  that does not stop in time is killed.
  """
  @spec stop(t()) :: :ok
  def stop(%{port: port} = pcap) do
    Port.command(port, "stop\n")
    await_exit(pcap)
  catch
    :error, :badarg -> :ok
  end

  defp in_netns(nil), do: []
  defp in_netns(netns), do: [System.find_executable("ip") || "ip", "netns", "exec", netns]

  defp await_listening(pcap, output) do
    receive do
      {port, {:data, data}} when port == pcap.port ->
        output = output <> data
        pcap = %{pcap | pid: pcap.pid || child_pid(output)}

        if output =~ "listening on" do
          {:ok, pcap}
        else
          await_listening(pcap, output)
        end

      {port, {:exit_status, _status}} when port == pcap.port ->
        {:error, String.trim(output)}
    after
      @start_timeout ->
        stop(pcap)
        {:error, "tcpdump did not start listening: " <> String.trim(output)}
    end
  end

  defp child_pid(output) do
    case Regex.run(~r/^pid (\d+)$/m, output) do
      [_line, pid] -> pid
      nil -> nil
    end
  end

  defp await_exit(%{port: port} = pcap) do
    receive do
      {^port, {:data, _data}} -> await_exit(pcap)
      {^port, {:exit_status, _status}} -> :ok
    after
      @stop_timeout ->
        if pcap.pid, do: System.cmd("kill", ["-KILL", pcap.pid], stderr_to_stdout: true)
        Port.close(port)
        :ok
    end
  end
end
