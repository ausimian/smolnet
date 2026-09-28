defmodule SmolNet.Integration.Crawl.Evidence do
  @moduledoc """
  Triage evidence for a host that failed over SmolNet and not over the
  kernel, cut from the run's rolling capture on the device, so that it
  costs the host no further request.

  `collect/5` writes the host's packets to `diag/<host>-<family>.pcap` and
  `diag/<host>-<family>.txt`, and summarises the failed visit's packets:
  the TCP options of SmolNet's SYN and of the host's SYN-ACK, and how many
  SYNs, RSTs and FINs each side sent.
  """

  @type summary :: %{atom() => term()}

  @doc """
  Cuts the packets exchanged with `address` from the capture files in
  `pcap_dir` into `diag_dir`, named for `name`, and summarises those sent
  between `from_us` and `to_us`, in microseconds of system time.
  """
  @spec collect(Path.t(), Path.t(), String.t(), :inet.ip_address(), {integer(), integer()}) ::
          {:ok, summary()} | {:error, String.t()}
  def collect(pcap_dir, diag_dir, name, address, {from_us, to_us}) do
    files = pcap_dir |> Path.join("*.pcap*") |> Path.wildcard() |> Enum.sort_by(&mtime/1)

    with tcpdump when is_binary(tcpdump) <- System.find_executable("tcpdump"),
         [_first | _rest] <- files do
      File.mkdir_p!(diag_dir)
      list = Path.join(diag_dir, ".#{name}.files")
      File.write!(list, Enum.join(files, "\n"))
      filter = ["host", :inet.ntoa(address) |> to_string()]
      pcap = Path.join(diag_dir, "#{name}.pcap")

      # A file tcpdump is still writing ends mid-packet, which it reports
      # and then carries on from, so its status is not the verdict.
      _written =
        System.cmd(tcpdump, ["-n", "-V", list, "-w", pcap | filter], stderr_to_stdout: true)

      {text, _status} =
        System.cmd(tcpdump, ["-n", "-tt", "-r", pcap | filter], stderr_to_stdout: true)

      File.rm(list)
      File.write!(Path.join(diag_dir, "#{name}.txt"), text)

      window = {from_us / 1_000_000 - 0.5, to_us / 1_000_000 + 1.0}
      summary = summarize(text, to_string(:inet.ntoa(address)), window)
      {:ok, Map.put(summary, :pcap, Path.join(Path.basename(diag_dir), "#{name}.pcap"))}
    else
      nil -> {:error, "tcpdump not found"}
      [] -> {:error, "no capture in #{pcap_dir}"}
    end
  end

  defp mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, stat} -> stat.mtime
      {:error, _reason} -> 0
    end
  end

  @doc """
  Summarises `tcpdump -n -tt` output for the packets exchanged with the
  peer at `peer` (its address, as tcpdump prints it) whose times fall in
  `window`, `{from, to}` in seconds: per side, its packets, SYNs, RSTs and
  FINs, and the options of the first SYN and SYN-ACK.
  """
  @spec summarize(String.t(), String.t(), {number(), number()}) :: summary()
  def summarize(text, peer, {from, to}) do
    packets =
      text
      |> String.split("\n")
      |> Enum.flat_map(&packet(&1, peer))
      |> Enum.filter(fn packet -> packet.at >= from and packet.at <= to end)

    {sent, received} = Enum.split_with(packets, &(&1.from == :local))

    %{
      packets: length(packets),
      local: tally(sent),
      peer: tally(received),
      syn_options: options(sent, "S"),
      syn_ack_options: options(received, "S.")
    }
  end

  @line ~r/^(\d+\.\d+) IP6? (\S+) > (\S+): Flags \[([^\]]*)\](.*)$/

  defp packet(line, peer) do
    case Regex.run(@line, line) do
      [_line, at, source, _destination, flags, rest] ->
        {at, ""} = Float.parse(at)
        from = if String.starts_with?(source, peer <> "."), do: :peer, else: :local

        options =
          case Regex.run(~r/options \[([^\]]*)\]/, rest) do
            [_match, options] -> options
            nil -> nil
          end

        [%{at: at, from: from, flags: flags, options: options}]

      nil ->
        []
    end
  end

  defp tally(packets) do
    %{
      packets: length(packets),
      syn: Enum.count(packets, &String.contains?(&1.flags, "S")),
      rst: Enum.count(packets, &String.contains?(&1.flags, "R")),
      fin: Enum.count(packets, &String.contains?(&1.flags, "F"))
    }
  end

  defp options(packets, flags) do
    case Enum.find(packets, &(&1.flags == flags)) do
      %{options: options} -> options
      nil -> nil
    end
  end
end
