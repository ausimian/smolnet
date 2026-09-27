defmodule SmolNet.Integration.Soak.PcapTest do
  # The capture's lifecycle, with a stand-in for tcpdump, which needs root.
  use ExUnit.Case, async: true

  alias SmolNet.Integration.Soak.Pcap

  @moduletag :tmp_dir

  # Behaves as tcpdump does: announces itself on stderr, then captures until
  # a signal ends it, recording which one.
  @fake """
  #!/bin/sh
  out=$(dirname "$0")/signal
  trap 'echo TERM > "$out"; exit 0' TERM
  trap 'echo INT > "$out"; exit 0' INT
  echo "listening on $2" >&2
  while :; do sleep 0.05; done
  """

  test "stops a capture promptly when asked", %{tmp_dir: dir} do
    fake = fake_tcpdump(dir, @fake)
    assert {:ok, pcap} = Pcap.start("tun9", Path.join(dir, "pcap"), tcpdump: fake)

    {elapsed, :ok} = :timer.tc(fn -> Pcap.stop(pcap) end)
    assert elapsed < 2_000_000
    assert File.read!(Path.join(dir, "signal")) == "TERM\n"
  end

  test "reports a tcpdump that cannot start", %{tmp_dir: dir} do
    fake = fake_tcpdump(dir, "#!/bin/sh\necho 'tcpdump: tun9: No such device' >&2\nexit 1\n")

    assert {:error, output} = Pcap.start("tun9", Path.join(dir, "pcap"), tcpdump: fake)
    assert output =~ "No such device"
  end

  defp fake_tcpdump(dir, script) do
    path = Path.join(dir, "tcpdump")
    File.write!(path, script)
    File.chmod!(path, 0o755)
    path
  end
end
