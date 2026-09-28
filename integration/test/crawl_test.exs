defmodule SmolNet.Integration.CrawlTest do
  # The crawl scenario's local target, over the helper's loopback and the
  # kernel's, and its pure pieces: the host list, the classification of
  # outcomes, the comparison and the capture summary. Needs no device, no
  # root and no internet.
  use ExUnit.Case, async: false

  alias SmolNet.Integration.Crawl.Evidence
  alias SmolNet.Integration.Crawl.Hosts
  alias SmolNet.Integration.Crawl.Probe
  alias SmolNet.Integration.Https
  alias SmolNet.Integration.Scenarios.Crawl
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  setup_all do
    {:ok, _started} = Application.ensure_all_started(:ssl)
    :ok
  end

  test "both clients see the local servers' outcomes over the helper's loopback", %{tmp_dir: dir} do
    argv =
      ~w(--self-check --target local --duration 0 --family both --host-timeout 1000 --out #{dir})

    assert {:pass, verdict} = run(argv)

    assert verdict.counters.rounds == 1
    # Four servers per family, each visited by both clients.
    assert verdict.counters.visits == 2 * 4 * 2
    assert verdict.results.comparison.smolnet_only == 0
    assert verdict.results.comparison.kernel_only == 0

    for client <- ["smolnet", "smolnet peer"] do
      outcomes = verdict.results.outcomes[client]
      assert outcomes[:ok] == 2
      assert outcomes[:tls_timeout] == 2
      assert outcomes[:refused] == 2
      assert (outcomes[:tls_closed] || 0) + (outcomes[:reset] || 0) == 2
    end

    assert Enum.any?(verdict.notes, &(&1 =~ "not pushed in self_check mode"))

    assert [header | rows] =
             dir |> Path.join("hosts.csv") |> File.read!() |> String.split("\n", trim: true)

    assert header =~ "round,family,host"
    assert length(rows) == 16
  end

  test "the local target runs over the kernel alone in baseline mode", %{tmp_dir: dir} do
    argv =
      ~w(--baseline --target local --duration 0 --family inet --host-timeout 1000 --out #{dir})

    assert {:pass, verdict} = run(argv)

    assert %{"kernel" => outcomes} = verdict.results.outcomes
    assert map_size(verdict.results.outcomes) == 1
    assert outcomes[:ok] == 1 and outcomes[:refused] == 1 and outcomes[:tls_timeout] == 1
  end

  test "the internet target is refused over the helper's loopback", %{tmp_dir: dir} do
    assert {:fail, verdict} = run(~w(--self-check --duration 0 --out #{dir}))
    assert [%{kind: :usage, summary: summary}] = verdict.failures
    assert summary =~ "--target local"
  end

  test "the internet target keeps its concurrency polite", %{tmp_dir: dir} do
    assert {:fail, verdict} = run(~w(--baseline --concurrency 64 --duration 0 --out #{dir}))
    assert [%{kind: :usage, summary: summary}] = verdict.failures
    assert summary =~ "at most 32"
  end

  describe "Hosts" do
    test "parses bare and ranked lines, skipping comments, duplicates and junk" do
      text =
        "# a list\r\n1,google.com\n\nExample.COM\n2,google.com\nnot_a_host\n3,localhost\n4,a-b.co.uk\n"

      assert Hosts.parse(text) == ["google.com", "example.com", "a-b.co.uk"]
    end

    test "loads the top of the fixture", %{tmp_dir: dir} do
      assert {:ok, hosts, info} = Hosts.load("fixture", 3, dir)
      assert hosts == ["google.com", "cloudflare.com", "wikipedia.org"]
      assert %{source: "fixture", top: 3, hosts: 3} = info
    end

    test "reports a list it cannot read", %{tmp_dir: dir} do
      assert {:error, message} = Hosts.load(Path.join(dir, "missing.csv"), 10, dir)
      assert message =~ "could not read"
    end
  end

  describe "Probe.classify/1" do
    test "names each stage's failures" do
      cases = [
        {{:http, {:ok, %{status: 301}}}, :ok},
        {{:tcp, {:error, :timeout}}, :connect_timeout},
        {{:tcp, {:error, :econnrefused}}, :refused},
        {{:tcp, {:error, :system_limit}}, :system_limit},
        {{:tcp, {:error, :ehostunreach}}, :unreachable},
        {{:tls, {:error, :econnreset}}, :reset},
        {{:tls, {:error, {:tls_alert, {:handshake_failure, ~c"TLS client: ..."}}}}, :tls_alert},
        {{:tls, {:error, :timeout}}, :tls_timeout},
        {{:tls, {:error, :closed}}, :tls_closed},
        {{:tls, {:error, {:options, :x}}}, :tls_error},
        {{:http, {:error, :timeout}}, :http_timeout},
        {{:http, {:error, :closed}}, :http_closed},
        {{:http, {:error, {:http_error, "junk"}}}, :http_error},
        {{:tcp, {:error, :einval}}, :other}
      ]

      for {result, class} <- cases do
        assert Probe.classify(result).class == class, inspect(result)
      end
    end

    test "groups TLS alerts by their description" do
      outcome = Probe.classify({:tls, {:error, {:tls_alert, {:unknown_ca, ~c"..."}}}})
      assert Probe.cause(outcome) == "tls_alert unknown_ca"
      assert Probe.cause(Probe.classify({:tcp, {:error, :timeout}})) == "connect_timeout"
    end
  end

  test "compares SmolNet's outcome of a visit with the kernel's" do
    ok = %{class: :ok}
    failed = %{class: :reset}

    assert Crawl.compare(ok, nil) == nil
    assert Crawl.compare(ok, ok) == nil
    assert Crawl.compare(failed, ok) == :smolnet_only
    assert Crawl.compare(ok, failed) == :kernel_only
    assert Crawl.compare(failed, %{class: :connect_timeout}) == :both_failed
  end

  test "summarises a visit's packets from tcpdump's output" do
    text = """
    100.000000 IP 10.77.0.2.49152 > 192.0.2.7.443: Flags [S], seq 1, win 64240, options [mss 1460,sackOK,TS val 1 ecr 0,nop,wscale 7], length 0
    100.020000 IP 192.0.2.7.443 > 10.77.0.2.49152: Flags [S.], seq 9, ack 2, win 65535, options [mss 1400,nop,nop,sackOK,nop,wscale 9], length 0
    100.040000 IP 10.77.0.2.49152 > 192.0.2.7.443: Flags [.], ack 1, win 502, length 0
    100.500000 IP 192.0.2.7.443 > 10.77.0.2.49152: Flags [R.], seq 1, ack 1, win 0, length 0
    200.000000 IP 10.77.0.2.49153 > 192.0.2.7.443: Flags [S], seq 1, win 64240, length 0
    """

    summary = Evidence.summarize(text, "192.0.2.7", {99.5, 101.0})

    assert summary.packets == 4
    assert summary.local == %{packets: 2, syn: 1, rst: 0, fin: 0}
    assert summary.peer == %{packets: 2, syn: 1, rst: 1, fin: 0}
    assert summary.syn_options == "mss 1460,sackOK,TS val 1 ecr 0,nop,wscale 7"
    assert summary.syn_ack_options == "mss 1400,nop,nop,sackOK,nop,wscale 9"
  end

  describe "Https.head/4" do
    test "reads the head, and not the body its content-length announces" do
      response = "HTTP/1.1 200 OK\r\ncontent-length: 1234\r\nServer: x\r\n\r\n"

      assert {:ok, %{status: 200, headers: %{"content-length" => "1234", "server" => "x"}}} =
               head(response, 1_000)
    end

    test "times out when the response does not come" do
      started = System.monotonic_time(:millisecond)
      assert {:error, :timeout} = head("HTTP/1.1 200", 200)
      assert System.monotonic_time(:millisecond) - started < 1_000
    end
  end

  defp run(argv), do: Soak.run(argv, Keyword.put(Crawl.config(), :quiet, true), &Crawl.run/1)

  # Serves `response` over TLS on the kernel's loopback, a byte at a time,
  # sends a HEAD with `timeout`, and returns its result.
  defp head(response, timeout) do
    key = [key: {:namedCurve, :secp256r1}, digest: :sha256]
    chain = %{root: key, intermediates: [], peer: key}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

    {:ok, listener} =
      :ssl.listen(0, cert: server[:cert], key: server[:key], active: false, mode: :binary)

    {:ok, {_address, port}} = :ssl.sockname(listener)
    test = self()

    serving =
      Task.async(fn ->
        {:ok, socket} = :ssl.transport_accept(listener)
        {:ok, socket} = :ssl.handshake(socket)
        {:ok, request} = :ssl.recv(socket, 0)
        send(test, {:request, request})
        for <<byte <- response>>, do: :ok = :ssl.send(socket, <<byte>>)
        # Held open, so that only the response's end, or the timeout, ends the read.
        receive do
          :done -> :ssl.close(socket)
        end
      end)

    options = [
      verify: :verify_peer,
      cacerts: client[:cacerts],
      server_name_indication: :disable,
      active: false,
      mode: :binary
    ]

    {:ok, socket} = :ssl.connect({127, 0, 0, 1}, port, options)
    result = Https.head(socket, "localhost", "/", timeout)
    assert_receive {:request, "HEAD / HTTP/1.1\r\n" <> _headers}
    send(serving.pid, :done)
    :ssl.close(socket)
    Task.await(serving)
    :ssl.close(listener)
    result
  end
end
