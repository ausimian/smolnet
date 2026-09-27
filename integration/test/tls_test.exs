defmodule SmolNet.Integration.TlsTest do
  # The TLS scenario's local target, over the helper's loopback and the
  # kernel's, and the HTTP framing it relies on. Needs no device, no root and
  # no internet.
  use ExUnit.Case, async: false

  alias SmolNet.Integration.Https
  alias SmolNet.Integration.Scenarios.Tls
  alias SmolNet.Integration.Soak

  @moduletag :tmp_dir

  setup_all do
    {:ok, _started} = Application.ensure_all_started(:ssl)
    :ok
  end

  test "SmolNet is a TLS client and server over the helper's loopback", %{tmp_dir: dir} do
    argv = ~w(--self-check --target local --duration 0 --concurrency 2 --out #{dir})
    argv = argv ++ ~w(--down-bytes 100000 --up-bytes 50000)
    assert {:pass, verdict} = run(argv)

    # Per family, four phases with each role as the client: two single
    # streams and two pairs.
    assert verdict.counters.rounds == 1
    assert verdict.counters.transfers == 2 * 2 * (1 + 1 + 2 + 2)
    assert verdict.counters.bytes == 2 * 2 * 2 * (100_000 + 50_000)

    rows = verdict.results.throughput
    assert length(rows) == 2 * 2 * 4

    assert %{client: "smolnet peer", server: "smolnet", runs: 1, bytes: 100_000} =
             Enum.find(
               rows,
               &(&1.family == :inet6 and &1.phase == "down x2" and
                   &1.client == "smolnet peer")
             )

    assert Enum.any?(
             verdict.notes,
             &(&1 =~ "throughput inet up x1, smolnet client to smolnet peer")
           )
  end

  test "the local target runs over the kernel in baseline mode", %{tmp_dir: dir} do
    argv = ~w(--baseline --target local --duration 0 --concurrency 1 --family inet --out #{dir})
    assert {:pass, verdict} = run(argv ++ ~w(--down-bytes 1000 --up-bytes 1000))

    assert verdict.counters.transfers == 2 * 2

    assert verdict.results.throughput |> Enum.map(&{&1.phase, &1.client}) |> Enum.sort() == [
             {"down x1", "kernel"},
             {"down x1", "kernel peer"},
             {"up x1", "kernel"},
             {"up x1", "kernel peer"}
           ]
  end

  test "the internet target is refused over the helper's loopback", %{tmp_dir: dir} do
    assert {:fail, verdict} = run(~w(--self-check --duration 0 --out #{dir}))
    assert [%{kind: :usage, summary: summary}] = verdict.failures
    assert summary =~ "--target local"
  end

  describe "Https" do
    test "reads a body framed by content-length" do
      response = "HTTP/1.1 200 OK\r\ncontent-length: 11\r\n\r\nhello world"

      for mode <- [:passive, :active] do
        assert {:ok, %{status: 200, bytes: 11, sha256: sha256}} = get(response, mode)
        assert sha256 == Https.sha256("hello world")
      end
    end

    test "reads a chunked body, with extensions and trailers" do
      response =
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" <>
          "5;name=value\r\nhello\r\n6\r\n world\r\n0\r\nx-trailer: 1\r\n\r\n"

      for mode <- [:passive, :active] do
        assert {:ok, %{status: 200, bytes: 11, sha256: sha256}} = get(response, mode)
        assert sha256 == Https.sha256("hello world")
      end
    end

    test "reports a body cut short" do
      response = "HTTP/1.1 200 OK\r\ncontent-length: 20\r\n\r\nhello"

      assert {:error, {:closed, %{read: 5, remaining: 15}}} = get(response, :passive)
    end
  end

  defp run(argv), do: Soak.run(argv, Keyword.put(Tls.config(), :quiet, true), &Tls.run/1)

  # Serves `response` over TLS on the kernel's loopback, a byte at a time so
  # that every boundary is split, and GETs it.
  defp get(response, mode) do
    key = [key: {:namedCurve, :secp256r1}, digest: :sha256]
    chain = %{root: key, intermediates: [], peer: key}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

    {:ok, listener} = :ssl.listen(0, cert: server[:cert], key: server[:key], active: false)
    {:ok, {_address, port}} = :ssl.sockname(listener)

    serving =
      Task.async(fn ->
        {:ok, socket} = :ssl.transport_accept(listener)
        {:ok, socket} = :ssl.handshake(socket)
        {:ok, _request} = :ssl.recv(socket, 0)
        for <<byte <- response>>, do: :ok = :ssl.send(socket, <<byte>>)
        :ssl.close(socket)
      end)

    options = [
      verify: :verify_peer,
      cacerts: client[:cacerts],
      server_name_indication: :disable,
      active: false,
      mode: :binary
    ]

    {:ok, socket} = :ssl.connect({127, 0, 0, 1}, port, options)
    result = Https.get(socket, "localhost", "/", mode)
    :ssl.close(socket)
    Task.await(serving)
    :ssl.close(listener)
    result
  end
end
