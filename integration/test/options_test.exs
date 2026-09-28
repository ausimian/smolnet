defmodule SmolNet.Integration.Soak.OptionsTest do
  use ExUnit.Case, async: true

  alias SmolNet.Integration.Soak.Options

  @config [
    name: "example",
    default_duration: "10m",
    default_concurrency: 4,
    switches: [size: :integer],
    defaults: [size: 7]
  ]

  describe "parse_duration/1" do
    test "reads bare seconds and every unit" do
      assert {:ok, 90_000} = Options.parse_duration("90")
      assert {:ok, 250} = Options.parse_duration("250ms")
      assert {:ok, 90_000} = Options.parse_duration("90s")
      assert {:ok, 600_000} = Options.parse_duration("10m")
      assert {:ok, 21_600_000} = Options.parse_duration("6h")
      assert {:ok, 86_400_000} = Options.parse_duration("1d")
    end

    test "adds up compound durations" do
      assert {:ok, 5_400_000} = Options.parse_duration("1h30m")
      assert {:ok, 61_500} = Options.parse_duration("1m1s500ms")
    end

    test "rejects anything else" do
      for text <- ["", "h", "1x", "1.5h", "-1s", "1h 30m", "s1"] do
        assert :error = Options.parse_duration(text), text
      end
    end
  end

  describe "parse/2" do
    test "applies the defaults" do
      assert {:ok, options} = Options.parse([], @config)

      assert %{
               script: "example",
               mode: :smolnet,
               duration_ms: 600_000,
               concurrency: 4,
               families: [:inet, :inet6],
               netem: nil,
               egress_credit: {64, 131_072},
               pcap: true,
               keep_pcap: false,
               pcap_snaplen: 0,
               metrics_interval_ms: 10_000,
               warmup_ms: 120_000,
               extra: %{size: 7}
             } = options

      assert options.out_dir =~ ~r|integration/runs/example-\d{8}T\d{6}\.\d{6}Z$|
    end

    test "reads every shared switch" do
      argv =
        ~w(--duration 6h --concurrency 16 --family inet6 --netem delay --device tun7) ++
          ~w(--egress-credit 8:12000 --out some/dir --no-pcap --metrics-interval 1s --warmup 30m)

      assert {:ok, options} = Options.parse(argv, @config)

      assert %{
               duration_ms: 21_600_000,
               concurrency: 16,
               families: [:inet6],
               netem: "delay",
               device: "tun7",
               egress_credit: {8, 12_000},
               out_dir: out_dir,
               pcap: false,
               metrics_interval_ms: 1_000,
               warmup_ms: 1_800_000
             } = options

      assert out_dir == Path.expand("some/dir")
    end

    test "caps the default warm-up at ten minutes" do
      assert {:ok, %{warmup_ms: 600_000}} = Options.parse(~w(--duration 6h), @config)
      assert {:ok, %{warmup_ms: 6_000}} = Options.parse(~w(--duration 30s), @config)
    end

    test "reads the script's own switches over their defaults" do
      assert {:ok, %{extra: %{size: 99}}} = Options.parse(~w(--size 99), @config)
    end

    test "selects the baseline and self-check modes, which capture nothing" do
      assert {:ok, %{mode: :kernel, pcap: false} = kernel} =
               Options.parse(~w(--baseline), @config)

      assert kernel.out_dir =~ ~r/-kernel$/

      assert {:ok, %{mode: :self_check, pcap: false} = self_check} =
               Options.parse(~w(--self-check), @config)

      assert self_check.out_dir =~ ~r/-self-check$/
    end

    test "keeps a passing run's capture, and trims its packets, on request" do
      assert {:ok, %{pcap: true, keep_pcap: true, pcap_snaplen: 128}} =
               Options.parse(~w(--keep-pcap --pcap-snaplen 128), @config)

      assert {:error, message} = Options.parse(~w(--pcap-snaplen -1), @config)
      assert message =~ "--pcap-snaplen must be 0 or more"
    end

    test "accepts unlimited egress credit" do
      assert {:ok, %{egress_credit: :infinity}} =
               Options.parse(~w(--egress-credit infinity), @config)
    end

    test "returns the usage for --help" do
      assert {:help, usage} = Options.parse(~w(--help), @config)
      assert usage =~ "usage: mix run integration/example.exs"
      assert usage =~ "(default 10m)"
    end

    test "rejects bad input with a reason and the usage" do
      for {argv, reason} <- [
            {~w(--duration forever), "invalid duration"},
            {~w(--metrics-interval 0), "must be positive"},
            {~w(--concurrency 0), "concurrency must be positive"},
            {~w(--family ipx), "family must be"},
            {~w(--netem hurricane), "unknown netem profile"},
            {~w(--netem delay --baseline), "--netem shapes the TUN device"},
            {~w(--netem delay --self-check), "--netem shapes the TUN device"},
            {~w(--baseline --self-check), "cannot be combined"},
            {~w(--egress-credit 8), "egress credit must be"},
            {~w(--egress-credit a:b), "egress credit must be"},
            {~w(--egress-credit 4294967296:1), "egress credit must be"},
            {~w(--bogus), "invalid option --bogus"},
            {~w(stray), "unexpected argument"}
          ] do
        assert {:error, message} = Options.parse(argv, @config)
        assert message =~ reason
        assert message =~ "usage:"
      end
    end
  end
end
