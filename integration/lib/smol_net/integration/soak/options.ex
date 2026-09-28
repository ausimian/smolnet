defmodule SmolNet.Integration.Soak.Options do
  @moduledoc """
  Parses the command line every soak script shares.

  A script adds its own switches through its config (see
  `SmolNet.Integration.Soak`); their values arrive in `:extra`.
  """

  alias SmolNet.Integration.Soak.Netem

  @runs_dir Path.expand("../../../../runs", __DIR__)
  @max_warmup_ms :timer.minutes(10)
  # The most egress credit a stack accepts, in packets or bytes.
  @max_credit 0xFFFF_FFFF

  @switches [
    duration: :string,
    concurrency: :integer,
    family: :string,
    netem: :string,
    baseline: :boolean,
    self_check: :boolean,
    device: :string,
    egress_credit: :string,
    out: :string,
    pcap: :boolean,
    keep_pcap: :boolean,
    pcap_snaplen: :integer,
    metrics_interval: :string,
    warmup: :string,
    help: :boolean
  ]

  @type mode :: :smolnet | :self_check | :kernel

  @type t :: %{
          script: String.t(),
          mode: mode(),
          duration_ms: non_neg_integer(),
          concurrency: pos_integer(),
          families: [:inet | :inet6],
          netem: String.t() | nil,
          device: String.t(),
          egress_credit: {non_neg_integer(), non_neg_integer()} | :infinity,
          out_dir: Path.t(),
          pcap: boolean(),
          keep_pcap: boolean(),
          pcap_snaplen: non_neg_integer(),
          metrics_interval_ms: pos_integer(),
          warmup_ms: non_neg_integer(),
          extra: map()
        }

  @doc """
  Parses `argv` for the script described by `config`.

  Returns `{:help, usage}` for `--help` and `{:error, message}`, with the
  usage appended, for anything it cannot parse.
  """
  @spec parse([String.t()], keyword()) :: {:ok, t()} | {:help, String.t()} | {:error, String.t()}
  def parse(argv, config) do
    extra_switches = Keyword.get(config, :switches, [])

    case OptionParser.parse(argv, strict: @switches ++ extra_switches) do
      {parsed, [], []} ->
        if parsed[:help], do: {:help, usage(config)}, else: build(parsed, config)

      {_parsed, [argument | _rest], _invalid} ->
        error("unexpected argument #{inspect(argument)}", config)

      {_parsed, _arguments, [{switch, _value} | _rest]} ->
        error("invalid option #{switch}", config)
    end
  end

  @doc """
  Parses a duration: an integer number of seconds, or one or more amounts
  with units `ms`, `s`, `m`, `h` or `d`, such as `90s`, `6h` or `1h30m`.
  Returns milliseconds.
  """
  @spec parse_duration(String.t()) :: {:ok, non_neg_integer()} | :error
  def parse_duration(text) do
    cond do
      text =~ ~r/\A\d+\z/ ->
        {:ok, String.to_integer(text) * 1_000}

      text =~ ~r/\A(\d+(ms|s|m|h|d))+\z/ ->
        {:ok,
         ~r/(\d+)(ms|s|m|h|d)/
         |> Regex.scan(text, capture: :all_but_first)
         |> Enum.map(fn [amount, unit] -> String.to_integer(amount) * unit_ms(unit) end)
         |> Enum.sum()}

      true ->
        :error
    end
  end

  @doc "Returns the directory runs write to unless given `--out`."
  @spec runs_dir() :: Path.t()
  def runs_dir, do: @runs_dir

  @doc "Returns the usage text for the script described by `config`."
  @spec usage(keyword()) :: String.t()
  def usage(config) do
    defaults = defaults(config)

    """
    usage: mix run integration/#{Keyword.fetch!(config, :name)}.exs [options]

      --duration D          how long to run: 90s, 10m, 6h, 1h30m (default #{defaults[:duration]})
      --concurrency N       concurrent workers (default #{defaults[:concurrency]})
      --family F            inet, inet6 or both (default both)
      --netem P             impair the device with a profile: #{Enum.join(Netem.profiles(), ", ")}
      --baseline            run the workload over the kernel's stack instead of SmolNet
      --self-check          run SmolNet over the helper's loopback: no device, no root
      --device NAME         the TUN device (default $SMOLNET_TUN, or tun0)
      --egress-credit C     the link's credit, PACKETS:BYTES or infinity (default 64:131072)
      --out DIR             where artifacts go (default integration/runs/<script>-<time>)
      --no-pcap             do not capture packets on the device
      --keep-pcap           keep the capture when the run passes too
      --pcap-snaplen N      capture only the first N bytes of each packet (default 0, all)
      --metrics-interval D  how often to sample metrics (default 10s)
      --warmup D            how long before metric trends count (default: a fifth of the run, at most 10m)
      --help                show this text
    """ <> Keyword.get(config, :usage, "")
  end

  defp defaults(config) do
    [
      duration: Keyword.get(config, :default_duration, "30s"),
      concurrency: Keyword.get(config, :default_concurrency, 1),
      family: "both",
      device: System.get_env("SMOLNET_TUN", "tun0"),
      egress_credit: "64:131072",
      metrics_interval: "10s",
      pcap: true,
      keep_pcap: false,
      pcap_snaplen: 0
    ]
  end

  defp build(parsed, config) do
    name = Keyword.fetch!(config, :name)
    options = Keyword.merge(defaults(config), parsed)

    with {:ok, mode} <- mode(options),
         {:ok, duration} <- duration(options[:duration]),
         {:ok, interval} <- positive_duration(options[:metrics_interval]),
         {:ok, warmup} <- warmup(options[:warmup], duration),
         {:ok, concurrency} <- concurrency(options[:concurrency]),
         {:ok, families} <- families(options[:family]),
         {:ok, netem} <- netem(options[:netem], mode),
         {:ok, snaplen} <- snaplen(options[:pcap_snaplen]),
         {:ok, credit} <- egress_credit(options[:egress_credit]) do
      {:ok,
       %{
         script: name,
         mode: mode,
         duration_ms: duration,
         concurrency: concurrency,
         families: families,
         netem: netem,
         device: options[:device],
         egress_credit: credit,
         out_dir: out_dir(options[:out], name, mode),
         pcap: mode == :smolnet and options[:pcap],
         keep_pcap: options[:keep_pcap],
         pcap_snaplen: snaplen,
         metrics_interval_ms: interval,
         warmup_ms: warmup,
         extra: extra(parsed, config)
       }}
    else
      {:error, message} -> error(message, config)
    end
  end

  defp mode(options) do
    case {options[:baseline], options[:self_check]} do
      {true, true} -> {:error, "--baseline and --self-check cannot be combined"}
      {true, _self_check} -> {:ok, :kernel}
      {_baseline, true} -> {:ok, :self_check}
      _neither -> {:ok, :smolnet}
    end
  end

  defp duration(text) do
    case parse_duration(text) do
      {:ok, milliseconds} -> {:ok, milliseconds}
      :error -> {:error, "invalid duration #{inspect(text)}"}
    end
  end

  defp positive_duration(text) do
    case duration(text) do
      {:ok, 0} -> {:error, "duration #{inspect(text)} must be positive"}
      result -> result
    end
  end

  defp warmup(nil, duration), do: {:ok, min(div(duration, 5), @max_warmup_ms)}
  defp warmup(text, _duration), do: duration(text)

  defp concurrency(count) when is_integer(count) and count > 0, do: {:ok, count}
  defp concurrency(count), do: {:error, "concurrency must be positive, got #{count}"}

  defp families("inet"), do: {:ok, [:inet]}
  defp families("inet6"), do: {:ok, [:inet6]}
  defp families("both"), do: {:ok, [:inet, :inet6]}
  defp families(other), do: {:error, "family must be inet, inet6 or both, got #{inspect(other)}"}

  defp netem(nil, _mode), do: {:ok, nil}

  defp netem(profile, :smolnet) do
    if Netem.profile?(profile) do
      {:ok, profile}
    else
      profiles = Enum.join(Netem.profiles(), ", ")
      {:error, "unknown netem profile #{inspect(profile)}; choose one of #{profiles}"}
    end
  end

  # Netem shapes the TUN device, which neither the kernel baseline's traffic
  # nor the helper's loopback passes through.
  defp netem(_profile, _mode) do
    {:error, "--netem shapes the TUN device, so it needs neither --baseline nor --self-check"}
  end

  defp snaplen(bytes) when is_integer(bytes) and bytes >= 0, do: {:ok, bytes}
  defp snaplen(bytes), do: {:error, "--pcap-snaplen must be 0 or more, got #{bytes}"}

  defp egress_credit("infinity"), do: {:ok, :infinity}

  defp egress_credit(text) do
    with [packets, bytes] <- String.split(text, ":"),
         {packets, ""} <- Integer.parse(packets),
         {bytes, ""} <- Integer.parse(bytes),
         true <- packets in 0..@max_credit and bytes in 0..@max_credit do
      {:ok, {packets, bytes}}
    else
      _invalid ->
        {:error, "egress credit must be PACKETS:BYTES or infinity, got #{inspect(text)}"}
    end
  end

  defp out_dir(nil, name, mode) do
    # Microseconds keep runs started in the same second apart.
    stamp = Calendar.strftime(DateTime.utc_now(), "%Y%m%dT%H%M%S.%fZ")
    suffix = if mode == :smolnet, do: "", else: "-" <> String.replace(to_string(mode), "_", "-")
    Path.join(@runs_dir, "#{name}-#{stamp}#{suffix}")
  end

  defp out_dir(path, _name, _mode), do: Path.expand(path)

  defp extra(parsed, config) do
    switches = Keyword.get(config, :switches, [])

    config
    |> Keyword.get(:defaults, [])
    |> Map.new()
    |> Map.merge(Map.new(Keyword.take(parsed, Keyword.keys(switches))))
  end

  defp error(message, config), do: {:error, message <> "\n\n" <> usage(config)}

  defp unit_ms("ms"), do: 1
  defp unit_ms("s"), do: 1_000
  defp unit_ms("m"), do: 60_000
  defp unit_ms("h"), do: 3_600_000
  defp unit_ms("d"), do: 86_400_000
end
