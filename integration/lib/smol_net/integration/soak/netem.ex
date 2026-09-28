defmodule SmolNet.Integration.Soak.Netem do
  @moduledoc """
  Named `tc netem` impairment profiles for the TUN device.

  A profile impairs both directions. Packets towards SmolNet are shaped on
  the device's own egress; packets from SmolNet are redirected from the
  device's ingress to an `ifb` device and shaped on its egress. Applying a
  profile needs root. `clear/1` removes everything `impair/2` added. After
  an interrupted run, `integration/teardown.sh` removes the device and with
  it its qdiscs, and removes the `ifb` if it carries the alias
  `smolnet-integration` that `impair/2` gives it, so that an `ifb` of
  the same name that something else made is left alone.

  The settings, per direction, are starting points for the impairment matrix
  in #91:

    * `delay` - 100 ms, 20 ms normally distributed jitter, in order
    * `loss-burst` - Gilbert-Elliott loss, about 2.8% on average, in bursts
    * `reorder` - 20 ms delay, 10% of packets sent early
    * `duplicate` - 2% duplication
    * `corrupt` - 0.5% corruption
    * `bufferbloat` - 20 ms delay into a 10 Mbit/s bucket with a 1 s queue
    * `high-bdp` - 100 ms delay at 100 Mbit/s, so 200 ms round trips
  """

  # Each profile is its netem arguments, and optionally the arguments of a
  # tbf child that shapes the netem output.
  @profiles %{
    # netem reorders packets whose jitter overtakes the one before, unless
    # it also paces them at a rate; one far above the link keeps them in
    # order and costs nothing. Reordering is its own profile.
    "delay" => {~w(delay 100ms 20ms distribution normal rate 1gbit), nil},
    # 1% chance of entering the bad state, 25% of leaving it, 70% loss in
    # it and 0.1% loss out of it.
    "loss-burst" => {~w(loss gemodel 1% 25% 70% 0.1%), nil},
    "reorder" => {~w(delay 20ms reorder 10% 50%), nil},
    "duplicate" => {~w(duplicate 2%), nil},
    "corrupt" => {~w(corrupt 0.5%), nil},
    "bufferbloat" => {~w(delay 20ms), ~w(rate 10mbit burst 32kbit latency 1000ms)},
    "high-bdp" => {~w(delay 100ms rate 100mbit limit 100000), nil}
  }

  # The round trip each profile adds to an otherwise idle path, in
  # milliseconds: its delay both ways, plus for bufferbloat the queue that
  # a sender filling the bucket builds in front of it.
  @round_trips %{
    "delay" => 200,
    "loss-burst" => 0,
    "reorder" => 40,
    "duplicate" => 0,
    "corrupt" => 0,
    "bufferbloat" => 40 + 1_000,
    "high-bdp" => 200
  }

  @doc "Returns the profile names."
  @spec profiles() :: [String.t()]
  def profiles, do: @profiles |> Map.keys() |> Enum.sort()

  @doc "Returns whether `name` is a profile."
  @spec profile?(String.t()) :: boolean()
  def profile?(name), do: Map.has_key?(@profiles, name)

  @doc """
  Returns the round trip, in milliseconds, that `profile` adds to a path
  that has none of its own: its delay both ways, and for `bufferbloat` the
  full queue too. Jitter and reordering are left out.
  """
  @spec round_trip_ms(String.t()) :: non_neg_integer()
  def round_trip_ms(profile), do: Map.fetch!(@round_trips, profile)

  @doc "Returns the alias that marks an `ifb` as one `impair/2` created."
  @spec ifb_alias() :: String.t()
  def ifb_alias, do: "smolnet-integration"

  @doc "Returns the `ifb` device a profile uses for `device`'s ingress."
  @spec ifb(String.t()) :: String.t()
  def ifb(device), do: String.slice("ifb-" <> device, 0, 15)

  @doc """
  Returns the commands that apply `profile` to `device`, in order, each as
  `[executable | arguments]`.
  """
  @spec commands(String.t(), String.t()) :: [[String.t()]]
  def commands(profile, device) do
    {netem, tbf} = Map.fetch!(@profiles, profile)
    ifb = ifb(device)

    [
      ~w(ip link add) ++ [ifb, "type", "ifb"],
      ~w(ip link set) ++ [ifb, "alias", ifb_alias(), "up"],
      ~w(tc qdisc add dev) ++ [device, "handle", "ffff:", "ingress"],
      ~w(tc filter add dev) ++
        [device | ~w(parent ffff: matchall action mirred egress redirect dev)] ++ [ifb]
    ] ++ Enum.flat_map([device, ifb], &shape(&1, netem, tbf))
  end

  @doc "Returns the commands that remove what `commands/2` adds to `device`."
  @spec clear_commands(String.t()) :: [[String.t()]]
  def clear_commands(device) do
    [
      ~w(tc qdisc del dev) ++ [device, "root"],
      ~w(tc qdisc del dev) ++ [device, "ingress"],
      ~w(ip link del) ++ [ifb(device)]
    ]
  end

  @doc """
  Applies `profile` to `device`.

  Stops at the first command that fails, removes what the commands before
  it added (and nothing that was there before), and returns
  `{:error, message}`.
  """
  @spec impair(String.t(), String.t()) :: :ok | {:error, String.t()}
  def impair(profile, device) do
    profile
    |> commands(device)
    |> Enum.reduce_while([], fn command, applied ->
      case run(command) do
        :ok -> {:cont, [command | applied]}
        {:error, message} -> {:halt, {:error, message, Enum.reverse(applied)}}
      end
    end)
    |> case do
      {:error, message, applied} ->
        applied |> undo_commands() |> Enum.each(&run/1)
        {:error, message}

      _applied ->
        :ok
    end
  end

  @doc """
  Returns the commands that remove what `applied`, a prefix of
  `commands/2`, added, newest first.
  """
  @spec undo_commands([[String.t()]]) :: [[String.t()]]
  def undo_commands(applied) do
    applied |> Enum.reverse() |> Enum.flat_map(&undo/1)
  end

  defp undo(["ip", "link", "add", ifb | _rest]), do: [~w(ip link del) ++ [ifb]]

  defp undo(["tc", "qdisc", "add", "dev", device, "handle", "ffff:", "ingress"]),
    do: [~w(tc qdisc del dev) ++ [device, "ingress"]]

  defp undo(["tc", "qdisc", "add", "dev", device, "root" | _rest]),
    do: [~w(tc qdisc del dev) ++ [device, "root"]]

  # Bringing a link up, a filter on the ingress qdisc and a qdisc under a
  # root all go with what they belong to.
  defp undo(_command), do: []

  @doc "Removes any profile from `device`, ignoring what was never applied."
  @spec clear(String.t()) :: :ok
  def clear(device) do
    Enum.each(clear_commands(device), &run/1)
  end

  defp shape(device, netem, nil) do
    [~w(tc qdisc add dev) ++ [device | ~w(root handle 1: netem)] ++ netem]
  end

  defp shape(device, netem, tbf) do
    shape(device, netem, nil) ++
      [~w(tc qdisc add dev) ++ [device | ~w(parent 1:1 handle 10: tbf)] ++ tbf]
  end

  defp run([executable | arguments] = command) do
    case System.find_executable(executable) do
      nil ->
        {:error, "#{executable} not found"}

      path ->
        case System.cmd(path, arguments, stderr_to_stdout: true) do
          {_output, 0} -> :ok
          {output, status} -> {:error, "#{Enum.join(command, " ")} (exit #{status}): #{output}"}
        end
    end
  end
end
