defmodule SmolNet.Integration.Blackhole do
  @moduledoc """
  Makes chosen TCP connections vanish silently, with nftables.

  `drop/2` drops every packet of a connection, either way, on one
  interface, so that neither end sees anything more from the other, not
  even a FIN or RST, until `restore/2`. That is a peer that crashed, lost
  power or dropped off the network, a path that went down, or a NAT or
  stateful firewall that forgot an idle connection's mapping.

  It needs root and `nft`. Everything lives in one table,
  `inet smolnet_blackhole_<interface>` (see `table/1`), which `stop/1`
  deletes, as does a guardian process if the process that started it exits
  first, and as `integration/teardown.sh` does after an interrupted run.
  """

  @enforce_keys [:table, :interface, :guardian]
  defstruct [:table, :interface, :guardian]

  @type t :: %__MODULE__{table: String.t(), interface: String.t(), guardian: pid()}

  @typedoc "A connection, as its two ends' ports."
  @type pair :: {:inet.port_number(), :inet.port_number()}

  @stop_timeout 10_000

  @doc "Returns the name of the table for `interface`."
  @spec table(String.t()) :: String.t()
  def table(interface) do
    "smolnet_blackhole_" <> String.replace(interface, ~r/[^A-Za-z0-9_]/, "_")
  end

  @doc """
  Returns the ruleset `start/1` loads for `interface`. A connection is
  matched by its ports, which the set holds both ways round.
  """
  @spec ruleset(String.t()) :: String.t()
  def ruleset(interface) do
    """
    table inet #{table(interface)} {
      set pairs {
        type inet_service . inet_service
      }

      chain drop_in {
        type filter hook input priority filter; policy accept;
        iifname "#{interface}" tcp sport . tcp dport @pairs drop
      }

      chain drop_out {
        type filter hook output priority filter; policy accept;
        oifname "#{interface}" tcp sport . tcp dport @pairs drop
      }
    }
    """
  end

  @doc """
  Loads the table for `interface`, replacing any an interrupted run left.
  Returns `{:error, message}` without root or `nft`.
  """
  @spec start(String.t()) :: {:ok, t()} | {:error, String.t()}
  def start(interface) do
    table = table(interface)
    _leftover = nft(["delete", "table", "inet", table])

    path =
      Path.join(System.tmp_dir!(), "smolnet-blackhole-#{System.unique_integer([:positive])}.nft")

    File.write!(path, ruleset(interface))

    loaded =
      try do
        nft(["-f", path])
      after
        File.rm(path)
      end

    with :ok <- loaded do
      owner = self()
      guardian = spawn(fn -> guard(owner, table) end)
      {:ok, %__MODULE__{table: table, interface: interface, guardian: guardian}}
    end
  end

  @doc "Drops every packet of the connection `pair` on the interface."
  @spec drop(t(), pair()) :: :ok | {:error, String.t()}
  def drop(blackhole, pair), do: elements(blackhole, "add", pair)

  @doc "Lets the connection `pair` through again."
  @spec restore(t(), pair()) :: :ok | {:error, String.t()}
  def restore(blackhole, pair), do: elements(blackhole, "delete", pair)

  defp elements(blackhole, verb, {a, b}) do
    nft([verb, "element", "inet", blackhole.table, "pairs", "{ #{a} . #{b}, #{b} . #{a} }"])
  end

  @doc "Deletes the table, which restores every connection."
  @spec stop(t()) :: :ok
  def stop(blackhole) do
    monitor = Process.monitor(blackhole.guardian)
    send(blackhole.guardian, :stop)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    after
      @stop_timeout -> Process.demonitor(monitor, [:flush])
    end

    :ok
  end

  # Deletes the table when its owner stops it or exits, however it exits.
  defp guard(owner, table) do
    monitor = Process.monitor(owner)

    receive do
      :stop -> :ok
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end

    nft(["delete", "table", "inet", table])
  end

  defp nft(arguments) do
    case System.find_executable("nft") do
      nil ->
        {:error, "nft not found"}

      nft ->
        case System.cmd(nft, arguments, stderr_to_stdout: true) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, "nft #{Enum.join(arguments, " ")} (exit #{status}): #{output}"}
        end
    end
  end
end
