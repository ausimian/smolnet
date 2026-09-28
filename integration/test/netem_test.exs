defmodule SmolNet.Integration.Soak.NetemTest do
  use ExUnit.Case, async: true

  alias SmolNet.Integration.Soak.Netem

  test "names the profiles the impairment matrix starts from" do
    assert Netem.profiles() ==
             ~w(bufferbloat corrupt delay duplicate high-bdp loss-burst reorder)

    assert Netem.profile?("delay")
    refute Netem.profile?("hurricane")
  end

  test "impairs both directions: the device's egress and, through an ifb, its ingress" do
    assert Netem.commands("duplicate", "tun0") == [
             ~w(ip link add ifb-tun0 type ifb),
             ~w(ip link set ifb-tun0 alias smolnet-integration up),
             ~w(tc qdisc add dev tun0 handle ffff: ingress),
             ~w(tc filter add dev tun0 parent ffff: matchall action mirred egress redirect dev ifb-tun0),
             ~w(tc qdisc add dev tun0 root handle 1: netem duplicate 2%),
             ~w(tc qdisc add dev ifb-tun0 root handle 1: netem duplicate 2%)
           ]
  end

  test "shapes a profile's netem output with its tbf" do
    commands = Netem.commands("bufferbloat", "tun0")
    tbf = ~w(parent 1:1 handle 10: tbf rate 10mbit burst 32kbit latency 1000ms)

    assert (~w(tc qdisc add dev tun0) ++ tbf) in commands
    assert (~w(tc qdisc add dev ifb-tun0) ++ tbf) in commands
  end

  test "clears what it applies" do
    assert Netem.clear_commands("tun0") == [
             ~w(tc qdisc del dev tun0 root),
             ~w(tc qdisc del dev tun0 ingress),
             ~w(ip link del ifb-tun0)
           ]
  end

  test "undoes only what was applied before a failure" do
    commands = Netem.commands("bufferbloat", "tun0")

    assert Netem.undo_commands(Enum.take(commands, 2)) == [~w(ip link del ifb-tun0)]

    assert Netem.undo_commands(Enum.take(commands, 6)) == [
             ~w(tc qdisc del dev tun0 root),
             ~w(tc qdisc del dev tun0 ingress),
             ~w(ip link del ifb-tun0)
           ]

    assert Netem.undo_commands([]) == []
  end

  test "knows the round trip each profile adds" do
    assert Enum.all?(Netem.profiles(), &is_integer(Netem.round_trip_ms(&1)))
    assert Netem.round_trip_ms("high-bdp") == 200
    assert Netem.round_trip_ms("bufferbloat") == 1_040
  end

  test "keeps the ifb name within the kernel's interface name limit" do
    assert Netem.ifb("a-long-device") == "ifb-a-long-devi"
  end
end
