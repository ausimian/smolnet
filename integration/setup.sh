#!/bin/sh
# Prepares the host for SmolNet's real-network integration scripts:
#
#     sudo integration/setup.sh [--no-nat]
#
# Creates a persistent TUN device (SMOLNET_TUN, default tun0) owned by the
# user who ran sudo, so that the TUN helper can attach to it without root,
# and gives the host 10.77.0.1/24 and fd00:77::1/64 on it. SmolNet takes
# 10.77.0.2 and fd00:77::2 (see SmolNet.Integration.Network).
#
# Unless --no-nat is given, it also enables forwarding and has nftables
# masquerade both prefixes, so that SmolNet reaches the internet through
# the host. Where forwarding was off, only traffic from the device and
# replies to it are forwarded. Enabling IPv6 forwarding stops Linux accepting router
# advertisements on interfaces with accept_ra=1, so those are switched to
# accept_ra=2 until teardown.
#
# nftables forwards a packet only if every forward hook accepts it, so the
# host's own firewall can drop what setup.sh's table accepts. Docker does:
# it sets iptables' FORWARD policy to DROP. Where iptables' FORWARD chain
# (or ip6tables') could drop, setup.sh accepts the device's traffic first
# in Docker's DOCKER-USER chain, its place for user rules, or else first in
# FORWARD. Any other forward chain that could drop, such as another
# nftables table's, it cannot open safely, so it refuses to run.
#
# Every change is recorded in /run/smolnet-integration-<device>.state, and
# integration/teardown.sh undoes exactly those, after a partial setup too.

set -eu

device=${SMOLNET_TUN:-tun0}
state=/run/smolnet-integration-$device.state
table=smolnet_$(printf '%s' "$device" | tr -c 'A-Za-z0-9_' '_')
nat=1

for argument in "$@"; do
  case $argument in
    --no-nat) nat=0 ;;
    *)
      echo "usage: $0 [--no-nat]" >&2
      exit 2
      ;;
  esac
done

if [ "$(id -u)" -ne 0 ]; then
  echo "setup.sh must run as root: sudo $0" >&2
  exit 1
fi

if [ -e "$state" ]; then
  echo "$device is already set up ($state exists); run integration/teardown.sh first" >&2
  exit 1
fi

if ip link show dev "$device" >/dev/null 2>&1; then
  echo "$device exists and was not created by setup.sh; set SMOLNET_TUN to another name" >&2
  exit 1
fi

# Whether iptables command $1 has a FORWARD chain that could drop: a policy
# other than ACCEPT, or any rule. A command that is missing, or has no
# backend, cannot drop anything.
xt_blocks() {
  command -v "$1" >/dev/null 2>&1 || return 1
  rules=$("$1" -w -S FORWARD 2>/dev/null) || return 1
  [ "$(printf '%s\n' "$rules" | grep -v '^-P FORWARD ACCEPT$')" != "" ]
}

# Accepts the device's traffic first in DOCKER-USER, or else in FORWARD,
# with iptables command $1. Each rule is recorded before it is inserted, so
# that teardown.sh deletes exactly it.
open_xt() {
  if "$1" -w -S DOCKER-USER >/dev/null 2>&1; then chain=DOCKER-USER; else chain=FORWARD; fi
  mark="-m comment --comment smolnet-integration-$device"
  insert_xt "$1" "$chain" -o "$device" -m conntrack --ctstate RELATED,ESTABLISHED $mark -j ACCEPT
  insert_xt "$1" "$chain" -i "$device" $mark -j ACCEPT
  echo "$1: accepting $device's traffic first in $chain"
}

insert_xt() {
  command=$1
  chain=$2
  shift 2
  echo "xt_rule $command $chain $*" >>"$state"
  "$command" -w -I "$chain" 1 "$@"
}

# Lists the nftables forward chains, outside setup.sh's own table and the
# tables in $1 ("family name;" each), whose policy is drop or that hold a
# drop or reject rule.
forward_blockers() {
  nft list ruleset 2>/dev/null | awk -v own="inet $table" -v opened="$1" '
    $1 == "table" { table = $2 " " $3; skip = (table == own || index(opened, " " table ";")) }
    $1 == "chain" { chain = $2; forward = 0; found = 0 }
    /hook forward/ { forward = !skip }
    forward && !found && (/policy drop/ || / (drop|reject)( |;|$)/) {
      print "nft table " table ", chain " chain; found = 1
    }
  '
}

# Lists legacy iptables FORWARD chains that could drop when iptables itself
# is the nftables backend, so that the chains setup.sh opens are not the
# ones that drop. Reading /proc loads nothing.
legacy_blockers() {
  for command in iptables ip6tables; do
    case $command in
      iptables) names=/proc/net/ip_tables_names ;;
      ip6tables) names=/proc/net/ip6_tables_names ;;
    esac

    if "$command" -V 2>/dev/null | grep -q nf_tables && grep -qx filter "$names" 2>/dev/null &&
      xt_blocks "$command-legacy"; then
      echo "$command-legacy's FORWARD chain (setup.sh opens only $command's)"
    fi
  done
}

trap 'echo "setup.sh failed; integration/teardown.sh undoes what it did" >&2' EXIT

owner=${SUDO_USER:-$(id -un)}
: >"$state"

ip tuntap add dev "$device" mode tun user "$owner"
echo "device $device" >>"$state"

ip addr add 10.77.0.1/24 dev "$device"

if [ -d /proc/sys/net/ipv6 ]; then
  sysctl -qw "net.ipv6.conf.$device.disable_ipv6=0"
  ip -6 addr add fd00:77::1/64 dev "$device" nodad
else
  echo "IPv6 is unavailable on this host; run the scripts with --family inet" >&2
fi

ip link set dev "$device" mtu 1500 up

if [ "$nat" -eq 1 ]; then
  if ! command -v nft >/dev/null 2>&1; then
    echo "nft not found: install nftables, or pass --no-nat" >&2
    exit 1
  fi

  # A family whose forwarding was off forwards only to and from the device,
  # so that enabling it does not turn the host into a router for anything
  # else. The rules go in before forwarding is enabled, and teardown.sh
  # disables forwarding before it removes them.
  isolate=""
  ipv4_forward=$(sysctl -n net.ipv4.ip_forward)
  if [ "$ipv4_forward" = 0 ]; then isolate="$isolate meta nfproto ipv4 drop;"; fi

  if [ -d /proc/sys/net/ipv6 ]; then
    ipv6_forward=$(sysctl -n net.ipv6.conf.all.forwarding)
    if [ "$ipv6_forward" = 0 ]; then isolate="$isolate meta nfproto ipv6 drop;"; fi
  fi

  if nft list table inet "$table" >/dev/null 2>&1; then
    echo "nft table inet $table exists and was not created by setup.sh" >&2
    exit 1
  fi

  # iptables and ip6tables, whichever backend they use, are opened below;
  # when that backend is nftables, their tables are ip filter and ip6
  # filter, which the check that follows leaves to them.
  opened=""
  for command in iptables ip6tables; do
    if xt_blocks "$command"; then
      if "$command" -V 2>/dev/null | grep -q nf_tables; then
        case $command in
          iptables) opened="$opened ip filter;" ;;
          ip6tables) opened="$opened ip6 filter;" ;;
        esac
      fi
    fi
  done

  blocking=$(
    forward_blockers "$opened"
    legacy_blockers
  )
  if [ -n "$blocking" ]; then
    echo "another firewall can drop what $device forwards, and setup.sh cannot open it:" >&2
    printf '%s\n' "$blocking" | sed 's/^/  /' >&2
    echo "accept iifname $device, and oifname $device when established, in each of them first" >&2
    exit 1
  fi

  for command in iptables ip6tables; do
    if xt_blocks "$command"; then
      open_xt "$command"
    fi
  done

  # One transaction: the table is created whole or not at all, and recorded
  # once it exists.
  nft -f - <<EOF
table inet $table {
  chain forward {
    type filter hook forward priority filter; policy accept;
    iifname "$device" accept
    oifname "$device" ct state established,related accept
    $isolate
  }

  chain postrouting {
    type nat hook postrouting priority srcnat; policy accept;
    oifname != "$device" ip saddr 10.77.0.0/24 masquerade
    oifname != "$device" ip6 saddr fd00:77::/64 masquerade
  }
}
EOF
  echo "nft_table $table" >>"$state"

  echo "ipv4_forward $ipv4_forward" >>"$state"
  sysctl -qw net.ipv4.ip_forward=1

  # Only a switch from no forwarding to forwarding changes how Linux treats
  # router advertisements.
  if [ -d /proc/sys/net/ipv6 ] && [ "$ipv6_forward" = 0 ]; then
    for conf in /proc/sys/net/ipv6/conf/*/accept_ra; do
      interface=$(basename "$(dirname "$conf")")

      case $interface in
        all | default | lo | "$device") continue ;;
      esac

      if [ "$(cat "$conf")" = 1 ]; then
        echo "accept_ra $interface" >>"$state"
        echo 2 >"$conf"
      fi
    done
  fi

  if [ -d /proc/sys/net/ipv6 ]; then
    echo "ipv6_forward $ipv6_forward" >>"$state"
    sysctl -qw net.ipv6.conf.all.forwarding=1
  fi
fi

trap - EXIT
echo "$device is up: the host is 10.77.0.1 and fd00:77::1, SmolNet 10.77.0.2 and fd00:77::2"
