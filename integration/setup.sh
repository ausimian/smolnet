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
