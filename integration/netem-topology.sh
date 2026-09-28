#!/bin/sh
# Builds the kernel baseline of integration/netem_matrix.exs: a second
# path out of this namespace, beside the TUN device, that the matrix
# impairs with the same netem profile as the device, so that SmolNet's
# transfers can be compared with the kernel's on a path impaired alike.
#
#     SmolNet --tun0-- this namespace --netem-h==netem-p-- netem-peer
#
#   * The TUN device (SMOLNET_TUN, default tun0, from integration/setup.sh
#     --no-nat) carries SmolNet's transfers to and from the kernel here.
#   * netem-h, 10.80.0.1/24 and fd00:80::1/64, is this namespace's end of a
#     veth pair, and netem-p, 10.80.0.2 and fd00:80::2, its other end, in
#     the namespace netem-peer. The kernel here and the kernel there are
#     the baseline's two ends.
#
# The matrix impairs the device and netem-h alike, with
# SmolNet.Integration.Soak.Netem: the profile's qdiscs on the device's
# egress and, through an ifb, on its ingress. Each path is so impaired
# once each way, in this namespace, by the same qdiscs.
#
# netem acts on packets as the qdisc sees them, and the kernel's TCP
# hands it GSO packets of up to 64 KiB, split into segments only after
# it. One loss would then drop dozens of segments at once, and one delay
# or reordering move them together, where SmolNet, which sends a segment
# per packet, loses one. So up limits GSO to one segment per packet on the
# device and on both ends of the veth, and down restores the device's.
#
# up --thin-acks N makes the peer's kernel acknowledge as a server whose
# NIC coalesces what it receives (GRO, LRO) does, with stretch ACKs of
# several segments each (#126): nftables there drops all but one in N of
# its pure ACKs, those with no data, no SYN, FIN or RST and no SACK
# blocks, so that every duplicate ACK still arrives. The peer becomes the
# kernel end of SmolNet's transfers too: this namespace forwards between
# the device and the veth, so that SmolNet over the device and the kernel
# over the veth send to the same receiver and see the same ACKs.
#
#     integration/netem-topology.sh isolate COMMAND [ARG...]
#     integration/netem-topology.sh up [--thin-acks N]
#     integration/netem-topology.sh show
#     integration/netem-topology.sh down
#
# isolate is integration/pmtu-topology.sh's: COMMAND runs in a network
# and mount namespace of its own, with the device set up there, and
# without root in a user namespace too, so that it needs no sudo where
# the host allows those:
#
#     integration/netem-topology.sh isolate mix run integration/netem_matrix.exs
#
# up refuses to run in the host's own network namespace, and down removes
# what up added, and the ifb Netem gives netem-h if one is left over.

set -eu

PATH=$PATH:/usr/sbin:/sbin
here=$(cd "$(dirname "$0")" && pwd)
device=${SMOLNET_TUN:-tun0}
state=/run/smolnet-netem.state
peer=netem-peer
local_end=netem-h
peer_end=netem-p

usage() {
  sed -n '/^#     integration/s/^#     //p' "$0" >&2
  exit 2
}

die() {
  echo "netem-topology.sh: $*" >&2
  exit 1
}

require_root() {
  [ "$(id -u)" -eq 0 ] || die "$1 must run as root: use isolate, or sudo"
}

# Whether this is the network namespace of PID 1, the host's own.
in_host_namespace() {
  [ "$(readlink /proc/self/ns/net)" = "$(readlink /proc/1/ns/net 2>/dev/null || true)" ]
}

setting() {
  sed -n "s/^$1 //p" "$state"
}

save() {
  echo "$1 $2" >>"$state"
}

in_peer() { ip netns exec "$peer" "$@"; }

# The device's GSO limit $1, gso_max_segs or gso_max_size.
gso() {
  ip -d link show dev "$device" | sed -n "s/.* $1 \([0-9]*\).*/\1/p" | head -n 1
}

# One segment per GSO packet on link $2, run with $1 (nothing, or in_peer).
one_segment() {
  $1 ip link set dev "$2" gso_max_segs 1
}

# Has the peer send only one in $1 of its pure ACKs. A pure ACK's IP
# length is its TCP header's alone, which nftables can match only for
# each header length in turn.
thin_acks() {
  pure="tcp flags & (fin | syn | rst) == 0 tcp option sack missing"
  thin="numgen inc mod $1 != 0 drop"
  rules=""
  for doff in 5 6 7 8 9 10 11 12 13 14 15; do
    rules="$rules
    $pure ip length $((20 + 4 * doff)) tcp doff $doff $thin
    $pure ip6 length $((4 * doff)) tcp doff $doff $thin"
  done
  in_peer nft -f - <<EOF
table inet smolnet_thin_acks {
  chain output {
    type filter hook output priority filter; policy accept;$rules
  }
}
EOF
}

up() {
  thin=""
  case $# in
    0) ;;
    2)
      [ "$1" = --thin-acks ] || usage
      case $2 in '' | *[!0-9]*) usage ;; esac
      [ "$2" -ge 2 ] || die "--thin-acks needs 2 or more, not $2"
      thin=$2
      ;;
    *) usage ;;
  esac
  require_root up
  if in_host_namespace; then
    die "refusing to change the host's own network namespace; run under: $0 isolate COMMAND"
  fi
  [ ! -e "$state" ] || die "the baseline is already up ($state exists); run $0 down first"
  ip link show dev "$device" >/dev/null 2>&1 || die "no $device here: run integration/setup.sh --no-nat first"

  : >"$state"
  save device "$device"
  save gso_max_segs "$(gso gso_max_segs)"

  ip netns add "$peer"
  # No duplicate address detection, which would hold IPv6 back a while.
  in_peer sysctl -qw net.ipv6.conf.all.accept_dad=0
  in_peer sysctl -qw net.ipv6.conf.default.accept_dad=0
  ip link add "$local_end" type veth peer name "$peer_end" netns "$peer"
  sysctl -qw "net.ipv6.conf.$local_end.accept_dad=0"

  one_segment "" "$device"
  one_segment "" "$local_end"
  one_segment in_peer "$peer_end"

  ip link set dev "$local_end" mtu 1500 up
  in_peer ip link set dev lo up
  in_peer ip link set dev "$peer_end" mtu 1500 up
  ip addr add 10.80.0.1/24 dev "$local_end"
  ip -6 addr add fd00:80::1/64 dev "$local_end" nodad
  in_peer ip addr add 10.80.0.2/24 dev "$peer_end"
  in_peer ip -6 addr add fd00:80::2/64 dev "$peer_end" nodad

  if [ -n "$thin" ]; then
    save ip_forward "$(sysctl -n net.ipv4.ip_forward)"
    save ipv6_forwarding "$(sysctl -n net.ipv6.conf.all.forwarding)"
    sysctl -qw net.ipv4.ip_forward=1
    sysctl -qw net.ipv6.conf.all.forwarding=1
    # SmolNet's side of the device, integration/setup.sh's.
    in_peer ip route add 10.77.0.0/24 via 10.80.0.1
    in_peer ip -6 route add fd00:77::/64 via fd00:80::1
    thin_acks "$thin"
  fi

  ready 10.80.0.2
  ready fd00:80::2
  echo "the baseline is up: this namespace is 10.80.0.1 and fd00:80::1 on $local_end, the peer 10.80.0.2 and fd00:80::2"
}

ready() {
  tries=0
  until ping -c 1 -W 1 "$1" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 10 ] || die "cannot reach the peer at $1"
  done
}

show() {
  [ -e "$state" ] || die "the baseline is not up"
  cat "$state"
  ip -br addr show dev "$(setting device)"
  ip -br addr show dev "$local_end"
  in_peer ip -br addr show dev "$peer_end"
  for link in "$(setting device)" "$local_end"; do
    tc qdisc show dev "$link"
  done
}

down() {
  require_root down
  [ -e "$state" ] || { echo "the baseline is not up; nothing to undo" >&2; exit 0; }

  ip netns del "$peer" 2>/dev/null || true
  ip link del dev "$local_end" 2>/dev/null || true
  # The ifb SmolNet.Integration.Soak.Netem adds for netem-h's ingress,
  # if a run left one, and only one it marked as its own.
  ifb=$(printf 'ifb-%s' "$local_end" | cut -c1-15)
  if ip link show dev "$ifb" 2>/dev/null | grep -q '^ *alias smolnet-integration$'; then
    ip link del dev "$ifb"
  fi

  forward=$(setting ip_forward)
  if [ -n "$forward" ]; then sysctl -qw "net.ipv4.ip_forward=$forward"; fi
  forward=$(setting ipv6_forwarding)
  if [ -n "$forward" ]; then sysctl -qw "net.ipv6.conf.all.forwarding=$forward"; fi

  segs=$(setting gso_max_segs)
  if [ -n "$segs" ] && ip link show dev "$(setting device)" >/dev/null 2>&1; then
    ip link set dev "$(setting device)" gso_max_segs "$segs"
  fi
  rm -f "$state"
  echo "the baseline is down"
}

command=${1:-}
[ "$#" -eq 0 ] || shift

case $command in
  isolate) exec "$here/pmtu-topology.sh" isolate "$@" ;;
  up) up "$@" ;;
  show) show ;;
  down) down ;;
  *) usage ;;
esac
