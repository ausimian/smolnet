#!/bin/sh
# Builds the path of integration/pmtu.exs: a hop whose MTU is below
# SmolNet's, between SmolNet and a kernel peer.
#
#     SmolNet --tun0-- this namespace ==hop== pmtu-router ---- pmtu-peer
#
#   * This namespace holds the TUN device (SMOLNET_TUN, default tun0, from
#     integration/setup.sh --no-nat) and routes between it and the hop. Its
#     end of the hop is pmtu-h: 10.78.0.1/24 and fd00:78::1/64.
#   * pmtu-router's end of the hop is pmtu-rh: 10.78.0.2 and fd00:78::2.
#     Its other link, pmtu-rp, is 10.79.0.1/24 and fd00:79::1/64.
#   * pmtu-peer is the kernel peer: 10.79.0.2 and fd00:79::2, on pmtu-p.
#
# Both ends of the hop take the hop MTU, and every other link stays at
# 1500, or at SmolNet's MTU where that is larger, so the peer advertises an
# MSS for that and SmolNet one for its own MTU. A packet too big for the hop is refused where it would enter it:
# here for SmolNet's, in pmtu-router for the peer's. That router either
# sends ICMP "Fragmentation Needed" or "Packet Too Big" back to the
# sender, or, with --icmp off, drops it silently: a black hole.
#
#     integration/pmtu-topology.sh isolate COMMAND [ARG...]
#     integration/pmtu-topology.sh up [--mtu N]
#     integration/pmtu-topology.sh set [--hop-mtu N] [--icmp on|off]
#                                      [--clamp none|rt|fixed] [--peer-df on|off]
#     integration/pmtu-topology.sh show
#     integration/pmtu-topology.sh down
#
# isolate runs COMMAND in a new network and mount namespace, with a tmpfs
# on /run and the device set up by setup.sh --no-nat, so that nothing it
# does reaches the host's own network. Without root it maps the user to
# root in a new user namespace, so a host that allows unprivileged user
# namespaces needs no sudo at all:
#
#     integration/pmtu-topology.sh isolate mix run integration/pmtu.exs
#
# Everything goes with the namespace when COMMAND exits.
#
# up builds the path with the hop at --mtu (default 1500), the device's
# MTU too, and forwarding on here and in pmtu-router. It refuses to run
# in the host's own network namespace, whose forwarding and firewall it
# would change: run it under isolate, or in a namespace of your own.
#
# set changes the path, keeping what it is not given:
#
#   --hop-mtu N     the MTU of both ends of the hop. IPv6 needs 1280 or
#                   more; below that the hop carries IPv4 only.
#   --icmp off      drop, in both routers, the ICMP errors that report a
#                   packet too big for the hop; on (the default) sends them.
#   --clamp rt      lower the MSS of every forwarded SYN to what its route
#                   carries (`tcp option maxseg size set rt mtu`, the usual
#                   recipe): the smaller of the MTUs towards its destination
#                   and back towards its source, so either way through the
#                   hop.
#   --clamp fixed   lower the MSS of every forwarded SYN, either way, to a
#                   fixed MSS that fits the hop.
#   --clamp none    (the default) leave SYNs alone.
#   --peer-df off   have the peer's kernel send IPv4 without Don't
#                   Fragment (net.ipv4.ip_no_pmtu_disc=1), so that the
#                   router fragments what does not fit.
#
# and flushes every namespace's cached path MTUs, so that one case does not
# inherit what an earlier one learnt.
#
# down removes both namespaces, the hop, the firewall table and the state,
# and restores the forwarding and ICMP rate limits up changed.

set -eu

PATH=$PATH:/usr/sbin:/sbin
here=$(cd "$(dirname "$0")" && pwd)
device=${SMOLNET_TUN:-tun0}
state=/run/smolnet-pmtu.state
router=pmtu-router
peer=pmtu-peer
table=smolnet_pmtu

usage() {
  sed -n '/^#     integration/s/^#     //p' "$0" >&2
  exit 2
}

die() {
  echo "pmtu-topology.sh: $*" >&2
  exit 1
}

require_root() {
  [ "$(id -u)" -eq 0 ] || die "$1 must run as root: use isolate, or sudo"
}

# Whether this is the network namespace of PID 1, the host's own. A PID 1
# whose namespace cannot be read belongs to someone else, so this one is
# not it.
in_host_namespace() {
  [ "$(readlink /proc/self/ns/net)" = "$(readlink /proc/1/ns/net 2>/dev/null || true)" ]
}

setting() {
  sed -n "s/^$1 //p" "$state"
}

save() {
  grep -v "^$1 " "$state" >"$state.new" || true
  echo "$1 $2" >>"$state.new"
  mv "$state.new" "$state"
}

in_router() { ip netns exec "$router" "$@"; }
in_peer() { ip netns exec "$peer" "$@"; }

isolate() {
  [ "$#" -gt 0 ] || usage

  user=""
  if [ "$(id -u)" -ne 0 ]; then user=--map-root-user; fi

  # $user is one word or none; the script is expanded by the inner shell.
  # shellcheck disable=SC2086,SC2016
  exec unshare $user --net --mount sh -c '
    set -e
    export PATH="$PATH:/usr/sbin:/sbin"
    mount -t tmpfs tmpfs /run
    ip link set lo up
    "$0/setup.sh" --no-nat >&2
    exec "$@"
  ' "$here" "$@"
}

up() {
  mtu=1500

  while [ "$#" -gt 0 ]; do
    case $1 in
      --mtu) mtu=${2:?--mtu needs a value}; shift 2 ;;
      *) usage ;;
    esac
  done

  require_root up
  if in_host_namespace; then
    die "refusing to change the host's own network namespace; run under: $0 isolate COMMAND"
  fi
  [ ! -e "$state" ] || die "the path is already up ($state exists); run $0 down first"
  ip link show dev "$device" >/dev/null 2>&1 || die "no $device here: run integration/setup.sh --no-nat first"

  : >"$state"
  save device "$device"
  save ipv4_forward "$(sysctl -n net.ipv4.ip_forward)"
  save ipv6_forward "$(sysctl -n net.ipv6.conf.all.forwarding)"
  save icmp_ratelimit "$(sysctl -n net.ipv4.icmp_ratelimit)"
  save icmpv6_ratelimit "$(sysctl -n net.ipv6.icmp.ratelimit)"

  ip netns add "$router"
  ip netns add "$peer"
  # No duplicate address detection in the new namespaces, whose links
  # would otherwise wait on it, and again whenever the hop regains IPv6.
  for run in in_router in_peer; do
    $run sysctl -qw net.ipv6.conf.all.accept_dad=0
    $run sysctl -qw net.ipv6.conf.default.accept_dad=0
  done
  ip link add pmtu-h type veth peer name pmtu-rh netns "$router"
  in_router ip link add pmtu-rp type veth peer name pmtu-p netns "$peer"
  sysctl -qw net.ipv6.conf.pmtu-h.accept_dad=0

  ip link set dev "$device" mtu "$mtu"
  ip link set dev pmtu-h mtu "$mtu" up
  in_router ip link set dev lo up
  in_router ip link set dev pmtu-rh mtu "$mtu" up
  # The peer's link is never the narrow one: 1500, or SmolNet's MTU if
  # that is larger.
  wide=$((mtu > 1500 ? mtu : 1500))
  in_router ip link set dev pmtu-rp mtu "$wide" up
  in_peer ip link set dev lo up
  in_peer ip link set dev pmtu-p mtu "$wide" up

  ip addr add 10.78.0.1/24 dev pmtu-h
  in_router ip addr add 10.78.0.2/24 dev pmtu-rh
  in_router ip addr add 10.79.0.1/24 dev pmtu-rp
  in_peer ip addr add 10.79.0.2/24 dev pmtu-p
  ip route add 10.79.0.0/24 via 10.78.0.2
  in_router ip route add 10.77.0.0/24 via 10.78.0.1
  in_peer ip route add default via 10.79.0.1

  in_router ip -6 addr add fd00:79::1/64 dev pmtu-rp nodad
  in_peer ip -6 addr add fd00:79::2/64 dev pmtu-p nodad
  in_peer ip -6 route add default via fd00:79::1
  ipv6_hop

  for run in "" in_router; do
    $run sysctl -qw net.ipv4.ip_forward=1
    $run sysctl -qw net.ipv6.conf.all.forwarding=1
    # Every refused packet gets its error, however close together.
    $run sysctl -qw net.ipv4.icmp_ratelimit=0
    $run sysctl -qw net.ipv6.icmp.ratelimit=0
  done

  save mtu "$mtu"
  save hop_mtu "$mtu"
  save icmp on
  save clamp none
  save peer_df on
  apply
  echo "the path is up: SmolNet and $device at $mtu, the hop at $mtu, the peer at 10.79.0.2 and fd00:79::2"
}

# The hop's IPv6 addresses and routes. A link whose MTU falls below 1280
# loses IPv6 altogether, and comes back without them.
ipv6_hop() {
  ip -6 addr replace fd00:78::1/64 dev pmtu-h nodad
  in_router ip -6 addr replace fd00:78::2/64 dev pmtu-rh nodad
  ip -6 route replace fd00:79::/64 via fd00:78::2
  in_router ip -6 route replace fd00:77::/64 via fd00:78::1
}

set_path() {
  [ -e "$state" ] || die "the path is not up; run $0 up first"

  while [ "$#" -gt 0 ]; do
    case $1 in
      --hop-mtu) save hop_mtu "${2:?--hop-mtu needs a value}" ;;
      --icmp) case ${2:-} in on | off) save icmp "$2" ;; *) usage ;; esac ;;
      --clamp) case ${2:-} in none | rt | fixed) save clamp "$2" ;; *) usage ;; esac ;;
      --peer-df) case ${2:-} in on | off) save peer_df "$2" ;; *) usage ;; esac ;;
      *) usage ;;
    esac
    shift 2
  done

  apply
}

apply() {
  hop=$(setting hop_mtu)
  icmp=$(setting icmp)
  clamp=$(setting clamp)

  ip link set dev pmtu-h mtu "$hop"
  in_router ip link set dev pmtu-rh mtu "$hop"
  if [ "$hop" -ge 1280 ]; then ipv6_hop; fi

  case $(setting peer_df) in
    on) in_peer sysctl -qw net.ipv4.ip_no_pmtu_disc=0 ;;
    off) in_peer sysctl -qw net.ipv4.ip_no_pmtu_disc=1 ;;
  esac

  # Router-generated errors leave through the output hook.
  quiet=""
  if [ "$icmp" = off ]; then
    quiet='chain output {
      type filter hook output priority filter; policy accept;
      icmp type destination-unreachable icmp code frag-needed drop
      icmpv6 type packet-too-big drop
    }'
  fi

  # Only this namespace sees both the SmolNet side and the hop, so it does
  # the clamping. `rt mtu` is the smaller of the MTUs of the routes towards
  # the packet's destination and back towards its source (get_tcpmss in
  # the kernel's nft_rt.c), so this one rule also lowers the peer's
  # SYN-ACK, although that leaves by the device.
  mss=""
  case $clamp in
    rt) mss='tcp flags & (syn | rst) == syn tcp option maxseg size set rt mtu' ;;
    fixed)
      mss4=$((hop - 40))
      mss6=$((hop - 60))
      mss="meta nfproto ipv4 tcp flags & (syn | rst) == syn tcp option maxseg size > $mss4 tcp option maxseg size set $mss4
      meta nfproto ipv6 tcp flags & (syn | rst) == syn tcp option maxseg size > $mss6 tcp option maxseg size set $mss6"
      ;;
  esac

  for run in "" in_router; do
    $run nft delete table inet "$table" 2>/dev/null || true
  done

  nft -f - <<EOF
table inet $table {
  $quiet
  chain forward {
    type filter hook forward priority mangle; policy accept;
    $mss
  }
}
EOF

  in_router nft -f - <<EOF
table inet $table {
  $quiet
}
EOF

  for run in "" in_router in_peer; do
    $run ip route flush cache
    $run ip -6 route flush cache
    $run ip tcp_metrics flush all 2>/dev/null || true
  done

  # The peer reaches this end of the hop, over IPv6 too where the hop
  # carries it, before the path counts as ready.
  ready 10.78.0.1
  if [ "$hop" -ge 1280 ]; then ready fd00:78::1; fi
}

ready() {
  tries=0
  until in_peer ping -c 1 -W 1 "$1" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 10 ] || die "the peer cannot reach $1 over the hop"
  done
}

show() {
  [ -e "$state" ] || die "the path is not up"
  cat "$state"
  ip -br link show dev "$(setting device)"
  ip -br link show dev pmtu-h
  in_router ip -br link
  in_peer ip -br addr
  nft list table inet "$table"
  in_router nft list table inet "$table"
}

down() {
  require_root down
  [ -e "$state" ] || { echo "the path is not up; nothing to undo" >&2; exit 0; }

  ip netns del "$peer" 2>/dev/null || true
  ip netns del "$router" 2>/dev/null || true
  ip link del dev pmtu-h 2>/dev/null || true
  nft delete table inet "$table" 2>/dev/null || true

  sysctl -qw "net.ipv4.ip_forward=$(setting ipv4_forward)"
  sysctl -qw "net.ipv6.conf.all.forwarding=$(setting ipv6_forward)"
  sysctl -qw "net.ipv4.icmp_ratelimit=$(setting icmp_ratelimit)"
  sysctl -qw "net.ipv6.icmp.ratelimit=$(setting icmpv6_ratelimit)"
  rm -f "$state"
  echo "the path is down"
}

command=${1:-}
[ "$#" -eq 0 ] || shift

case $command in
  isolate) isolate "$@" ;;
  up) up "$@" ;;
  set) require_root set; set_path "$@" ;;
  show) show ;;
  down) down ;;
  *) usage ;;
esac
