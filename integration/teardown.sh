#!/bin/sh
# Undoes integration/setup.sh, and removes any netem impairment an
# interrupted run left on the device:
#
#     sudo integration/teardown.sh
#
# Only what setup.sh recorded in /run/smolnet-integration-<device>.state is
# undone, so a device of the same name that setup.sh did not create is left
# alone. If anything cannot be restored, the state file is kept, so that
# teardown.sh can be run again, and it exits non-zero.

set -u

device=${SMOLNET_TUN:-tun0}
state=/run/smolnet-integration-$device.state
ifb=$(printf 'ifb-%s' "$device" | cut -c1-15)
failed=0

if [ "$(id -u)" -ne 0 ]; then
  echo "teardown.sh must run as root: sudo $0" >&2
  exit 1
fi

if [ ! -e "$state" ]; then
  echo "$device was not set up by setup.sh ($state is missing); nothing to undo" >&2
  exit 0
fi

# Runs a restoring command, and remembers if it failed.
restore() {
  if ! "$@"; then
    echo "teardown.sh: could not run: $*" >&2
    failed=1
  fi
}

# The ifb SmolNet.Integration.Soak.Netem leaves when a run is interrupted,
# known by its alias. The device's own qdiscs go with the device.
if ip link show dev "$ifb" 2>/dev/null | grep -q '^ *alias smolnet-integration$'; then
  restore ip link del dev "$ifb"
fi

# The table SmolNet.Integration.Blackhole leaves when a run is interrupted,
# known by its name.
blackhole=smolnet_blackhole_$(printf '%s' "$device" | tr -c 'A-Za-z0-9_' '_')
if command -v nft >/dev/null 2>&1 && nft list table inet "$blackhole" >/dev/null 2>&1; then
  restore nft delete table inet "$blackhole"
fi

# Forwarding goes back first, while the rules that isolate it still stand.
while read -r key value; do
  case $key in
    ipv4_forward) restore sysctl -qw "net.ipv4.ip_forward=$value" ;;
    ipv6_forward) restore sysctl -qw "net.ipv6.conf.all.forwarding=$value" ;;
    accept_ra)
      conf=/proc/sys/net/ipv6/conf/$value/accept_ra
      if [ -e "$conf" ]; then restore sh -c 'echo 1 >"$1"' sh "$conf"; fi
      ;;
  esac
done <"$state"

# What is already gone needs no undoing.
while read -r key value; do
  case $key in
    xt_rule)
      # shellcheck disable=SC2086 # the recorded rule is its words
      set -- $value
      command=$1
      chain=$2
      shift 2
      if "$command" -w -C "$chain" "$@" 2>/dev/null; then
        restore "$command" -w -D "$chain" "$@"
      fi
      ;;
    nft_table)
      if nft list table inet "$value" >/dev/null 2>&1; then
        restore nft delete table inet "$value"
      fi
      ;;
    device)
      if ip link show dev "$value" >/dev/null 2>&1; then
        restore ip link del dev "$value"
      fi
      ;;
  esac
done <"$state"

if [ "$failed" -ne 0 ]; then
  echo "$device is only partly torn down; $state is kept for another try" >&2
  exit 1
fi

rm -f "$state"
echo "$device is torn down"
