#!/usr/bin/env bash
set -euo pipefail

nif=${1:?usage: verify-nif-linux.sh <nif> <x86_64|aarch64>}
architecture=${2:?usage: verify-nif-linux.sh <nif> <x86_64|aarch64>}
glibc_floor=2.35

case "$architecture" in
  x86_64)
    expected_machine='Advanced Micro Devices X86-64'
    dynamic_loader='ld-linux-x86-64.so.2'
    ;;
  aarch64)
    expected_machine='AArch64'
    dynamic_loader='ld-linux-aarch64.so.1'
    ;;
  *) echo "error: unsupported architecture $architecture" >&2; exit 1 ;;
esac

actual_machine=$(readelf -h "$nif" | sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
if [[ "$actual_machine" != "$expected_machine" ]]; then
  echo "error: expected $expected_machine, got $actual_machine" >&2
  exit 1
fi

allowed="libc.so.6 libgcc_s.so.1 libm.so.6 libdl.so.2 libpthread.so.0 librt.so.1 $dynamic_loader"
while IFS= read -r dependency; do
  if [[ " $allowed " != *" $dependency "* ]]; then
    echo "error: unexpected native dependency $dependency" >&2
    exit 1
  fi
done < <(readelf -d "$nif" | sed -n 's/.*Shared library: \[\([^]]*\)\].*/\1/p')

while IFS= read -r version; do
  newest=$(printf '%s\n' "$glibc_floor" "${version#GLIBC_}" | sort -V | tail -n 1)
  if [[ "$newest" != "$glibc_floor" ]]; then
    echo "error: $nif requires $version, newer than GLIBC_$glibc_floor" >&2
    exit 1
  fi
done < <(readelf --version-info "$nif" | grep -o 'GLIBC_[0-9][0-9.]*' | sort -Vu)

echo "verified $architecture NIF with GLIBC_$glibc_floor floor and allowlisted dependencies"
