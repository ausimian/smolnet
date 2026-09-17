#!/usr/bin/env bash
set -euo pipefail

nif=${1:?usage: verify-nif-macos.sh <nif>}
macos_floor=14.0

if ! file "$nif" | grep -q 'Mach-O 64-bit dynamically linked shared library arm64'; then
  echo "error: $nif is not an arm64 Mach-O shared library" >&2
  file "$nif" >&2
  exit 1
fi

minimum=$(
  otool -l "$nif" |
    awk '$1 == "cmd" && $2 == "LC_BUILD_VERSION" { build = 1; next }
         build && $1 == "minos" { print $2; exit }'
)

if [[ -z "$minimum" ]]; then
  echo "error: $nif has no LC_BUILD_VERSION minimum macOS version" >&2
  exit 1
fi

IFS=. read -r major minor patch <<< "$minimum"
minor=${minor:-0}
patch=${patch:-0}
if (( major > 14 || (major == 14 && minor > 0) || (major == 14 && minor == 0 && patch > 0) )); then
  echo "error: $nif requires macOS $minimum, newer than macOS $macos_floor" >&2
  exit 1
fi

install_name=$(otool -D "$nif" 2>/dev/null | sed -n '2p' || true)
allowed='/usr/lib/libSystem.B.dylib /usr/lib/libiconv.2.dylib'
while IFS= read -r dependency; do
  if [[ -n "$install_name" && "$dependency" == "$install_name" ]]; then
    continue
  fi

  if [[ " $allowed " != *" $dependency "* ]]; then
    echo "error: unexpected native dependency $dependency" >&2
    exit 1
  fi
done < <(otool -L "$nif" | tail -n +2 | awk '{print $1}')

echo "verified Apple Silicon NIF with macOS $macos_floor floor and allowlisted system dependencies"
