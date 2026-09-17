#!/usr/bin/env bash
set -euo pipefail

asset=$(cd "$(dirname "${1:?usage: smoke-test-package.sh <asset>}")" && pwd)/$(basename "$1")
repository=$(cd "$(dirname "$0")/.." && pwd)
checksum_file="$repository/checksum-Elixir.SmolNet.Native.exs"

if [[ -e "$checksum_file" ]]; then
  echo "error: refusing to overwrite existing $checksum_file" >&2
  exit 1
fi

temporary=$(mktemp -d)
cleanup() {
  rm -f "$checksum_file"
  rm -rf "$temporary"
}
trap cleanup EXIT

asset_name=$(basename "$asset")
if command -v sha256sum >/dev/null; then
  checksum=$(sha256sum "$asset" | awk '{print $1}')
else
  checksum=$(shasum -a 256 "$asset" | awk '{print $1}')
fi
printf '%%{"%s" => "sha256:%s"}\n' "$asset_name" "$checksum" > "$checksum_file"

package="$temporary/package"
cache="$temporary/cache"
consumer="$temporary/consumer"
mkdir -p "$cache" "$consumer"
cp "$asset" "$cache/$asset_name"

(cd "$repository" && mix hex.build --unpack --output "$package")

if [[ -d "$package/native" ]] || find "$package" -type f \( -name '*.so' -o -name '*.dylib' \) | grep -q .; then
  echo "error: Hex package contains native source or generated libraries" >&2
  exit 1
fi

printf '%s\n' \
  'defmodule SmolNetConsumer.MixProject do' \
  '  use Mix.Project' \
  '  def project, do: [app: :smolnet_consumer, version: "0.0.0", deps: deps()]' \
  '  def application, do: [extra_applications: [:logger]]' \
  '  defp deps, do: [{:smolnet, path: System.fetch_env!("SMOLNET_PACKAGE_PATH")}]' \
  'end' > "$consumer/mix.exs"

export SMOLNET_PACKAGE_PATH="$package"
export RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH="$cache"

(cd "$consumer" && mix deps.get)
(cd "$consumer" && mix run -e 'unless SmolNet.Native.health() == :ok, do: raise("NIF health check failed")')

echo "verified packaged consumer download, checksum, extraction, load, and health path"
