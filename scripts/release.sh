#!/usr/bin/env bash
# Builds the files for a GitHub release: the `ntx` command and the C library (`ntx_ffi`) for each
# platform, packed with `ntx.h`, a short README and the licences, plus SHA256SUMS.
#
# Usage: scripts/release.sh [target...]     Files go to dist/.
#
# Without targets it builds every target. Linux and Windows are cross-built with `zig` and
# `cargo-zigbuild`; a missing tool is named.
set -euo pipefail
cd "$(dirname "$0")/.."

version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
all_targets=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  x86_64-unknown-linux-gnu
  aarch64-unknown-linux-gnu
  x86_64-pc-windows-gnu
)
targets=("${@:-${all_targets[@]}}")
dist=dist
rm -rf "$dist"
mkdir -p "$dist"

need() {
  command -v "$1" >/dev/null || { echo "error: $1 is needed for $2 (install: $3)" >&2; exit 1; }
}

build() {
  local target=$1
  case $target in
    *-apple-darwin) cargo build --release --locked --target "$target" -p ntx-cli -p ntx-ffi ;;
    # glibc 2.17, so the files run on every Linux from about 2014 on.
    *-linux-gnu)
      need cargo-zigbuild "$target" "cargo install cargo-zigbuild; brew install zig"
      cargo zigbuild --release --locked --target "$target.2.17" -p ntx-cli -p ntx-ffi ;;
    *-windows-gnu)
      need cargo-zigbuild "$target" "cargo install cargo-zigbuild; brew install zig"
      cargo zigbuild --release --locked --target "$target" -p ntx-cli -p ntx-ffi ;;
    *) echo "error: no build recipe for $target" >&2; exit 1 ;;
  esac
}

package() {
  local target=$1 name="ntx-v$version-$1" out="target/$1/release"
  local stage="$dist/$name"
  mkdir -p "$stage/lib" "$stage/include"
  cp crates/ntx-ffi/include/ntx.h "$stage/include/"
  cp LICENSE-MIT LICENSE-APACHE "$stage/"
  cp scripts/release-readme.md "$stage/README.md"
  case $target in
    *-apple-darwin)
      cp "$out/ntx" "$stage/"
      cp "$out/libntx_ffi.dylib" "$out/libntx_ffi.a" "$stage/lib/"
      # Without this, the library names the build folder, which programs linked to it would search.
      install_name_tool -id @rpath/libntx_ffi.dylib "$stage/lib/libntx_ffi.dylib" ;;
    *-linux-gnu)
      cp "$out/ntx" "$stage/"
      cp "$out/libntx_ffi.so" "$out/libntx_ffi.a" "$stage/lib/" ;;
    *-windows-gnu)
      cp "$out/ntx.exe" "$stage/"
      cp "$out/ntx_ffi.dll" "$out/libntx_ffi.dll.a" "$out/libntx_ffi.a" "$stage/lib/" ;;
  esac
  case $target in
    *-windows-*) (cd "$dist" && zip -qr "$name.zip" "$name") ;;
    *) tar -C "$dist" -czf "$dist/$name.tar.gz" "$name" ;;
  esac
  rm -rf "$stage"
}

for target in "${targets[@]}"; do
  echo "== $target"
  build "$target"
  package "$target"
done
(cd "$dist" && shasum -a 256 ntx-v"$version"-* > SHA256SUMS)
echo "Done: $dist/"
ls -l "$dist"
