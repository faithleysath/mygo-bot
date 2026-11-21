#!/bin/sh
# Invoked in the pinned Debian Rust container with the checkout mounted read-only.
set -eu
cargo build --locked --release --jobs 2 --bin mygo-bot
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir "$stage/lib"
cp "$CARGO_TARGET_DIR/release/mygo-bot" "$stage/"
ldd "$stage/mygo-bot" | awk '$2 == "=>" && $3 ~ /^\// {print $3}' | xargs -r cp -Lt "$stage/lib"
cp -L /lib64/ld-linux-x86-64.so.2 "$stage/lib/"
"$stage/lib/ld-linux-x86-64.so.2" --library-path "$stage/lib" "$stage/mygo-bot" --version
tar -C "$stage" -czf /out/mygo.tgz .
