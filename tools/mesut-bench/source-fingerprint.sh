#!/bin/sh
# Read-only provenance for the local path dependencies and benchmark source.
set -eu
cd "$(dirname "$0")/../.."
find crates -type f \( -name '*.rs' -o -name Cargo.toml \) -print0 |
    sort -z | xargs -0 sha256sum
sha256sum Cargo.toml tools/mesut-bench/Cargo.toml tools/mesut-bench/Cargo.lock tools/mesut-bench/src/main.rs
