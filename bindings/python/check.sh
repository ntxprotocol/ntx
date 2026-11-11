#!/bin/sh
# Builds the extension with coverage instrumentation, checks the type stub against it (stubtest),
# runs the Python tests, then checks line coverage of the Python package and of the Rust binding
# code (both at least 90%).
# Needs maturin, mypy, pytest and pytest-cov in the active virtualenv, and cargo-llvm-cov.
set -eu
cd "$(dirname "$0")"
eval "$(cargo llvm-cov show-env --sh)"
cargo llvm-cov clean --workspace
maturin develop --quiet
python -m mypy.stubtest ntxprotocol._ntx
python -m pytest --cov=ntxprotocol --cov-fail-under=90
# Only the binding crate: the core's own tests already cover crates/.
cargo llvm-cov report --fail-under-lines 90 --ignore-filename-regex '/crates/'
