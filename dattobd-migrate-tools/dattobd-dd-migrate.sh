#!/usr/bin/env bash
set -euo pipefail

BLOCK_SIZE=4096
MINOR="${MINOR:-0}"
CACHE_MB="${CACHE_MB:-300}"
FALLOCATE_MB="${FALLOCATE_MB:-0}"

usage() {
  cat >&2 <<'USAGE'
Usage:
  dattobd-dd-migrate.sh full <base-device> <cow-file> <target-file-or-device>
  dattobd-dd-migrate.sh begin-incremental
  dattobd-dd-migrate.sh list <old-cow-file> <snapshot-device>
  dattobd-dd-migrate.sh apply-incremental <old-cow-file> <snapshot-device> <target-file-or-device>
  dattobd-dd-migrate.sh next-snapshot <new-cow-file>
  dattobd-dd-migrate.sh cleanup

Environment:
  MINOR=0             dattobd minor, producing /dev/datto0
  CACHE_MB=300        dbdctl cache size in MB
  FALLOCATE_MB=0      COW file allocation in MB; 0 means dattobd default

Typical flow:
  full /dev/sda1 /.datto0 /dev/target
  begin-incremental
  next-snapshot /.datto1
  list /.datto0 /dev/datto0
  apply-incremental /.datto0 /dev/datto0 /dev/target
  begin-incremental
USAGE
}

need_root() {
  if [[ "$(id -u)" != "0" ]]; then
    echo "must run as root" >&2
    exit 1
  fi
}

tool_dir() {
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd
}

build_lister_if_needed() {
  local dir
  dir="$(tool_dir)"
  if [[ ! -x "$dir/list-changed-blocks" ]]; then
    cc -O2 -Wall -Wextra -o "$dir/list-changed-blocks" "$dir/list-changed-blocks.c"
  fi
}

setup_snapshot() {
  local base_dev="$1"
  local cow="$2"
  local args=(setup-snapshot -c "$CACHE_MB")

  if [[ "$FALLOCATE_MB" != "0" ]]; then
    args+=(-f "$FALLOCATE_MB")
  fi

  args+=("$base_dev" "$cow" "$MINOR")
  dbdctl "${args[@]}"
}

full_copy() {
  local base_dev="$1"
  local cow="$2"
  local target="$3"

  need_root
  setup_snapshot "$base_dev" "$cow"
  dd if="/dev/datto${MINOR}" of="$target" bs=4M conv=fsync status=progress
}

begin_incremental() {
  need_root
  dbdctl transition-to-incremental "$MINOR"
}

next_snapshot() {
  local new_cow="$1"
  local args=(transition-to-snapshot)

  need_root
  if [[ "$FALLOCATE_MB" != "0" ]]; then
    args+=(-f "$FALLOCATE_MB")
  fi
  args+=("$new_cow" "$MINOR")
  dbdctl "${args[@]}"
}

list_changed() {
  local old_cow="$1"
  local snapshot="$2"

  build_lister_if_needed
  "$(tool_dir)/list-changed-blocks" --ranges "$old_cow" "$snapshot"
}

apply_incremental() {
  local old_cow="$1"
  local snapshot="$2"
  local target="$3"

  need_root
  build_lister_if_needed

  "$(tool_dir)/list-changed-blocks" --ranges "$old_cow" "$snapshot" |
    while IFS=, read -r start_block _offset _length blocks; do
      [[ -n "${start_block:-}" ]] || continue
      dd if="$snapshot" of="$target" \
        bs="$BLOCK_SIZE" \
        skip="$start_block" \
        seek="$start_block" \
        count="$blocks" \
        conv=notrunc,fsync \
        status=none
      echo "synced start_block=$start_block blocks=$blocks bytes=$((blocks * BLOCK_SIZE))"
    done
}

cleanup() {
  need_root
  dbdctl destroy "$MINOR"
}

cmd="${1:-}"
case "$cmd" in
  full)
    [[ $# -eq 4 ]] || { usage; exit 2; }
    full_copy "$2" "$3" "$4"
    ;;
  begin-incremental)
    [[ $# -eq 1 ]] || { usage; exit 2; }
    begin_incremental
    ;;
  next-snapshot)
    [[ $# -eq 2 ]] || { usage; exit 2; }
    next_snapshot "$2"
    ;;
  list)
    [[ $# -eq 3 ]] || { usage; exit 2; }
    list_changed "$2" "$3"
    ;;
  apply-incremental)
    [[ $# -eq 4 ]] || { usage; exit 2; }
    apply_incremental "$2" "$3" "$4"
    ;;
  cleanup)
    [[ $# -eq 1 ]] || { usage; exit 2; }
    cleanup
    ;;
  *)
    usage
    exit 2
    ;;
esac
