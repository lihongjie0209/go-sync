#!/usr/bin/env bash
# The Go tests own all containers through Testcontainers; this only selects versions.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" != --one ]]; then
  versions=("${@}")
  if [[ ${#versions[@]} == 0 ]]; then
    versions=(9.4.26 9.5.25 9.6.24 10.23 11.22 12.22 13.23 14.24 15.19 16.15 17.11 18.6)
  fi
  printf '%s\n' "${versions[@]}" | xargs -P "${JOBS:-2}" -n 1 bash scripts/test-matrix.sh --one
  exit
fi
version="$2"
[[ "$version" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || exit 2
artifact_dir="$(mktemp -d /tmp/go-sync-matrix.XXXXXXXX)"
printf 'PostgreSQL %s: Testcontainers; logs %s\n' "$version" "$artifact_dir"
if GO_SYNC_TEST_PG_VERSION="$version" go test -p 1 -race -count=1 -timeout=30m -tags=integration ./internal/app ./internal/capture >"$artifact_dir/tests.log" 2>&1; then
  printf 'PostgreSQL %s: PASS (%s/tests.log)\n' "$version" "$artifact_dir"
else
  cat "$artifact_dir/tests.log"
  exit 1
fi
