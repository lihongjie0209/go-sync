#!/bin/sh
set -eu
set -- postgres -D /data -c 'listen_addresses=*' -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20
if postgres -D /data -C output_plugin_libraries >/dev/null 2>&1; then
  set -- "$@" -c output_plugin_libraries=wal2json
fi
exec "$@"
