#!/bin/sh
set -eu
bun run migration:run
exec "$@"
