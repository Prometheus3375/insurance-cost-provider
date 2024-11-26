#!/bin/sh
set -e

exec uvicorn api:app --host 0.0.0.0 --port 2127 --log-config server/logging-config.json
