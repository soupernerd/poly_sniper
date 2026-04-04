#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required (3.11+)."
  exit 1
fi

python3 scripts/setup_public.py
