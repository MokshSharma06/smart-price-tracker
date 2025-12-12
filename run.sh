#!/bin/bash
set -euo pipefail

echo "Running smart-price-tracker (main.py) inside conda/micromamba env..."

# using Conda base image:
if command -v conda >/dev/null 2>&1; then
  conda run -n spt-env python main.py
  exit $?
fi

# fallback
python main.py
