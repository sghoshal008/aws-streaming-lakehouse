#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

echo "Compiling Python sources..."
python3 -m py_compile app/acquisition/yt_sales_acquisition.py app/glue/landing-to-msk/yt_sales_landing_to_msk.py app/glue/bronze-to-silver/yt_sales_bronze_to_silver.py

echo "Checking shell syntax..."
for f in infra/scripts/*.sh; do bash -n "$f"; done

echo "Checking JSON..."
python3 - <<'PY'
import json
from pathlib import Path
for p in Path('.').rglob('*.json'):
    if 'build' not in p.parts:
        json.loads(p.read_text())
        print('OK', p)
PY

if command -v cfn-lint >/dev/null 2>&1; then
  echo "Linting CloudFormation..."
  cfn-lint infra/templates/*.yaml
else
  echo "WARNING: cfn-lint not installed; skipping CloudFormation lint. Install with: pip install cfn-lint"
fi

echo "Validation complete."
