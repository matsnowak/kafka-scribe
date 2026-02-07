#!/usr/bin/env bash
set -euo pipefail

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Cleaning storage in $PROJECT_ROOT..."
rm -rf "$PROJECT_ROOT/test-storage"
rm -rf "$PROJECT_ROOT/stores"
echo "Done."
