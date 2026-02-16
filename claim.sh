#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <issue-number>" >&2
  exit 1
fi

ISSUE="$1"

gh issue edit "$ISSUE" --add-assignee @me --add-label "status:in-progress"
echo "Claimed #${ISSUE}"
