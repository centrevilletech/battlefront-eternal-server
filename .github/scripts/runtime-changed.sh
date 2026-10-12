#!/usr/bin/env bash
# Exit zero when an image/runtime update is required, one when it is unchanged.
set -euo pipefail
revision="${1:?Pass the deployed image commit}"
[[ "$revision" =~ ^[a-f0-9]{40}$ ]] || exit 0
git cat-file -e "$revision^{commit}" 2>/dev/null || exit 0
if git diff --quiet "$revision" HEAD -- src package.json package-lock.json Dockerfile .dockerignore compose.yml .github/scripts .github/workflows/build.yml; then
  exit 1
fi
exit 0
