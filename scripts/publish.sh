#!/usr/bin/env bash
# Build and publish a release from this machine — no CI, no cost.
#
#   ./scripts/publish.sh v0.1.0
#
# GitHub Actions bills macOS runners at 10x on private repos, and the Mac
# agent must be built on Apple Silicon, so releases are cut locally. See
# .github/workflows/release.yml for the CI equivalent, which becomes free if
# this repo ever goes public.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  echo "usage: $0 <version>   e.g. $0 v0.1.0" >&2
  exit 1
fi
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: version must look like v1.2.3 (got '$VERSION')" >&2
  exit 1
fi

command -v gh >/dev/null 2>&1 || {
  echo "error: gh CLI not found (brew install gh)" >&2; exit 1; }
gh auth status >/dev/null 2>&1 || {
  echo "error: not logged in to GitHub (run: gh auth login)" >&2; exit 1; }

# Binaries that do not correspond to a commit are unreproducible and
# undebuggable: a bug report names a version nobody can check out.
if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is dirty; commit or stash first" >&2
  git status --short >&2
  exit 1
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
  echo "error: tag $VERSION already exists" >&2
  exit 1
fi
if gh release view "$VERSION" >/dev/null 2>&1; then
  echo "error: release $VERSION already published" >&2
  exit 1
fi

echo "==> tests"
make test >/dev/null
echo "    ok"

echo "==> building release artifacts"
make release >/dev/null
( cd dist && shasum -a 256 -c SHA256SUMS >/dev/null )
echo "    ok: $(ls dist | tr '\n' ' ')"

# `doctor` initialises Metal through cgo, so this catches a broken link that
# `go build` accepts and that would only fail on a provider's machine.
echo "==> smoke-testing the macOS binary"
./dist/gridlink-agent-darwin-arm64 doctor || {
  echo "error: the macOS binary cannot use the GPU; refusing to publish" >&2; exit 1; }

echo "==> tagging $VERSION"
git tag -a "$VERSION" -m "$VERSION"
git push origin "$VERSION"

echo "==> publishing release"
gh release create "$VERSION" \
  --title "$VERSION" \
  --generate-notes \
  dist/gridlink-agent-darwin-arm64 \
  dist/gridlink-agent-linux-arm64 \
  dist/gridlink-agent-linux-amd64 \
  dist/SHA256SUMS

echo
echo "Published $VERSION. Providers install with:"
echo "  curl -fsSL https://raw.githubusercontent.com/CarlJohanBalck/gridlink/main/scripts/install.sh | sh"
echo
echo "Note: the repo must be public for that URL to work for anyone else."
