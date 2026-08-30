#!/usr/bin/env sh
# GridLink provider installer.
#
#   curl -fsSL https://<host>/install.sh | sh
#
# Installs the agent to ~/.local/bin. No sudo, no Docker, no Python, no
# Homebrew — the agent carries its own inference engine.
#
# Deliberately POSIX sh, not bash: macOS ships bash 3.2 and some Linux images
# have no bash at all.
#
# Note on code signing: files fetched with curl carry no macOS quarantine
# attribute, so an unsigned binary runs. Notarization is only needed for a
# double-clickable app downloaded in a browser.
set -eu

BASE_URL="${GRIDLINK_BASE_URL:-https://github.com/carljohanbalck/gridlink/releases/latest/download}"
INSTALL_DIR="${GRIDLINK_INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="gridlink-agent"

say()  { printf '%s\n' "$*"; }
err()  { printf 'error: %s\n' "$*" >&2; exit 1; }

detect_target() {
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os" in
    Darwin) os_id="darwin" ;;
    Linux)  os_id="linux" ;;
    *) err "unsupported OS: $os (GridLink supports macOS and Linux)" ;;
  esac
  case "$arch" in
    arm64|aarch64) arch_id="arm64" ;;
    x86_64|amd64)  arch_id="amd64" ;;
    *) err "unsupported architecture: $arch" ;;
  esac

  # Apple Silicon is the only target with a GPU engine today. Intel Macs have
  # no Metal path in this build, so say so rather than installing something
  # that silently takes no work.
  if [ "$os_id" = "darwin" ] && [ "$arch_id" != "arm64" ]; then
    err "Intel Macs are not supported: the engine requires Apple Silicon"
  fi

  TARGET="${os_id}-${arch_id}"
}

fetch() {
  # -f so an HTML error page is never written to disk as if it were a binary.
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    err "neither curl nor wget is available"
  fi
}

checksum() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    echo ""
  fi
}

main() {
  detect_target
  say "==> GridLink agent for ${TARGET}"

  tmp="$(mktemp -d)"
  # shellcheck disable=SC2064  # expand tmp now, not at trap time
  trap "rm -rf '$tmp'" EXIT INT TERM

  asset="gridlink-agent-${TARGET}"
  say "==> downloading"
  fetch "${BASE_URL}/${asset}" "$tmp/$BIN_NAME" \
    || err "download failed: ${BASE_URL}/${asset}"

  # Verify against the published checksum list. A tampered or truncated
  # download must not be installed, and this runs on strangers' machines.
  if fetch "${BASE_URL}/SHA256SUMS" "$tmp/SHA256SUMS" 2>/dev/null; then
    want="$(grep " ${asset}\$" "$tmp/SHA256SUMS" | cut -d' ' -f1 || true)"
    got="$(checksum "$tmp/$BIN_NAME")"
    if [ -z "$want" ]; then
      say "    warning: no checksum published for ${asset}"
    elif [ -z "$got" ]; then
      say "    warning: no sha256 tool found; skipping verification"
    elif [ "$want" != "$got" ]; then
      err "checksum mismatch for ${asset}: expected $want, got $got"
    else
      say "==> checksum ok"
    fi
  else
    say "    warning: SHA256SUMS not published; skipping verification"
  fi

  mkdir -p "$INSTALL_DIR"
  chmod +x "$tmp/$BIN_NAME"
  mv "$tmp/$BIN_NAME" "$INSTALL_DIR/$BIN_NAME"
  say "==> installed ${INSTALL_DIR}/${BIN_NAME}"

  case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *) say ""
       say "    ${INSTALL_DIR} is not on your PATH. Add it with:"
       say "      echo 'export PATH=\"\$PATH:${INSTALL_DIR}\"' >> ~/.zshrc" ;;
  esac

  say ""
  say "Next: point the agent at a coordinator and start earning."
  say ""
  say "  GRIDLINK_TOKEN=<token> \\"
  say "  GRIDLINK_COORDINATOR=<host:50051> \\"
  say "  ${BIN_NAME}"
  say ""
  say "The first run compiles GPU shaders and takes about 10 seconds."
}

main "$@"
