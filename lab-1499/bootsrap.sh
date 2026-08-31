#!/usr/bin/env bash
# LAB-1499 — get this VM ready. The first thing you run.
#
#     curl -L ibm.biz/txc26-1499-bootstrap | bash -
#
# It fetches the lab, installs uv, creates the Python virtual environment, and
# leaves you ready to fill in your .env file. It is safe to run again: a second
# run updates the checkout and leaves an existing .env alone.
#
# It runs under a pipe, so stdin is the script itself and a plain `read` would
# swallow the rest of it. The one question it can ask — "this does not look
# like a lab VM, carry on?" — it asks on /dev/tty instead. Everything else you
# might want to choose is an environment variable passed before `bash -`:
#
#     curl -L ibm.biz/txc26-1499-bootstrap | LAB_DIR=~/somewhere bash -
#     curl -L ibm.biz/txc26-1499-bootstrap | LAB_REF=some-branch bash -
#     curl -L ibm.biz/txc26-1499-bootstrap | LAB_FORCE=1 bash -   # not a lab VM

set -euo pipefail

REPO_URL="${LAB_REPO:-https://github.com/IBM/txc2026-docling.git}"
REPO_SLUG="${LAB_REPO_SLUG:-IBM/txc2026-docling}"
REPO_REF="${LAB_REF:-main}"
LAB_SUBDIR="lab-1499"
LAB_DIR="${LAB_DIR:-$HOME/txc2026-docling}"

step() { printf '\n\033[1m▸ %s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
warn() { printf '  \033[33m! %s\033[0m\n' "$*" >&2; }
die()  { printf '\n\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

# --- 0. is this a lab VM? ----------------------------------------------------
LAB_VM_USER="itzuser"
LAB_VM_HOST_PREFIX="itzvsi"

os_id() { [[ -r /etc/os-release ]] && ( . /etc/os-release && printf '%s' "${ID:-}" ); }

confirm() {
  local reply=""
  printf '  \033[33m%s\033[0m [y/N] ' "$1" 2>/dev/null >/dev/tty || return 1
  read -r reply 2>/dev/null </dev/tty || return 1
  [[ "$reply" == [Yy] || "$reply" == [Yy][Ee][Ss] ]]
}

check_lab_vm() {
  local user host osid wrong=() w
  user="$(id -un)"
  host="${HOSTNAME:-$(uname -n)}"; host="${host%%.*}"
  osid="$(os_id || true)"

  [[ "$user" == "$LAB_VM_USER" ]] || wrong+=("the user is '$user', not '$LAB_VM_USER'")
  [[ "$host" == "$LAB_VM_HOST_PREFIX"* ]] || wrong+=("the hostname is '$host', which does not start with '$LAB_VM_HOST_PREFIX'")
  [[ "$osid" == "rhel" ]] || wrong+=("the OS is '${osid:-unknown}', not 'rhel'")

  if [[ ${#wrong[@]} -eq 0 ]]; then
    info "lab VM $host, as expected"
    return 0
  fi

  warn "this does not look like a LAB-1499 VM:"
  for w in "${wrong[@]}"; do warn "  - $w"; done
  warn "it may edit your shell startup files — run on a lab VM only."

  if [[ -n "${LAB_FORCE:-}" ]]; then
    info "LAB_FORCE is set — continuing anyway"
    return 0
  fi
  if confirm "Continue anyway?"; then
    info "continuing"
    return 0
  fi
  die "stopped. If you did mean it:
  curl -L ibm.biz/txc26-1499-bootstrap | LAB_FORCE=1 bash -"
}

# --- 1. git, if it is missing and we can get it ------------------------------
have_git() { command -v git >/dev/null 2>&1; }

install_git() {
  have_git && { info "git is already installed"; return 0; }
  local sudo=""
  if [[ "$(id -u)" != "0" ]]; then
    sudo -n true 2>/dev/null && sudo="sudo -n" || {
      warn "git is missing and this account cannot install it without a password."
      return 1
    }
  fi
  info "installing git ..."
  if command -v dnf >/dev/null 2>&1; then
    $sudo dnf install -y -q git >/dev/null 2>&1 || return 1
  elif command -v apt-get >/dev/null 2>&1; then
    $sudo apt-get update -qq >/dev/null 2>&1 && $sudo apt-get install -y -qq git >/dev/null 2>&1 || return 1
  else
    return 1
  fi
  have_git
}

# --- 2. uv -------------------------------------------------------------------
install_uv() {
  if command -v uv >/dev/null 2>&1; then
    info "uv $(uv --version | awk '{print $2}') is already installed"
    return 0
  fi
  info "installing uv ..."
  curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1 \
    || die "could not install uv — check the VM's network access to astral.sh"
  export PATH="$HOME/.local/bin:$PATH"
  command -v uv >/dev/null 2>&1 || die "uv installed but is not on PATH"
}

ensure_path() {
  export PATH="$HOME/.local/bin:$PATH"
  local rc="$HOME/.bashrc" line='export PATH="$HOME/.local/bin:$PATH"'
  grep -qF "$line" "$rc" 2>/dev/null && return 0
  printf '\n# added by the LAB-1499 bootstrap\n%s\n' "$line" >> "$rc"
  info "added ~/.local/bin to your PATH in ~/.bashrc"
}

# --- 3. the lab itself -------------------------------------------------------
fetch_with_git() {
  if [[ -d "$LAB_DIR/.git" ]]; then
    info "updating the existing checkout in $LAB_DIR"
    git -C "$LAB_DIR" fetch --quiet origin "$REPO_REF"
    git -C "$LAB_DIR" checkout --quiet "$REPO_REF"
    git -C "$LAB_DIR" pull --quiet --ff-only origin "$REPO_REF" || \
      warn "could not fast-forward — your checkout has local changes, which is fine"
    return 0
  fi
  info "cloning $REPO_SLUG ($LAB_SUBDIR only)"
  git clone --quiet --filter=blob:none --sparse --branch "$REPO_REF" "$REPO_URL" "$LAB_DIR"
  git -C "$LAB_DIR" sparse-checkout set "$LAB_SUBDIR"
}

fetch_with_curl() {
  [[ -d "$LAB_DIR/$LAB_SUBDIR" ]] && die \
    "$LAB_DIR/$LAB_SUBDIR already exists and git is not available to update it.
  Remove it and run this again, or install git first."
  info "downloading $REPO_SLUG as an archive (no git available)"
  local staging
  staging="$(mktemp -d)"
  curl -fsSL "https://codeload.github.com/$REPO_SLUG/tar.gz/refs/heads/$REPO_REF" \
    | tar xz -C "$staging" --strip-components=1 \
    || die "could not download the lab — check the VM's network access to github.com"
  [[ -d "$staging/$LAB_SUBDIR" ]] || die "the archive has no $LAB_SUBDIR directory in it"
  mkdir -p "$LAB_DIR"
  mv "$staging/$LAB_SUBDIR" "$LAB_DIR/$LAB_SUBDIR"
  rm -rf "$staging"
  warn "this is a download, not a clone: 'git pull' will not work here."
}

# --- 4. Python virtual environment -------------------------------------------
create_venv() {
  local lab="$1"
  if [[ -d "$lab/.venv" ]]; then
    info "virtual environment already exists — left untouched"
    return 0
  fi
  info "creating virtual environment with Python 3.14 ..."
  uv venv --python 3.14 --quiet "$lab/.venv" \
    || die "uv venv failed — check that uv can download Python (network access required)"
  info "installing docling-client ..."
  uv pip install --quiet --python "$lab/.venv/bin/python" "docling-client" \
    || die "uv pip install failed"
}

# --- go ----------------------------------------------------------------------
printf '\n\033[1mLAB-1499 — Hands-On with Docling for IBM watsonx\033[0m\n'

step "This machine"
check_lab_vm

step "System packages"
if install_git; then :; else warn "continuing without git"; fi

step "uv"
install_uv
ensure_path

step "The lab"
if have_git; then fetch_with_git; else fetch_with_curl; fi
LAB="$LAB_DIR/$LAB_SUBDIR"
[[ -d "$LAB" ]] || die "expected $LAB after fetching, and it is not there"
cd "$LAB"

step "Python virtual environment"
create_venv "$LAB"

step "Your configuration"
# Never overwritten — a student who re-runs this after filling in their key
# must not lose it.
if [[ -f .env ]]; then
  info ".env is already here — left untouched"
elif [[ -f .env.example ]]; then
  cp .env.example .env
  info ".env created from .env.example — add your Service URL and API key"
fi

printf '\n\033[1m✓ Ready.\033[0m\n\n'
cat <<NEXT
  Your lab is in:   $LAB

  Next steps:
  1  cd $LAB
  2  open .env and fill in DOCLING_SERVICE_URL and DOCLING_SERVICE_API_KEY
  3  source .venv/bin/activate
  4  python scripts/test_connection.py

  The lab guide has the rest (README.md). If uv is not found in a new
  terminal, run:  export PATH="\$HOME/.local/bin:\$PATH"

NEXT
