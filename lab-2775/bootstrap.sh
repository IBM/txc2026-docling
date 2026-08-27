#!/usr/bin/env bash
# LAB-2775 — get this VM ready. The first thing you run.
#
#     curl -L ibm.biz/txc26-2775-bootstrap | bash -
#
# It fetches the lab, installs uv, builds the two Python environments and
# leaves you a lab.yaml to fill in. It is safe to run again: a second run
# updates the checkout and leaves your lab.yaml alone.
#
# It runs under a pipe, so it never asks a question — stdin is the script
# itself, and a `read` here would swallow the rest of it. Anything you might
# want to choose is an environment variable instead:
#
#     LAB_DIR=~/somewhere   curl -L ibm.biz/txc26-2775-bootstrap | bash -
#     LAB_REF=some-branch   ...
#
# The VM is a vanilla RHEL 9 with a desktop. Nearly everything needed is
# already there — curl, tar and coreutils are in @core, Firefox comes with the
# desktop — so the only package this installs is git, and there is a fallback
# for when even that is not available.

set -euo pipefail

REPO_URL="${LAB_REPO:-https://github.com/IBM/txc2026-docling.git}"
REPO_SLUG="${LAB_REPO_SLUG:-IBM/txc2026-docling}"
REPO_REF="${LAB_REF:-main}"
LAB_SUBDIR="lab-2775"
LAB_DIR="${LAB_DIR:-$HOME/txc2026-docling}"

step() { printf '\n\033[1m▸ %s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
warn() { printf '  \033[33m! %s\033[0m\n' "$*" >&2; }
die()  { printf '\n\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

# --- 1. git, if it is missing and we can get it ------------------------------
# RHEL 9's "Server with GUI" does not include git, and a lab VM may not be
# subscribed to any repository — in which case dnf fails and the tarball path
# below is what saves the class. Nothing else is installed: the lab needs no
# gettext (the descriptor is rendered in Python), no jq, and no system Python
# beyond what uv brings.
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
# It brings its own Python. RHEL 9 ships 3.9 and this lab needs 3.12 (see
# .python-version), so nothing here touches the system interpreter.
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

# RHEL 9's /etc/profile already puts ~/.local/bin on PATH for login shells, so
# this is only for the case where it does not.
ensure_path() {
  export PATH="$HOME/.local/bin:$PATH"
  local rc="$HOME/.bashrc" line='export PATH="$HOME/.local/bin:$PATH"'
  grep -qF "$line" "$rc" 2>/dev/null && return 0
  printf '\n# added by the LAB-2775 bootstrap\n%s\n' "$line" >> "$rc"
  info "added ~/.local/bin to your PATH in ~/.bashrc"
}

# --- 3. the lab itself -------------------------------------------------------
# A sparse checkout of one directory: the repository holds several labs and
# this is one of them. It is a real clone, so a fix published during the class
# reaches you with `git pull`.
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

# No git, no subscription, no problem: the same directory as a tarball. What is
# lost is `git pull`, so this path says so rather than failing quietly.
fetch_with_curl() {
  [[ -d "$LAB_DIR/$LAB_SUBDIR" ]] && die \
    "$LAB_DIR/$LAB_SUBDIR already exists and git is not available to update it.
  Remove it and run this again, or install git first."
  info "downloading $REPO_SLUG as an archive (no git available)"
  local staging
  staging="$(mktemp -d)"
  # The whole archive, then one directory out of it. Extracting the subdirectory
  # directly would need a glob, and the flag that enables one differs between
  # GNU tar (RHEL, --wildcards) and BSD tar (no such flag) — not worth being
  # clever about on the path that only runs when everything else has failed.
  curl -fsSL "https://codeload.github.com/$REPO_SLUG/tar.gz/refs/heads/$REPO_REF" \
    | tar xz -C "$staging" --strip-components=1 \
    || die "could not download the lab — check the VM's network access to github.com"
  [[ -d "$staging/$LAB_SUBDIR" ]] || die "the archive has no $LAB_SUBDIR directory in it"
  mkdir -p "$LAB_DIR"
  mv "$staging/$LAB_SUBDIR" "$LAB_DIR/$LAB_SUBDIR"
  rm -rf "$staging"
  warn "this is a download, not a clone: 'git pull' will not work here."
}

# --- go ----------------------------------------------------------------------
printf '\n\033[1mLAB-2775 — From Bucket to RAG\033[0m\n'

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

step "Python environments"
# Both of them, now rather than on first use: the dashboard's is a separate
# environment on purpose (it needs streamlit and nothing else does, and a
# Streamlit upgrade must never move something the pipeline depends on), and
# building it here is what keeps './pipeline.sh inspect' from stalling for a
# minute in the middle of the lab.
info "the lab's own environment ..."
uv sync --frozen --quiet
info "the dashboard's ..."
UV_PROJECT_ENVIRONMENT="$LAB/.venv-dashboard" \
  uv sync --frozen --quiet --no-default-groups --group dashboard

step "Your configuration"
# Never overwritten. A student who re-runs this after filling half of it in
# must not lose that.
if [[ -f lab.yaml ]]; then
  info "lab.yaml is already here — left untouched"
elif [[ -f "$HOME/lab.yaml" ]]; then
  # Pre-seeded with this class's values when the VM was handed out.
  cp "$HOME/lab.yaml" lab.yaml
  info "lab.yaml taken from the one prepared on this VM"
elif [[ -n "${LAB_CONFIG:-}" && -f "$LAB_CONFIG" ]]; then
  cp "$LAB_CONFIG" lab.yaml
  info "lab.yaml taken from $LAB_CONFIG"
else
  cp lab.yaml.example lab.yaml
  info "lab.yaml created from the example — it needs filling in"
fi
# The broker CA, if the VM was handed one.
if [[ ! -f certs/kafka-ca.crt && -f "$HOME/kafka-ca.crt" ]]; then
  cp "$HOME/kafka-ca.crt" certs/kafka-ca.crt
  info "broker CA taken from the one prepared on this VM"
fi

printf '\n\033[1m✓ Ready.\033[0m\n\n'
cat <<NEXT
  Your lab is in:   $LAB

  1  cd $LAB
  2  open lab.yaml and fill in what is marked YOU
  3  ./setup.sh check

  The lab guide has the rest. If this is a new terminal and the commands are
  not found, run:  export PATH="\$HOME/.local/bin:\$PATH"

NEXT
