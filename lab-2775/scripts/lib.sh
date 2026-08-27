#!/usr/bin/env bash
# Shared by ./setup.sh and ./pipeline.sh — sourced, never executed.
#
# Everything here is about *one* student, the one whose id is in lab.yaml. The
# instructor's fan-out over the whole class lives outside this directory and
# drives the same naming through the same module (`labtools.config --student`)
# rather than restating it.

set -euo pipefail

LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$LAB_ROOT"

# `uv run` is how everything Python here starts: it resolves the project's own
# environment, so there is no venv to activate, no PY to export and no PATH to
# get wrong in a second terminal. --frozen makes it use uv.lock as it stands
# instead of re-resolving, which is both faster and the right answer on a lab
# VM with a slow network.
UV_RUN=(uv run --frozen)

lab_require_uv() {
  command -v uv >/dev/null || {
    echo "uv is not installed. Run the bootstrap again:" >&2
    echo "    curl -L ibm.biz/txc26-2775-bootstrap | bash -" >&2
    exit 127
  }
}

lab_require_config() {
  [[ -f lab.yaml ]] || {
    echo "There is no lab.yaml here yet. Start from the example:" >&2
    echo "    cp lab.yaml.example lab.yaml" >&2
    echo "then fill in your student id and the values from Phase 1 and 2." >&2
    exit 2
  }
}

# Load the whole configuration into this shell. Every name below — the topics,
# the application, the index — comes from here, so nothing in these scripts
# derives a name of its own.
lab_load() { # lab_load [simple|full]
  lab_require_uv
  lab_require_config
  local variant="${1:-simple}" exports
  if ! exports="$("${UV_RUN[@]}" python -m labtools.config env --variant "$variant" 2>&1)"; then
    printf '%s\n' "$exports" >&2
    exit 2
  fi
  eval "$exports"
}

lab_check_variant() {
  case "${1:-}" in
    simple|full) return 0 ;;
    *) echo "unknown pipeline '${1:-}' — expected 'simple' or 'full'" >&2; return 2 ;;
  esac
}

# --- output -----------------------------------------------------------------
# Escapes only for a terminal: these blocks get piped into a scratch file often
# enough to matter, and a CRN with an escape code in it is a CRN that does not
# match.
if [[ -t 1 ]]; then
  lab_head() { printf '\n\033[1m%s\033[0m\n' "$*"; }
  lab_dim()  { printf '\033[2m%s\033[0m\n' "$*"; }
else
  lab_head() { printf '\n%s\n' "$*"; }
  lab_dim()  { printf '%s\n' "$*"; }
fi
# Trailing blanks trimmed, for the same reason.
lab_note() { printf '  %-14s %-30s %s\n' "$1" "$2" "${3:-}" | sed 's/[[:space:]]*$//'; }
lab_say()  { printf '%s\n' "$*"; }

# --- what to carry into the next step ---------------------------------------
# The lab is a chain of steps and every one of them ends by handing two or three
# strings to the next: a bucket CRN copied out of the console, a topic name a
# subscription has to match exactly, an index name and the dimension that has to
# agree with the embedding model. Every one of those has been mistyped in a
# rehearsal, and each mistype looks like a healthy green pipeline producing
# nothing. So whatever creates something prints what it is called, in the
# spelling the next step wants to be given.

lab_topic_summary() { # lab_topic_summary [simple|full]
  lab_head "Your Kafka topics"
  lab_note "chunks" "$KAFKA_CHUNKS_TOPIC" "Docling writes here — your subscription's chunkstopic"
  lab_note "enriched" "$KAFKA_OUTPUT_TOPIC" "what your pipeline writes"
  if [[ "${1:-simple}" == "full" ]]; then
    lab_note "policy" "$KAFKA_POLICY_TOPIC" "rules for the running job"
    lab_note "pii" "$KAFKA_QUARANTINE_TOPIC" "originals of anything redacted"
    lab_note "rejected" "$KAFKA_REJECTED_TOPIC" "what the quality gate dropped"
  fi
  lab_note "group" "$KAFKA_CONSUMER_GROUP" "yours alone — a shared one splits partitions"
}

lab_index_summary() {
  lab_head "Your OpenSearch index"
  lab_note "index" "$OPENSEARCH_INDEX" "create it with: ./setup.sh index"
  lab_note "user" "$OPENSEARCH_USERNAME" "yours alone — you may write ${OPENSEARCH_USERNAME}-* and nothing else"
  lab_note "hosts" "${OPENSEARCH_HOSTS:-<not set — see lab.yaml>}"
  lab_note "dimension" "${EMBEDDING_DIMENSION}" "must match $EMBEDDING_MODEL_ID"
  lab_note "sink" "${SINK_TYPE}" "the topic and the index, both"
}

lab_bucket_summary() {
  lab_head "Your COS bucket"
  if [[ -n "${COS_BUCKET_CRN:-}" ]]; then
    lab_note "name" "${COS_BUCKET}" "from the CRN in your lab.yaml"
    lab_note "CRN" "$COS_BUCKET_CRN"
  else
    lab_note "name" "${COS_BUCKET:-<no template configured>}" "a suggestion — create it in the console"
    lab_note "CRN" "<not set>" "paste it into lab.yaml once the bucket exists"
  fi
}

# The console form, field by field, with this student's values already in it.
# Not a CLI command: the bucket lives in an account these scripts are not
# logged in to, the subscription is created in the IBM Cloud UI, and the lab
# needs no ibmcloud CLI at all.
lab_subscription_summary() {
  lab_head "Subscribe your bucket — IBM Cloud console, Code Engine ▸ ${CODE_ENGINE_APP:-docling-trigger} ▸ Event subscriptions"
  lab_dim "  Create an Object Storage subscription with these values:"
  lab_note "Bucket" "${COS_BUCKET:-<your bucket>}"
  lab_note "Event type" "write"
  lab_dim "  ...and these four CloudEvent extensions, which are what tell the shared"
  lab_dim "  trigger app to send YOUR documents to YOUR Docling instance:"
  lab_note "doclingurl" "${DOCLING_SERVICE_URL:-<your Docling service URL>}"
  # Deliberately not printed: it is a credential, and this block gets pasted
  # into scratch files and screenshared.
  lab_note "doclingkey" "<docling.api_key from your lab.yaml>"
  lab_note "chunkstopic" "$KAFKA_CHUNKS_TOPIC"
  lab_note "studentid" "$STUDENT_ID"
}
