#!/usr/bin/env bash
# LAB-2775 — your Flink pipeline: deploy it, watch it, change it, remove it.
#
#   ./pipeline.sh deploy simple    # chunks -> prepare -> embed -> sink
#   ./pipeline.sh deploy full      # ...plus policy, PII guard, dedup, quarantine
#   ./pipeline.sh status           # which pipeline is deployed, and what it is doing
#   ./pipeline.sh policy '{"pii_redact": true}'    # change the running job
#   ./pipeline.sh inspect          # the dashboard, pointed at your topics
#   ./pipeline.sh delete           # remove it
#
# `deploy` is a switch, not an add: exactly one pipeline is deployed at a time,
# so it deletes whatever is there — the other variant, or an earlier copy of
# this one — and waits for its pods to go before creating the new application.
# That is deliberate rather than tidy-minded. A CMF FlinkApplication is
# application mode (one CR, one JobManager, one job), the cluster is packed
# close to its memory ceiling, and two of these at once is two TaskManagers
# holding memory for one student.

. "$(dirname "$0")/scripts/lib.sh"

action="${1:-status}"
variant="${2:-}"

# --- CMF ---------------------------------------------------------------------
# POST to the *collection* upserts, keeping creationTimestamp. PUT and PATCH on
# the item are "not supported" on this CMF: they answer 500, not 405.
#
# -k: the CMF ingress presents a certificate for an internal name, so curl
# cannot verify it from here. The credential still travels over TLS.
cmf() { curl -sk -u "$CMF_AUTH" "$@"; }
cmf_api() { echo "$CMF_URL/api/v1/environments/$CMF_ENVIRONMENT/applications"; }

app_exists() { [[ "$(cmf -o /dev/null -w '%{http_code}' "$(cmf_api)/$1")" == "200" ]]; }

app_field() { # app_field NAME state|module
  cmf "$(cmf_api)/$1" | "${UV_RUN[@]}" python -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
if "spec" not in d:
    raise SystemExit(0)
if sys.argv[1] == "state":
    print(d.get("status", {}).get("jobStatus", {}).get("state", ""))
else:
    a = d.get("spec", {}).get("job", {}).get("args", [])
    print(a[a.index("-pym") + 1] if "-pym" in a else "")
' "$2" 2>/dev/null
}

# Every application name that could belong to you, including the un-suffixed one
# from before the two variants existed: a switch has to sweep that too, or it
# sits alongside the new application holding a second TaskManager.
my_apps() { echo "ws-${STUDENT_ID}-simple"; echo "ws-${STUDENT_ID}-full"; echo "ws-${STUDENT_ID}"; }

# CMF answers the DELETE before the operator has torn the pods down. On a
# cluster packed to the edge that matters: the replacement must not be admitted
# while the old TaskManager still holds its memory.
wait_gone() { # wait_gone NAME [timeout_s]
  local name="$1" deadline=$(( SECONDS + ${2:-120} ))
  while app_exists "$name"; do
    (( SECONDS < deadline )) || return 1
    sleep 3
  done
}

remove_everything_deployed() {
  local name
  for name in $(my_apps); do
    app_exists "$name" || continue
    lab_say "▸ removing $name"
    cmf -X DELETE "$(cmf_api)/$name" -o /dev/null
    wait_gone "$name" || {
      echo "  $name is still there after two minutes — ask an instructor before retrying" >&2
      return 1
    }
    lab_say "  gone"
  done
}

case "$action" in
  deploy)
    lab_check_variant "$variant"
    lab_load "$variant"

    # The sink names a list, and when it names opensearch the job needs a host
    # and an index that exists. Refusing here beats a job that starts, consumes
    # the whole topic and then fails on its first write.
    if [[ "$SINK_TYPE" == *opensearch* && -z "${OPENSEARCH_HOSTS:-}" ]]; then
      echo "SINK_TYPE is '$SINK_TYPE' but opensearch.hosts is empty in lab.yaml." >&2
      echo "Fill it in, or deploy with SINK_TYPE=kafka to skip the index for now." >&2
      exit 2
    fi

    remove_everything_deployed

    lab_say "▸ deploying $APP_NAME ($JOB_MODULE)"
    descriptor="$("${UV_RUN[@]}" python -m labtools.config render \
                   flink/application-workshop.json.tmpl --variant "$variant")"
    code="$(cmf -H 'Content-Type: application/json' -X POST "$(cmf_api)" \
                -d "$descriptor" -o /dev/null -w '%{http_code}')"
    case "$code" in
      2*) lab_say "  accepted ($code)" ;;
      *)  echo "  CMF refused the application (HTTP $code)" >&2; exit 1 ;;
    esac

    lab_say ""
    lab_say "  reads   $KAFKA_CHUNKS_TOPIC"
    lab_say "  writes  $KAFKA_OUTPUT_TOPIC"
    [[ "$SINK_TYPE" == *opensearch* ]] && \
      lab_say "  indexes $OPENSEARCH_INDEX (ask it questions in the inspector's Ask tab)"
    if [[ "$variant" == "full" ]]; then
      lab_say "  audit   $KAFKA_QUARANTINE_TOPIC (PII originals) · $KAFKA_REJECTED_TOPIC (dropped)"
      lab_say "  policy  $KAFKA_POLICY_TOPIC — publish a rule there and the running job picks it up"
    fi
    lab_say ""
    lab_say "  It starts from the beginning of your chunk topic, so everything you have"
    lab_say "  uploaded so far is processed again by this pipeline."
    lab_say ""
    lab_say "  watch it:   ./pipeline.sh status     (or: ./pipeline.sh inspect)"
    [[ -n "${COS_BUCKET:-}" ]] && lab_say "  upload to:  $COS_BUCKET"
    ;;

  status)
    lab_load "${variant:-simple}"
    found=0
    for name in $(my_apps); do
      app_exists "$name" || continue
      found=$(( found + 1 ))
      module="$(app_field "$name" module)"
      # The module a job was submitted with is the authority on which pipeline
      # an application is running; the name only says which it was meant to be.
      case "$module" in
        pipeline.full_job)   which=full ;;
        pipeline.enrich_job) which=simple ;;
        *)                   which="${name##*-}" ;;
      esac
      printf '%-16s %-8s %-10s %s\n' "$name" "$which" "$(app_field "$name" state)" "$module"
    done
    if (( found == 0 )); then
      lab_say "no pipeline deployed for ws-${STUDENT_ID} — start with: ./pipeline.sh deploy simple"
    elif (( found > 1 )); then
      lab_say ""
      lab_say "Two pipelines are deployed at once, which is one more than your share of the"
      lab_say "cluster. Re-run './pipeline.sh deploy <simple|full>' to leave just one."
    fi
    ;;

  policy)
    # The one change that needs no deploy at all: the guard keeps the latest
    # rule in broadcast state, so this takes effect on the next record.
    rule="${2:-}"
    [[ -n "$rule" ]] || {
      echo "usage: $0 policy '{\"pii_redact\": true, \"drop_low_quality\": true, \"min_chars\": 200}'" >&2
      echo "keys: pii_detectors, pii_redact, min_chars, drop_low_quality, blocked_doc_ids" >&2
      exit 2
    }
    lab_load full
    lab_say "▸ $KAFKA_POLICY_TOPIC <- $rule"
    "${UV_RUN[@]}" python scripts/kafka_tool.py produce "$KAFKA_POLICY_TOPIC" "$rule"
    lab_say "  the running job picks it up on its next record — nothing is redeployed"
    ;;

  delete)
    lab_load "${variant:-simple}"
    remove_everything_deployed
    ;;

  inspect)
    # The dashboard reads the environment, which lab_load has just filled with
    # exactly this student's names — the five topics, the application and the
    # bucket its "Upload a document" button opens.
    lab_load "${variant:-simple}"
    lab_say "inspecting ws-${STUDENT_ID}: $KAFKA_CHUNKS_TOPIC"
    if [[ -z "${COS_BUCKET_CRN:-}" ]]; then
      lab_say "  (no bucket CRN in lab.yaml — the upload button will not link anywhere)"
    fi
    # Its own environment, on purpose: this needs streamlit, nothing else does,
    # and a Streamlit upgrade must never move something the pipeline depends on.
    exec env UV_PROJECT_ENVIRONMENT="$LAB_ROOT/.venv-dashboard" \
      uv run --frozen --no-default-groups --group dashboard \
      streamlit run src/inspector/app.py --browser.gatherUsageStats false "${@:2}"
    ;;

  *) sed -n '2,17p' "$0" >&2; exit 2 ;;
esac
