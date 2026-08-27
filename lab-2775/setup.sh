#!/usr/bin/env bash
# LAB-2775 — the things that have to exist before a pipeline can run.
#
#   ./setup.sh check          # is your lab.yaml right, and is everything reachable?
#   ./setup.sh topics         # create your five Kafka topics
#   ./setup.sh index          # create your OpenSearch index
#   ./setup.sh info           # every name the next step needs — creates nothing
#
# Nothing here creates your COS bucket or its event subscription: both are made
# in the IBM Cloud console, in an account this script is not logged in to.
# `info` prints exactly what to type into those two forms.
#
# Your id comes from `student.id` in lab.yaml. Everything else you own is
# derived from it.

. "$(dirname "$0")/scripts/lib.sh"

action="${1:-check}"
variant="${2:-}"

case "$action" in
  check)
    lab_require_uv
    lab_require_config
    lab_head "Your lab.yaml"
    if problems="$("${UV_RUN[@]}" python -m labtools.config check 2>&1)"; then
      lab_say "  ✓ complete"
    else
      lab_say "  ✗ not ready yet:"
      printf '%s\n' "$problems"
      lab_say ""
      lab_say "  Fix those in lab.yaml and run this again."
      exit 1
    fi
    lab_load "${variant:-simple}"
    lab_say ""
    "${UV_RUN[@]}" python scripts/check_reachable.py
    ;;

  topics)
    # `full` by default: the two extra topics cost nothing while empty, and a
    # student who reaches the full pipeline should not discover a missing topic
    # there. The brokers do not auto-create, exactly as Confluent Cloud does not.
    lab_load "${variant:-full}"
    lab_head "Creating your topics"
    for t in $("${UV_RUN[@]}" python -m labtools.config topics --variant "${variant:-full}"); do
      "${UV_RUN[@]}" python scripts/kafka_tool.py create "$t" --partitions 1
    done
    lab_topic_summary "${variant:-full}"
    lab_index_summary
    lab_bucket_summary
    lab_subscription_summary
    lab_say ""
    ;;

  index)
    # Separate from `topics` because it talks to a different system with a
    # different credential — and because a student whose OpenSearch password is
    # not in lab.yaml yet should get that message here, not from a job that has
    # already started and consumed their whole topic.
    lab_load "${variant:-simple}"
    if [[ -z "${OPENSEARCH_PASSWORD:-}" ]]; then
      lab_say "student.opensearch_password is empty in lab.yaml."
      lab_say "Your account is '$OPENSEARCH_USERNAME'; the password is on the classroom handout."
      exit 2
    fi
    lab_head "Creating $OPENSEARCH_INDEX on ${OPENSEARCH_HOSTS:-<opensearch.hosts not set>}"
    "${UV_RUN[@]}" python scripts/setup_opensearch.py
    lab_index_summary
    lab_say ""
    ;;

  info)
    lab_load "${variant:-full}"
    lab_topic_summary "${variant:-full}"
    lab_index_summary
    lab_bucket_summary
    lab_subscription_summary
    lab_say ""
    ;;

  *) sed -n '2,15p' "$0" >&2; exit 2 ;;
esac
