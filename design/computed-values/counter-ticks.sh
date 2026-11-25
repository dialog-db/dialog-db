#!/usr/bin/env bash
# Correctness under concurrency, for a counter kept as its increments: a
# copy of tonk's research/concurrent-cli-writes/harness/counter.sh. N
# writers each fire one increment at once; the count should end at N.
#
#   counter-ticks.sh <variant> <N> <label> <out>
#   variant: queue  - everyone on main, TONK_EVAL_POLICY=queue
#            branch - a branch per writer, queued reconciler, no rules on pull
#            branch+rules - the same, with rules run after every pull
# Ticks model: schema from $COUNTER_SCHEMA; each increment carries a
# distinct time, and the count is queried from the derived tally.
# Needs tonk's experiment build (its harness/eval-policies.patch).
set -euo pipefail
TONK="${TONK:?}"; VARIANT="$1"; N="$2"; DIR="$4/$3"
rm -rf "$DIR"; mkdir -p "$DIR"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export HOME="$WORK/home" DO_NOT_TRACK=1 TONK_NO_UPDATE_CHECK=1 TONK_NO_SYNC=1 \
  TONK_SPACES_STATE="$WORK/state" TONK_TELEMETRY_STATE="$WORK/state" \
  TONK_UPDATE_STATE="$WORK/state" TONK_SPACE=bench
mkdir -p "$HOME" "$TONK_SPACES_STATE"
"$TONK" space new bench --site "$WORK/site" > /dev/null 2>&1
cp "${COUNTER_SCHEMA:?}" "$WORK/schema.tonk"
"$TONK" eval "$WORK/schema.tonk" > "$DIR/setup.txt" 2>&1
COUNTER=$(grep -o 'the-counter: [^ ]*' "$DIR/setup.txt" | awk '{print $2}')
[ -n "$COUNTER" ] || { echo "no counter" >&2; cat "$DIR/setup.txt" >&2; exit 1; }

case "$VARIANT" in
  queue) export TONK_EVAL_POLICY=queue ;;
  branch|branch+rules)
    export TONK_RECONCILE_QUEUE="$WORK/queue"
    [ "$VARIANT" = branch+rules ] && export TONK_INDUCE_ON_PULL=1
    TONK_RECONCILER=1 TONK_RECONCILE_STOP="$WORK/stop" "$TONK" eval -c x 2> "$DIR/reconcile.log" > /dev/null &
    RECONCILER=$! ;;
esac

GO="$WORK/go"; mkfifo "$GO"
for i in $(seq 1 "$N"); do
  ( read -r _ < "$GO" || true; set +e
    if [ "$VARIANT" = queue ]; then B=; else B="w$i"; fi
    TONK_BRANCH="$B" "$TONK" eval -c "increment!:
  subject: $COUNTER
  time: $i.$(date +%N)" > "$DIR/w$i.out" 2> "$DIR/w$i.err"; echo $? > "$DIR/w$i.code" ) &
  PIDS+=($!)
done
sleep 0.2; for i in $(seq 1 "$N"); do echo go; done > "$GO"
wait "${PIDS[@]}"
if [ -n "${RECONCILER:-}" ]; then touch "$WORK/stop"; wait "$RECONCILER" || true; fi
unset TONK_BRANCH
"$TONK" eval -c "tally:
  this: $COUNTER
  count: ?count" > "$DIR/final.txt" 2>&1 || true
count=$(grep -o 'count: [0-9]*' "$DIR/final.txt" | tail -1 | awk '{print $2}')
failed=$(cat "$DIR"/w*.code | grep -vc '^0$' || true)
echo "variant=$VARIANT N=$N failed=$failed count=$count expected=$N" | tee "$DIR/summary"
