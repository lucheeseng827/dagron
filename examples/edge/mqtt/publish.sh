#!/bin/sh
# Gateway stand-in: publish one sensor-window job (01_sensor_window.yaml with
# the parameters filled in) to plant/<line>/jobs over MQTT, QoS 1, not retained.
#
#   ./publish.sh                      # line1, six readings off the clock
#   ./publish.sh line2                # another line → topic plant/line2/jobs
#   ./publish.sh line1 --seq 42       # prepend `seq: 42` (exactly-once demo:
#                                     #   MQTT_POSITION_FIELD=seq on the engine)
#   ./publish.sh line1 --poison       # publish something that is not a spec
#                                     #   (dead-letter demo)
#
# Reads the same MQTT_URL / MQTT_USERNAME / MQTT_PASSWORD the engine reads, so
# one exported environment drives both sides. Needs mosquitto_pub (package
# `mosquitto-clients` on Debian/Ubuntu, `mosquitto` on Homebrew/Alpine).
set -eu

if ! command -v mosquitto_pub >/dev/null 2>&1; then
  echo "publish.sh: mosquitto_pub not found — install mosquitto-clients (Debian/Ubuntu) or mosquitto (brew/apk)" >&2
  exit 127
fi

LINE="line1"
SEQ=""
POISON=0
while [ $# -gt 0 ]; do
  case "$1" in
    --seq)    SEQ="${2:?--seq needs a number}"; shift 2 ;;
    --poison) POISON=1; shift ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *)        LINE="$1"; shift ;;
  esac
done

# mqtt://host[:port] → host + port (TLS via mqtts:// is the engine's concern;
# mosquitto_pub needs --cafile for it, which is out of scope for a demo).
URL="${MQTT_URL:-mqtt://127.0.0.1:1883}"
SCHEME="${URL%%://*}"
HOSTPORT="${URL#*://}"; HOSTPORT="${HOSTPORT%%/*}"
# The engine reads mqtts:// as TLS on 8883 (see docs/STREAMING.md). Follow it
# here rather than quietly publishing plaintext to 1883 against a TLS broker.
case "$SCHEME" in
  mqtt)          DEFAULT_PORT=1883; TLS=0 ;;
  mqtts|ssl)     DEFAULT_PORT=8883; TLS=1 ;;
  *) echo "MQTT_URL scheme '$SCHEME' not supported here — use mqtt:// or mqtts://" >&2; exit 2 ;;
esac
case "$HOSTPORT" in
  *:*) HOST="${HOSTPORT%:*}"; PORT="${HOSTPORT##*:}" ;;
  *)   HOST="$HOSTPORT";      PORT="$DEFAULT_PORT" ;;
esac
TOPIC="plant/$LINE/jobs"

# Built as positional parameters rather than one string: a password with a
# space in it must reach mosquitto_pub as a single argument, and an unquoted
# "$AUTH" would split it into two.
# `if`, not `cmd && cmd`: under `set -e` a trailing `&&` list that evaluates
# false is a non-zero status and would abort the script — which for the TLS
# check would mean every plain mqtt:// run exiting before it published.
set --
if [ "$TLS" -eq 1 ]; then
  # Platform root store, matching what the engine's rustls client trusts.
  set -- --capath "${MQTT_CAPATH:-/etc/ssl/certs}"
fi
if [ -n "${MQTT_USERNAME:-}" ]; then
  set -- "$@" -u "$MQTT_USERNAME"
  if [ -n "${MQTT_PASSWORD:-}" ]; then
    set -- "$@" -P "$MQTT_PASSWORD"
  fi
fi

if [ "$POISON" -eq 1 ]; then
  # Parses as YAML, is not a workflow — the ingest actor dead-letters it.
  printf 'not: [a workflow spec\n' | \
    mosquitto_pub -h "$HOST" -p "$PORT" -t "$TOPIC" -q 1 -s "$@"
  echo "published a poison message to $TOPIC"
  exit 0
fi

# Six readings, 18.0–27.9 °C, a deterministic wobble off the epoch second, so
# two windows a second apart differ but a replay of the same command does not.
now=$(date +%s)
READINGS=""
i=0
while [ $i -lt 6 ]; do
  t=$((now - i * 10))
  r="$((18 + t % 10)).$((t % 9))"
  READINGS="${READINGS:+$READINGS,}$r"
  i=$((i + 1))
done
WINDOW_END=$(date -u +%Y-%m-%dT%H:%M:%SZ)

SPEC=$(dirname "$0")/01_sensor_window.yaml
{
  [ -n "$SEQ" ] && printf 'seq: %s\n' "$SEQ"
  sed -e "s|^  line: .*|  line: \"$LINE\"|" \
      -e "s|^  window_end: .*|  window_end: \"$WINDOW_END\"|" \
      -e "s|^  readings: .*|  readings: \"$READINGS\"|" "$SPEC"
} | mosquitto_pub -h "$HOST" -p "$PORT" -t "$TOPIC" -q 1 -s "$@"

echo "published sensor_window for $LINE ($READINGS) to $TOPIC${SEQ:+ with seq $SEQ}"
