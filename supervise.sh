#!/bin/zsh
# One terminal: caffeinate + live WATCH on this tty. Crash → 15s restart.
# Clean stop (Ctrl+C, SIGTERM, daily loss) exits 0 and stays down.
cd /Users/adamcarrington/Desktop/flow || exit 1
if pgrep -f '/opt/homebrew/bin/python3 -u flow.py run' >/dev/null \
    || pgrep -f '/opt/homebrew/bin/python3 flow.py run' >/dev/null; then
  echo "flow.py is already running. One live process only. Watch that terminal." >&2
  exit 1
fi
eval "$(grep '^export KALSHI' "$HOME/.zshrc")"
echo "LIVE  caffeinate -dismu  flow.py  (Ctrl+C stops; daily-loss stays down)"
while true; do
  /usr/bin/caffeinate -dismu /opt/homebrew/bin/python3 -u flow.py run
  code=$?
  echo "$(date '+%Y-%m-%d %H:%M:%S') supervisor: bot exited with code $code" >> supervisor.log
  [ $code -eq 0 ] && break
  echo "crash exit $code — restarting in 15s"
  sleep 15
done
