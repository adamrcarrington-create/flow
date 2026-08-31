#!/bin/zsh
# Start the momentum bot
cd /Users/adamcarrington/Desktop/flow || exit 1
eval "$(grep '^export KALSHI' "$HOME/.zshrc")"
exec /opt/homebrew/bin/python3 -u flow.py run
