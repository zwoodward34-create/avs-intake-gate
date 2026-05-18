#!/bin/bash
cd "$(dirname "$0")"
git push origin main
echo ""
echo "Pushed! Render will deploy in ~1 min."
echo "Watch at: https://dashboard.render.com"
read -p "Press Enter to close..."
