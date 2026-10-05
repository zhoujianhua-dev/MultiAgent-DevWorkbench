#!/bin/bash
# ============================================================
# 多智能体协同软件开发工作台 —— 一键部署引导脚本（公开交付版）
# ------------------------------------------------------------
# 【安全说明】
#   原始部署脚本以 base64 内嵌方式携带了 6 组 mTLS 证书（含身份私钥）
#   以及全部源码。为避免参赛身份私钥随公开材料外泄，本交付版已
#   移除全部内嵌证书、私钥与源码，改为从交付包拷贝。
#   证书由参赛方从竞赛平台获取后放置于 certs/<角色>/ 下。
# ============================================================
set -e
mkdir -p /opt/agent/certs /opt/agent/systemd
cd /opt/agent

# ---- 1) 系统依赖 ----
timeout 150 apt-get update -y >/dev/null 2>&1 || true
timeout 200 apt-get install -y python3-venv python3-pip curl >/dev/null 2>&1 || true

# ---- 2) Python 虚拟环境与依赖 ----
if python3 -m venv /opt/agent/venv >/dev/null 2>&1; then
  PIP="/opt/agent/venv/bin/pip install"
else
  PIP="pip3 install --break-system-packages"
fi
timeout 300 $PIP -q -i https://pypi.tuna.tsinghua.edu.cn/simple fastapi uvicorn pika kafka-python-ng \
  || timeout 300 $PIP -q fastapi uvicorn pika kafka-python

# ---- 3) 校验证书与源码是否就位（本脚本不再内嵌任何证书/私钥）----
MISSING=""
for r in leader requirement architecture code_review test_design documentation; do
  for f in agent-cert.pem agent-key.pem trust-bundle.pem; do
    [ -f "/opt/agent/certs/$r/$f" ] || MISSING="$MISSING certs/$r/$f"
  done
done
for s in main_mq.py partners.py heartbeat_loop.py; do
  [ -f "/opt/agent/$s" ] || MISSING="$MISSING $s"
done
if [ -n "$MISSING" ]; then
  echo "[!] 部署前置文件未就位:$MISSING"
  echo "    请先把 certs/<角色>/（agent-cert.pem / agent-key.pem / trust-bundle.pem）"
  echo "    与 main_mq.py / partners.py / heartbeat_loop.py 上传至 /opt/agent/ 后再执行。"
  exit 1
fi

# ---- 4) cloudflared（可选 HTTP 隧道）----
CF=/usr/local/bin/cloudflared
if [ ! -x $CF ] || [ "$(stat -c%s $CF 2>/dev/null || echo 0)" -lt 10000000 ]; then
  for u in \
    "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64" \
    "https://ghproxy.net/https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64"; do
    timeout 150 curl -fsSL "$u" -o $CF 2>/dev/null || continue
    [ "$(stat -c%s $CF 2>/dev/null || echo 0)" -gt 10000000 ] && break
    rm -f $CF
  done
  chmod +x $CF 2>/dev/null || true
fi

# ---- 5) 注册并启动 systemd 服务（单元文件随交付包提供，不含证书内容）----
[ -d ./systemd ] && cp -f ./systemd/*.service /etc/systemd/system/ 2>/dev/null || true
systemctl daemon-reload
systemctl enable --now aic-main aic-partners aic-heartbeat aic-tunnel aic-urlwatch 2>/dev/null || true

# ---- 6) 自检 ----
systemctl --no-pager --full status aic-main aic-partners aic-heartbeat 2>/dev/null | head -30 || true
echo "[✓] 部署流程结束。检查 6 个智能体是否全部 online："
echo "    journalctl -u aic-partners -n 50 --no-pager"
