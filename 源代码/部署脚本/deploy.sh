#!/bin/bash
set -e
whoami > /tmp/whoami_result.txt
hostname >> /tmp/whoami_result.txt
apt update -y >> /tmp/deploy.log 2>&1
apt install -y python3-pip wget >> /tmp/deploy.log 2>&1
pip3 install fastapi uvicorn >> /tmp/deploy.log 2>&1
wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /usr/local/bin/cloudflared >> /tmp/deploy.log 2>&1
chmod +x /usr/local/bin/cloudflared
mkdir -p /opt/agent
cat > /opt/agent/main.py << 'PYEOF'
from fastapi import FastAPI
import uvicorn
app = FastAPI()
@app.get("/")
def root():
    return {"status": "online", "agent": "leader"}
@app.get("/health")
def health():
    return {"status": "online", "agent": "leader", "tasks": 0}
@app.post("/rpc")
async def rpc():
    return {"jsonrpc": "2.0", "result": {"status": "processed"}, "id": 1}
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=18080)
PYEOF
pkill -f cloudflared 2>/dev/null || true
pkill -f "main.py" 2>/dev/null || true
nohup python3 /opt/agent/main.py > /var/log/agent.log 2>&1 &
sleep 2
nohup /usr/local/bin/cloudflared tunnel --protocol http2 --url http://localhost:18080 > /var/log/cf.log 2>&1 &
sleep 8
grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' /var/log/cf.log | head -1 > /tmp/tunnel_url.txt
echo "DEPLOY_DONE" >> /tmp/whoami_result.txt
