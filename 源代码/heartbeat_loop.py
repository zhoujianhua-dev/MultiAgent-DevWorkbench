"""最小心跳上报：让6个智能体在梧桐平台显示alive。
往 wt.ioa.pub:19092 的 amp.heartbeat topic 发 {"logType":"heartbeat","aic":...}
"""
import json, time, sys
from kafka import KafkaProducer

BROKER = "wt.ioa.pub:19092"
TOPIC = "amp.heartbeat"

AGENTS = {
    "leader":        "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB",
    "requirement":   "1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9",
    "architecture":  "1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U",
    "code_review":   "1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y",
    "test_design":   "1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7",
    "documentation": "1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N",
}

def main():
    print(f"[heartbeat] connecting {BROKER} ...", flush=True)
    prod = KafkaProducer(
        bootstrap_servers=BROKER,
        security_protocol="PLAINTEXT",
        api_version=(3, 7, 0),
        retries=3,
    )
    print("[heartbeat] connected, sending heartbeats every 25s", flush=True)
    while True:
        for name, aic in AGENTS.items():
            payload = json.dumps({"logType": "heartbeat", "aic": aic}, ensure_ascii=False).encode("utf-8")
            try:
                fut = prod.send(TOPIC, value=payload, key=aic.encode("utf-8"))
                fut.get(timeout=5)
                print(f"  [{time.strftime('%H:%M:%S')}] {name:14s} OK", flush=True)
            except Exception as e:
                print(f"  [{time.strftime('%H:%M:%S')}] {name:14s} FAIL {e}", flush=True)
        prod.flush()
        time.sleep(25)

if __name__ == "__main__":
    main()
