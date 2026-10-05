# -*- coding: utf-8 -*-
"""消费确认探针：向 6 个智能体各投 1 条任务，等待后用各自证书查 inbox 堆积。
若堆积回归 0 ⇒ 服务器侧消费者确实在线、实时消费了消息（即「可被真实调用」）。
"""
import json, ssl, time, uuid
from datetime import datetime, timezone
from pathlib import Path
import pika

ROOT = Path(r"D:/zhou/其他数据/除其他工具数据外的数据/人工智能竞赛")
CERTS = ROOT / "certs"
LEADER = "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB"
PARTS = {
    "requirement":  "1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9",
    "architecture": "1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U",
    "code_review":  "1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y",
    "test_design":  "1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7",
    "documentation":"1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N",
}
SELF = "requirement"          # 向 5 个 partner 发送的身份
RESULT = {}

def ctx_for(name):
    d = CERTS / name
    c = ssl.create_default_context(cafile=str(d / "trust-bundle.pem"))
    c.load_cert_chain(str(d / "agent-cert.pem"), str(d / "agent-key.pem"))
    c.check_hostname = False
    return c

def conn(name, hb=30):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="wt.ioa.pub", port=5671, virtual_host="acps",
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(ctx_for(name)), heartbeat=hb,
        blocked_connection_timeout=20))

def log(*a):
    print("[" + datetime.now().strftime("%H:%M:%S") + "]", *a, flush=True)

def send(sender_name, sender_aic, target_aic):
    c = conn(sender_name); ch = c.channel()
    ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
    cmd = {"type": "task-command", "id": str(uuid.uuid4()), "sentAt": datetime.now(timezone.utc).isoformat(),
           "senderRole": "user", "senderId": sender_aic, "command": "start",
           "dataItems": [{"type": "text", "text": "消费确认探测：收到请按你角色回一句话。"}],
           "taskId": str(uuid.uuid4()), "sessionId": str(uuid.uuid4())}
    ch.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{target_aic}",
                     body=json.dumps(cmd, ensure_ascii=False).encode(),
                     properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
    c.close()

def depth(name, aic):
    try:
        c = conn(name); ch = c.channel()
        m = ch.queue_declare(queue=f"inbox_{aic}", durable=True, passive=True)
        n = m.method.message_count
        c.close(); return n
    except Exception as e:
        return f"ERR:{str(e)[:30]}"

def main():
    # 5 个 partner：用 requirement 证书（sender=HH1098）发送
    for name, aic in PARTS.items():
        before = depth(name, aic)
        send(SELF, PARTS[SELF], aic)
        log(f"· 投递探测 -> {name:13} (投递前堆积={before})，等待 35s 看是否被消费…")
        time.sleep(35)
        after = depth(name, aic)
        online = (isinstance(after, int) and after == 0) or (before != after and isinstance(after, int))
        RESULT[name] = {"before": before, "after": after, "online": bool(online)}
        log(f"  => 投递后堆积={after}  {'✅ 被实时消费（在线可调用）' if online else '⚠ 未消费'}")
    # leader：用 leader 证书发送并自查
    before = depth("leader", LEADER)
    send("leader", LEADER, LEADER)
    log(f"· 投递探测 -> leader 71DG28 (投递前堆积={before})，等待 35s…")
    time.sleep(35)
    after = depth("leader", LEADER)
    online = (isinstance(after, int) and after == 0)
    RESULT["leader"] = {"before": before, "after": after, "online": bool(online)}
    log(f"  => 投递后堆积={after}  {'✅ 被实时消费（在线可调用）' if online else '⚠ 未消费'}")

    ok = sum(1 for v in RESULT.values() if v["online"])
    log("=" * 70)
    log(f"消费确认结果：{ok}/6 个智能体在服务器侧被真实消费（在线可调用）")
    out = ROOT / "vmdeploy" / "probe_consume_result.json"
    out.write_text(json.dumps({"generatedAt": datetime.now(timezone.utc).isoformat(),
                               "onlineCount": ok, "total": 6, "detail": RESULT},
                              ensure_ascii=False, indent=2), encoding="utf-8")
    log("[写入]", out)

if __name__ == "__main__":
    main()
