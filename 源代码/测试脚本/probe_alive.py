# -*- coding: utf-8 -*-
"""只读探测：验证「服务器侧的 6 个智能体是否真的活着、能被外部真实调用」。

做法（不在本地起任何消费者，完全靠服务器侧响应）：
1) 用 requirement(HH1098) 身份守候自己的 inbox；
2) 依次向 5 个子智能体的 inbox 各发 1 条真实 task-command；
3) 再向主控 71DG28 发 1 条编排任务；
4) 统计各智能体是否回发 TaskResult（按 senderId 归因）；
5) 同时用各子智能体证书查自己队列堆积，判断「被消费」情况。
"""
import json, ssl, time, uuid, threading
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
SELF_NAME = "requirement"
SELF = PARTS[SELF_NAME]
TAIL = {v[-12:]: k for k, v in PARTS.items()}
TAIL[LEADER[-12:]] = "leader"

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

def now():
    return datetime.now(timezone.utc).isoformat()

def log(*a):
    print("[" + datetime.now().strftime("%H:%M:%S") + "]", *a, flush=True)

def publish(sender_name, target, text):
    c = conn(sender_name); ch = c.channel()
    ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
    cmd = {"type": "task-command", "id": str(uuid.uuid4()), "sentAt": now(),
           "senderRole": "user", "senderId": SELF, "command": "start",
           "dataItems": [{"type": "text", "text": text}],
           "taskId": str(uuid.uuid4()), "sessionId": str(uuid.uuid4())}
    ch.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{target}",
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
        return f"ERR:{str(e)[:40]}"

REPLIES = []
STOP = [0]

def listener():
    c = conn(SELF_NAME)
    ch = c.channel()
    q = f"inbox_{SELF}"
    ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
    ch.queue_declare(queue=q, durable=True, passive=True)

    def on_msg(ch_, method, props, body):
        try:
            d = json.loads(body)
        except Exception:
            ch_.basic_ack(delivery_tag=method.delivery_tag); return
        sender = d.get("senderId", "")
        texts = []
        for p in d.get("products", []) or []:
            for di in p.get("dataItems", []) or []:
                if di.get("type") == "text":
                    texts.append(di.get("text", ""))
        if not texts:
            texts = [di.get("text", "") for di in d.get("dataItems", []) if di.get("type") == "text"]
        who = TAIL.get(sender[-12:], sender[-12:])
        REPLIES.append({"who": who, "type": d.get("type"), "senderTail": sender[-12:],
                        "taskId": d.get("taskId", "")[:8], "text": (texts[0] if texts else "")})
        log(f"★ 回发捕获 <- {who:13} type={d.get('type')} task={d.get('taskId','')[:8]} "
            f"摘要={(texts[0] if texts else '')[:120].replace(chr(10),' ')}")
        ch_.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=q, on_message_callback=on_msg)
    log(f"守候调用方收件箱 ...{q[-24:]}")
    while not STOP[0]:
        try:
            c.process_data_events(time_limit=1)
        except Exception as e:
            log("listener 异常:", e); break
    try: c.close()
    except Exception: pass

def main():
    threading.Thread(target=listener, daemon=True).start()
    time.sleep(4)

    log("=" * 88)
    log("阶段A：逐一真实调用 5 个子智能体（sender=requirement）")
    for name, aic in PARTS.items():
        publish(SELF_NAME, aic, f"请以「{name}」角色，针对「在线计算器 Web 应用」给出 3 条职责要点。")
        log(f"  -> 已投递直调任务: {name}")
        time.sleep(2)

    log("=" * 88)
    log("阶段B：向主控 71DG28 发起编排调用（sender=requirement）")
    publish(SELF_NAME, LEADER, "为「在线计算器 Web 应用」协同产出交付物：需求/架构/审查/测试/文档，各 3 条要点。")

    log("等待 75 秒收集回发 …")
    end = time.time() + 75
    while time.time() < end:
        time.sleep(3)

    log("=" * 88)
    log("阶段C：检查各子智能体队列堆积（0 且期间有新消息 = 服务器侧消费者在实时消费）")
    for name, aic in PARTS.items():
        log(f"  {name:13} inbox 堆积 = {depth(name, aic)}")

    STOP[0] = 1
    time.sleep(2)

    log("=" * 88)
    log("验证结论")
    got = {}
    for r in REPLIES:
        got[r["who"]] = got.get(r["who"], 0) + 1
    targets = ["leader"] + list(PARTS.keys())
    ok = 0
    for t in targets:
        n = got.get(t, 0)
        if n:
            ok += 1
        log(f"  {t:13} 真实回发 {n} 次  {'✅ 可被真实调用' if n else '❌ 本次未捕获回发'}")
    log(f"  合计 {ok}/{len(targets)} 个智能体在本轮真实调用中成功响应；共捕获回发 {len(REPLIES)} 条")

    out = ROOT / "vmdeploy" / "probe_alive_result.json"
    out.write_text(json.dumps({"generatedAt": now(), "repliesByAgent": got,
                               "totalReplies": len(REPLIES), "okCount": ok,
                               "totalAgents": len(targets), "replies": REPLIES},
                              ensure_ascii=False, indent=2), encoding="utf-8")
    log("[写入]", out)

if __name__ == "__main__":
    main()
