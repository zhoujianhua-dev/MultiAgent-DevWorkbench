# -*- coding: utf-8 -*-
"""端到端协同「内容级」验证：守候子 agent 真实收件箱，抓主控的派发与最终综合答复。

原理：
- 主控收到 task-command 后 orchestrate(): 向 5 个 partner 的 inbox 各发一条派发命令；
  再等 partner 回 task-result；再调 StepFun 综合；最后 publish_to(f"inbox_{sender}")。
- 因此只要发送方的 senderId 用「真实子 agent AIC」（我用 requirement=HH1098，持有其证书），
  主控的【派发命令】与【最终综合答复】都会落到 inbox_HH1098 —— 我用自己的证书消费即可全量捕获。
用法: python capture_reply.py [任务数] [监听秒数]
"""
import json, uuid, time, ssl, threading
from datetime import datetime, timezone
import pika

ROOT = r"D:/zhou/其他数据/除其他工具数据外的数据/人工智能竞赛"
LEADER_AIC = "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB"
SELF_DIR = f"{ROOT}/certs/requirement"
SELF_AIC = "1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9"
LEADER_DIR = f"{ROOT}/certs/leader"

def ctx_for(d):
    c = ssl.create_default_context(cafile=f"{d}/trust-bundle.pem")
    c.load_cert_chain(f"{d}/agent-cert.pem", f"{d}/agent-key.pem")
    c.check_hostname = False
    return c

def conn(d):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="wt.ioa.pub", port=5671, virtual_host="acps",
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(ctx_for(d)), heartbeat=60))

got = []

def on_msg(ch, method, props, body):
    try:
        d = json.loads(body)
    except Exception:
        d = {}
    ts = datetime.now().strftime("%H:%M:%S")
    mtype = d.get("type", "?")
    sender = d.get("senderId", "")
    tid = d.get("taskId", "")[:8]
    texts = []
    for p in d.get("products", []) or []:
        for di in p.get("dataItems", []) or []:
            if di.get("type") == "text":
                texts.append(di.get("text", ""))
    if not texts:
        texts = [di.get("text", "") for di in d.get("dataItems", []) if di.get("type") == "text"]
    line = f"[{ts}] type={mtype} task={tid} from={sender[-12:]} state={(d.get('status') or {}).get('state','')}"
    print("=" * 100, flush=True)
    print("★ 捕获消息 " + line, flush=True)
    for t in texts:
        print("---- 内容 ----", flush=True)
        print(t[:2500], flush=True)
    got.append((mtype, tid, texts))
    ch.basic_ack(delivery_tag=method.delivery_tag)

def sender(n):
    time.sleep(4)
    c = conn(LEADER_DIR); ch = c.channel()
    for i in range(n):
        tid = str(uuid.uuid4()); sid = str(uuid.uuid4())
        cmd = {"type": "task-command", "id": str(uuid.uuid4()),
               "sentAt": datetime.now(timezone.utc).isoformat(),
               "senderRole": "user", "senderId": SELF_AIC, "command": "start",
               "dataItems": [{"type": "text", "text":
                   "为「在线计算器 Web 应用」出一份协同交付物：1)需求分析 2)架构设计 3)代码审查要点 4)测试设计 5)文档大纲。每项3-5条要点。"}],
               "taskId": tid, "sessionId": sid}
        ch.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{LEADER_AIC}",
                         body=json.dumps(cmd, ensure_ascii=False).encode(),
                         properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
        print(f"[发送 {i+1}/{n}] task={tid[:8]} sender={SELF_AIC[-12:]}", flush=True)
        time.sleep(3)
    ch.close(); c.close()
    print("[发送完毕]", flush=True)

def main(n=4, listen=150):
    q = f"inbox_{SELF_AIC}"
    c = conn(SELF_DIR); ch = c.channel()
    ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
    m = ch.queue_declare(queue=q, durable=True, passive=True)
    print(f"[监听] 队列 {q[-26:]} 存在, 当前堆积={m.method.message_count}", flush=True)
    ch.basic_consume(queue=q, on_message_callback=on_msg, auto_ack=False)
    threading.Thread(target=sender, args=(n,), daemon=True).start()
    end = time.time() + listen
    while time.time() < end:
        try:
            c.process_data_events(time_limit=1)
        except Exception as e:
            print("[监听异常]", e, flush=True); break
    print(f"\n[结果] 共捕获 {len(got)} 条消息: " + str([(a, b) for a, b, _ in got]), flush=True)
    try: c.close()
    except Exception: pass

if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    L = int(sys.argv[2]) if len(sys.argv) > 2 else 150
    main(n, L)
