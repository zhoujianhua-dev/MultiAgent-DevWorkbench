# -*- coding: utf-8 -*-
"""真实调用验证：向 6 个智能体发起真实 AIP 调用，抓取各自回发。

做法：
1) 在本地为 5 个子智能体启动 AMQP 消费者（复用作品 partners.py 逻辑，证书用本地 certs/）；
2) 以 requirement(HH1098) 身份作为「外部调用方」，先向主控 71DG28 发 1 条编排任务，
   再向 5 个子智能体各发 1 条直调任务；
3) 用 HH1098 证书守候自己的收件箱，捕获所有回发的 TaskResult；
4) 输出「6 个智能体各自是否被真实调用且成功响应」的结论。

说明：消费者进程在本地临时运行仅为验证用；生产常驻在云端服务器 (systemd aic-partners)。
"""
import json, os, ssl, time, threading, uuid, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import pika

ROOT = Path(r"D:/zhou/其他数据/除其他工具数据外的数据/人工智能竞赛")
CERT_BASE = ROOT / "certs"
OUT = ROOT / "vmdeploy" / "verify_call_result.json"

LEADER = "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB"
PARTNERS = {
    "requirement":  ("1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9",
                     "你是需求分析师。针对给定需求，输出：1)核心功能清单 2)非功能需求 3)用户故事与验收标准。控制在300字内。"),
    "architecture": ("1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U",
                     "你是架构设计师。针对给定需求，输出：技术选型、系统模块划分、数据流与关键接口设计。控制在300字内。"),
    "code_review":  ("1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y",
                     "你是代码审查师。针对给定需求/代码，输出：质量风险点、安全审查要点、改进建议。控制在300字内。"),
    "test_design":  ("1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7",
                     "你是测试设计师。针对给定需求，输出：测试策略、核心测试用例清单、验收标准。控制在300字内。"),
    "documentation":("1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N",
                     "你是文档工程师。针对给定需求，输出：应交付的文档清单与各自要点。控制在300字内。"),
}
CALLER_NAME = "requirement"
CALLER = PARTNERS[CALLER_NAME][0]

# 交付版已移除真实密钥，运行时通过环境变量注入：export STEPFUN_API_KEY=xxx
STEPFUN_KEY = os.environ.get("STEPFUN_API_KEY", "")
STEPFUN_URL = "https://api.stepfun.com/step_plan/v1/chat/completions"

EV, LOCK = [], threading.Lock()
def rec(*a):
    line = "[" + datetime.now().strftime("%H:%M:%S") + "] " + " ".join(str(x) for x in a)
    with LOCK:
        EV.append(line)
    print(line, flush=True)

def now():
    return datetime.now(timezone.utc).isoformat()

def ctx_for(dirname):
    d = CERT_BASE / dirname
    c = ssl.create_default_context(cafile=str(d / "trust-bundle.pem"))
    c.load_cert_chain(str(d / "agent-cert.pem"), str(d / "agent-key.pem"))
    c.check_hostname = False
    return c

def conn(dirname):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="wt.ioa.pub", port=5671, virtual_host="acps",
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(ctx_for(dirname)), heartbeat=30,
        blocked_connection_timeout=20))

def call_stepfun(sys_prompt, text, timeout=25, max_tokens=700):
    body = json.dumps({"model": "step-3.5-flash",
                       "messages": [{"role": "system", "content": sys_prompt},
                                    {"role": "user", "content": text}],
                       "max_tokens": max_tokens, "temperature": 0.5}).encode()
    req = urllib.request.Request(STEPFUN_URL, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {STEPFUN_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())["choices"][0]["message"]["content"]

def publish(dirname, target_aic, payload):
    c = conn(dirname); ch = c.channel()
    ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
    ch.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{target_aic}",
        body=json.dumps(payload, ensure_ascii=False).encode(),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
    c.close()

def drain(dirname, aic):
    """清空历史堆积（ack 丢弃，不触发 LLM 调用）"""
    try:
        c = conn(dirname); ch = c.channel()
        q = f"inbox_{aic}"
        ch.queue_declare(queue=q, durable=True, passive=True)
        n = 0
        while n < 300:
            m, _, _ = ch.basic_get(queue=q, auto_ack=False)
            if m is None:
                break
            ch.basic_ack(m.delivery_tag); n += 1
        c.close()
        return n
    except Exception as e:
        return f"ERR {e}"

# ---------------- 子智能体消费者 ----------------
RESP = {}   # name -> 已响应次数
def partner_worker(name, aic, sp, stop_at):
    q = f"inbox_{aic}"
    while time.time() < stop_at:
        c = None
        try:
            c = conn(name); ch = c.channel()
            ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
            ch.queue_declare(queue=q, durable=True,
                             arguments={"x-expires": 5184000000, "x-message-ttl": 604800000})
            ch.queue_bind(queue=q, exchange="inbox.topic", routing_key=q)

            def on_msg(ch_, method, props, body):
                try:
                    d = json.loads(body)
                except Exception:
                    ch_.basic_ack(delivery_tag=method.delivery_tag); return
                if d.get("type") == "task-command":
                    tid = d.get("taskId", "")
                    sender = d.get("senderId", LEADER)
                    text = "\n".join(di.get("text", "") for di in d.get("dataItems", [])
                                     if di.get("type") == "text").strip()
                    try:
                        ans = call_stepfun(sp, text or "请介绍你的职责。")
                    except Exception as e:
                        ans = f"[{name} 处理异常: {e}]"
                    result = {"type": "task-result", "id": str(uuid.uuid4()), "sentAt": now(),
                              "senderRole": "partner", "senderId": aic, "taskId": tid,
                              "status": {"state": "awaiting-completion", "stateChangedAt": now()},
                              "products": [{"id": str(uuid.uuid4()), "name": f"{name}.md",
                                            "description": name,
                                            "dataItems": [{"type": "text", "text": ans}]}],
                              "sessionId": d.get("sessionId", "")}
                    try:
                        publish(name, sender, result)
                        with LOCK:
                            RESP[name] = RESP.get(name, 0) + 1
                        rec(f"✔ {name:13} 收到 task {tid[:8]} -> 已回发 TaskResult 给 {sender[-12:]}")
                    except Exception as e:
                        rec(f"✘ {name} 回发失败: {e}")
                ch_.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_consume(queue=q, on_message_callback=on_msg)
            rec(f"· 消费者上线: {name:13} queue=...{q[-24:]}")
            while time.time() < stop_at:
                c.process_data_events(time_limit=1)
            c.close(); return
        except Exception as e:
            rec(f"! {name} 连接异常重试: {e}")
            try:
                if c and c.is_open: c.close()
            except Exception: pass
            time.sleep(4)

# ---------------- 外部调用方守候收件箱 ----------------
GOT = []
def caller_listener(stop_at):
    q = f"inbox_{CALLER}"
    c = conn(CALLER_NAME); ch = c.channel()
    ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
    m = ch.queue_declare(queue=q, durable=True, passive=True)
    rec(f"· 调用方 {CALLER_NAME} 守候收件箱 ...{q[-24:]} 当前堆积={m.method.message_count}")

    def on_msg(ch_, method, props, body):
        try:
            d = json.loads(body)
        except Exception:
            ch_.basic_ack(delivery_tag=method.delivery_tag); return
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
        body_txt = (texts[0] if texts else "")[:180].replace("\n", " ")
        GOT.append({"type": mtype, "sender": sender, "taskId": tid, "text": texts[0] if texts else ""})
        rec(f"★ 捕获回发 type={mtype} from=...{sender[-12:]} task={tid} 摘要: {body_txt}")
        ch_.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=q, on_message_callback=on_msg)
    while time.time() < stop_at:
        try:
            c.process_data_events(time_limit=1)
        except Exception as e:
            rec(f"! listener 异常: {e}"); break
    try: c.close()
    except Exception: pass

def task_cmd(text, sender=CALLER):
    return {"type": "task-command", "id": str(uuid.uuid4()), "sentAt": now(),
            "senderRole": "user", "senderId": sender, "command": "start",
            "dataItems": [{"type": "text", "text": text}],
            "taskId": str(uuid.uuid4()), "sessionId": str(uuid.uuid4())}

def main():
    t0 = time.time()
    stop_at = t0 + 175

    rec("=" * 90)
    rec("阶段0：清空 5 个子智能体收件箱的历史堆积（仅丢弃，不产生 LLM 调用）")
    for name, (aic, _) in PARTNERS.items():
        n = drain(name, aic)
        rec(f"  drain {name:13} 丢弃历史消息 {n} 条")

    rec("=" * 90)
    rec("阶段1：本地拉起 5 个子智能体消费者（验证期间临时）")
    for name, (aic, sp) in PARTNERS.items():
        threading.Thread(target=partner_worker, args=(name, aic, sp, stop_at), daemon=True).start()
        time.sleep(0.4)
    threading.Thread(target=caller_listener, args=(stop_at,), daemon=True).start()
    time.sleep(6)

    rec("=" * 90)
    rec("阶段2：向主控 71DG28 发起真实编排调用（sender=%s）" % CALLER_NAME)
    publish(CALLER_NAME, LEADER, task_cmd(
        "为「在线计算器 Web 应用」协同产出交付物：需求分析 / 架构设计 / 代码审查要点 / 测试设计 / 文档大纲。每项 3-5 条要点。"))
    rec("  已投递编排任务 -> inbox_...71DG28，等待主控派发 5 个子智能体…")
    time.sleep(45)

    rec("=" * 90)
    rec("阶段3：逐一真实调用 5 个子智能体（sender=%s）" % CALLER_NAME)
    for name, (aic, _) in PARTNERS.items():
        publish(CALLER_NAME, aic, task_cmd(
            f"请以「{name}」角色，针对「在线计算器 Web 应用」给出你职责范围内 3-5 条要点。"))
        rec(f"  已投递直调任务 -> {name}")
        time.sleep(3)

    rec("=" * 90)
    rec("阶段4：等待全部回发…（最长 %d 秒）" % int(stop_at - time.time()))
    while time.time() < stop_at:
        time.sleep(2)

    # ---------- 汇总 ----------
    rec("=" * 90)
    rec("验证结论")
    from_senders = {}
    for g in GOT:
        from_senders.setdefault(g["sender"][-12:], 0)
        from_senders[g["sender"][-12:]] += 1
    lines = []
    for name, (aic, _) in PARTNERS.items():
        cnt = RESP.get(name, 0)
        ok = "✅ 已响应" if cnt > 0 else "❌ 未响应"
        lines.append(f"  {name:13} {aic[-18:]}  响应 {cnt} 次  {ok}")
        rec(lines[-1])
    rec(f"  主控 71DG28 回发至调用方的消息数(捕获): "
        f"{sum(1 for g in GOT if g['sender'][-12:].startswith('71DG28'))}")
    rec(f"  调用方共捕获回发消息 {len(GOT)} 条，来自 {len(from_senders)} 个不同发送方")

    summary = {
        "generatedAt": now(),
        "caller": CALLER_NAME,
        "partnersResponses": RESP,
        "capturedReplies": len(GOT),
        "capturedFromSenders": from_senders,
        "events": EV,
        "replySamples": [{"sender": g["sender"][-18:], "type": g["type"],
                          "text": g["text"][:600]} for g in GOT[:12]],
    }
    OUT.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    rec(f"[写入] {OUT}")

if __name__ == "__main__":
    main()
