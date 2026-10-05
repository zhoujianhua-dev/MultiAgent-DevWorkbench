"""Leader v2 — MQ优先编排服务
- 消费 inbox_{V2_AIC}（证书: certs/leader-v2/）接收叮当/平台下发的任务
- 收到 task-command → 分发给5个Partner inbox → 汇总 → StepFun综合 → TaskResult回发sender
- 兼容群组邀请（入群+群队列消费）
- HTTP /health /rpc 仍保留（ endPoint 若恢复HTTP可双通道）
V2_AIC 从 /opt/agent/v2.aic 读取
"""
import json, os, uuid, time, threading, urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pika, ssl
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

def _read_aic():
    try:
        return open("/opt/agent/v2.aic").read().strip()
    except Exception:
        return "1.2.156.3088.1.BUPT.NMLQ3K.G9K6HC.DLMEF0.0XRJ"

V2_AIC = _read_aic()
V1_AIC = "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB"
PARTNERS = {
    "requirement":   "1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9",
    "architecture":  "1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U",
    "code_review":   "1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y",
    "test_design":   "1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7",
    "documentation": "1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N",
}
# 交付版已移除真实密钥，运行时通过环境变量注入：export STEPFUN_API_KEY=xxx
STEPFUN_KEY = os.environ.get("STEPFUN_API_KEY", "")
STEPFUN_URL = "https://api.stepfun.com/step_plan/v1/chat/completions"
CERT_DIR = "/opt/agent/certs/leader-v2"

def now():
    return datetime.now(timezone.utc).isoformat()

def call_stepfun(messages, timeout=60, max_tokens=2000):
    body = json.dumps({"model": "step-3.5-flash", "messages": messages,
                       "max_tokens": max_tokens, "temperature": 0.5}).encode()
    req = urllib.request.Request(STEPFUN_URL, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {STEPFUN_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())["choices"][0]["message"]["content"]

def v2_ssl():
    d = Path(CERT_DIR)
    ctx = ssl.create_default_context(cafile=str(d / "trust-bundle.pem"))
    ctx.load_cert_chain(str(d / "agent-cert.pem"), str(d / "agent-key.pem"))
    ctx.check_hostname = False
    return ctx

def v2_conn():
    params = pika.ConnectionParameters(
        host="wt.ioa.pub", port=5671, virtual_host="acps",
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(v2_ssl()), heartbeat=30)
    return pika.BlockingConnection(params)

def publish_to(target_inbox, payload):
    conn = v2_conn()
    try:
        ch = conn.channel()
        ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
        ch.basic_publish(exchange="inbox.topic", routing_key=target_inbox,
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
    finally:
        try: conn.close()
        except Exception: pass

def task_command(cmd, text, task_id, session_id):
    return {"type": "task-command", "id": str(uuid.uuid4()), "sentAt": now(),
            "senderRole": "leader", "senderId": V2_AIC, "command": cmd,
            "dataItems": ([{"type": "text", "text": text}] if text else []),
            "taskId": task_id, "sessionId": session_id}

def task_result(state, task_id, session_id, products=None):
    return {"type": "task-result", "id": str(uuid.uuid4()), "sentAt": now(),
            "senderRole": "partner", "senderId": V2_AIC, "taskId": task_id,
            "status": {"state": state, "stateChangedAt": now()},
            "products": products if products is not None else [],
            "sessionId": session_id}

def text_product(name, text):
    return [{"id": str(uuid.uuid4()), "name": name, "description": name,
             "dataItems": [{"type": "text", "text": text}]}]

partner_replies = {}   # taskId -> {"n":int,"notes":{}}
tasks = {}

# ---------------- 编排: 分发伙伴 → 汇总 → 综合 → 回发 ----------------
def orchestrate(task_id, session_id, sender_inbox, text):
    t = tasks.setdefault(task_id, {"state": "working", "answer": ""})
    try:
        partner_replies.setdefault(task_id, {"n": 0, "notes": {}})
        for pname, paic in PARTNERS.items():
            cmd = task_command("start", text, task_id, session_id)
            threading.Thread(target=publish_to, args=(f"inbox_{paic}", cmd), daemon=True).start()
        deadline = time.time() + 75
        while time.time() < deadline:
            if partner_replies.get(task_id, {}).get("n", 0) >= len(PARTNERS):
                break
            time.sleep(2)
        notes = partner_replies.get(task_id, {}).get("notes", {})
        block = "\n\n".join(f"【{k}】{v}" for k, v in notes.items() if v) or "（各专业智能体暂未返回，请直接作答）"
        answer = call_stepfun([
            {"role": "system", "content": "你是软件开发任务主控智能体。整合各子智能体（需求分析/架构设计/代码审查/测试设计/文档生成）的专业意见，给出最终、完整、结构化的中文答复。"},
            {"role": "user", "content": f"用户需求：{text}\n\n各子智能体意见：\n{block}\n\n请给出最终综合答复。"}],
            timeout=90)
        t["answer"] = answer
        t["state"] = "awaiting-completion"
        publish_to(sender_inbox, task_result("awaiting-completion", task_id, session_id,
                                             products=text_product("答复.md", answer)))
        print(f"[orch] task {task_id[:8]} DONE -> {sender_inbox[:40]}", flush=True)
    except Exception as e:
        print(f"[orch] task {task_id[:8]} ERR: {e}", flush=True)
        try:
            publish_to(sender_inbox, task_result("failed", task_id, session_id,
                                                 products=text_product("错误.md", f"处理失败: {e}")))
        except Exception:
            pass

# ---------------- inbox 消费者 ----------------
def handle_v2_msg(ch_, method, props, body, conn_ref):
    print(f"[v2inbox] RX {method.routing_key or method.delivery_tag}: {body[:220].decode('utf-8','replace')}", flush=True)
    try:
        d = json.loads(body)
    except Exception:
        ch_.basic_ack(delivery_tag=method.delivery_tag); return
    mtype = d.get("type", "")
    tid = d.get("taskId", "")
    sender = d.get("senderId", "")
    if mtype == "task-command" and sender != V2_AIC:
        command = d.get("command", "start")
        text = "\n".join(di.get("text", "") for di in d.get("dataItems", []) if di.get("type") == "text").strip()
        session_id = d.get("sessionId") or str(uuid.uuid4())
        print(f"[v2inbox] task {tid[:8]} cmd={command} from {sender[:40]}", flush=True)
        if command in ("start", "continue"):
            threading.Thread(target=orchestrate, args=(tid, session_id, f"inbox_{sender}", text), daemon=True).start()
        elif command == "complete":
            t = tasks.get(tid)
            publish_to(f"inbox_{sender}", task_result("completed", tid, session_id,
                      products=text_product("答复.md", (t or {}).get("answer", "任务已完成"))))
    elif mtype == "task-result" and tid:
        rec = partner_replies.setdefault(tid, {"n": 0, "notes": {}})
        note = ""
        try:
            for p in d.get("products", []):
                for di in p.get("dataItems", []):
                    if di.get("type") == "text":
                        note += di.get("text", "") + "\n"
        except Exception:
            pass
        for nm, aic in PARTNERS.items():
            if aic == sender:
                rec["notes"][nm] = note.strip()
        rec["n"] += 1
    elif mtype == "group-invitation":
        gid = d.get("group", {}).get("groupId", "")
        ex = d.get("amqp", {}).get("exchange", "")
        joined = {"type": "group-mgmt-result", "id": f"mgmt-{uuid.uuid4()}", "sentAt": now(),
                  "senderRole": "partner", "senderId": V2_AIC, "groupId": gid,
                  "status": {"connected": True, "muted": False}, "dataItems": []}
        if ex:
            try:
                ch_.basic_publish(exchange=ex, routing_key="",
                    body=json.dumps(joined, ensure_ascii=False).encode(),
                    properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
                gq = f"{ex}_{V2_AIC}"
                ch_.queue_declare(queue=gq, durable=True, auto_delete=True)
                ch_.queue_bind(queue=gq, exchange=ex, routing_key="")
                ch_.basic_consume(queue=gq, on_message_callback=lambda c_, m_, p_, b_: handle_v2_msg(c_, m_, p_, b_, conn_ref))
                print(f"[v2inbox] joined group {gid[:24]} queue bound+consuming", flush=True)
            except Exception as e:
                print(f"[v2inbox] group join fail: {e}", flush=True)
    ch_.basic_ack(delivery_tag=method.delivery_tag)

def consume_v2_inbox():
    q = f"inbox_{V2_AIC}"
    while True:
        conn = None
        try:
            conn = v2_conn()
            ch = conn.channel()
            ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
            ch.queue_declare(queue=q, durable=True, arguments={"x-expires": 5184000000, "x-message-ttl": 604800000})
            ch.queue_bind(queue=q, exchange="inbox.topic", routing_key=q)

            def on_msg(ch_, method, props, body):
                handle_v2_msg(ch_, method, props, body, conn)

            ch.basic_consume(queue=q, on_message_callback=on_msg)
            print(f"[v2inbox] consuming {q[:60]}", flush=True)
            while True:
                conn.process_data_events(time_limit=1)
        except Exception as e:
            print(f"[v2inbox] reconnect: {str(e)[:120]}", flush=True)
            time.sleep(5)
            try:
                if conn and conn.is_open: conn.close()
            except Exception:
                pass

# ---------------- FastAPI (HTTP 保留) ----------------
app = FastAPI(title="Leader Agent v2")

@app.on_event("startup")
def _startup():
    threading.Thread(target=consume_v2_inbox, daemon=True).start()

@app.get("/")
@app.get("/health")
def health():
    return {"status": "online", "agent": "leader-v2", "aic": V2_AIC, "tasks": len(tasks), "time": now()}

@app.get("/rpc")
def rpc_get():
    return {"jsonrpc": "2.0", "id": None, "result": {"status": "online", "agent": "leader-v2"}}

@app.post("/rpc")
async def rpc(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
    req_id = body.get("id")
    method = body.get("method", "")
    params = body.get("params") or {}
    if method != "rpc":
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}})
    cmd = params.get("command") or {}
    command = cmd.get("command", "start")
    task_id = cmd.get("taskId") or str(uuid.uuid4())
    session_id = cmd.get("sessionId") or str(uuid.uuid4())
    text = "\n".join(di.get("text", "") for di in cmd.get("dataItems", []) if di.get("type") == "text").strip()
    t = tasks.setdefault(task_id, {"state": "working", "answer": "", "session": session_id})
    if command in ("start", "continue"):
        threading.Thread(target=orchestrate, args=(task_id, session_id, None, text), daemon=True).start()
        deadline = time.time() + 100
        while time.time() < deadline and t["state"] == "working":
            time.sleep(2)
        if t["state"] == "awaiting-completion":
            return JSONResponse({"jsonrpc": "2.0", "id": req_id,
                "result": task_result("awaiting-completion", task_id, session_id, products=text_product("答复.md", t["answer"]))})
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": task_result("working", task_id, session_id)})
    if command == "complete":
        t["state"] = "completed"
        return JSONResponse({"jsonrpc": "2.0", "id": req_id,
            "result": task_result("completed", task_id, session_id, products=text_product("答复.md", t.get("answer", "任务已完成")))})
    return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": task_result(t.get("state", "working"), task_id, session_id)})

if __name__ == "__main__":
    threading.Thread(target=lambda: uvicorn.run(app, host="0.0.0.0", port=18080, log_level="warning"), daemon=True).start()
    consume_v2_inbox()
