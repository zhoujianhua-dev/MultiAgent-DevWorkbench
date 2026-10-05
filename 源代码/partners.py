"""6个AMQP智能体常驻消费者（leader v1 + 5个Partner）
- group-invitation → 广播入群确认
- task-command → 调StepFun按角色作答 → TaskResult发回senderId的inbox
"""
import json, os, uuid, time, threading, urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pika, ssl

V2_AIC = "1.2.156.3088.1.BUPT.NMLQ3K.196F8B.2NKB31.SBHO"
# 交付版已移除真实密钥，运行时通过环境变量注入：export STEPFUN_API_KEY=xxx
STEPFUN_KEY = os.environ.get("STEPFUN_API_KEY", "")
STEPFUN_URL = "https://api.stepfun.com/step_plan/v1/chat/completions"
CERT_BASE = Path("/opt/agent/certs")

AGENTS = {
    "leader":        ("1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB",
                      "你是软件开发主控智能体（AMQP侧）。负责任务拆解与调度。"),
    "requirement":   ("1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9",
                      "你是需求分析师。针对给定需求，输出：1)核心功能清单 2)非功能需求 3)用户故事与验收标准。控制在300字内。"),
    "architecture":  ("1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U",
                      "你是架构设计师。针对给定需求，输出：技术选型、系统模块划分、数据流与关键接口设计。控制在300字内。"),
    "code_review":   ("1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y",
                      "你是代码审查师。针对给定需求/代码，输出：质量风险点、安全审查要点、改进建议。控制在300字内。"),
    "test_design":   ("1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7",
                      "你是测试设计师。针对给定需求，输出：测试策略、核心测试用例清单、验收标准。控制在300字内。"),
    "documentation": ("1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N",
                      "你是文档工程师。针对给定需求，输出：应交付的文档清单与各自要点。控制在300字内。"),
}

def now():
    return datetime.now(timezone.utc).isoformat()

def call_stepfun(sys_prompt, text, timeout=25, max_tokens=800):
    body = json.dumps({"model": "step-3.5-flash",
                       "messages": [{"role": "system", "content": sys_prompt},
                                    {"role": "user", "content": text}],
                       "max_tokens": max_tokens, "temperature": 0.5}).encode()
    req = urllib.request.Request(STEPFUN_URL, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {STEPFUN_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())["choices"][0]["message"]["content"]

def agent_ssl(name):
    d = CERT_BASE / name
    ctx = ssl.create_default_context(cafile=str(d / "trust-bundle.pem"))
    ctx.load_cert_chain(str(d / "agent-cert.pem"), str(d / "agent-key.pem"))
    ctx.check_hostname = False
    return ctx

def connect(name):
    params = pika.ConnectionParameters(
        host="wt.ioa.pub", port=5671, virtual_host="acps",
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(agent_ssl(name)), heartbeat=30)
    return pika.BlockingConnection(params)

def reply(sender_cert, target_aic, payload):
    """独立短连接回复，避免consume线程同时publish"""
    try:
        conn = connect(sender_cert)
        ch = conn.channel()
        ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
        ch.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{target_aic}",
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
        conn.close()
    except Exception as e:
        print(f"[reply] FAIL -> {target_aic[:40]}: {e}", flush=True)

def group_task_handler(name, aic, ex):
    """群队列回调: 消费task-command → StepFun作答 → TaskResult发布回群交换机"""
    def on_group_msg(ch_, method, props, body):
        print(f"[{name}] group msg: {body[:150].decode('utf-8','replace')}", flush=True)
        try:
            d = json.loads(body)
        except Exception:
            ch_.basic_ack(delivery_tag=method.delivery_tag); return
        mtype = d.get("type", "")
        if mtype == "task-command":
            tid = d.get("taskId", str(uuid.uuid4()))
            sid = d.get("sessionId", "")
            sender = d.get("senderId", "")
            text = "\n".join(di.get("text", "") for di in d.get("dataItems", [])
                             if di.get("type") == "text").strip()
            try:
                answer = call_stepfun(sys_prompt_of(name), text or "请介绍你的职责。")
                state = "awaiting-completion"
            except Exception as e:
                answer = f"[{name} 处理失败: {e}]"
                state = "awaiting-completion"
            result = {"type": "task-result", "id": str(uuid.uuid4()), "sentAt": now(),
                      "senderRole": "partner", "senderId": aic, "taskId": tid,
                      "status": {"state": state, "stateChangedAt": now()},
                      "products": [{"id": str(uuid.uuid4()), "name": f"{name}.md",
                                    "description": name,
                                    "dataItems": [{"type": "text", "text": answer}]}],
                      "sessionId": sid}
            try:
                ch_.basic_publish(exchange=ex, routing_key="",
                    body=json.dumps(result, ensure_ascii=False).encode(),
                    properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
                print(f"[{name}] group task {tid[:8]} done -> published to group", flush=True)
            except Exception as e:
                print(f"[{name}] publish fail: {e}", flush=True)
        ch_.basic_ack(delivery_tag=method.delivery_tag)
    return on_group_msg

def sys_prompt_of(name):
    return AGENTS.get(name, ("", "你是软件研发专业智能体。"))[1]

def run_agent(name, aic, sys_prompt):
    q = f"inbox_{aic}"
    while True:
        conn = None
        try:
            conn = connect(name)
            ch = conn.channel()
            ch.exchange_declare(exchange="inbox.topic", exchange_type="topic", durable=True, passive=True)
            ch.queue_declare(queue=q, durable=True, arguments={"x-expires":5184000000,"x-message-ttl":604800000})
            ch.queue_bind(queue=q, exchange="inbox.topic", routing_key=q)

            def on_msg(ch_, method, props, body):
                print(f"[{name}] inbox: {body[:160].decode('utf-8','replace')}", flush=True)
                try:
                    d = json.loads(body)
                except Exception:
                    ch_.basic_ack(delivery_tag=method.delivery_tag); return
                mtype = d.get("type", "")
                if mtype == "group-invitation":
                    gid = d.get("group", {}).get("groupId", "")
                    ex = d.get("amqp", {}).get("exchange", "")
                    joined = {"type": "group-mgmt-result", "id": f"mgmt-{uuid.uuid4()}",
                              "sentAt": now(), "senderRole": "partner", "senderId": aic,
                              "groupId": gid, "status": {"connected": True, "muted": False},
                              "dataItems": []}
                    if ex:
                        ch_.basic_publish(exchange=ex, routing_key="",
                            body=json.dumps(joined, ensure_ascii=False).encode(),
                            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
                    else:
                        leader_aic = d.get("group", {}).get("leader", {}).get("aic", V2_AIC)
                        ch_.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{leader_aic}",
                            body=json.dumps(joined, ensure_ascii=False).encode(),
                            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
                    print(f"[{name}] joined group {gid[:24]}", flush=True)
                    # 按SDK协议: 创建 {群交换机}_{本方AIC} 队列, 绑定群交换机, 消费群任务
                    try:
                        gq = f"{ex}_{aic}"
                        ch_.queue_declare(queue=gq, durable=True, auto_delete=True)
                        ch_.queue_bind(queue=gq, exchange=ex, routing_key="")
                        ch_.basic_consume(queue=gq, on_message_callback=group_task_handler(name, aic, ex))
                        print(f"[{name}] group queue {gq[:70]} bound & consuming", flush=True)
                    except Exception as e:
                        print(f"[{name}] group queue setup fail: {e}", flush=True)
                elif mtype == "task-command":
                    tid = d.get("taskId", str(uuid.uuid4()))
                    sid = d.get("sessionId", "")
                    sender = d.get("senderId", V2_AIC)
                    text = "\n".join(di.get("text", "") for di in d.get("dataItems", [])
                                     if di.get("type") == "text").strip()
                    try:
                        answer = call_stepfun(sys_prompt, text or "请介绍你的职责。")
                        state = "awaiting-completion"
                    except Exception as e:
                        answer = f"[{name} 处理失败: {e}]"
                        state = "awaiting-completion"
                    result = {"type": "task-result", "id": str(uuid.uuid4()), "sentAt": now(),
                              "senderRole": "partner", "senderId": aic, "taskId": tid,
                              "status": {"state": state, "stateChangedAt": now()},
                              "products": [{"id": str(uuid.uuid4()), "name": f"{name}.md",
                                            "description": name,
                                            "dataItems": [{"type": "text", "text": answer}]}],
                              "sessionId": sid}
                    threading.Thread(target=reply, args=(name, sender, result), daemon=True).start()
                    print(f"[{name}] replied task {tid[:8]} -> {sender[:40]}", flush=True)
                ch_.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_consume(queue=q, on_message_callback=on_msg)
            print(f"[{name}] listening {q[:60]}", flush=True)
            while True:
                conn.process_data_events(time_limit=1)
        except Exception as e:
            print(f"[{name}] reconnect: {e}", flush=True)
            time.sleep(5)
            try:
                if conn and conn.is_open: conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    for name, (aic, sp) in AGENTS.items():
        threading.Thread(target=run_agent, args=(name, aic, sp), daemon=True).start()
        time.sleep(0.5)
    print("all agents online", flush=True)
    while True:
        time.sleep(60)
