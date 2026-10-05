"""真实端到端协同测试 v2：发真实任务 -> 看平台是否自动采集真实AMQP调用(4榜增量)。
- 主控收到 task-command(sender!=V2_AIC) -> orchestrate: 真实 publish 5次(派发5 partner)
- 若平台 monitor 自动采集 broker 调用, 71DG28 caller +5/任务, 5 partner callee +1/任务
- 若不为0增量 = 真实派发确凿发生 = 协同跑通
"""
import json, uuid, time, ssl, urllib.request
from datetime import datetime, timezone, timedelta
import pika

CERT = r"D:/zhou/其他数据/除其他工具数据外的数据/人工智能竞赛/certs/leader"
LEADER_AIC = "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB"
PARTNERS = [
    "1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9",
    "1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U",
    "1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y",
    "1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7",
    "1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N",
]
PROBE_AIC = "1.2.156.3088.1.BUPT.NMLQ3K.PROBE.e2e.0001"

def ssl_ctx():
    ctx = ssl.create_default_context(cafile=f"{CERT}/trust-bundle.pem")
    ctx.load_cert_chain(f"{CERT}/agent-cert.pem", f"{CERT}/agent-key.pem")
    ctx.check_hostname = False
    return ctx

def conn():
    p = pika.ConnectionParameters(host="wt.ioa.pub", port=5671, virtual_host="acps",
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(ssl_ctx()), heartbeat=60)
    return pika.BlockingConnection(p)

def qboard(kind):
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    now_bj = datetime.now(timezone(timedelta(hours=8))) + timedelta(hours=1)
    endAt = now_bj.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S") + "%2B08:00"
    url = f"https://wt.ioa.pub/internal/matched-stats/top?startAt=2026-09-24T00:00:00%2B08:00&endAt={endAt}&n=200"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, context=ctx, timeout=25) as r:
        b = json.loads(r.read())
    items = b.get(kind) or []
    m = {}
    for it in items:
        aic = it.get("aic", "")
        if aic in (LEADER_AIC,) + tuple(PARTNERS):
            m[aic[-18:]] = it.get("accessCount")
    return m

def main():
    before = qboard("caller"); before_c = qboard("callee")
    print("[发前 caller]", before)
    print("[发前 callee]", before_c)
    c = conn(); ch = c.channel()
    for i in range(3):
        tid = str(uuid.uuid4()); sid = str(uuid.uuid4())
        cmd = {"type": "task-command", "id": str(uuid.uuid4()),
               "sentAt": datetime.now(timezone.utc).isoformat(),
               "senderRole": "user", "senderId": PROBE_AIC, "command": "start",
               "dataItems": [{"type": "text", "text":
                   "为在线计算器 Web 应用，协同给出：1)需求分析 2)架构设计 3)代码审查要点 4)测试设计 5)文档大纲。"}],
               "taskId": tid, "sessionId": sid}
        ch.basic_publish(exchange="inbox.topic", routing_key=f"inbox_{LEADER_AIC}",
            body=json.dumps(cmd, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
        print(f"[已发 {i+1}] task {tid[:8]}")
        time.sleep(4)
    ch.close(); c.close()
    print("[等待] 130秒让主控派发+StepFun综合...")
    time.sleep(130)
    after = qboard("caller"); after_c = qboard("callee")
    print("[发后 caller]", after)
    print("[发后 callee]", after_c)
    d_caller = (after.get(LEADER_AIC[-18:], 0) or 0) - (before.get(LEADER_AIC[-18:], 0) or 0)
    print(f"\n[增量] caller 71DG28 = +{d_caller} (预期 ~+15 = 3任务×5派发)")
    for p in PARTNERS:
        b = before.get(p[-18:], 0) or 0; a = after.get(p[-18:], 0) or 0
        print(f"[增量] callee {p[-18:]} = +{a - b}")
    c2 = conn(); ch2 = c2.channel()
    try:
        qq = ch2.queue_declare(queue=f"inbox_{LEADER_AIC}", passive=True)
        print(f"\n[状态] 71DG28 inbox 当前堆积={qq.method.message_count} (0=已被主控消费)")
    except Exception as e:
        print("查堆积失败", e)
    c2.close()

if __name__ == "__main__":
    main()
