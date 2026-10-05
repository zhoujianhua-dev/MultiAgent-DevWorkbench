# -*- coding: utf-8 -*-
"""查 6 个我的 agent 的 inbox 队列在 broker 上的消费者数。
这是判断「他人调用能否被消费」的最硬证据。"""
import sys, os
_HERE = os.path.dirname(os.path.abspath(__file__))
while _HERE in sys.path:
    sys.path.remove(_HERE)

import json, ssl
import pika

CERT = "D:/zhou/其他数据/除其他工具数据外的数据/人工智能竞赛/certs/leader"
BROKER_HOST, BROKER_PORT, VHOST = "wt.ioa.pub", 5671, "acps"

MY = [
    ("主控 71DG28", "1.2.156.3088.1.BUPT.NMLQ3K.71DG28.S7TATG.0WXB"),
    ("需求 HH1098", "1.2.156.3088.1.BUPT.NMLQ3K.HH1098.OJ1Q6D.06A9"),
    ("架构 C9Z1N0", "1.2.156.3088.1.BUPT.NMLQ3K.C9Z1N0.51CG1S.100U"),
    ("代码审查 2EJ55A", "1.2.156.3088.1.BUPT.NMLQ3K.2EJ55A.EGP7TY.035Y"),
    ("测试 W7WY1V", "1.2.156.3088.1.BUPT.NMLQ3K.W7WY1V.2BRQ1W.0SV7"),
    ("文档 WHVAY8", "1.2.156.3088.1.BUPT.NMLQ3K.WHVAY8.F1OM44.036N"),
]

def ssl_ctx():
    ctx = ssl.create_default_context(cafile=f"{CERT}/trust-bundle.pem")
    ctx.load_cert_chain(f"{CERT}/agent-cert.pem", f"{CERT}/agent-key.pem")
    ctx.check_hostname = False
    return ctx

def main():
    params = pika.ConnectionParameters(
        host=BROKER_HOST, port=BROKER_PORT, virtual_host=VHOST,
        credentials=pika.credentials.ExternalCredentials(),
        ssl_options=pika.SSLOptions(ssl_ctx()), heartbeat=30,
        blocked_connection_timeout=20,
    )
    print(f"连接 broker {BROKER_HOST}:{BROKER_PORT} vhost={VHOST} ...")
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    print("连接成功。\n")
    any_online = False
    for name, aic in MY:
        qname = f"inbox_{aic}"
        try:
            m = ch.queue_declare(queue=qname, passive=True)
            cc = m.method.consumer_count
            mc = m.method.message_count
            flag = "✅ 在线" if cc >= 1 else "❌ 无消费者"
            if cc >= 1:
                any_online = True
            print(f"  {name:14} consumer_count={cc}  队列堆积={mc}   {flag}")
        except pika.exceptions.ChannelClosedByBroker as e:
            # 404 = 队列不存在（消费者未创建/已离线）
            print(f"  {name:14} 队列不存在(code={e.reply_code}) ❌ 消费者离线")
            try:
                ch = conn.channel()
            except Exception:
                conn = pika.BlockingConnection(params); ch = conn.channel()
        except Exception as e:
            print(f"  {name:14} 查询异常 {type(e).__name__}: {str(e)[:80]}")
    conn.close()
    print()
    if any_online:
        print("结论：至少部分 agent 的 inbox 有消费者在线 → VM104 运行时未挂，他人 AMQP 调用能被接收处理。")
    else:
        print("结论：所有 inbox 无人消费 → VM104 消费者进程当前不在线，他人调用将无人应答。")

if __name__ == "__main__":
    main()
