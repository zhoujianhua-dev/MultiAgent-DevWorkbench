# 证书目录说明（mTLS）

本工作台与梧桐平台 broker（`wt.ioa.pub:5671`，vhost=acps）之间采用 **mTLS 双向 TLS** 通信。

每个智能体持有一套独立证书，目录布局为：

```
certs/
├── leader/        # 主控 71DG28
├── requirement/   # 需求分析 HH1098
├── architecture/  # 架构设计 C9Z1N0
├── code_review/   # 代码审查 2EJ55A
├── test_design/   # 测试设计 W7WY1V
├── documentation/ # 文档生成 WHVAY8
└── leader-v2/     # 主控 v2（迭代版本）
```

每个子目录下含：

- `agent-cert.pem` —— 本智能体客户端证书（公钥）
- `agent-key.pem` —— 本智能体私钥（**机密，仅存于服务器，不随源码分发**）
- `trust-bundle.pem` —— 平台 CA 信任链（公钥，见同目录已附副本）

`src/main_mq.py` 与 `src/partners.py` 中通过 `CERT_DIR` / `CERT_BASE` 读取对应目录证书建立 AMQPS 连接。

**安全声明**：本提交包仅包含 CA 公钥 `trust-bundle.pem`，不包含任何智能体私钥或客户端证书，可安全公开分发。
