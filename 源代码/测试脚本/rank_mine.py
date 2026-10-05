# -*- coding: utf-8 -*-
"""定位我的智能体在 4 块榜的精确名次 + 完整 AIC 编号"""
import urllib.request, urllib.parse, json, ssl, datetime, sys, os

ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
BASE = "https://wt.ioa.pub"
# 交付版已移除真实账号，运行时通过环境变量注入：WUTONG_USER / WUTONG_PWD
USER = os.environ.get("WUTONG_USER", "")
PWD = os.environ.get("WUTONG_PWD", "")

MY = ["71DG28.S7TATG.0WXB", "HH1098.OJ1Q6D.06A9", "C9Z1N0.51CG1S.100U",
      "2EJ55A.EGP7TY.035Y", "W7WY1V.2BRQ1W.0SV7", "WHVAY8.F1OM44.036N"]
TEAM = "NMLQ3K"


def req(path, method="GET", body=None, token=None, ctype="application/json", timeout=30):
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode("utf-8")
    r = urllib.request.Request(BASE + path, data=data, method=method)
    if ctype:
        r.add_header("Content-Type", ctype)
    if token:
        r.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, context=ctx, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            try:
                return resp.status, json.loads(raw)
            except Exception:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw
    except Exception as e:
        return 0, repr(e)


def login():
    st, b = req("/registry-server/api/v1/auth/login", "POST",
                urllib.parse.urlencode({"username": USER, "password": PWD}).encode(),
                ctype="application/x-www-form-urlencoded")
    if st == 200 and isinstance(b, dict):
        return b.get("access_token")
    print("LOGIN FAIL", st, str(b)[:300]); return None


BOARDS = [
    ("caller",            "调用榜(含内部调用)"),
    ("callee",            "被调用榜(含内部调用)"),
    ("callerNonInternal", "调用榜(不含内部调用)"),
    ("calleeNonInternal", "被调用榜(不含内部调用)"),
]

if __name__ == "__main__":
    tok = login()
    print("token:", (tok or "")[:20], "...\n")
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))) + datetime.timedelta(hours=1)
    endAt = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S") + "%2B08:00"
    startAt = "2026-09-24T00:00:00%2B08:00"

    results = {}
    for kind, label in BOARDS:
        p = f"/internal/matched-stats/top?startAt={startAt}&endAt={endAt}&n=200"
        st, b = req(p, token=tok)
        items = (b.get(kind) or []) if isinstance(b, dict) else []
        print("=" * 90)
        print(f"【{label}】  ({kind})  HTTP={st}  榜单总条目={len(items)}")
        print("=" * 90)
        hits = []
        for i, it in enumerate(items):
            aic = str(it.get("aic") or "")
            if any(m in aic for m in MY) or TEAM in aic:
                hits.append((i + 1, aic, it.get("accessCount"), it.get("messageCount")))
        if not hits:
            print("   我的智能体：未上榜（0 次）")
        else:
            print(f"   {'名次':<6} {'完整编号 (AIC)':<52} access  msg")
            for rk, aic, ac, mc in hits:
                print(f"   第{rk:<4} {aic:<52} {ac}    {mc}")
        results[kind] = {"label": label, "total": len(items), "hits": hits}
        print()

    json.dump(results, open("rank_mine_result.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("已保存 -> rank_mine_result.json")
