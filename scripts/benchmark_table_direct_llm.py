"""方案 B 升级版：给每张物理表做 LLM 扩展（类似 I-07 对 VT 做的），再跑 embedding 匹配。

对比公平性：
  - 清洗表文本（不放样例值，只放字段名+注释+表注释）
  - LLM 为每张表生成：table_summary / typical_questions / topic_aliases
  - 构造 recall_text = 表中文名 + 表注释 + 字段列表 + LLM 扩展
  - 用同样 text-embedding-v3 + cosine 匹配 60 CSV
"""
from __future__ import annotations

import concurrent.futures
import json
import sys
import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat as chat_client, embed as embed_client  # noqa: E402
from src.pipeline.evaluator import load_benchmark  # noqa: E402


DDL_CSV = REPO_ROOT / "data" / "phrase_2" / "二期_DDL_all_with_sample.csv"
OUT_ENRICHMENT = REPO_ROOT / "output" / "table_enrichment.json"
OUT_COMPARE = REPO_ROOT / "output" / "table_direct_comparison.md"


SYSTEM_PROMPT = """你是 text2sql 表召回助手。任务：为一张数据库物理表生成"召回描述"，帮助系统能在用户自然语言问题中找到它。

输出三部分：
1. summary：一句话概括这张表记录了什么业务
2. typical_questions：3-5 条用户可能用到这张表的自然语言提问
3. topic_aliases：3-5 个这张表业务主题的同义表达

严格输出 JSON：{"summary": "...", "typical_questions": [...], "topic_aliases": [...]}"""


USER_TEMPLATE = """## 物理表信息

- 英文名: {table_en}
- 中文名: {table_cn}
- 表注释: {table_comment}

## 主要字段（前 20 个）

{field_list}

请生成 JSON。"""


def build_table_summary_input(sub: pd.DataFrame) -> dict:
    """从 DDL 子集构造给 LLM 的输入。"""
    table_en = str(sub["table"].iloc[0] or "")
    table_cn = str(sub["table_cn_name"].iloc[0] or "")
    table_comment = str(sub["table_comment"].iloc[0] or "")
    # 取前 20 个字段名+注释
    field_lines = []
    for _, r in sub.head(20).iterrows():
        fn = str(r.get("field", "") or "")
        fc = str(r.get("comment", "") or "")
        if fn and fn.lower() not in {"rn", "dt", "ds", "pt", "etl_time"}:
            field_lines.append(f"- {fn}: {fc}")
    return {
        "table_en": table_en,
        "table_cn": table_cn,
        "table_comment": table_comment[:200],
        "field_list": "\n".join(field_lines[:20]),
    }


def call_llm_for_table(payload: dict) -> dict:
    user = USER_TEMPLATE.format(**payload)
    raw = chat_client(
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}],
        temperature=0.0,
        json_mode=True,
        use_cache=True,
    )
    try:
        result = json.loads(raw)
    except Exception:
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)[1]
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip("`").strip()
        try:
            result = json.loads(stripped)
        except Exception:
            return {"summary": "", "typical_questions": [], "topic_aliases": []}
    return {
        "summary": result.get("summary", "") or "",
        "typical_questions": [q for q in (result.get("typical_questions") or []) if q],
        "topic_aliases": [a for a in (result.get("topic_aliases") or []) if a],
    }


def enrich_all_tables(ddl: pd.DataFrame, concurrency: int = 10) -> dict[str, dict]:
    table_payloads: dict[str, dict] = {}
    for table, sub in ddl.groupby("table"):
        table_payloads[table] = build_table_summary_input(sub)

    print(f"共 {len(table_payloads)} 张表需要 LLM 扩展")
    lock = threading.Lock()
    counter = {"done": 0}
    t0 = time.time()
    results: dict[str, dict] = {}

    def worker(table: str, payload: dict) -> tuple[str, dict]:
        try:
            r = call_llm_for_table(payload)
        except Exception as exc:
            r = {"summary": "", "typical_questions": [], "topic_aliases": [], "_error": str(exc)}
        with lock:
            counter["done"] += 1
            if counter["done"] % 30 == 0 or counter["done"] == len(table_payloads):
                print(f"  LLM {counter['done']}/{len(table_payloads)} ({time.time()-t0:.1f}s)", flush=True)
        return table, r

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = [exe.submit(worker, t, p) for t, p in table_payloads.items()]
        for f in concurrent.futures.as_completed(futures):
            t, r = f.result()
            results[t] = r

    return results


def build_table_recall_text(table_en: str, payload: dict, enrichment: dict) -> str:
    """干净的 recall_text：表元信息 + 字段名注释 + LLM 扩展。"""
    parts = [
        payload.get("table_cn", ""),
        payload.get("table_comment", ""),
    ]
    # 字段
    for line in payload.get("field_list", "").splitlines():
        # line 形如 "- xm: 姓名"
        parts.append(line.lstrip("- "))
    # LLM 扩展
    e = enrichment.get(table_en, {})
    if e.get("summary"):
        parts.append(e["summary"])
    parts.extend(e.get("typical_questions", []) or [])
    parts.extend(e.get("topic_aliases", []) or [])
    return " ".join(p for p in parts if p).strip()


def run_compare() -> None:
    ddl = pd.read_csv(DDL_CSV, encoding="utf-8")

    # 1) LLM 扩展每张表
    if OUT_ENRICHMENT.exists():
        enrichment = json.loads(OUT_ENRICHMENT.read_text(encoding="utf-8"))
        print(f"复用 table_enrichment.json: {len(enrichment)} 条")
    else:
        enrichment = enrich_all_tables(ddl, concurrency=10)
        OUT_ENRICHMENT.write_text(json.dumps(enrichment, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"写入 {OUT_ENRICHMENT}")

    # 2) 为每张表构造干净 recall_text
    payloads: dict[str, dict] = {}
    for table, sub in ddl.groupby("table"):
        payloads[table] = build_table_summary_input(sub)

    table_names = list(payloads.keys())
    recall_texts = [build_table_recall_text(t, payloads[t], enrichment) for t in table_names]
    print(f"生成 {len(table_names)} 条 recall_text")

    # 3) benchmark 60 CSV
    bench = load_benchmark(source="csv")
    print(f"benchmark: {len(bench)} 条 query")

    # 4) embedding
    print("embed tables ...")
    v_emb = np.array(embed_client(recall_texts, use_cache=True))
    print("embed queries ...")
    q_emb = np.array(embed_client([b["query_text"] for b in bench], use_cache=True))

    qn = np.linalg.norm(q_emb, axis=1, keepdims=True)
    vn = np.linalg.norm(v_emb, axis=1, keepdims=True)
    q_emb = q_emb / np.where(qn == 0, 1, qn)
    v_emb = v_emb / np.where(vn == 0, 1, vn)
    sim = q_emb @ v_emb.T

    # 5) 评估
    topks = [1, 3, 5, 10]
    metrics = {k: {"topic_hit": [], "recall": []} for k in topks}
    for qi, b in enumerate(bench):
        exp = set(b.get("expected_tables", []))
        if not exp:
            continue
        order = (-sim[qi]).argsort()
        for k in topks:
            topk_tables = {table_names[order[j]] for j in range(min(k, len(order)))}
            hit = bool(topk_tables & exp)
            recall = len(topk_tables & exp) / len(exp)
            metrics[k]["topic_hit"].append(1.0 if hit else 0.0)
            metrics[k]["recall"].append(recall)

    # 6) 输出
    print("\n## 方案 B+（LLM 扩展后直接表匹配）60 CSV 结果\n")
    print("| topK | topic_hit | table_recall |")
    print("| --- | --- | --- |")
    table_b_plus = {}
    for k in topks:
        th = float(np.mean(metrics[k]["topic_hit"])) * 100
        tr = float(np.mean(metrics[k]["recall"])) * 100
        table_b_plus[k] = (th, tr)
        print(f"| @{k} | {th:.1f}% | {tr:.1f}% |")

    # 加载方案 A / 原 B 对比
    ev = json.loads((REPO_ROOT / "output" / "evaluation.json").read_text(encoding="utf-8"))
    method_a = ev["by_channel"]["embedding__csv"]["by_topk"]
    method_a_rerank = ev["by_channel"]["rerank__csv"]["by_topk"]

    report_lines = [
        "# 方案 A（VT 层） vs 方案 B（直接表匹配） 对比",
        "",
        "## 60 条 CSV benchmark 结果",
        "",
        "| 通路 | @1 | @3 | @5 | @10 | recall@5 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for label, m in [("方案 A（VT 层 embedding）", method_a),
                      ("方案 A（VT + rerank）", method_a_rerank)]:
        row = f"| {label}"
        for k in topks:
            row += f" | {m[f'k={k}']['topic_hit_rate']*100:.1f}%"
        row += f" | {method_a['k=5' if m is method_a else 'k=5']['table_recall_rate']*100:.1f}% |"
        # 上面写错了，手动修
        vals = []
        for k in topks:
            vals.append(f"{m[f'k={k}']['topic_hit_rate']*100:.1f}%")
        recall5 = m["k=5"]["table_recall_rate"] * 100
        report_lines.append(f"| {label} | {' | '.join(vals)} | {recall5:.1f}% |")
    # 方案 B+
    row_vals = [f"{table_b_plus[k][0]:.1f}%" for k in topks]
    report_lines.append(f"| 方案 B+（LLM 扩展后直接表） | {' | '.join(row_vals)} | {table_b_plus[5][1]:.1f}% |")

    # 查看旧方案 B（raw 文本）数据
    report_lines += [
        "",
        "## 方案 B 原版（未扩展）作为参考（手动记录）",
        "",
        "| topK | topic_hit | recall |",
        "| --- | --- | --- |",
        "| @1 | 16.7% | 8.3% |",
        "| @3 | 30.0% | 18.7% |",
        "| @5 | 43.3% | 26.9% |",
        "| @10 | 61.7% | 37.0% |",
        "",
        "## 结论",
        "",
        f"方案 B+ (LLM 扩展后) @5 = {table_b_plus[5][0]:.1f}% vs 方案 A = {method_a['k=5']['topic_hit_rate']*100:.1f}% (差距 {method_a['k=5']['topic_hit_rate']*100 - table_b_plus[5][0]:+.1f}%)",
    ]
    OUT_COMPARE.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\n报告: {OUT_COMPARE}")


if __name__ == "__main__":
    run_compare()
