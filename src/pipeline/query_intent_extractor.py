"""I-08b: 把 benchmark query 用 LLM 结构化成意图 + 所需字段。

核心思路：原始 query 是自然语言问句，直接 embed 后与 VT 的"字段词表"语义不对称。
让 LLM 先拆成 intent_topics + required_fields，再去匹配 VT。
"""
from __future__ import annotations

import concurrent.futures
import json
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat  # noqa: E402
from src.pipeline.evaluator import load_benchmark  # noqa: E402


OUT_JSON = REPO_ROOT / "output" / "query_intents.json"


SYSTEM_PROMPT = """你是 text2sql 表召回助手。用户会问一个需要查询数据库的自然语言问题。
你的任务是把问题拆成结构化意图，帮助系统找到正确的候选表。

要求：
1. intent_topics：2-4 个业务主题短语（如"人员主档"、"加油行为"、"出入境记录"、"共现关系"）
2. required_fields：具体字段的中文名列表（如"身份证号"、"加油时间"、"加油站地点"）
3. filter_conditions：筛选条件（time_range / location / other 列表）

严格输出 JSON 对象，不要任何额外解释或 markdown。"""


USER_TEMPLATE = """问题：{query}

输出严格 JSON：
{{
  "intent_topics": ["...", "..."],
  "required_fields": ["...", "..."],
  "filter_conditions": {{
    "time_range": "...",
    "location": "...",
    "other": ["..."]
  }}
}}"""


def extract_intent(query: str) -> dict:
    raw = chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_TEMPLATE.format(query=query)},
        ],
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
            result = {"intent_topics": [], "required_fields": [], "filter_conditions": {}}

    # 标准化
    return {
        "intent_topics": [t for t in (result.get("intent_topics") or []) if t],
        "required_fields": [f for f in (result.get("required_fields") or []) if f],
        "filter_conditions": result.get("filter_conditions") or {},
    }


def build_query_intents(concurrency: int = 8, benchmark_source: str = "json", flag_filter: float | None = None, merge_existing: bool = True) -> list[dict]:
    bench = load_benchmark(source=benchmark_source, flag_filter=flag_filter)
    print(f"共 {len(bench)} 条 query 需要结构化 (source={benchmark_source} flag={flag_filter})")

    results: dict[int, dict] = {}
    lock = threading.Lock()
    counter = {"done": 0}
    t0 = time.time()

    def worker(idx: int, record: dict) -> tuple[int, dict]:
        try:
            intent = extract_intent(record["query_text"])
        except Exception as exc:
            intent = {"intent_topics": [], "required_fields": [], "filter_conditions": {},
                      "_error": str(exc)}
        with lock:
            counter["done"] += 1
            print(f"  [{counter['done']}/{len(bench)}] {record['query_text'][:40]}... "
                  f"→ topics={len(intent.get('intent_topics', []))} "
                  f"fields={len(intent.get('required_fields', []))} "
                  f"({time.time()-t0:.1f}s)", flush=True)
        return idx, intent

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = [exe.submit(worker, i, r) for i, r in enumerate(bench)]
        for f in concurrent.futures.as_completed(futures):
            idx, intent = f.result()
            results[idx] = intent

    out = []
    for i, record in enumerate(bench):
        out.append({
            "query_text": record["query_text"],
            "expected_tables": record.get("expected_tables", []),
            "intent": results.get(i, {}),
        })
    return out


def main(benchmark_source: str = "json", flag_filter: float | None = None) -> None:
    intents = build_query_intents(benchmark_source=benchmark_source, flag_filter=flag_filter)

    # 合并已有的 intents（按 query_text 去重，新数据覆盖旧的）
    if OUT_JSON.exists():
        try:
            existing = json.loads(OUT_JSON.read_text(encoding="utf-8")).get("query_intents", [])
        except Exception:
            existing = []
        by_q = {it["query_text"]: it for it in existing}
        for it in intents:
            by_q[it["query_text"]] = it
        merged = list(by_q.values())
    else:
        merged = intents

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump({"query_intents": merged}, f, ensure_ascii=False, indent=2)

    # 简要摘要
    print(f"\n=== I-08b 完成 ===")
    print(f"本次新增/更新 intents: {len(intents)}")
    print(f"总 intents（合并去重后）: {len(merged)}")
    print(f"示例（前 2 条）：")
    for it in intents[:2]:
        print(f"  query: {it['query_text'][:50]}...")
        print(f"    intent_topics: {it['intent']['intent_topics']}")
        print(f"    required_fields: {it['intent']['required_fields']}")
    print(f"JSON: {OUT_JSON}")


if __name__ == "__main__":
    main()
