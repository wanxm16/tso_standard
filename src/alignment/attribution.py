"""W5-D benchmark 召回失败归因

对每条 topK_5_topic_hit=False 的 query：
1. 从 query_intents.json 拿 required_fields（关键词）
2. 从 expected_tables 反查到 expected_vt_ids（通过 slot_definitions 里的 mapped_fields.table_en）
3. 对每个 expected VT，扫 required_fields 逐词：
   - 在该 VT 的任意 slot 的 name/cn_name/aliases/description 里有无 token 命中？
   - 有命中但该 slot 的 mapped_fields 为空 → `unmapped_field`
   - 无任何 slot 命中 → `missing_slot`（疑似缺槽位）
4. 综合归因，产出 output/evaluation_attribution.parquet
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[2]
EVAL_DETAILS = ROOT / "output" / "evaluation_details.parquet"
QUERY_INTENTS = ROOT / "output" / "query_intents.json"
SLOT_DEF = ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS = ROOT / "data" / "slot_library" / "base_slots.yaml"
OUTPUT = ROOT / "output" / "evaluation_attribution.parquet"


def load_base_slot_map() -> dict[str, dict]:
    with BASE_SLOTS.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {b["name"]: b for b in data.get("base_slots", [])}


def load_slot_index():
    """返回：
    - table_to_vts: table_en → set(vt_id)（通过 mapped_fields 反查）
    - vt_to_slots: vt_id → list[enriched slot dict]（base 引用已补全 cn_name/aliases/description）
    """
    with SLOT_DEF.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    base_map = load_base_slot_map()

    table_to_vts: dict[str, set[str]] = defaultdict(set)
    vt_to_slots: dict[str, list[dict]] = {}
    for vt in data["virtual_tables"]:
        vt_id = vt["vt_id"]
        enriched: list[dict] = []
        for slot in vt.get("slots", []):
            merged = dict(slot)
            if slot.get("from") == "base":
                ref = base_map.get(slot["name"])
                if ref:
                    for k in ("cn_name", "description", "aliases"):
                        if not merged.get(k) and ref.get(k):
                            merged[k] = ref[k]
            enriched.append(merged)
            for m in slot.get("mapped_fields") or []:
                t = m.get("table_en")
                if t:
                    table_to_vts[t].add(vt_id)
        vt_to_slots[vt_id] = enriched
    return table_to_vts, vt_to_slots


def keyword_matches_slot(keyword: str, slot: dict) -> bool:
    """关键词和 slot 的 name / cn_name / aliases / description 做双向 substring 匹配。

    为什么是双向：query_intents 里的关键词常常是 LLM 拼合的复合词
    （如 "人员身份证号" = "人员" + "身份证号"），而 slot aliases 是干净原子词
    （如 "身份证号"）。单向 `kw in haystack` 会漏（长串不可能在短串里），
    反向 `haystack in kw`（+ haystack 长度 ≥2 避免单字误匹）能救回这类场景。
    """
    kw = keyword.lower().strip()
    if not kw:
        return False
    haystacks = [
        (slot.get("name") or "").lower(),
        slot.get("cn_name") or "",
        slot.get("description") or "",
    ]
    haystacks.extend(slot.get("aliases") or slot.get("synonyms") or [])
    for h in haystacks:
        hs = str(h).lower()
        if not hs:
            continue
        if kw in hs:
            return True
        # 反向：slot alias 作为 query 关键词的子串（alias 长度 ≥2 避免单字噪声）
        if len(hs) >= 2 and hs in kw:
            return True
    return False


def analyze_query(query_row: dict, intent: dict, table_to_vts, vt_to_slots) -> dict:
    """单条 query 的归因"""
    required = intent.get("required_fields", []) if intent else []
    expected_tables = [t for t in (query_row.get("expected_tables") or "").split(",") if t]

    # 找 expected_vt_ids
    expected_vt_ids: set[str] = set()
    for t in expected_tables:
        expected_vt_ids |= table_to_vts.get(t, set())

    # 对每个 required keyword，看是否在 expected VT 的任一 slot 里命中
    missing_keywords: list[str] = []
    unmapped_keywords: list[dict] = []  # 命中 slot 但 mapped_fields 空
    hit_keywords: list[str] = []

    for kw in required:
        matched_any = False
        matched_mapped = False
        matched_slot_info = None
        for vt_id in expected_vt_ids:
            for slot in vt_to_slots.get(vt_id, []):
                if keyword_matches_slot(kw, slot):
                    matched_any = True
                    if slot.get("mapped_fields"):
                        matched_mapped = True
                    else:
                        matched_slot_info = {
                            "vt_id": vt_id,
                            "slot_name": slot.get("name"),
                            "cn_name": slot.get("cn_name"),
                        }
        if not matched_any:
            missing_keywords.append(kw)
        elif matched_any and not matched_mapped and matched_slot_info:
            unmapped_keywords.append({"keyword": kw, **matched_slot_info})
        else:
            hit_keywords.append(kw)

    # 决定 failure_type：先看 top5 是否命中（命中场景语义和失败场景不同）
    top5_hit = bool(query_row.get("topK_5_topic_hit"))
    if top5_hit:
        # 命中场景：失败诊断不适用，只区分"完全覆盖" vs "部分覆盖（aliases 仍有缺口）"
        if missing_keywords or unmapped_keywords:
            failure_type = "partial_coverage"
        else:
            failure_type = "ok"
    else:
        # 失败场景（top5 未命中）：按原优先级归因
        if missing_keywords:
            failure_type = "missing_slot"
        elif unmapped_keywords:
            failure_type = "unmapped_field"
        elif not required:
            failure_type = "intent_miss"
        else:
            failure_type = "other"

    return {
        "query_text": query_row.get("query_text"),
        "expected_tables": query_row.get("expected_tables"),
        "expected_vt_ids": sorted(expected_vt_ids),
        "top1_hit": bool(query_row.get("topK_1_topic_hit")),
        "top5_hit": bool(query_row.get("topK_5_topic_hit")),
        "top10_hit": bool(query_row.get("topK_10_topic_hit")),
        "top5_recall": float(query_row.get("topK_5_table_recall") or 0),
        "channel": query_row.get("channel"),
        "failure_type": failure_type,
        "required_keywords": required,
        "missing_keywords": missing_keywords,
        "unmapped_keywords": unmapped_keywords,
        "hit_keywords": hit_keywords,
        "suggested_slot_names": missing_keywords,
    }


def main() -> int:
    if not EVAL_DETAILS.exists():
        print(f"❌ {EVAL_DETAILS} 不存在。先跑 run_pipeline.py --from evaluation 或单独的 evaluator")
        return 1
    if not QUERY_INTENTS.exists():
        print(f"❌ {QUERY_INTENTS} 不存在")
        return 1

    df = pd.read_parquet(EVAL_DETAILS)
    with QUERY_INTENTS.open(encoding="utf-8") as f:
        intents_data = json.load(f)
    intents_by_query = {i["query_text"]: i.get("intent", {}) for i in intents_data["query_intents"]}

    table_to_vts, vt_to_slots = load_slot_index()

    rows = []
    for _, r in df.iterrows():
        row_dict = r.to_dict()
        intent = intents_by_query.get(row_dict.get("query_text"), {})
        attr = analyze_query(row_dict, intent, table_to_vts, vt_to_slots)
        rows.append(attr)

    out_df = pd.DataFrame(rows)
    # 将 list/dict 列转成 JSON 字符串便于 parquet 存储（不然 pyarrow 类型不稳）
    for col in ["expected_vt_ids", "required_keywords", "missing_keywords",
                "unmapped_keywords", "hit_keywords", "suggested_slot_names"]:
        out_df[col] = out_df[col].apply(json.dumps, ensure_ascii=False)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(OUTPUT)

    # 简报
    total = len(out_df)
    failed = out_df[out_df["top5_hit"] == False]  # noqa: E712
    by_type = failed["failure_type"].value_counts().to_dict() if not failed.empty else {}
    print(f"✅ 归因完成 → {OUTPUT}")
    print(f"  总 query: {total} · top5 失败: {len(failed)} ({len(failed)/max(total,1)*100:.1f}%)")
    print(f"  失败类型分布（top5 fail）: {by_type}")

    # 按 VT 聚合："疑似缺槽位最多"的 top 10 VT
    vt_missing_count: dict[str, int] = defaultdict(int)
    for _, r in failed.iterrows():
        for vt in json.loads(r["expected_vt_ids"]):
            if json.loads(r["missing_keywords"]):
                vt_missing_count[vt] += len(json.loads(r["missing_keywords"]))
    top_vts = sorted(vt_missing_count.items(), key=lambda x: -x[1])[:10]
    print(f"  Top VT（疑似缺槽位最多，缺关键词计数）:")
    for vt, c in top_vts:
        print(f"    {vt}: {c}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
