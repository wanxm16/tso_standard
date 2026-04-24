"""I-04: 归槽决策 + 冲突检测 + LLM 兜底。

输入：slot_scores.parquet（长表）+ slot_scores_top3.parquet（宽表）+ field_features.parquet
输出：
  - output/field_normalization.parquet  每字段×VT 的归一化结果
  - output/review_queue.csv             需要人工审核的字段
  - output/field_normalization_diagnostic.md

决策阈值（按 I-03 实际分布调整，保留设计文档的三档结构）：
  - auto_accepted : top1 >= 0.65 且 (top1 - top2) >= 0.10
  - needs_review  : 0.45 <= top1 < 0.65
  - low_confidence: top1 < 0.45
  - conflict      : (top1 - top2) < 0.05 且 top1 >= 0.45（优先级最高）
"""
from __future__ import annotations

import concurrent.futures
import json
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat  # noqa: E402

FEATURES_PARQUET = REPO_ROOT / "output" / "field_features.parquet"
SCORES_PARQUET = REPO_ROOT / "output" / "slot_scores.parquet"
TOP3_PARQUET = REPO_ROOT / "output" / "slot_scores_top3.parquet"
SLOT_YAML = REPO_ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS_YAML = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"

OUT_PARQUET = REPO_ROOT / "output" / "field_normalization.parquet"
OUT_REVIEW = REPO_ROOT / "output" / "review_queue.csv"
OUT_DIAG = REPO_ROOT / "output" / "field_normalization_diagnostic.md"

# 人工审核结果（由 backend mark_new_slot/accept_top1 等写入），用于跳过已审字段的自动决策
REVIEWED_PARQUET = REPO_ROOT / "output" / "field_normalization_reviewed.parquet"
REVIEWED_DECISIONS_TAKE = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot"}


def load_reviewed_decisions() -> dict[tuple[str, str, str], dict]:
    """加载人工审核决策 → {(table_en, field_name, vt_id): {decision, decision_slot, reviewer_note}}"""
    if not REVIEWED_PARQUET.exists():
        return {}
    try:
        df = pd.read_parquet(REVIEWED_PARQUET)
    except Exception:
        return {}
    if df.empty:
        return {}
    out: dict[tuple[str, str, str], dict] = {}
    for _, r in df.iterrows():
        out[(str(r["table_en"]), str(r["field_name"]), str(r["vt_id"]))] = {
            "decision": str(r.get("decision") or ""),
            "decision_slot": str(r.get("decision_slot") or "") if pd.notna(r.get("decision_slot")) else "",
            "reviewer_note": str(r.get("reviewer_note") or ""),
        }
    return out


# ============ 阈值（按 I-03 实际分布调整）============

THRESHOLD_HIGH = 0.65
THRESHOLD_LOW = 0.45
GAP_CONFIRM = 0.10
GAP_CONFLICT = 0.05


# ============ 数据加载 ============


def load_slot_roles() -> dict[tuple[str, str], str]:
    """(vt_id, slot_name) → role"""
    data = yaml.safe_load(SLOT_YAML.read_text(encoding="utf-8"))
    base = {s["name"]: s for s in yaml.safe_load(BASE_SLOTS_YAML.read_text(encoding="utf-8"))["base_slots"]}
    result: dict[tuple[str, str], str] = {}
    for vt in data.get("virtual_tables", []):
        for slot in vt.get("slots", []) or []:
            role = slot.get("role", "")
            if not role and slot.get("from") == "base":
                role = base.get(slot.get("name", ""), {}).get("role", "")
            result[(vt["vt_id"], slot["name"])] = role
    return result


# ============ 决策核心 ============


def classify_status(top1_score: float, top2_score: float | None) -> str:
    t2 = top2_score if top2_score is not None and not np.isnan(top2_score) else 0.0
    gap = top1_score - t2
    if gap < GAP_CONFLICT and top1_score >= THRESHOLD_LOW:
        return "conflict"
    if top1_score >= THRESHOLD_HIGH and gap >= GAP_CONFIRM:
        return "auto_accepted"
    if top1_score >= THRESHOLD_LOW:
        return "needs_review"
    return "low_confidence"


# ============ 冲突检测（4 类）============


def detect_same_name_different_slots(top3: pd.DataFrame) -> set[tuple[str, str, str]]:
    """1. 同名不同义：同 (table_en, field_name) 在多个 VT 被归到不同 slot。"""
    result: set[tuple[str, str, str]] = set()
    grouped = top3.groupby(["table_en", "field_name"])
    for (t, f), g in grouped:
        slots = set(g["top1_slot"].dropna().tolist())
        if len(slots) > 1:
            for vt_id in g["vt_id"]:
                result.add((t, f, vt_id))
    return result


def detect_role_conflict(row: dict, slot_roles: dict) -> bool:
    """2. 同义不同角色：top1.role != top2.role 且两者都是强候选（分差小、分数高）。

    设计文档示例"本人身份证号 vs 关系人身份证号"：同字段被两个不同 role 的 slot 强争抢。
    严格条件：top1 >= 0.50、top2 >= 0.50、(top1 - top2) < 0.05。
    """
    if not row.get("top2_slot"):
        return False
    t1 = row.get("top1_score_embedding") or 0
    t2 = row.get("top2_score_embedding") or 0
    if t1 < 0.50 or t2 < 0.50 or (t1 - t2) >= 0.05:
        return False
    r1 = slot_roles.get((row["vt_id"], row["top1_slot"]), "")
    r2 = slot_roles.get((row["vt_id"], row["top2_slot"]), "")
    return bool(r1 and r2 and r1 != r2)


def detect_near_duplicate_slots(row: dict) -> bool:
    """4. 相似槽位并存：top1/top2 都 ≥ 0.60 且分差 < 0.05。"""
    t1 = row.get("top1_score_embedding") or 0
    t2 = row.get("top2_score_embedding") or 0
    return t1 >= 0.60 and t2 >= 0.60 and (t1 - t2) < 0.05


def detect_logical_type_conflict_in_slot(
    accepted_mapping: list[dict],
    field_features: pd.DataFrame,
) -> set[tuple[str, str]]:
    """3. 同槽位内 logical_type 冲突：归入同槽位的字段 sample_patterns 交集为空。

    返回 {(vt_id, slot_name)} 冲突集合。冲突槽位上的所有字段都会被标。
    """
    if not accepted_mapping:
        return set()

    by_slot: dict[tuple[str, str], list[set[str]]] = defaultdict(list)
    feat_lookup = {
        (r["table_en"], r["field_name"]): set(_to_list(r.get("sample_patterns")))
        for _, r in field_features.iterrows()
    }
    for row in accepted_mapping:
        patterns = feat_lookup.get((row["table_en"], row["field_name"]), set())
        # 剔除无区分度的 pattern
        patterns = {p for p in patterns if p not in {"all_null_or_empty"}}
        if patterns:
            by_slot[(row["vt_id"], row["selected_slot"])].append(patterns)

    conflicts: set[tuple[str, str]] = set()
    # 收紧规则：归入字段数 >=5 且 pattern 并集 >=6 种 且 无公共 pattern
    for key, pattern_sets in by_slot.items():
        if len(pattern_sets) < 5:
            continue
        common = pattern_sets[0]
        for ps in pattern_sets[1:]:
            common = common & ps
        if common:
            continue
        union: set[str] = set()
        for ps in pattern_sets:
            union |= ps
        if len(union) >= 6:
            conflicts.add(key)
    return conflicts


def _to_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    try:
        return list(v)
    except TypeError:
        return []


# ============ LLM 兜底 ============


LLM_SYSTEM_PROMPT = """你是数据治理专家。任务：为一个物理字段判定最合适的语义槽位。

背景：
- 系统基于规则+向量打分已经给出 top3 候选槽位，但当前字段进入兜底场景
- 你的判断只作为"附加证据"，不改变规则分数；review 时人工裁决

输出严格 JSON，字段与 schema 完全一致。"""


def call_llm_fallback(row: dict, scenario: str) -> dict:
    """调 qwen3-max 给出建议。"""
    top_candidates = []
    for i in (1, 2, 3):
        name = row.get(f"top{i}_slot")
        score = row.get(f"top{i}_score_embedding")
        if name:
            top_candidates.append({"slot": name, "score": round(float(score or 0), 3)})

    user = f"""## 字段信息
- 物理表: {row.get('table_en')}
- 字段名: {row.get('field_name')}
- 字段注释: {row.get('field_comment') or '(无)'}
- 所属虚拟表: {row.get('vt_id')}

## 兜底场景: {scenario}
  - A: 缩写字段名 + 注释缺失
  - B: 规则-样例冲突
  - C: 注释模糊
  - D: top1/top2 分差过小

## 候选槽位（规则+向量打分给出）
{json.dumps(top_candidates, ensure_ascii=False, indent=2)}

请给出建议：
1. 该字段是否归入某个候选槽位？若是，哪一个？
2. 或者判断该字段应新建槽位（slot 建议名 + 中文概念）？
3. 一句话说明理由。

输出 JSON:
{{
  "suggested_slot": "候选槽位名" 或 null,
  "propose_new_slot": {{"name": "xxx", "cn_name": "xxx"}} 或 null,
  "reason": "..."
}}"""
    raw = chat(
        messages=[
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        temperature=0.0,
        json_mode=True,
        use_cache=True,
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)[1]
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip("`").strip()
        try:
            return json.loads(stripped)
        except Exception:
            return {"suggested_slot": None, "reason": raw[:200]}


def detect_llm_trigger(row: dict, feat_row: dict) -> str | None:
    """识别 4 种 LLM 兜底场景。返回触发类型 A/B/C/D 或 None。"""
    field_name = row.get("field_name", "") or ""
    comment = (row.get("field_comment") or "").strip()
    top1 = row.get("top1_score_embedding") or 0
    top2 = row.get("top2_score_embedding") or 0

    # D: top1-top2 小分差（优先判定，这是最值得调 LLM 的场景）
    if top1 >= THRESHOLD_LOW and (top1 - top2) < GAP_CONFLICT:
        return "D"

    # A: 缩写字段名 + 注释缺失
    is_abbrev = bool(field_name) and len(field_name) <= 6 and re.fullmatch(r"[a-z_]+", field_name.lower()) is not None
    if is_abbrev and not comment:
        return "A"

    # C: 注释模糊（长但多关键词类命中）
    kws = feat_row.get("comment_keywords", [])
    kws = _to_list(kws)
    if len(comment) > 15 and len(kws) >= 3:
        return "C"

    # B: 规则-样例冲突
    # 条件：comment 命中某类关键词（如 "id_card"），但 sample_patterns 不含对应 pattern
    pattern_hits = set(_to_list(feat_row.get("sample_patterns")))
    if "id_card" in kws and not (pattern_hits & {"id_card_18", "id_card_15"}):
        return "B"
    if "phone" in kws and not (pattern_hits & {"cn_mobile"}):
        return "B"

    return None


# ============ 主流程 ============


def run_decisions(enable_llm: bool = True, limit_vt_id: str | None = None) -> None:
    print("加载输入...")
    features = pd.read_parquet(FEATURES_PARQUET)
    top3 = pd.read_parquet(TOP3_PARQUET)
    if limit_vt_id:
        top3 = top3[top3["vt_id"] == limit_vt_id].copy()
    # field 特征查询表
    feat_by_key = {
        (r["table_en"], r["field_name"]): r.to_dict()
        for _, r in features.iterrows()
    }
    slot_roles = load_slot_roles()
    print(f"  字段数: {len(top3)}")
    print(f"  槽位角色映射: {len(slot_roles)}")

    # 加载人工审核决策（已审字段会被直接采纳，跳过冲突检测和 LLM 兜底）
    reviewed = load_reviewed_decisions()
    if reviewed:
        print(f"  已加载人工审核决策: {len(reviewed)} 条")

    # top3 parquet 里列名是 top1_score_tfidf / top1_score_embedding，统一抽一份 rows
    rows = top3.to_dict("records")
    # 添加 comment 字段（从 features 取）
    for r in rows:
        feat = feat_by_key.get((r["table_en"], r["field_name"]), {})
        if "field_comment" not in r or not r.get("field_comment"):
            r["field_comment"] = feat.get("field_comment", "")

    # 1. 冲突类型 1：同名不同义
    print("冲突检测 1/4：同名不同义...")
    same_name_conflict = detect_same_name_different_slots(top3)
    print(f"  命中: {len(same_name_conflict)} 个 (table, field, vt)")

    # 2. 按行规则计算
    print("应用归槽决策 + 逐行冲突检测...")
    for r in rows:
        # —— 已审字段：直接采纳人工决策，跳过自动冲突检测 ——
        rev = reviewed.get((str(r["table_en"]), str(r["field_name"]), str(r["vt_id"])))
        if rev:
            decision = rev["decision"]
            if decision in REVIEWED_DECISIONS_TAKE and rev["decision_slot"]:
                # 已审且确认归属：覆盖 top1_slot 为 decision_slot
                r["top1_slot"] = rev["decision_slot"]
                r["review_status"] = "manual_new" if decision == "mark_new_slot" else "manual"
                r["conflict_types"] = []
                r["_skip_llm"] = True  # 跳过 LLM 兜底标记
                continue
            elif decision == "mark_noise":
                r["review_status"] = "noise"
                r["conflict_types"] = []
                r["_skip_llm"] = True
                continue
            elif decision == "skip":
                r["review_status"] = "skipped"
                r["conflict_types"] = []
                r["_skip_llm"] = True
                continue
            # 其他未知 decision 类型 → 走自动归一兜底（理论上不会到这）

        # —— 未审字段：走原有自动决策逻辑 ——
        top1 = r.get("top1_score_embedding") or 0
        top2 = r.get("top2_score_embedding") or 0
        status = classify_status(top1, top2)

        conflicts: list[str] = []
        if (r["table_en"], r["field_name"], r["vt_id"]) in same_name_conflict:
            conflicts.append("same_name_different_slots")
        if detect_role_conflict(r, slot_roles):
            conflicts.append("role_conflict")
        if detect_near_duplicate_slots(r):
            conflicts.append("near_duplicate_slots")

        if conflicts and status not in ("conflict",):
            # 有 conflict 标记时，状态升级为 conflict（除非已是 low_confidence 要保留）
            if status != "low_confidence":
                status = "conflict"

        r["review_status"] = status
        r["conflict_types"] = conflicts

    # 3. 冲突类型 3：同槽位内 logical_type 冲突
    print("冲突检测 3/4：同槽位 logical_type 冲突...")
    accepted = [
        {"table_en": r["table_en"], "field_name": r["field_name"],
         "vt_id": r["vt_id"], "selected_slot": r["top1_slot"]}
        for r in rows if r["review_status"] in ("auto_accepted", "needs_review")
    ]
    type_conflict_slots = detect_logical_type_conflict_in_slot(accepted, features)
    if type_conflict_slots:
        type_conflict_rows = 0
        for r in rows:
            if (r["vt_id"], r["top1_slot"]) in type_conflict_slots:
                if "logical_type_conflict" not in r["conflict_types"]:
                    r["conflict_types"].append("logical_type_conflict")
                    type_conflict_rows += 1
                    if r["review_status"] == "auto_accepted":
                        r["review_status"] = "conflict"
        print(f"  命中槽位数: {len(type_conflict_slots)}, 波及行: {type_conflict_rows}")
    else:
        print("  无命中")

    # 4. LLM 兜底
    if enable_llm:
        print("识别 LLM 兜底触发场景...")
        triggers: list[tuple[int, str]] = []
        for i, r in enumerate(rows):
            if r["review_status"] == "auto_accepted":
                continue  # auto 不需要兜底
            if r.get("_skip_llm"):
                continue  # 已人工审核，跳过 LLM 兜底
            feat = feat_by_key.get((r["table_en"], r["field_name"]), {})
            trigger = detect_llm_trigger(r, feat)
            if trigger:
                triggers.append((i, trigger))
        print(f"  触发场景数: {len(triggers)}")

        # 并发调 LLM
        concurrency = 10
        t0 = time.time()

        def call_one(idx_trigger):
            idx, trig = idx_trigger
            r = rows[idx]
            try:
                return idx, trig, call_llm_fallback(r, trig)
            except Exception as exc:
                return idx, trig, {"suggested_slot": None, "reason": f"LLM error: {exc}"}

        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as exe:
            futures = [exe.submit(call_one, it) for it in triggers]
            for f in concurrent.futures.as_completed(futures):
                idx, trig, result = f.result()
                rows[idx]["applied_llm"] = True
                rows[idx]["llm_trigger"] = trig
                rows[idx]["llm_suggested_slot"] = result.get("suggested_slot")
                rows[idx]["llm_propose_new_slot"] = result.get("propose_new_slot")
                rows[idx]["llm_reason"] = result.get("reason", "")
                done += 1
                if done % 50 == 0 or done == len(triggers):
                    print(f"  LLM 兜底 {done}/{len(triggers)} ({time.time()-t0:.1f}s)")
    else:
        print("LLM 兜底已禁用 (--no-llm)")

    # 组装最终 DataFrame
    print("组装输出...")
    out_rows = []
    for r in rows:
        out_rows.append({
            "table_en": r["table_en"],
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment", ""),
            "vt_id": r["vt_id"],
            "selected_slot": r["top1_slot"],
            "selected_score": round(float(r.get("top1_score_embedding") or 0), 4),
            "selected_slot_from": r.get("top1_slot_from", ""),
            "review_status": r["review_status"],
            "conflict_types": r["conflict_types"],
            "top1_slot": r.get("top1_slot"),
            "top1_score": round(float(r.get("top1_score_embedding") or 0), 4),
            "top2_slot": r.get("top2_slot"),
            "top2_score": round(float(r.get("top2_score_embedding") or 0), 4) if r.get("top2_score_embedding") else None,
            "top3_slot": r.get("top3_slot"),
            "top3_score": round(float(r.get("top3_score_embedding") or 0), 4) if r.get("top3_score_embedding") else None,
            "score_gap_top1_top2": round(
                float((r.get("top1_score_embedding") or 0) - (r.get("top2_score_embedding") or 0)), 4
            ),
            "applied_llm": bool(r.get("applied_llm", False)),
            "llm_trigger": r.get("llm_trigger"),
            "llm_suggested_slot": r.get("llm_suggested_slot"),
            "llm_propose_new_slot": json.dumps(r.get("llm_propose_new_slot")) if r.get("llm_propose_new_slot") else None,
            "llm_reason": r.get("llm_reason"),
        })
    df = pd.DataFrame(out_rows)

    # limit_vt_id 模式：保留其他 VT 的行
    if limit_vt_id and OUT_PARQUET.exists():
        existing = pd.read_parquet(OUT_PARQUET)
        kept = existing[existing["vt_id"] != limit_vt_id]
        df = pd.concat([kept, df], ignore_index=True)
        print(f"  [merge] field_normalization: 保留 {len(kept)} + 新增 {len(df) - len(kept)} 行")

    # 写产物
    df.to_parquet(OUT_PARQUET, index=False)

    # review_queue.csv（不含 auto_accepted）
    review_df = df[df["review_status"] != "auto_accepted"].copy()
    priority_order = {"conflict": 0, "low_confidence": 1, "needs_review": 2}
    review_df["_priority"] = review_df["review_status"].map(priority_order)
    review_df = review_df.sort_values(["_priority", "selected_score"], ascending=[True, False])
    review_df = review_df.drop(columns=["_priority"])
    review_df.to_csv(OUT_REVIEW, index=False, encoding="utf-8-sig")

    write_diagnostic(df)

    # 摘要输出
    status_counts = df["review_status"].value_counts().to_dict()
    print(f"\n=== I-04 完成 ===")
    print(f"字段×VT 行数: {len(df)}")
    print(f"review_status 分布: {json.dumps(status_counts, ensure_ascii=False)}")
    print(f"LLM 兜底命中: {int(df['applied_llm'].sum())}")
    print(f"field_normalization.parquet: {OUT_PARQUET}")
    print(f"review_queue.csv: {OUT_REVIEW}")
    print(f"诊断: {OUT_DIAG}")


def write_diagnostic(df: pd.DataFrame) -> None:
    lines: list[str] = [
        "# I-04 归槽决策诊断",
        "",
        "## 总体",
        "",
        f"- 字段×VT 行数: {len(df)}",
        f"- 独立字段数: {df['field_name'].nunique()}",
        f"- 独立 VT 数: {df['vt_id'].nunique()}",
        "",
        "## review_status 分布",
        "",
    ]
    for status, c in df["review_status"].value_counts().items():
        pct = c / len(df) * 100
        lines.append(f"- {status}: {c} ({pct:.1f}%)")
    lines.append("")

    # 冲突类型分布
    lines += ["## 冲突类型分布", ""]
    conflict_counter: Counter[str] = Counter()
    for cts in df["conflict_types"]:
        for ct in (cts if isinstance(cts, (list, tuple, np.ndarray)) else []):
            conflict_counter[ct] += 1
    for ct, c in conflict_counter.most_common():
        lines.append(f"- {ct}: {c}")
    lines.append("")

    # LLM 兜底
    lines += ["## LLM 兜底使用情况", ""]
    llm_count = int(df["applied_llm"].sum())
    lines.append(f"- 触发字段数: {llm_count}")
    if llm_count:
        by_trig = df[df["applied_llm"]]["llm_trigger"].value_counts()
        for t, c in by_trig.items():
            lines.append(f"  - {t}: {c}")
        lines.append("")
        # LLM 建议新槽位的统计
        propose_new = df[df["llm_propose_new_slot"].notna() & (df["llm_propose_new_slot"] != "null")]
        lines.append(f"- LLM 建议新建槽位的字段数: {len(propose_new)}")
    lines.append("")

    # auto_accepted top 5 slot
    lines += ["## auto_accepted top1_slot 分布 Top 10", ""]
    auto = df[df["review_status"] == "auto_accepted"]
    for slot, c in auto["selected_slot"].value_counts().head(10).items():
        lines.append(f"- {slot}: {c}")
    lines.append("")

    # low_confidence 热点字段抽样
    lines += ["## low_confidence 字段抽样（前 15 条）", ""]
    low = df[df["review_status"] == "low_confidence"].head(15)
    for _, r in low.iterrows():
        lines.append(f"- `{r['field_name']}` ({r['field_comment'][:30]}) | vt={r['vt_id']} | top1={r['top1_slot']} {r['top1_score']}")
    lines.append("")

    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


def main(enable_llm: bool = True, limit_vt_id: str | None = None) -> None:
    run_decisions(enable_llm=enable_llm, limit_vt_id=limit_vt_id)
