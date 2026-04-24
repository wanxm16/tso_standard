"""W5-0 槽位命名诊断

产出 output/naming_diagnosis.yaml，供 UI /naming/diagnosis 展示 + 后续 A/B/C 对齐脚本消费。

统计维度：
- 跨 VT 重名分布（extended slot_name 出现次数 ≥2）
- L2 内 extended 重名冲突（scope 乙硬标准的反例）
- base-extended 冲突（base_slots 里已有 name，但某 VT extended 同名）
- 同名异义候选（同 name 但 cn_name 不同）
- VT 已用字段数分布（给 W5-F 阈值校准用）
"""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[2]
SLOT_DEF = ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS = ROOT / "data" / "slot_library" / "base_slots.yaml"
FIELD_FEATURES = ROOT / "output" / "field_features.parquet"
OUTPUT = ROOT / "output" / "naming_diagnosis.yaml"


def load_slot_definitions() -> dict[str, Any]:
    with SLOT_DEF.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_base_slot_names() -> set[str]:
    with BASE_SLOTS.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {s["name"] for s in data.get("base_slots", [])}


def collect_extended_index(slot_def: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """name → [{vt_id, cn_name, description, l1, l2, role, synonyms}] for from==extended"""
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for vt in slot_def["virtual_tables"]:
        vt_id = vt["vt_id"]
        l1, l2 = (vt.get("l2_path") or [None, None])[:2]
        for slot in vt.get("slots", []):
            if slot.get("from") != "extended":
                continue
            index[slot["name"]].append({
                "vt_id": vt_id,
                "cn_name": slot.get("cn_name"),
                "description": slot.get("description") or "",
                "role": slot.get("role"),
                "synonyms": slot.get("aliases") or slot.get("synonyms") or [],
                "l1": l1,
                "l2": l2,
            })
    return index


def compute_vt_used_field_count(slot_def: dict[str, Any]) -> dict[str, int]:
    """每 VT 的已用字段数（from field_features.usage_count > 0 + related_vt_ids）"""
    if not FIELD_FEATURES.exists():
        return {}
    df = pd.read_parquet(FIELD_FEATURES)
    # 基本过滤：用过的字段
    used = df[df["usage_count"] > 0]
    # 展开 related_vt_ids（list）
    counts: dict[str, int] = defaultdict(int)
    for _, row in used.iterrows():
        vts = row.get("related_vt_ids")
        if vts is None or (hasattr(vts, '__len__') and len(vts) == 0):
            continue
        for vt_id in vts:
            counts[str(vt_id)] += 1
    # 保证所有 VT 都有值（没命中的为 0）
    for vt in slot_def["virtual_tables"]:
        counts.setdefault(vt["vt_id"], 0)
    return dict(counts)


def histogram(values: list[int], bins: list[tuple[int, int | None]]) -> list[dict[str, Any]]:
    out = []
    for lo, hi in bins:
        if hi is None:
            n = sum(1 for v in values if v >= lo)
            label = f">={lo}"
        else:
            n = sum(1 for v in values if lo <= v < hi)
            label = f"{lo}-{hi-1}"
        out.append({"range": label, "count": n})
    return out


def main() -> int:
    slot_def = load_slot_definitions()
    base_names = load_base_slot_names()
    ext_index = collect_extended_index(slot_def)

    # —— 重名分布（跨 VT 出现 ≥2）——
    repeats: list[dict[str, Any]] = []
    for name, entries in sorted(ext_index.items()):
        if len(entries) < 2:
            continue
        repeats.append({
            "name": name,
            "vt_count": len(entries),
            "cn_name_variants": sorted({e["cn_name"] for e in entries if e["cn_name"]}),
        })
    repeats.sort(key=lambda x: -x["vt_count"])

    # —— L2 内重名冲突 ——
    l2_conflicts: list[dict[str, Any]] = []
    for name, entries in ext_index.items():
        by_l2: dict[tuple, list[dict]] = defaultdict(list)
        for e in entries:
            by_l2[(e["l1"], e["l2"])].append(e)
        for (l1, l2), group in by_l2.items():
            if len(group) < 2:
                continue
            l2_conflicts.append({
                "l1": l1,
                "l2": l2,
                "name": name,
                "vt_count": len(group),
                "cn_name_variants": sorted({e["cn_name"] for e in group if e["cn_name"]}),
                "members": [{"vt_id": e["vt_id"], "cn_name": e["cn_name"]} for e in group],
            })
    l2_conflicts.sort(key=lambda x: (x["l1"] or "", x["l2"] or "", -x["vt_count"]))

    # —— base-extended 冲突 ——
    base_ext_conflicts = []
    for name, entries in ext_index.items():
        if name in base_names:
            base_ext_conflicts.append({
                "name": name,
                "vt_count": len(entries),
                "members": [{"vt_id": e["vt_id"], "cn_name": e["cn_name"]} for e in entries],
            })
    base_ext_conflicts.sort(key=lambda x: -x["vt_count"])

    # —— 同名异义候选（cn_name 不同）——
    homonyms = []
    for name, entries in ext_index.items():
        cn_groups: dict[str, list[dict]] = defaultdict(list)
        for e in entries:
            cn = e["cn_name"] or "(无)"
            cn_groups[cn].append(e)
        if len(cn_groups) < 2:
            continue
        homonyms.append({
            "name": name,
            "cn_variant_count": len(cn_groups),
            "total_vt_count": len(entries),
            "variants": [
                {
                    "cn_name": cn,
                    "vt_count": len(members),
                    "descriptions": sorted({m["description"] for m in members if m["description"]})[:3],
                    "members": [{"vt_id": m["vt_id"], "l1": m["l1"], "l2": m["l2"]} for m in members],
                }
                for cn, members in sorted(cn_groups.items(), key=lambda x: -len(x[1]))
            ],
        })
    homonyms.sort(key=lambda x: -x["cn_variant_count"])

    # —— VT 已用字段直方图（给 W5-F 校准）——
    used_count_by_vt = compute_vt_used_field_count(slot_def)
    used_values = list(used_count_by_vt.values())
    hist = histogram(used_values, [(0, 5), (5, 10), (10, 20), (20, 50), (50, None)])

    # —— 每 VT 已用字段数 & W5-F 候选 ——
    small_vts = []
    for vt in slot_def["virtual_tables"]:
        c = used_count_by_vt.get(vt["vt_id"], 0)
        stc = vt.get("source_table_count", 0)
        # W5-F 触发：已用字段 ≤ 10 为主；10-20 且 source_table_count ≤ 2 为辅
        trigger = "small" if c <= 10 else ("small_aux" if c <= 20 and stc <= 2 else None)
        if trigger:
            small_vts.append({
                "vt_id": vt["vt_id"],
                "topic": vt.get("topic"),
                "source_table_count": stc,
                "used_field_count": c,
                "trigger": trigger,
            })
    small_vts.sort(key=lambda x: (x["trigger"] != "small", x["used_field_count"]))

    result = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "summary": {
            "vt_count": len(slot_def["virtual_tables"]),
            "base_slot_count": len(base_names),
            "extended_unique_names": len(ext_index),
            "extended_repeat_name_count": len(repeats),
            "l2_conflict_count": len(l2_conflicts),
            "base_extended_conflict_count": len(base_ext_conflicts),
            "homonym_candidate_count": len(homonyms),
            "w5f_trigger_count": len(small_vts),
        },
        "used_field_histogram": hist,
        "w5f_candidates": small_vts,
        "repeats": repeats,
        "l2_conflicts": l2_conflicts,
        "base_extended_conflicts": base_ext_conflicts,
        "homonym_candidates": homonyms,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as f:
        yaml.safe_dump(result, f, allow_unicode=True, sort_keys=False, width=120)

    # 控制台简报
    s = result["summary"]
    print(f"✅ 诊断完成 → {OUTPUT}")
    print(f"  VT: {s['vt_count']} · base: {s['base_slot_count']} · extended unique: {s['extended_unique_names']}")
    print(f"  跨 VT 重名 name: {s['extended_repeat_name_count']}")
    print(f"  L2 内冲突: {s['l2_conflict_count']}  · base-extended 冲突: {s['base_extended_conflict_count']}")
    print(f"  同名异义候选（→ W5-0 LLM 判断）: {s['homonym_candidate_count']}")
    print(f"  W5-F 触发 VT（字段稀缺）: {s['w5f_trigger_count']}")
    print(f"  已用字段数分布: {hist}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
