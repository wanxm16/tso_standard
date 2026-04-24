"""W5-C · base 槽位提升器

扫 `output/slot_definitions.yaml` 所有 VT 的 extended slot，按 slot_name 聚合。
对每个 slot_name：
  - 覆盖的 L1 集合（l2_path[0]）
  - 若覆盖 ≥min_l1_coverage（默认 2）且 name 不在 base_slots.yaml 已有 name → 候选
对每个候选走 LLM 生成 base_entry（description / aliases / logical_type / applicable_table_types），
产物：`output/base_promotion_proposals.yaml`

用法：
  python3 -m src.alignment.base_promote
  python3 -m src.alignment.base_promote --min-l1 1    # 低门槛（数据不足时用于验证 pipeline）
  python3 -m src.alignment.base_promote --only-name certificate_type
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml

from src.alignment.l2_align import load_base_slot_map, load_slot_def
from src.llm_client import chat

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "output" / "base_promotion_proposals.yaml"

DEFAULT_MIN_L1 = 2

PROMOTE_SYSTEM_PROMPT = """你是一个数据建模助手。
下面给出一个在多张虚拟表（VT）里复现的 extended 槽位。请你产出将它提升为 base_slots 的登记条目。

输出严格 JSON：
{
  "cn_name": "中文名（主展示）",
  "logical_type": "person_name / id_card_no / phone_no / datetime / region_code / address / status_code / amount / count / text 之一，或自定义 custom_xxx",
  "role": "subject / subject_id / relation_subject / display / time / location / measure / filter / source / description 之一（10 种字段角色）",
  "description": "一句话业务语义",
  "aliases": ["中/英/拼音同义词，10 个以内"],
  "applicable_table_types": ["主档", "关系", "事件", "标签", "聚合"],
  "confidence": 0.0-1.0,
  "rationale": "为什么值得提升（出现在哪些领域，为什么是跨域共享概念）"
}

要求：
- logical_type 必须有业务意义；若无合适值就用 text
- role 必须是 10 种中的一个
- aliases 合并各 VT 原 aliases（去重）
- applicable_table_types 保守覆盖（默认 5 种全给，除非明显不适用）
"""


def collect_promotion_candidates(slot_def, min_l1: int, only_name: str | None = None):
    """返回 {slot_name: {members: [{vt_id, l1, l2, slot_index, snapshot}], l1_set, vt_count}}
    仅保留：
      - from == extended
      - slot_name 不在 base_slots.yaml 已有 name
      - 覆盖 L1 数 >= min_l1
    """
    base_name_set = set(load_base_slot_map().keys())
    by_name: dict[str, list[dict]] = defaultdict(list)
    for vt in slot_def["virtual_tables"]:
        l1, l2 = (vt.get("l2_path") or [None, None])[:2]
        if l1 is None:
            continue
        for i, slot in enumerate(vt.get("slots", [])):
            if slot.get("from") != "extended":
                continue
            name = slot.get("name")
            if not name:
                continue
            if name in base_name_set:
                continue
            if only_name and name != only_name:
                continue
            by_name[name].append({
                "vt_id": vt["vt_id"],
                "l1": l1,
                "l2": l2 or "",
                "slot_index": i,
                "snapshot": dict(slot),  # 保留完整原 slot 用于 revert
                "mapped_fields_count": len(slot.get("mapped_fields") or []),
            })

    candidates = {}
    for name, members in by_name.items():
        l1_set = {m["l1"] for m in members}
        vt_count = len({m["vt_id"] for m in members})
        if len(l1_set) < min_l1:
            continue
        candidates[name] = {
            "members": members,
            "l1_set": sorted(l1_set),
            "vt_count": vt_count,
        }
    return candidates


def llm_propose_base_entry(name: str, members: list[dict]) -> dict:
    aliases_all = []
    descriptions = []
    cn_names = []
    roles = []
    for m in members:
        s = m["snapshot"]
        for a in (s.get("aliases") or s.get("synonyms") or []):
            if a not in aliases_all:
                aliases_all.append(a)
        if s.get("description"):
            descriptions.append(f"- {s.get('description')} (from {m['vt_id']}/{m['l1']}/{m['l2']})")
        if s.get("cn_name"):
            cn_names.append(s["cn_name"])
        if s.get("role"):
            roles.append(s["role"])

    user_parts = [
        f"槽位 name: {name}",
        f"覆盖 L1: {sorted({m['l1'] for m in members})}",
        f"覆盖 VT 数: {len({m['vt_id'] for m in members})}",
        f"出现过的 cn_name: {sorted(set(cn_names))}",
        f"出现过的 role: {sorted(set(roles))}",
        f"aliases 合并候选: {aliases_all[:30]}",
        "原 description（前 8 条）：",
        *descriptions[:8],
    ]
    user = "\n".join(user_parts) + "\n\n请产出 base_slots 登记 JSON。"
    resp = chat(
        messages=[
            {"role": "system", "content": PROMOTE_SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        json_mode=True,
    )
    try:
        data = json.loads(resp)
    except json.JSONDecodeError:
        data = {
            "cn_name": cn_names[0] if cn_names else name,
            "logical_type": "text",
            "role": roles[0] if roles else "display",
            "description": "LLM 解析失败，请人工填写",
            "aliases": aliases_all,
            "applicable_table_types": ["主档", "关系", "事件", "标签", "聚合"],
            "confidence": 0.0,
            "rationale": "parse_error",
        }
    # 强制 base_entry 字段齐全
    base_entry = {
        "name": name,
        "cn_name": data.get("cn_name") or (cn_names[0] if cn_names else name),
        "logical_type": data.get("logical_type") or "text",
        "role": data.get("role") or (roles[0] if roles else "display"),
        "description": data.get("description") or "",
        "aliases": list(data.get("aliases") or aliases_all),
        "sample_patterns": [],
        "applicable_table_types": list(data.get("applicable_table_types") or ["主档", "关系", "事件", "标签", "聚合"]),
    }
    return {
        "base_entry": base_entry,
        "confidence": data.get("confidence", 0.0),
        "rationale": data.get("rationale", ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-l1", type=int, default=DEFAULT_MIN_L1, help="最少覆盖 L1 数（默认 2）")
    parser.add_argument("--only-name", type=str, default=None, help="只跑指定 slot_name")
    parser.add_argument("--no-llm", action="store_true", help="不跑 LLM，只列候选（快速调试）")
    args = parser.parse_args()

    slot_def = load_slot_def()
    candidates = collect_promotion_candidates(slot_def, args.min_l1, args.only_name)
    print(f"扫出 {len(candidates)} 个 base 提升候选（min_l1={args.min_l1}）")

    proposals = []
    for i, (name, info) in enumerate(sorted(candidates.items(), key=lambda x: -x[1]["vt_count"])):
        print(f"  [{i+1}/{len(candidates)}] {name} · L1={info['l1_set']} · {info['vt_count']} VT")
        if args.no_llm:
            proposal_extra = {"base_entry": None, "confidence": None, "rationale": "no_llm"}
        else:
            proposal_extra = llm_propose_base_entry(name, info["members"])
        proposals.append({
            "canonical_name": name,
            "l1_coverage": info["l1_set"],
            "l1_count": len(info["l1_set"]),
            "vt_count": info["vt_count"],
            "members": [
                {
                    "vt_id": m["vt_id"],
                    "l1": m["l1"],
                    "l2": m["l2"],
                    "slot_index": m["slot_index"],
                    "before_name": name,
                    "mapped_fields_count": m["mapped_fields_count"],
                    "extended_snapshot": m["snapshot"],
                }
                for m in info["members"]
            ],
            **proposal_extra,
        })

    out = {
        "generated_at": datetime.utcnow().isoformat(),
        "min_l1": args.min_l1,
        "summary": {
            "candidate_count": len(proposals),
            "total_vts": sum(p["vt_count"] for p in proposals),
        },
        "proposals": proposals,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True, sort_keys=False, width=120)
    print(f"✅ → {OUTPUT}")
    print(f"   summary: {out['summary']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
