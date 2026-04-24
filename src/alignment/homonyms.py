"""W5-0 同名异义 LLM 判断

输入：output/naming_diagnosis.yaml 的 homonym_candidates
输出：output/homonym_proposals.yaml

每 candidate（同 name，多个 cn_name variant）过一次 LLM：
- 判断各 variant 是"同义"还是"异义"
- 若异义：给出每个 variant 的建议新 name（原 name + 后缀）
- 若同义：维持原 name，合并 cn_name/description/synonyms
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import yaml

from src.llm_client import chat

ROOT = Path(__file__).resolve().parents[2]
DIAGNOSIS = ROOT / "output" / "naming_diagnosis.yaml"
OUTPUT = ROOT / "output" / "homonym_proposals.yaml"


SYSTEM_PROMPT = """你是一个帮助做数据建模"槽位语义对齐"的助手。

背景：同一个 slot_name 在多个虚拟表中出现，但中文名 cn_name 不同。你需要判断这些中文名指代的是否是**同一业务语义**。

判断规则：
- 同义（synonym）：不同中文名表达同一业务概念，只是措辞不同（如"电话" vs "联系电话"）
- 异义（homonym）：中文名指代不同业务概念（如"文化程度"（小学/中学/大学）vs "学历"（学位层次））
- 若有任何 variant 与其他不同语义，判为异义，并对每个 variant 提议带后缀的新 name

输出严格 JSON：
{
  "judgement": "synonym" | "homonym" | "mixed",
  "reason": "一句话解释",
  "confidence": 0.0-1.0,
  "groups": [
    {
      "canonical_cn": "合并后的中文名（若同义组）",
      "variants": ["cn_name_1", "cn_name_2"],
      "suggested_name": "slot_name_or_with_suffix"
    }
  ]
}

要求：
- suggested_name 必须 snake_case，字母开头
- 若 homonym：原 name 加后缀区分（如 education_level__cultural、education_level__academic），用 __ 双下划线分隔
- 若 synonym：单个 group，suggested_name = 原 name
- mixed = 部分同义部分异义：多个 group
"""


def build_user_prompt(name: str, variants: list[dict[str, Any]]) -> str:
    lines = [f"slot_name: {name}", "", "各中文名 variant："]
    for v in variants:
        cn = v["cn_name"]
        descs = v.get("descriptions") or []
        desc_text = " | ".join(descs) if descs else "(无描述)"
        vt_count = v["vt_count"]
        lines.append(f"- `{cn}` (出现在 {vt_count} 张 VT) · {desc_text}")
    lines.append("")
    lines.append("判断这些 variant 的业务语义关系，产出 JSON。")
    return "\n".join(lines)


def judge_one(candidate: dict[str, Any]) -> dict[str, Any]:
    name = candidate["name"]
    variants = candidate["variants"]
    user = build_user_prompt(name, variants)
    resp = chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        json_mode=True,
    )
    try:
        parsed = json.loads(resp)
    except json.JSONDecodeError:
        parsed = {
            "judgement": "unknown",
            "reason": f"LLM output parse failed: {resp[:200]}",
            "confidence": 0.0,
            "groups": [],
        }
    return parsed


def main() -> int:
    if not DIAGNOSIS.exists():
        print(f"❌ {DIAGNOSIS} 不存在，请先跑 python3 -m src.alignment.diagnose")
        return 1
    with DIAGNOSIS.open(encoding="utf-8") as f:
        diag = yaml.safe_load(f)

    candidates = diag.get("homonym_candidates", [])
    print(f"处理 {len(candidates)} 个同名异义候选...")

    proposals: list[dict[str, Any]] = []
    for i, cand in enumerate(candidates):
        name = cand["name"]
        print(f"  [{i+1}/{len(candidates)}] {name} ({cand['cn_variant_count']} variants)...")
        judge = judge_one(cand)
        # 构造 proposal：把每个 original variant 映射到它被分到的 group 的 suggested_name
        cn_to_new_name: dict[str, str] = {}
        for group in judge.get("groups", []):
            suggested = group.get("suggested_name", name)
            for cn in group.get("variants", []):
                cn_to_new_name[cn] = suggested

        # 展平每个 VT → 建议的新 name
        member_proposals = []
        for variant in cand["variants"]:
            cn = variant["cn_name"]
            after_name = cn_to_new_name.get(cn, name)  # 未命中则保持原名
            for m in variant["members"]:
                member_proposals.append({
                    "vt_id": m["vt_id"],
                    "l1": m["l1"],
                    "l2": m["l2"],
                    "cn_name": cn,
                    "before_name": name,
                    "after_name": after_name,
                    "changed": after_name != name,
                })

        proposals.append({
            "name": name,
            "judgement": judge.get("judgement"),
            "reason": judge.get("reason"),
            "confidence": judge.get("confidence"),
            "groups": judge.get("groups", []),
            "member_proposals": member_proposals,
        })

    summary = {
        "total_candidates": len(candidates),
        "judged_synonym": sum(1 for p in proposals if p["judgement"] == "synonym"),
        "judged_homonym": sum(1 for p in proposals if p["judgement"] == "homonym"),
        "judged_mixed": sum(1 for p in proposals if p["judgement"] == "mixed"),
        "total_vt_affected": sum(len(p["member_proposals"]) for p in proposals),
        "total_vt_needs_rename": sum(
            sum(1 for m in p["member_proposals"] if m["changed"]) for p in proposals
        ),
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            {"summary": summary, "proposals": proposals},
            f,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )
    print(f"✅ 产出 {OUTPUT}")
    print(f"  summary: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
