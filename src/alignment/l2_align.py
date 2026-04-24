"""W5-A · L2 槽位对齐器

按 L2 分组（每组 ≥2 VT）收集 extended slot → embedding → AgglomerativeClustering（cosine+average）
→ 对每个 size≥2 cluster 走 LLM 命名 → output/l2_alignment_proposals.yaml

用法：
  python3 -m src.alignment.l2_align                        # 全量扫所有合规 L2
  python3 -m src.alignment.l2_align --only-l2 "人员主档"   # POC 单 L2
  python3 -m src.alignment.l2_align --threshold 0.15        # 覆盖 distance_threshold
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import yaml
from sklearn.cluster import AgglomerativeClustering

from src.llm_client import chat, embed

ROOT = Path(__file__).resolve().parents[2]
SLOT_DEF = ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS = ROOT / "data" / "slot_library" / "base_slots.yaml"
OUTPUT = ROOT / "output" / "l2_alignment_proposals.yaml"

DEFAULT_THRESHOLD = 0.18  # cosine distance；sim > 0.82 合并


NAMING_SYSTEM_PROMPT = """你是一个数据建模助手，帮我把一组语义相近的"候选槽位"合并为一个统一槽位。

我会给你若干来自不同虚拟表 (VT) 的槽位（name + cn_name + description + aliases）。
这些槽位已经被聚类算法判为高度相似。请你：
1. 给出一个 canonical snake_case 英文名（若它们原本多数用同一个 name，就沿用；否则选最准确的那个或重新取名）
2. 给出一个统一的中文名
3. 给出合并后的 description（一句话概括共同业务语义）
4. 合并 aliases/synonyms（去重）

如果你发现这组候选里有 1-2 个明显不是同一语义（聚类错误），请在 outliers 里列出它们的 name。

严格输出 JSON:
{
  "canonical_name": "snake_case",
  "canonical_cn_name": "中文名",
  "canonical_description": "...",
  "canonical_synonyms": ["..."],
  "outliers": ["name_a", "name_b"],  // 不是同一语义的原 name，需从本 cluster 剔除
  "confidence": 0.0-1.0
}"""


def load_slot_def():
    with SLOT_DEF.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_base_slot_map():
    with BASE_SLOTS.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {b["name"]: b for b in data.get("base_slots", [])}


def collect_candidates_by_l2(slot_def, only_l2: str | None = None):
    """返回 {(l1, l2): [candidate_dict]}。candidate 字段：
       vt_id, name, cn_name, description, aliases, role
    """
    base_map = load_base_slot_map()
    result: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for vt in slot_def["virtual_tables"]:
        l1, l2 = (vt.get("l2_path") or [None, None])[:2]
        if only_l2 and l2 != only_l2 and f"{l1}/{l2}" != only_l2:
            continue
        for slot in vt.get("slots", []):
            if slot.get("from") != "extended":
                continue
            cand = {
                "vt_id": vt["vt_id"],
                "name": slot["name"],
                "cn_name": slot.get("cn_name") or "",
                "description": slot.get("description") or "",
                "aliases": slot.get("aliases") or slot.get("synonyms") or [],
                "role": slot.get("role"),
            }
            result[(l1, l2)].append(cand)
    return result


def embed_candidates(cands: list[dict]) -> np.ndarray:
    texts = []
    for c in cands:
        bits = [c["name"], c["cn_name"], c["description"]]
        if c["aliases"]:
            bits.append(" ".join(c["aliases"][:8]))
        texts.append(" · ".join(b for b in bits if b))
    vecs = embed(texts)
    return np.array(vecs)


def run_clustering(vecs: np.ndarray, distance_threshold: float) -> np.ndarray:
    """返回 cluster labels（n_samples,）。单点 cluster 也允许。"""
    if len(vecs) < 2:
        return np.zeros(len(vecs), dtype=int)
    model = AgglomerativeClustering(
        n_clusters=None,
        metric="cosine",
        linkage="average",
        distance_threshold=distance_threshold,
    )
    return model.fit_predict(vecs)


def llm_name_cluster(members: list[dict]) -> dict:
    lines = ["候选槽位列表：", ""]
    for i, m in enumerate(members):
        aliases_str = ", ".join(m["aliases"][:6]) if m["aliases"] else "(无)"
        lines.append(
            f"{i+1}. name={m['name']} cn_name={m['cn_name']} "
            f"[{m['role']}] desc={m['description'] or '(无)'} aliases={aliases_str} (from {m['vt_id']})"
        )
    user = "\n".join(lines) + "\n\n请判断它们是否可以合并为一个统一槽位，并产出 JSON。"
    resp = chat(
        messages=[
            {"role": "system", "content": NAMING_SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        json_mode=True,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        return {
            "canonical_name": members[0]["name"],
            "canonical_cn_name": members[0]["cn_name"],
            "canonical_description": "LLM 解析失败",
            "canonical_synonyms": [],
            "outliers": [],
            "confidence": 0.0,
        }


def process_l2(l1: str, l2: str, candidates: list[dict], threshold: float) -> dict:
    """对一个 L2 做聚类 + LLM 命名，返回 proposal 结构"""
    if len(candidates) < 2:
        return {
            "l1": l1,
            "l2": l2,
            "vt_count": len({c["vt_id"] for c in candidates}),
            "candidate_count": len(candidates),
            "clusters": [],
        }
    vecs = embed_candidates(candidates)
    labels = run_clustering(vecs, threshold)
    cluster_groups: dict[int, list[dict]] = defaultdict(list)
    for lbl, c in zip(labels, candidates):
        cluster_groups[int(lbl)].append(c)

    clusters_out = []
    for cid, members in sorted(cluster_groups.items()):
        # 只对 size≥2 且跨 VT 的 cluster 跑 LLM + 产生 proposal
        vt_set = {m["vt_id"] for m in members}
        needs_align = len(members) >= 2 and len(vt_set) >= 2
        entry = {
            "cluster_id": cid,
            "size": len(members),
            "cross_vt": len(vt_set) >= 2,
            "members": members,
        }
        if needs_align:
            llm_res = llm_name_cluster(members)
            entry.update({
                "canonical_name": llm_res.get("canonical_name"),
                "canonical_cn_name": llm_res.get("canonical_cn_name"),
                "canonical_description": llm_res.get("canonical_description"),
                "canonical_synonyms": llm_res.get("canonical_synonyms") or [],
                "outliers": llm_res.get("outliers") or [],
                "confidence": llm_res.get("confidence"),
                # 生成 rename_plan
                "rename_plan": [
                    {
                        "vt_id": m["vt_id"],
                        "before_name": m["name"],
                        "after_name": llm_res.get("canonical_name") or m["name"],
                        "changed": m["name"] != (llm_res.get("canonical_name") or m["name"]),
                        "excluded_as_outlier": m["name"] in (llm_res.get("outliers") or []),
                    }
                    for m in members
                ],
            })
        clusters_out.append(entry)
    return {
        "l1": l1,
        "l2": l2,
        "vt_count": len({c["vt_id"] for c in candidates}),
        "candidate_count": len(candidates),
        "clusters": clusters_out,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-l2", type=str, default=None, help="只跑指定 L2（支持 'L2' 或 'L1/L2' 格式）")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    args = parser.parse_args()

    slot_def = load_slot_def()
    candidates_by_l2 = collect_candidates_by_l2(slot_def, only_l2=args.only_l2)
    print(f"处理 {len(candidates_by_l2)} 个 L2（threshold={args.threshold}）")

    proposals = []
    for (l1, l2), cands in candidates_by_l2.items():
        vt_set = {c["vt_id"] for c in cands}
        if len(vt_set) < 2:
            print(f"  skip {l1}/{l2}（仅 {len(vt_set)} VT）")
            continue
        print(f"  {l1}/{l2}: {len(cands)} candidates across {len(vt_set)} VTs → 聚类...")
        p = process_l2(l1, l2, cands, args.threshold)
        aligned = sum(1 for c in p["clusters"] if c.get("canonical_name"))
        print(f"    ✓ {len(p['clusters'])} clusters, {aligned} 需对齐 (size≥2 且跨 VT)")
        proposals.append(p)

    out = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat(),
        "threshold": args.threshold,
        "only_l2": args.only_l2,
        "summary": {
            "l2_count": len(proposals),
            "total_clusters": sum(len(p["clusters"]) for p in proposals),
            "alignable_clusters": sum(
                sum(1 for c in p["clusters"] if c.get("canonical_name")) for p in proposals
            ),
            "total_vt_would_rename": sum(
                sum(
                    sum(1 for r in c.get("rename_plan", []) if r.get("changed"))
                    for c in p["clusters"]
                )
                for p in proposals
            ),
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
