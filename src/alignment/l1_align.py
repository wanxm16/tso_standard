"""W5-B · L1 槽位对齐器

按 L1 分组（组内需覆盖 ≥2 个 L2，且 ≥2 VT）收集 extended slot →
embedding → AgglomerativeClustering(cosine+average) →
对每个 size≥2 且跨 L2 的 cluster 走 LLM 命名 →
output/l1_alignment_proposals.yaml

用法：
  python3 -m src.alignment.l1_align                        # 全量 ≥2 L2 的 L1
  python3 -m src.alignment.l1_align --only-l1 "主体主档"   # POC 单 L1
  python3 -m src.alignment.l1_align --threshold 0.15       # 覆盖 distance_threshold
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import yaml
from sklearn.cluster import AgglomerativeClustering

from src.alignment.l2_align import (
    NAMING_SYSTEM_PROMPT,
    embed_candidates,
    llm_name_cluster,
    load_base_slot_map,
    load_slot_def,
    run_clustering,
)

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "output" / "l1_alignment_proposals.yaml"

DEFAULT_THRESHOLD = 0.18


def collect_candidates_by_l1(slot_def, only_l1: str | None = None):
    """返回 {l1: [candidate_dict]}。candidate 字段：
       vt_id, l2, name, cn_name, description, aliases, role
    """
    result: dict[str, list[dict]] = defaultdict(list)
    for vt in slot_def["virtual_tables"]:
        l1, l2 = (vt.get("l2_path") or [None, None])[:2]
        if l1 is None:
            continue
        if only_l1 and l1 != only_l1:
            continue
        for slot in vt.get("slots", []):
            if slot.get("from") != "extended":
                continue
            cand = {
                "vt_id": vt["vt_id"],
                "l2": l2 or "",
                "name": slot["name"],
                "cn_name": slot.get("cn_name") or "",
                "description": slot.get("description") or "",
                "aliases": slot.get("aliases") or slot.get("synonyms") or [],
                "role": slot.get("role"),
            }
            result[l1].append(cand)
    return result


def process_l1(l1: str, candidates: list[dict], threshold: float) -> dict:
    """对一个 L1 做聚类 + LLM 命名，返回 proposal 结构"""
    l2_set_all = {c["l2"] for c in candidates}
    vt_set_all = {c["vt_id"] for c in candidates}
    if len(candidates) < 2 or len(l2_set_all) < 2:
        return {
            "l1": l1,
            "l2_count": len(l2_set_all),
            "vt_count": len(vt_set_all),
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
        vt_set = {m["vt_id"] for m in members}
        l2_set = {m["l2"] for m in members}
        # W5-B 条件：size≥2 且跨 L2（跨 VT 不够；那是 W5-A 漏网的，这里不处理）
        needs_align = len(members) >= 2 and len(l2_set) >= 2
        entry = {
            "cluster_id": cid,
            "size": len(members),
            "cross_vt": len(vt_set) >= 2,
            "cross_l2": len(l2_set) >= 2,
            "l2_coverage": sorted(l2_set),
            "members": members,
        }
        if needs_align:
            llm_res = llm_name_cluster(members)
            canonical_name = llm_res.get("canonical_name")
            entry.update({
                "canonical_name": canonical_name,
                "canonical_cn_name": llm_res.get("canonical_cn_name"),
                "canonical_description": llm_res.get("canonical_description"),
                "canonical_synonyms": llm_res.get("canonical_synonyms") or [],
                "outliers": llm_res.get("outliers") or [],
                "confidence": llm_res.get("confidence"),
                "rename_plan": [
                    {
                        "vt_id": m["vt_id"],
                        "l2": m["l2"],
                        "before_name": m["name"],
                        "after_name": canonical_name or m["name"],
                        "changed": m["name"] != (canonical_name or m["name"]),
                        "excluded_as_outlier": m["name"] in (llm_res.get("outliers") or []),
                    }
                    for m in members
                ],
            })
        clusters_out.append(entry)

    return {
        "l1": l1,
        "l2_count": len(l2_set_all),
        "vt_count": len(vt_set_all),
        "candidate_count": len(candidates),
        "clusters": clusters_out,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-l1", type=str, default=None, help="只跑指定 L1（空=全量 ≥2 L2 的 L1）")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    args = parser.parse_args()

    slot_def = load_slot_def()
    candidates_by_l1 = collect_candidates_by_l1(slot_def, only_l1=args.only_l1)
    print(f"处理 {len(candidates_by_l1)} 个 L1（threshold={args.threshold}）")

    proposals = []
    for l1, cands in candidates_by_l1.items():
        l2_set = {c["l2"] for c in cands}
        if len(l2_set) < 2:
            print(f"  skip {l1}（仅 {len(l2_set)} 个 L2）")
            continue
        print(f"  {l1}: {len(cands)} candidates across {len(l2_set)} L2 / "
              f"{len({c['vt_id'] for c in cands})} VT → 聚类...")
        p = process_l1(l1, cands, args.threshold)
        aligned = sum(1 for c in p["clusters"] if c.get("canonical_name"))
        print(f"    ✓ {len(p['clusters'])} clusters, {aligned} 需对齐 (size≥2 且跨 L2)")
        proposals.append(p)

    out = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat(),
        "threshold": args.threshold,
        "only_l1": args.only_l1,
        "summary": {
            "l1_count": len(proposals),
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
