"""I-13: 找出可合并的 VT 候选组。

逻辑：
1. 按 (l2_path, table_type) 分组
2. 组内两两比对：topic+grain_desc embedding cosine sim + source_tables Jaccard
3. merge_score = 0.6 * embedding_sim + 0.4 * source_overlap
4. merge_score ≥ SCORE_THRESHOLD 的对构成 union-find 边
5. 聚合成 ≥2 成员的候选组输出

输出：output/vt_merge_candidates.yaml

TASK: tasks/TASK-I-13-VT合并清单.md
"""
from __future__ import annotations

import hashlib
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from sklearn.metrics.pairwise import cosine_similarity

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import embed  # noqa: E402


SCAFFOLD_YAML = REPO_ROOT / "output" / "virtual_tables_scaffold_final.yaml"
FIELD_NORM = REPO_ROOT / "output" / "field_normalization.parquet"
OUT_YAML = REPO_ROOT / "output" / "vt_merge_candidates.yaml"

# 阈值（任一信号达标即构成合并候选边）
EMBEDDING_SIM_THRESHOLD = 0.80   # 单独 embedding 相似度达标
SOURCE_OVERLAP_THRESHOLD = 0.60  # 单独源表覆盖率达标
COMBINED_SCORE_THRESHOLD = 0.65  # 组合分数
EMBEDDING_WEIGHT = 0.6
SOURCE_WEIGHT = 0.4

# 排除的 L1 类别（参考/字典表不适合做 VT，不出现在合并候选中）
EXCLUDED_L1 = {"字典维表"}


def group_key(vt: dict) -> tuple:
    """分组键 = (L1, L2, table_type)。"""
    l2 = tuple(vt.get("l2_path", []) or [])
    return (l2, vt.get("table_type", ""))


def vt_semantic_text(vt: dict) -> str:
    parts = [vt.get("topic", ""), vt.get("grain_desc", "")]
    return " | ".join(p for p in parts if p)


def source_set(vt: dict) -> set[str]:
    return {t.get("en") for t in (vt.get("candidate_tables") or []) if t.get("en")}


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def overlap_ratio(a: set, b: set) -> float:
    """取较小集合的覆盖率：|A ∩ B| / min(|A|, |B|)"""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def load_field_hit_stats() -> dict[str, dict]:
    """每个 VT 的归一命中统计（auto + review + conflict + low）。"""
    if not FIELD_NORM.exists():
        return {}
    fn = pd.read_parquet(FIELD_NORM)
    result: dict[str, dict] = {}
    for vt_id, sub in fn.groupby("vt_id"):
        status = sub["review_status"].value_counts().to_dict()
        result[vt_id] = {
            "total": int(len(sub)),
            "auto": int(status.get("auto_accepted", 0)),
            "needs_review": int(status.get("needs_review", 0)),
            "conflict": int(status.get("conflict", 0)),
            "low": int(status.get("low_confidence", 0)),
        }
    return result


def find_candidates() -> tuple[list[dict], dict]:
    with SCAFFOLD_YAML.open(encoding="utf-8") as f:
        scaffold = yaml.safe_load(f)
    all_vts = scaffold.get("virtual_tables", []) or []
    # 排除 EXCLUDED_L1 下的 VT
    vts = []
    excluded_count = 0
    for v in all_vts:
        l2p = v.get("l2_path") or []
        l1 = l2p[0] if l2p else ""
        if l1 in EXCLUDED_L1:
            excluded_count += 1
            continue
        vts.append(v)
    print(f"[1/4] 加载 {len(all_vts)} 张 VT（排除 L1={EXCLUDED_L1} 共 {excluded_count} 张，剩 {len(vts)}）")

    hits = load_field_hit_stats()
    print(f"[2/4] 加载归一命中统计 {len(hits)} VT")

    # embedding
    print(f"[3/4] 计算 {len(vts)} 个 VT 的 semantic embedding")
    texts = [vt_semantic_text(v) for v in vts]
    embs = np.array(embed(texts), dtype=float)
    vt_to_idx = {v["vt_id"]: i for i, v in enumerate(vts)}

    # 按 group_key 分组
    groups: dict[tuple, list[int]] = defaultdict(list)
    for i, v in enumerate(vts):
        groups[group_key(v)].append(i)

    # 只保留 ≥2 成员的分组
    active_groups = [(k, idx) for k, idx in groups.items() if len(idx) >= 2]
    print(f"[4/4] 分组完成: {len(active_groups)} 个 (L2, table_type) 组有 ≥2 个 VT")

    # 两两比对 → union-find 边
    parent = list(range(len(vts)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    edges: list[dict] = []
    for gkey, members in active_groups:
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                a, b = members[i], members[j]
                sa, sb = source_set(vts[a]), source_set(vts[b])
                sim = float(cosine_similarity(embs[a].reshape(1, -1), embs[b].reshape(1, -1))[0, 0])
                ovr = overlap_ratio(sa, sb)
                combined = EMBEDDING_WEIGHT * sim + SOURCE_WEIGHT * ovr

                # 任一信号达标即构成合并候选边
                triggers = []
                if sim >= EMBEDDING_SIM_THRESHOLD:
                    triggers.append("embedding")
                if ovr >= SOURCE_OVERLAP_THRESHOLD:
                    triggers.append("source_overlap")
                if combined >= COMBINED_SCORE_THRESHOLD:
                    triggers.append("combined")

                if triggers:
                    union(a, b)
                    edges.append({
                        "a": vts[a]["vt_id"],
                        "b": vts[b]["vt_id"],
                        "embedding_sim": round(sim, 3),
                        "source_overlap": round(ovr, 3),
                        "source_jaccard": round(jaccard(sa, sb), 3),
                        "score": round(combined, 3),
                        "triggers": triggers,
                    })

    # 聚合
    cluster_map: dict[int, list[int]] = defaultdict(list)
    for i in range(len(vts)):
        cluster_map[find(i)].append(i)

    candidate_groups: list[dict] = []
    gid = 0
    for root, idxs in cluster_map.items():
        if len(idxs) < 2:
            continue
        gid += 1
        members_vts = [vts[i] for i in idxs]

        # suggested primary：命中字段最多
        def score_vt(vt):
            h = hits.get(vt["vt_id"], {})
            return (h.get("auto", 0) + h.get("needs_review", 0), vt.get("source_table_count", 0))

        members_sorted = sorted(members_vts, key=lambda v: -score_vt(v)[0])
        primary = members_sorted[0]

        # 本组边
        member_ids = {v["vt_id"] for v in members_vts}
        group_edges = [e for e in edges if e["a"] in member_ids and e["b"] in member_ids]
        avg_score = round(float(np.mean([e["score"] for e in group_edges])), 3) if group_edges else 0.0

        candidate_groups.append({
            "group_id": f"mg_{gid:04d}",
            "l1_path": members_vts[0].get("l2_path", [""])[0] if members_vts[0].get("l2_path") else "",
            "l2_path": list(members_vts[0].get("l2_path", [])),
            "table_type": members_vts[0].get("table_type", ""),
            "avg_score": avg_score,
            "suggested_primary": primary["vt_id"],
            "members": [
                {
                    "vt_id": v["vt_id"],
                    "topic": v.get("topic", ""),
                    "grain_desc": v.get("grain_desc", ""),
                    "source_table_count": v.get("source_table_count", 0),
                    "source_tables": [t.get("en") for t in (v.get("candidate_tables") or [])],
                    "field_hit_auto": hits.get(v["vt_id"], {}).get("auto", 0),
                    "field_hit_needs_review": hits.get(v["vt_id"], {}).get("needs_review", 0),
                    "field_hit_total": hits.get(v["vt_id"], {}).get("total", 0),
                }
                for v in members_sorted
            ],
            "pairwise_evidence": group_edges,
            "status": "pending",
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })

    # 按平均分降序
    candidate_groups.sort(key=lambda g: -g["avg_score"])
    return candidate_groups, {
        "total_vts": len(vts),
        "total_groups": len(candidate_groups),
        "total_vts_involved": sum(len(g["members"]) for g in candidate_groups),
        "total_edges": len(edges),
    }


def main():
    t0 = time.time()
    groups, stats = find_candidates()

    output = {
        "meta": {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "thresholds": {
                "embedding_sim": EMBEDDING_SIM_THRESHOLD,
                "source_overlap": SOURCE_OVERLAP_THRESHOLD,
                "combined": COMBINED_SCORE_THRESHOLD,
            },
            **stats,
        },
        "groups": groups,
    }

    OUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    with OUT_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(output, f, allow_unicode=True, sort_keys=False, width=200)

    print(f"\n完成，耗时 {time.time() - t0:.1f}s")
    print(f"  合并候选组: {stats['total_groups']}")
    print(f"  涉及 VT 数: {stats['total_vts_involved']} / {stats['total_vts']}")
    print(f"  → {OUT_YAML.relative_to(REPO_ROOT)}")

    # 打印 Top 10 组预览
    print(f"\nTop 10 合并候选（按平均相似度降序）:")
    for g in groups[:10]:
        print(f"  [{g['group_id']}] {' / '.join(g['l2_path'])} | {g['table_type']} | {len(g['members'])} 成员 | avg={g['avg_score']}")
        for m in g["members"][:3]:
            print(f"    - {m['vt_id']}: {m['topic']} (src={m['source_table_count']}, auto={m['field_hit_auto']})")


if __name__ == "__main__":
    main()
