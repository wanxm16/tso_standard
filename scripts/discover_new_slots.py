"""I-05b: 新槽位自动发现（embedding 聚类 + LLM 命名 + 跨 VT 聚合）

流程：
1. 加载 field_normalization / field_features / 已有 slot_definitions + base_slots
2. 筛选入口样本：review_status ∈ {needs_review, low_confidence, conflict}
3. 每个 (table_en, field_name) 算 embedding（text = 字段名 | 注释 | 样例）
4. 每个 VT 内做 HDBSCAN 聚类（cosine, min_cluster_size=3）
5. 丢弃：成员 <3、聚类内平均相似度 <0.55
6. 每个聚类调 qwen3-max 生成 {name, cn_name, logical_type, role, aliases, description, confidence, similar_existing}
7. 与已有槽位查重（name exact + embedding 相似度 >0.85 丢弃）
8. 合并 field_normalization.llm_propose_new_slot 的原有 LLM 建议（按 name 聚合）
9. 跨 VT 聚合：≥3 VT → base / 同 L1 且 ≥2 VT → domain / 否则 vt_local
10. 输出 output/slot_proposals.yaml + output/slot_proposals_diagnostic.md

设计文档：§ 10.8.11 反馈回写机制
TASK: tasks/TASK-I-05b-新槽位自动发现.md
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from sklearn.cluster import HDBSCAN
from sklearn.metrics.pairwise import cosine_similarity


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat, embed  # noqa: E402
from src.naming_lint import (  # noqa: E402
    SLOT_NAMING_GUARDRAILS,
    format_naming_retry_feedback,
    validate_slot_name,
)


FIELD_NORM = REPO_ROOT / "output" / "field_normalization.parquet"
FIELD_FEATURES = REPO_ROOT / "output" / "field_features.parquet"
SLOT_DEFS = REPO_ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"

OUT_YAML = REPO_ROOT / "output" / "slot_proposals.yaml"
OUT_DIAG = REPO_ROOT / "output" / "slot_proposals_diagnostic.md"


# 聚类参数
MIN_CLUSTER_SIZE = 3
MIN_COHESION = 0.55
MAX_MEMBERS_IN_PROMPT = 10
MAX_MEMBERS_IN_OUTPUT = 10
MAX_SAMPLE_VALUES = 3

# 查重 / 聚合阈值
DEDUP_EMBEDDING_SIM = 0.85
CROSS_VT_MERGE_SIM = 0.85
PROPOSAL_INTRA_VT_MERGE_SIM = 0.80   # 同 VT 内两个 proposal semantic centroid ≥ 此值视作同义
BASE_UPGRADE_VT_COUNT = 3
DOMAIN_UPGRADE_VT_COUNT = 2

REVIEW_STATUSES = {"needs_review", "low_confidence", "conflict"}

# 允许的 logical_type（§ 10.7 15 种 + 扩展）
ALLOWED_LOGICAL_TYPES = {
    # § 10.7
    "person_name", "id_card_no", "id_card_no_or_passport", "passport_no",
    "phone_no", "vehicle_plate_no", "datetime", "region_code", "address",
    "track_point_name", "relation_type", "case_no", "status_code",
    "source_system", "amount", "count",
    # 常见扩展（和 base_slots 已有 logical_type 对齐）
    "date", "time", "text", "code", "name", "id", "ratio", "score",
    "device_id", "ip_address", "url", "coordinates", "duration",
    "category", "enum", "boolean", "description", "keyword", "tag",
}

# 允许的 role（§ 10.3 10 种）
ALLOWED_ROLES = {
    "subject", "subject_id", "relation_subject", "display",
    "time", "location", "filter", "measure", "source", "description",
}


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    fn = pd.read_parquet(FIELD_NORM)
    ff = pd.read_parquet(FIELD_FEATURES)
    with open(SLOT_DEFS, "r", encoding="utf-8") as f:
        sd = yaml.safe_load(f)
    with open(BASE_SLOTS, "r", encoding="utf-8") as f:
        bs = yaml.safe_load(f)
    return fn, ff, sd, bs


def collect_existing_slots(slot_defs: dict, base_slots: dict) -> list[dict]:
    """收集已有槽位：[{name, cn_name, logical_type, role, description?, scope, vt_id?}]"""
    result: list[dict] = []
    for s in base_slots.get("base_slots", []) or []:
        result.append({
            "name": s["name"],
            "cn_name": s.get("cn_name", ""),
            "logical_type": s.get("logical_type", ""),
            "role": s.get("role", ""),
            "description": s.get("description", ""),
            "scope": "base",
        })
    for vt in slot_defs.get("virtual_tables", []) or []:
        vt_id = vt.get("vt_id")
        for s in vt.get("slots", []) or []:
            if s.get("from") == "extended":
                result.append({
                    "name": s.get("name", ""),
                    "cn_name": s.get("cn_name", ""),
                    "logical_type": s.get("logical_type", ""),
                    "role": s.get("role", ""),
                    "description": s.get("description", ""),
                    "scope": "extended",
                    "vt_id": vt_id,
                })
    return result


def build_field_text(row: dict) -> str:
    """构造聚类 embedding 的文本表征。"""
    parts = []
    fname = row.get("field_name") or ""
    comment = row.get("field_comment") or ""
    if fname:
        parts.append(fname)
    if comment and comment.strip() and comment.strip() != fname:
        parts.append(f"注释={comment.strip()}")
    samples = row.get("sample_values")
    if samples is not None:
        if isinstance(samples, np.ndarray):
            samples = samples.tolist()
        samples = [str(s)[:40] for s in list(samples)[:MAX_SAMPLE_VALUES] if s is not None]
        if samples:
            parts.append(f"样例={','.join(samples)}")
    return " | ".join(parts)


def build_slot_text(slot: dict) -> str:
    """已有槽位的文本表征，用于和 cluster centroid 做相似度比对。"""
    parts = [slot.get("name", "")]
    if slot.get("cn_name"):
        parts.append(slot["cn_name"])
    if slot.get("description"):
        parts.append(slot["description"])
    return " | ".join(p for p in parts if p)


def build_cluster_input(fn: pd.DataFrame, ff: pd.DataFrame) -> pd.DataFrame:
    rows = fn[fn["review_status"].isin(REVIEW_STATUSES)].copy()
    merge_cols = ["table_en", "field_name", "sample_values", "table_l1", "table_l2"]
    rows = rows.merge(
        ff[merge_cols].drop_duplicates(["table_en", "field_name"]),
        on=["table_en", "field_name"],
        how="left",
    )
    return rows


def embed_texts(texts: list[str]) -> np.ndarray:
    """调用 llm_client.embed（带缓存），返回 (N, D) np.ndarray。"""
    if not texts:
        return np.zeros((0, 0))
    vecs = embed(texts)
    arr = np.array(vecs, dtype=float)
    return arr


def cluster_per_vt(rows: pd.DataFrame, emb_map: dict[tuple[str, str], np.ndarray]) -> list[dict]:
    """每 VT 做 HDBSCAN；返回 cluster 列表。"""
    clusters: list[dict] = []
    for vt_id, group in rows.groupby("vt_id"):
        group = group.reset_index(drop=True)
        if len(group) < MIN_CLUSTER_SIZE:
            continue
        vectors = []
        keep_idx = []
        for i, r in group.iterrows():
            key = (r["table_en"], r["field_name"])
            v = emb_map.get(key)
            if v is None or v.size == 0:
                continue
            vectors.append(v)
            keep_idx.append(i)
        if len(vectors) < MIN_CLUSTER_SIZE:
            continue
        X = np.stack(vectors)
        group = group.iloc[keep_idx].reset_index(drop=True)
        try:
            hdb = HDBSCAN(
                min_cluster_size=MIN_CLUSTER_SIZE,
                metric="cosine",
                cluster_selection_method="eom",
            )
            labels = hdb.fit_predict(X)
        except Exception as e:
            print(f"  [WARN] HDBSCAN failed on {vt_id}: {e}")
            continue
        for label in sorted(set(labels)):
            if label == -1:
                continue
            idx = [i for i, l in enumerate(labels) if l == label]
            if len(idx) < MIN_CLUSTER_SIZE:
                continue
            member_vecs = X[idx]
            sim = cosine_similarity(member_vecs)
            n = sim.shape[0]
            upper = sim[np.triu_indices(n, k=1)]
            cohesion = float(upper.mean()) if upper.size else 0.0
            if cohesion < MIN_COHESION:
                continue
            members = group.iloc[idx].to_dict("records")
            clusters.append({
                "vt_id": vt_id,
                "cohesion": cohesion,
                "centroid": member_vecs.mean(axis=0),
                "members": members,
                "table_l1": members[0].get("table_l1"),
            })
    return clusters


NAMING_SYSTEM_PROMPT = f"""你是一位数据治理专家。任务是为一组"语义相近的物理字段"命名一个统一的"语义槽位"（slot），供 text2sql 表召回层使用。

输入：
- 若干物理字段（字段名 + 中文注释 + 样例），它们被聚类到一起，意味着它们很可能指向同一个业务概念。
- 一份已有的槽位清单（必须避免重复命名）。

输出：
一个新的槽位定义。必须严格返回 JSON，不要添加解释。

严格原则：
1. **优先查重**：如果聚类指向的概念和 existing_slots 中某个槽位相同或高度相似，应在 `similar_existing` 字段列出，并把 `confidence` 调低到 0.5 以下，由人工决定是否合并。
2. **命名规范**：name 用 snake_case，简短（≤25 字符）；cn_name 是中文主展示。
3. **logical_type 严格在枚举内**：${LOGICAL_TYPES}；若都不合适用 `text` 作为保底。
4. **role 严格在枚举内**：subject / subject_id / relation_subject / display / time / location / filter / measure / source / description
5. **aliases 至少 3 个**：中/英/拼音/缩写同义词，便于后续归一。
6. **confidence**：对命名准确性的自评（0.0-1.0）。如果聚类证据稀疏或噪声大，应 <0.6。

{SLOT_NAMING_GUARDRAILS}"""


NAMING_USER_TEMPLATE = """## 已有槽位参考（命名必须避免重复）

{existing_slots_compact}

## 聚类成员（{n_members} 个物理字段，平均内聚度 {cohesion:.2f}）

{members_text}

## 所在虚拟表

- vt_id: {vt_id}
- 聚类覆盖的 top1_slot 分布: {top1_distribution}

## 输出 JSON schema（严格）

```json
{{
  "name": "snake_case 英文名",
  "cn_name": "中文名",
  "logical_type": "见系统提示中的枚举",
  "role": "见系统提示中的枚举",
  "description": "一句话说明这个槽位代表什么",
  "aliases": ["至少 3 个中/英/拼音同义词"],
  "confidence": 0.0-1.0,
  "similar_existing": [
    {{"name": "existing_slot_name", "reason": "为什么像"}}
  ]
}}
```
"""


def compact_existing_slots(existing: list[dict], limit: int = 60) -> str:
    """精简已有槽位给 prompt 用。按 base 先 + extended 后，截断。"""
    lines = []
    for s in existing[:limit]:
        lines.append(
            f"- {s['name']} | {s.get('cn_name', '')} | "
            f"type={s.get('logical_type', '')} | role={s.get('role', '')} | "
            f"scope={s.get('scope', '')}"
        )
    if len(existing) > limit:
        lines.append(f"- ...（共 {len(existing)} 个，已省略 {len(existing) - limit}）")
    return "\n".join(lines)


def compact_cluster_members(members: list[dict], limit: int = MAX_MEMBERS_IN_PROMPT) -> str:
    lines = []
    for m in members[:limit]:
        samples = m.get("sample_values")
        if isinstance(samples, np.ndarray):
            samples = samples.tolist()
        samples = samples or []
        sample_str = ",".join(str(s)[:30] for s in list(samples)[:3])
        lines.append(
            f"- `{m['field_name']}` | {m.get('field_comment', '') or '(无注释)'} | "
            f"table={m['table_en']} | top1={m.get('top1_slot', '-')} | samples={sample_str}"
        )
    if len(members) > limit:
        lines.append(f"- ...（共 {len(members)} 个成员，已省略 {len(members) - limit}）")
    return "\n".join(lines)


def llm_name_cluster(cluster: dict, existing_slots: list[dict]) -> dict | None:
    """调 LLM 给聚类命名。返回 proposal dict 或 None。"""
    members = cluster["members"]
    top1_dist = Counter(m.get("top1_slot") for m in members if m.get("top1_slot"))
    top1_str = ", ".join(f"{k}({v})" for k, v in top1_dist.most_common(5))

    sys_prompt = NAMING_SYSTEM_PROMPT.replace(
        "${LOGICAL_TYPES}", ", ".join(sorted(ALLOWED_LOGICAL_TYPES))
    )
    user_prompt = NAMING_USER_TEMPLATE.format(
        existing_slots_compact=compact_existing_slots(existing_slots),
        n_members=len(members),
        cohesion=cluster["cohesion"],
        members_text=compact_cluster_members(members),
        vt_id=cluster["vt_id"],
        top1_distribution=top1_str or "(无)",
    )

    existing_names = {str(s.get("name") or "").strip() for s in existing_slots if s.get("name")}
    messages = [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user_prompt},
    ]
    for attempt in range(2):
        try:
            resp = chat(
                messages,
                temperature=0.0,
                json_mode=True,
            )
            parsed = json.loads(resp)
            # schema 校验
            required = {"name", "cn_name", "logical_type", "role", "aliases"}
            if not required.issubset(parsed.keys()):
                raise ValueError(f"missing keys: {required - set(parsed.keys())}")
            if parsed["logical_type"] not in ALLOWED_LOGICAL_TYPES:
                parsed["logical_type"] = "text"  # 回退
            if parsed["role"] not in ALLOWED_ROLES:
                raise ValueError(f"invalid role: {parsed['role']}")
            if not isinstance(parsed["aliases"], list) or len(parsed["aliases"]) < 1:
                raise ValueError("aliases must be non-empty list")
            parsed.setdefault("description", "")
            parsed.setdefault("confidence", 0.5)
            parsed.setdefault("similar_existing", [])
            name_issues = validate_slot_name(
                parsed["name"],
                source="extended",
                base_slot_names=existing_names,
            )
            if name_issues and attempt == 0:
                messages.extend([
                    {"role": "assistant", "content": json.dumps(parsed, ensure_ascii=False)},
                    {"role": "user", "content": format_naming_retry_feedback(name_issues)},
                ])
                continue
            if name_issues:
                parsed["naming_warnings"] = name_issues
                print(
                    "  [WARN] LLM naming kept after retry for vt="
                    f"{cluster['vt_id']}: {'; '.join(name_issues)}"
                )
            return parsed
        except Exception as e:
            if attempt == 1:
                print(f"  [WARN] LLM naming failed after retry for vt={cluster['vt_id']}: {e}")
                return None
    return None


def dedup_vs_existing(
    proposal: dict,
    cluster_centroid: np.ndarray,
    existing_slots: list[dict],
    existing_slot_emb: np.ndarray,
) -> tuple[bool, list[dict]]:
    """判断是否与已有槽位重复。返回 (是否重复, 近似槽位 list)。"""
    # name exact
    for s in existing_slots:
        if s["name"] == proposal["name"]:
            return True, [{"name": s["name"], "reason": "name_exact_match", "similarity": 1.0}]

    # cn_name exact
    if proposal.get("cn_name"):
        for s in existing_slots:
            if s.get("cn_name") and s["cn_name"] == proposal["cn_name"]:
                return True, [{"name": s["name"], "reason": "cn_name_exact_match", "similarity": 1.0}]

    # embedding similarity
    similar: list[dict] = []
    if cluster_centroid.size and existing_slot_emb.size:
        sims = cosine_similarity(cluster_centroid.reshape(1, -1), existing_slot_emb)[0]
        top_indices = np.argsort(sims)[::-1][:5]
        for i in top_indices:
            sim = float(sims[i])
            if sim >= DEDUP_EMBEDDING_SIM:
                return True, [{
                    "name": existing_slots[i]["name"],
                    "reason": "embedding_near_duplicate",
                    "similarity": round(sim, 3),
                }]
            if sim >= 0.65:
                similar.append({
                    "name": existing_slots[i]["name"],
                    "reason": "embedding_similar",
                    "similarity": round(sim, 3),
                })
    return False, similar[:3]


def proposal_id_for(source: str, name: str, vt_id: str | None = None) -> str:
    key = f"{source}|{name}|{vt_id or ''}"
    return "sp_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]


def _dedupe_similar(entries: list[dict]) -> list[dict]:
    """按 name 去重 similar_existing 列表。同名时优先保留带 similarity 的、数值更高的。"""
    if not entries:
        return []
    by_name: dict[str, dict] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        name = e.get("name")
        if not name:
            continue
        prev = by_name.get(name)
        if prev is None:
            by_name[name] = dict(e)
            continue
        # 合并：取 similarity 更高的那条
        prev_sim = prev.get("similarity")
        cur_sim = e.get("similarity")
        if cur_sim is not None and (prev_sim is None or cur_sim > prev_sim):
            by_name[name] = dict(e)
        # 若两边都没 similarity，保留首次；reason 可拼接
        elif prev_sim is None and cur_sim is None:
            prev_reason = prev.get("reason") or ""
            cur_reason = e.get("reason") or ""
            if cur_reason and cur_reason not in prev_reason:
                prev["reason"] = f"{prev_reason}；{cur_reason}" if prev_reason else cur_reason
    # 按 similarity 降序（无 similarity 的排后）
    return sorted(
        by_name.values(),
        key=lambda x: (x.get("similarity") is None, -(x.get("similarity") or 0)),
    )


def cluster_to_proposal(
    cluster: dict,
    naming: dict,
    similar_existing: list[dict],
) -> dict:
    members = cluster["members"][:MAX_MEMBERS_IN_OUTPUT]
    return {
        "proposal_id": proposal_id_for("cluster", naming["name"], cluster["vt_id"]),
        "source": "cluster",
        "scope": "vt_local",
        "target_vt_ids": [cluster["vt_id"]],
        "target_domain": None,
        "name": naming["name"],
        "cn_name": naming["cn_name"],
        "logical_type": naming["logical_type"],
        "role": naming["role"],
        "description": naming.get("description", ""),
        "aliases": naming.get("aliases", []),
        "sample_patterns": [],
        "support_count": len(cluster["members"]),
        "member_fields": [
            {
                "table_en": m["table_en"],
                "field_name": m["field_name"],
                "field_comment": m.get("field_comment", ""),
                "vt_id": cluster["vt_id"],
                "top1_slot": m.get("top1_slot"),
                "top1_score": float(m.get("top1_score") or 0.0),
            }
            for m in members
        ],
        "cluster_cohesion": round(cluster["cohesion"], 3),
        "llm_naming_confidence": float(naming.get("confidence", 0.5)),
        "naming_warnings": naming.get("naming_warnings", []),
        "similar_existing_slots": _dedupe_similar(similar_existing + (naming.get("similar_existing") or [])),
        "status": "pending",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def merge_llm_suggestions(
    fn: pd.DataFrame,
    existing_slots: list[dict],
    existing_slot_emb: np.ndarray,
) -> list[dict]:
    """把 field_normalization.llm_propose_new_slot 的 1091 条聚合成 source=llm_suggestion。"""
    if "llm_propose_new_slot" not in fn.columns:
        return []
    llm_rows = fn[fn["llm_propose_new_slot"].notna() & (fn["llm_propose_new_slot"] != "")].copy()
    llm_rows = llm_rows[llm_rows["llm_propose_new_slot"] != "null"]
    grouped: dict[str, list[dict]] = defaultdict(list)
    name_to_cn: dict[str, str] = {}
    for _, r in llm_rows.iterrows():
        raw = r["llm_propose_new_slot"]
        if isinstance(raw, str):
            try:
                obj = json.loads(raw)
            except Exception:
                continue
        else:
            obj = raw
        name = obj.get("name")
        if not name:
            continue
        name_to_cn[name] = obj.get("cn_name", name_to_cn.get(name, ""))
        grouped[name].append({
            "table_en": r["table_en"],
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment", ""),
            "vt_id": r["vt_id"],
            "top1_slot": r.get("top1_slot"),
            "top1_score": float(r.get("top1_score") or 0.0),
        })

    proposals = []
    existing_names = {s["name"] for s in existing_slots}
    for name, members in grouped.items():
        if name in existing_names:
            continue  # 已有同名槽位，跳过
        vt_set = sorted({m["vt_id"] for m in members})
        cn_name = name_to_cn.get(name, name)
        proposals.append({
            "proposal_id": proposal_id_for("llm_suggestion", name),
            "source": "llm_suggestion",
            "scope": "vt_local",  # 后续跨 VT 聚合会升级
            "target_vt_ids": vt_set,
            "target_domain": None,
            "name": name,
            "cn_name": cn_name,
            "logical_type": "text",  # LLM 原建议没给 logical_type，默认保底
            "role": "display",       # 保底；后续人工可调
            "description": f"LLM 在归一阶段建议的新槽位，{len(members)} 个字段指向它",
            "aliases": [],
            "sample_patterns": [],
            "support_count": len(members),
            "member_fields": members[:MAX_MEMBERS_IN_OUTPUT],
            "cluster_cohesion": None,
            "llm_naming_confidence": None,
            "similar_existing_slots": [],
            "status": "pending",
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
    return proposals


def aggregate_cross_vt(
    proposals: list[dict],
    proposal_embeddings: np.ndarray,
) -> list[dict]:
    """按 name exact match（先）合并；scope 按 target_vt_ids 数和 L1 决定。"""
    # 1) name-based 合并
    by_name: dict[str, list[int]] = defaultdict(list)
    for i, p in enumerate(proposals):
        by_name[p["name"]].append(i)

    merged: list[dict] = []
    used = set()
    for name, indices in by_name.items():
        if len(indices) == 1:
            merged.append(proposals[indices[0]])
            used.update(indices)
            continue
        # 合并
        all_vts = set()
        all_members = []
        all_similar = []
        cohesions = []
        confidences = []
        sources = set()
        for idx in indices:
            p = proposals[idx]
            all_vts.update(p["target_vt_ids"])
            all_members.extend(p["member_fields"])
            all_similar.extend(p["similar_existing_slots"])
            if p.get("cluster_cohesion") is not None:
                cohesions.append(p["cluster_cohesion"])
            if p.get("llm_naming_confidence") is not None:
                confidences.append(p["llm_naming_confidence"])
            sources.add(p["source"])
        base = proposals[indices[0]]
        merged.append({
            **base,
            "proposal_id": proposal_id_for("merged_" + "+".join(sorted(sources)), name),
            "source": "merged:" + "+".join(sorted(sources)),
            "target_vt_ids": sorted(all_vts),
            "support_count": len(all_members),
            "member_fields": all_members[:MAX_MEMBERS_IN_OUTPUT],
            "cluster_cohesion": round(sum(cohesions) / len(cohesions), 3) if cohesions else None,
            "llm_naming_confidence": round(sum(confidences) / len(confidences), 3) if confidences else None,
            "similar_existing_slots": _dedupe_similar(all_similar)[:5],
        })
        used.update(indices)

    # 2) 按 VT 数决定 scope
    # 先收集每个 proposal 的 L1（member_fields[*].table_en 对应的 L1 可从 field_features 查；简化：看 target_vt_ids 对应的 table_l1 集合）
    # 这里 member_fields 里没 L1 信息，简化为：
    #   - ≥ BASE_UPGRADE_VT_COUNT 张 VT → base
    #   - 2 张 VT → domain（target_domain 留空，人工填）
    #   - 1 张 VT → vt_local
    for p in merged:
        n_vt = len(p["target_vt_ids"])
        if n_vt >= BASE_UPGRADE_VT_COUNT:
            p["scope"] = "base"
        elif n_vt >= DOMAIN_UPGRADE_VT_COUNT:
            p["scope"] = "domain"
        else:
            p["scope"] = "vt_local"

    return merged


def build_proposal_semantic_text(p: dict) -> str:
    """为 proposal 计算 semantic centroid 的文本：name + cn_name + description + 前 5 个 aliases。"""
    parts = [p.get("name", ""), p.get("cn_name", "")]
    if p.get("description"):
        parts.append(p["description"])
    aliases = p.get("aliases") or []
    if aliases:
        parts.append("aliases=" + ",".join(str(a) for a in aliases[:5]))
    return " | ".join(p_ for p_ in parts if p_)


def merge_similar_intra_vt(
    proposals: list[dict],
    sim_threshold: float = PROPOSAL_INTRA_VT_MERGE_SIM,
) -> tuple[list[dict], list[dict]]:
    """同 VT 内语义相近的 proposal 用 union-find 合并。

    返回 (merged_proposals, merge_audit)
      merge_audit: [{kept_id, kept_name, absorbed: [{id, name, similarity}]}]
    """
    if not proposals:
        return proposals, []

    # 1) 所有 proposal 的 semantic centroid embedding
    texts = [build_proposal_semantic_text(p) for p in proposals]
    embs = embed_texts(texts)  # (N, D)

    # 2) 建反向索引：vt_id → [proposal indices]
    vt_to_indices: dict[str, list[int]] = defaultdict(list)
    for i, p in enumerate(proposals):
        for vt in p.get("target_vt_ids") or []:
            vt_to_indices[vt].append(i)

    # 3) Union-Find
    parent = list(range(len(proposals)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # 4) 同 VT 内两两比对（去重过的 proposal pair）
    checked: set[tuple[int, int]] = set()
    edges: list[tuple[int, int, float]] = []
    for vt, indices in vt_to_indices.items():
        if len(indices) < 2:
            continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                a, b = indices[i], indices[j]
                if a > b:
                    a, b = b, a
                if (a, b) in checked:
                    continue
                checked.add((a, b))
                ea, eb = embs[a], embs[b]
                if ea.size == 0 or eb.size == 0:
                    continue
                sim = float(cosine_similarity(ea.reshape(1, -1), eb.reshape(1, -1))[0, 0])
                if sim >= sim_threshold:
                    union(a, b)
                    edges.append((a, b, sim))

    # 5) 按 union-find 分组
    groups: dict[int, list[int]] = defaultdict(list)
    for i in range(len(proposals)):
        groups[find(i)].append(i)

    # 6) 合并：每组保留 support_count 最大的作为主，其他并入
    merged: list[dict] = []
    audit: list[dict] = []
    for root, members in groups.items():
        if len(members) == 1:
            merged.append(proposals[members[0]])
            continue
        # 选主：support_count 最大；tie-break 上 cluster 优先 > 有 cohesion 的优先
        members_sorted = sorted(
            members,
            key=lambda idx: (
                -proposals[idx].get("support_count", 0),
                0 if proposals[idx].get("source", "").startswith("cluster") or "cluster" in proposals[idx].get("source", "") else 1,
                0 if proposals[idx].get("cluster_cohesion") is not None else 1,
            ),
        )
        main_idx = members_sorted[0]
        main = dict(proposals[main_idx])
        absorbed = [proposals[i] for i in members_sorted[1:]]

        # 合并字段
        all_vts = set(main.get("target_vt_ids") or [])
        all_members = list(main.get("member_fields") or [])
        all_aliases = list(main.get("aliases") or [])
        all_similar = list(main.get("similar_existing_slots") or [])
        sources = {main.get("source", "")}
        total_support = main.get("support_count", 0)

        for p in absorbed:
            all_vts.update(p.get("target_vt_ids") or [])
            all_members.extend(p.get("member_fields") or [])
            for a in p.get("aliases") or []:
                if a not in all_aliases:
                    all_aliases.append(a)
            all_similar.extend(p.get("similar_existing_slots") or [])
            sources.add(p.get("source", ""))
            total_support += p.get("support_count", 0)
            # 被合并掉的 name 作为 alias 追加（如果不同于主）
            if p.get("name") and p["name"] != main.get("name") and p["name"] not in all_aliases:
                all_aliases.append(p["name"])
            if p.get("cn_name") and p["cn_name"] != main.get("cn_name") and p["cn_name"] not in all_aliases:
                all_aliases.append(p["cn_name"])

        main["target_vt_ids"] = sorted(all_vts)
        main["member_fields"] = all_members[:MAX_MEMBERS_IN_OUTPUT]
        main["aliases"] = all_aliases
        main["similar_existing_slots"] = _dedupe_similar(all_similar)[:5]
        main["support_count"] = total_support
        # 源汇总：用 "intra_vt_merged:" 前缀保留审计
        combined_source = "intra_vt_merged:" + "+".join(sorted({s.split(":")[0] for s in sources if s}))
        main["source"] = combined_source
        main["proposal_id"] = proposal_id_for(combined_source, main["name"])
        main["absorbed_proposal_ids"] = [p["proposal_id"] for p in absorbed]

        merged.append(main)
        # 审计记录
        sims_for_audit = []
        for a_idx, b_idx, sim in edges:
            if find(a_idx) == root and (a_idx == main_idx or b_idx == main_idx):
                other = b_idx if a_idx == main_idx else a_idx
                sims_for_audit.append({
                    "id": proposals[other]["proposal_id"],
                    "name": proposals[other]["name"],
                    "similarity": round(sim, 3),
                })
        audit.append({
            "kept_id": main["proposal_id"],
            "kept_name": main["name"],
            "kept_support": main["support_count"],
            "absorbed": [
                {"id": p["proposal_id"], "name": p["name"], "support": p.get("support_count", 0)}
                for p in absorbed
            ],
            "direct_edges": sims_for_audit,
        })

    return merged, audit


def write_yaml(proposals: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # sort: base > domain > vt_local; 内部按 support_count 降序
    scope_order = {"base": 0, "domain": 1, "vt_local": 2}
    proposals_sorted = sorted(
        proposals,
        key=lambda p: (scope_order.get(p["scope"], 9), -p["support_count"], p["name"]),
    )
    out = {
        "meta": {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "total": len(proposals_sorted),
            "by_scope": dict(Counter(p["scope"] for p in proposals_sorted)),
            "by_source": dict(Counter(p["source"].split(":")[0] for p in proposals_sorted)),
            "by_status": dict(Counter(p["status"] for p in proposals_sorted)),
        },
        "proposals": proposals_sorted,
    }
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True, sort_keys=False, width=200)
    print(f"  → wrote {len(proposals_sorted)} proposals to {path.relative_to(REPO_ROOT)}")


def write_diagnostic(
    rows: pd.DataFrame,
    clusters: list[dict],
    cluster_proposals: list[dict],
    llm_proposals: list[dict],
    final: list[dict],
    path: Path,
    elapsed_sec: float,
    merge_audit: list[dict] | None = None,
) -> None:
    lines: list[str] = []
    lines.append("# I-05b 新槽位自动发现诊断\n")
    lines.append(f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- 耗时: {elapsed_sec:.1f}s\n")

    lines.append("## 入口样本")
    lines.append(f"- 参与聚类的 (field × VT) 行数: {len(rows)}")
    lines.append(f"- 独立 VT 数: {rows['vt_id'].nunique()}")
    lines.append(f"- 独立字段数: {rows[['table_en', 'field_name']].drop_duplicates().shape[0]}")
    status_dist = rows["review_status"].value_counts().to_dict()
    lines.append(f"- review_status 分布: {status_dist}\n")

    lines.append("## 聚类")
    lines.append(f"- VT 内 HDBSCAN 总聚类数（满足 min_cluster_size={MIN_CLUSTER_SIZE} + cohesion≥{MIN_COHESION}）: {len(clusters)}")
    if clusters:
        sizes = [len(c["members"]) for c in clusters]
        cohesions = [c["cohesion"] for c in clusters]
        lines.append(f"- 聚类大小: min={min(sizes)} / median={int(np.median(sizes))} / max={max(sizes)} / avg={np.mean(sizes):.1f}")
        lines.append(f"- 内聚度: min={min(cohesions):.2f} / median={np.median(cohesions):.2f} / max={max(cohesions):.2f}")

        # 聚类大小分布分桶
        bins = [(3, 5), (5, 10), (10, 30), (30, 100), (100, 10000)]
        lines.append("- 聚类大小分布：")
        for lo, hi in bins:
            cnt = sum(1 for s in sizes if lo <= s < hi)
            lines.append(f"  - [{lo}, {hi}): {cnt}")

        # 大聚类 Top 20（warning：可能是噪声聚类）
        lines.append("\n### ⚠️ 大聚类 Top 20（size ≥ 20，建议人工检查是否噪声）")
        big_clusters = sorted(clusters, key=lambda c: -len(c["members"]))[:20]
        for c in big_clusters:
            if len(c["members"]) < 20:
                continue
            members = c["members"]
            sample_fields = ", ".join(f"`{m['field_name']}`({m.get('field_comment', '')[:10]})" for m in members[:5])
            top1_dist = Counter(m.get("top1_slot") for m in members if m.get("top1_slot"))
            top1_str = ", ".join(f"{k}({v})" for k, v in top1_dist.most_common(3))
            lines.append(
                f"- vt={c['vt_id']} | size={len(members)} | cohesion={c['cohesion']:.2f} | "
                f"top1_slots=[{top1_str}] | samples={sample_fields}"
            )
    lines.append("")

    lines.append("## Proposals 来源")
    lines.append(f"- 聚类命名成功: {len(cluster_proposals)}")
    lines.append(f"- LLM 归一阶段原建议聚合: {len(llm_proposals)}")
    lines.append(f"- 最终（去重 + 跨 VT 聚合后）: {len(final)}")
    lines.append("")

    lines.append("## 按 scope 分布")
    scope_counter = Counter(p["scope"] for p in final)
    for sc, cnt in scope_counter.most_common():
        lines.append(f"- {sc}: {cnt}")
    lines.append("")

    if merge_audit:
        lines.append(f"## 同 VT 内语义合并审计（sim≥{PROPOSAL_INTRA_VT_MERGE_SIM}）")
        lines.append(f"- 合并组数: {len(merge_audit)}")
        lines.append(f"- 被合并的 proposal 总数: {sum(len(a['absorbed']) for a in merge_audit)}")
        lines.append("\n### Top 20 合并组")
        top_audit = sorted(merge_audit, key=lambda a: -len(a["absorbed"]))[:20]
        for a in top_audit:
            abs_str = ", ".join(
                f"`{ab['name']}`(support={ab['support']})" for ab in a["absorbed"]
            )
            lines.append(f"- 保留 `{a['kept_name']}` (support={a['kept_support']}) ← {abs_str}")
        lines.append("")

    lines.append("## Top 20 支持数最高的 proposal")
    top = sorted(final, key=lambda p: -p["support_count"])[:20]
    for p in top:
        lines.append(
            f"- `{p['name']}` ({p.get('cn_name', '')}) | scope={p['scope']} | "
            f"support={p['support_count']} | VT={len(p['target_vt_ids'])} | "
            f"source={p['source']} | conf={p.get('llm_naming_confidence')}"
        )
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  → wrote diagnostic to {path.relative_to(REPO_ROOT)}")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="I-05b 新槽位自动发现")
    parser.add_argument("--limit-vt", type=int, default=None, help="只处理前 N 个 VT（smoke 用）")
    parser.add_argument("--skip-llm", action="store_true", help="跳过 LLM 命名，只输出聚类统计（调试用）")
    args = parser.parse_args()

    t0 = time.time()
    print("[1/8] 加载输入")
    fn, ff, sd, bs = load_inputs()
    existing_slots = collect_existing_slots(sd, bs)
    print(f"  已有槽位: {len(existing_slots)}（base + extended）")

    print("[2/8] 构造聚类入口样本")
    rows = build_cluster_input(fn, ff)
    if args.limit_vt:
        keep_vts = sorted(rows["vt_id"].unique())[: args.limit_vt]
        rows = rows[rows["vt_id"].isin(keep_vts)].copy()
        print(f"  [smoke] 限定前 {args.limit_vt} 个 VT: {keep_vts}")
    print(f"  样本行数: {len(rows)}")

    print("[3/8] 字段 embedding")
    unique_fields = rows.drop_duplicates(["table_en", "field_name"])
    field_records = unique_fields.to_dict("records")
    field_texts = [build_field_text(r) for r in field_records]
    print(f"  独立字段数: {len(field_texts)}")
    field_embs = embed_texts(field_texts)
    emb_map = {
        (r["table_en"], r["field_name"]): field_embs[i]
        for i, r in enumerate(field_records)
    }

    print("[4/8] 已有槽位 embedding（查重用）")
    slot_texts = [build_slot_text(s) for s in existing_slots]
    slot_embs = embed_texts(slot_texts) if slot_texts else np.zeros((0, 0))
    print(f"  已有槽位 embedding 数: {slot_embs.shape[0]}")

    print("[5/8] VT 内 HDBSCAN 聚类")
    clusters = cluster_per_vt(rows, emb_map)
    print(f"  产生聚类数: {len(clusters)}")

    print("[6/8] LLM 命名 + 查重")
    cluster_proposals: list[dict] = []
    if args.skip_llm:
        print("  [skip-llm] 跳过 LLM 命名")
    else:
        for i, cl in enumerate(clusters, 1):
            if i % 20 == 0 or i == len(clusters):
                print(f"  [{i}/{len(clusters)}] naming clusters...")
            naming = llm_name_cluster(cl, existing_slots)
            if naming is None:
                continue
            is_dup, similar = dedup_vs_existing(naming, cl["centroid"], existing_slots, slot_embs)
            if is_dup:
                continue
            cluster_proposals.append(cluster_to_proposal(cl, naming, similar))
    print(f"  命名成功且未与已有槽位重复: {len(cluster_proposals)}")

    print("[7/8] 合并 field_normalization 的 LLM 原建议")
    llm_proposals = merge_llm_suggestions(fn, existing_slots, slot_embs)
    print(f"  LLM 归一阶段建议聚合: {len(llm_proposals)}")

    print("[8/8] 跨 VT 聚合 + 同 VT 内语义合并 + 确定 scope + 输出")
    all_proposals = cluster_proposals + llm_proposals
    # proposal embeddings 留空（当前 aggregate 只用 name 合并）
    cross_merged = aggregate_cross_vt(all_proposals, np.zeros((0, 0)))
    print(f"  跨 VT name 合并后: {len(cross_merged)}")

    final, merge_audit = merge_similar_intra_vt(cross_merged)
    print(f"  同 VT 内语义合并后（sim≥{PROPOSAL_INTRA_VT_MERGE_SIM}）: {len(final)}")
    print(f"  合并组数: {len(merge_audit)}")

    write_yaml(final, OUT_YAML)
    write_diagnostic(
        rows, clusters, cluster_proposals, llm_proposals, final, OUT_DIAG,
        time.time() - t0, merge_audit=merge_audit,
    )
    print(f"\n完成，总耗时 {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
