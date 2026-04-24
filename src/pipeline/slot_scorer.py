"""I-03: 字段→槽位打分（TF-IDF + Embedding 双通路并行）。

五因子公式（§ 10.8.5）：
  slot_score = 0.30*lexical + 0.25*comment_semantic + 0.20*sample_pattern
             + 0.15*context_role + 0.10*usage

双通路仅 comment_semantic 子分不同：
  - slot_score_tfidf: 用 TF-IDF char n-gram 余弦
  - slot_score_embedding: 用 qwen text-embedding-v3 余弦

候选槽位范围：每个字段仅与其所属 VT 的槽位打分（避免 O(N²)）。
"""
from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import embed as embed_client  # noqa: E402


FEATURES_PARQUET = REPO_ROOT / "output" / "field_features.parquet"
SLOT_YAML = REPO_ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS_YAML = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"
SCAFFOLD_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"

OUT_PARQUET = REPO_ROOT / "output" / "slot_scores.parquet"
OUT_TOP3 = REPO_ROOT / "output" / "slot_scores_top3.parquet"
OUT_DIAG = REPO_ROOT / "output" / "slot_scores_diagnostic.md"

# 人工审核结果（用于扩展同 VT 内 slot 的 aliases，让未审字段更容易归到正确 slot）
REVIEWED_PARQUET = REPO_ROOT / "output" / "field_normalization_reviewed.parquet"
REVIEWED_DECISIONS_AS_ANCHOR = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot"}


def load_reviewed_anchors() -> dict[tuple[str, str], list[tuple[str, str]]]:
    """加载已审决策作为 slot 锚点。

    返回 {(vt_id, slot_name): [(field_name, field_comment), ...]}
    仅取确认归属类决策；mark_noise / skip 不算锚点。
    """
    if not REVIEWED_PARQUET.exists():
        return {}
    try:
        df = pd.read_parquet(REVIEWED_PARQUET)
    except Exception:
        return {}
    if df.empty:
        return {}
    anchors: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for _, r in df.iterrows():
        decision = str(r.get("decision") or "")
        if decision not in REVIEWED_DECISIONS_AS_ANCHOR:
            continue
        slot = r.get("decision_slot")
        if not slot or pd.isna(slot):
            continue
        key = (str(r["vt_id"]), str(slot))
        anchors.setdefault(key, []).append((str(r["table_en"]), str(r["field_name"])))
    return anchors


# ============ logical_type → expected sample patterns ============

LOGICAL_TYPE_PATTERNS: dict[str, set[str]] = {
    "id_card_no_or_passport": {"id_card_18", "id_card_15", "cn_passport", "generic_passport"},
    "passport_no": {"cn_passport", "generic_passport"},
    "phone_no": {"cn_mobile"},
    "vehicle_plate_no": {"cn_plate_blue", "cn_plate_green"},
    "vin_no": {"vin"},
    "device_id": {"imei", "mac"},
    "datetime": {"datetime_iso", "datetime_compact", "timestamp_ms", "timestamp_s"},
    "date": {"date_iso", "date_compact"},
    "region_code": {"region_code_6", "region_code_9", "region_code_12"},
    "coordinate": {"longitude_decimal", "latitude_decimal"},
    "amount": {"decimal_money"},
    "count": {"integer_short", "integer_long", "all_digit"},
    "person_name": {"all_chinese_2_4"},
    "organization_id": {"social_credit"},
    "address": {"chinese_with_punct", "all_chinese"},
    "track_point_name": {"chinese_with_punct", "all_chinese"},
    "status_code": {"integer_short", "all_chinese_2_4"},
    "relation_type": {"all_chinese_2_4", "all_chinese"},
    "case_no": {"all_alnum", "integer_long"},
    "source_system": {"all_chinese", "chinese_with_punct"},
    "free_text": set(),
}

# pattern 弱重叠判定（命中相邻 pattern 给 0.5 分）
PATTERN_ADJACENCY: dict[str, set[str]] = {
    "id_card_18": {"id_card_15", "all_digit", "integer_long"},
    "cn_mobile": {"all_digit", "integer_long"},
    "cn_plate_blue": {"cn_plate_green"},
    "datetime_iso": {"datetime_compact", "date_iso", "timestamp_ms"},
    "region_code_6": {"all_digit", "integer_short"},
    "social_credit": {"all_upper_letter", "all_alnum"},
}


# ============ 数据加载 ============


def load_slot_definitions() -> dict[str, list[dict]]:
    """返回 vt_id → slots list。"""
    data = yaml.safe_load(SLOT_YAML.read_text(encoding="utf-8"))
    result: dict[str, list[dict]] = {}
    for vt in data.get("virtual_tables", []):
        result[vt["vt_id"]] = vt.get("slots", [])
    return result


def load_vt_meta() -> dict[str, dict]:
    """从脚手架定稿加载 VT 元信息。"""
    import json
    data = json.loads(SCAFFOLD_JSON.read_text(encoding="utf-8"))
    return {vt["vt_id"]: vt for vt in data.get("virtual_tables", [])}


def load_base_slots() -> dict[str, dict]:
    """base_slots name → slot dict"""
    data = yaml.safe_load(BASE_SLOTS_YAML.read_text(encoding="utf-8"))
    return {s["name"]: s for s in data["base_slots"]}


# ============ 槽位表示（统一展开 base / extended）============


def materialize_slot(slot: dict, base_by_name: dict[str, dict]) -> dict:
    """把 base/extended 槽位统一展开成完整 dict。"""
    name = slot["name"]
    from_type = slot.get("from", "extended")
    if from_type == "base" and name in base_by_name:
        b = base_by_name[name]
        return {
            "name": name,
            "from": "base",
            "cn_name": b.get("cn_name", ""),
            "logical_type": b.get("logical_type", ""),
            "role": slot.get("role") or b.get("role", ""),
            "description": b.get("description", ""),
            "aliases": list(b.get("aliases", [])),
            "sample_patterns": [p.get("name", "") for p in b.get("sample_patterns", []) if p.get("name")],
            "applicable_table_types": list(b.get("applicable_table_types", [])),
        }
    else:
        logical_type = slot.get("logical_type", "custom") or "custom"
        expected_patterns = list(LOGICAL_TYPE_PATTERNS.get(logical_type, set()))
        return {
            "name": name,
            "from": "extended",
            "cn_name": slot.get("cn_name", name),
            "logical_type": logical_type,
            "role": slot.get("role", ""),
            "description": slot.get("llm_reason", ""),
            "aliases": list(slot.get("aliases", []) or []),
            "sample_patterns": expected_patterns,
            "applicable_table_types": list(slot.get("applicable_table_types", [])),
        }


# ============ 五因子分数 ============


def _token_set(text: str) -> set[str]:
    """把一段文本切成 token 集合：按非字母数字切分 + 字符级（中文）。"""
    if not text:
        return set()
    text = str(text).lower()
    tokens: set[str] = set()
    for t in re.split(r"[\s_\-,，/、()（）:：]+", text):
        if t:
            tokens.add(t)
    # 中文多字符：加上相邻 2-gram
    cn = re.findall(r"[一-鿿]+", text)
    for seg in cn:
        if len(seg) >= 2:
            for i in range(len(seg) - 1):
                tokens.add(seg[i : i + 2])
    return tokens


def _edit_sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    m, n = len(a), len(b)
    if m == 0 or n == 0:
        return 0.0
    # 动态规划编辑距离
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, n + 1):
            tmp = dp[j]
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + cost)
            prev = tmp
    dist = dp[n]
    return 1.0 - dist / max(m, n)


def compute_lexical_score(
    field_name: str,
    comment_clean: str,
    name_tokens: list[str],
    name_expanded: list[str],
    slot_aliases: list[str],
    slot_cn_name: str,
    slot_name: str,
) -> float:
    """词法相似度：取四种度量的 max。"""
    if not slot_aliases and not slot_cn_name:
        return 0.0

    all_alias_tokens: set[str] = set()
    all_alias_tokens.update(_token_set(slot_name))
    all_alias_tokens.update(_token_set(slot_cn_name))
    for a in slot_aliases:
        all_alias_tokens.update(_token_set(a))
    all_alias_tokens.discard("")

    # 1. token Jaccard
    field_tokens = set(name_tokens) | set(name_expanded) | set(_token_set(comment_clean))
    field_tokens.discard("")
    if field_tokens and all_alias_tokens:
        inter = len(field_tokens & all_alias_tokens)
        union = len(field_tokens | all_alias_tokens)
        token_jaccard = inter / union if union else 0.0
    else:
        token_jaccard = 0.0

    # 2. edit distance：对每个 alias 算 normalized，取 max；field_name_lower vs alias_lower
    field_lower = field_name.lower()
    best_edit = 0.0
    for a in [slot_name, slot_cn_name] + slot_aliases:
        if not a:
            continue
        s = _edit_sim(field_lower, a.lower())
        if s > best_edit:
            best_edit = s

    # 3. 缩写/展开词典命中：name_expanded 中的某项 == 某个 alias
    dict_hit = 0.0
    expanded_set = set(name_expanded)
    for a in [slot_cn_name] + slot_aliases:
        if a and (a in expanded_set or a.lower() in expanded_set):
            dict_hit = 1.0
            break

    # 4. 子串命中
    substring = 0.0
    for a in slot_aliases + [slot_cn_name]:
        if not a or len(a) < 2:
            continue
        a_lower = a.lower()
        if a_lower in field_lower or a in comment_clean:
            substring = max(substring, 0.8 if a in comment_clean else 0.7)

    return max(token_jaccard, best_edit, dict_hit, substring)


def compute_sample_pattern_score(
    field_patterns: list[str],
    slot_patterns: list[str],
) -> float:
    """样例 pattern 匹配分。

    - 槽位没指定期望 pattern（如 person_name、free_text）→ 中性分 0.5（不能因为槽位没定义 pattern 就惩罚字段）
    - 字段样例全空 → 中性 0.5（缺数据不扣分）
    - 强匹配 → 1.0；弱匹配 → 0.5；无匹配 → 0.1
    """
    # 槽位无期望 pattern → 中性
    if not slot_patterns:
        return 0.5

    # 字段无 pattern（全空样例）→ 中性
    if not field_patterns or field_patterns == ["all_null_or_empty"]:
        return 0.5

    field_set = set(field_patterns)
    slot_set = set(slot_patterns)

    if field_set & slot_set:
        return 1.0

    # 弱重叠：任一 field pattern 在某个 slot pattern 的相邻集合里
    for fp in field_patterns:
        adjacent = PATTERN_ADJACENCY.get(fp, set())
        if adjacent & slot_set:
            return 0.5
    for sp in slot_patterns:
        adjacent = PATTERN_ADJACENCY.get(sp, set())
        if adjacent & field_set:
            return 0.5

    # 明确不匹配也不给 0（避免过度惩罚字段类型未覆盖的情况）
    return 0.1


def compute_context_role_score(
    vt_table_type: str,
    has_subject_id: bool,
    has_time: bool,
    has_location: bool,
    slot_role: str,
    slot_applicable: list[str],
) -> float:
    """上下文匹配：表类型-槽位角色的兼容度 + 共现信号。"""
    score = 0.0

    # 表类型适配度
    if slot_applicable and vt_table_type in slot_applicable:
        score += 0.3
    elif not slot_applicable:
        score += 0.15  # 未指定视为中性

    # 角色 × 表类型 的匹配加成
    role_matrix = {
        ("主档", "subject_id"): 0.35,
        ("主档", "display"): 0.25,
        ("主档", "filter"): 0.2,
        ("主档", "time"): 0.15,
        ("事件", "subject_id"): 0.3,
        ("事件", "time"): 0.35,
        ("事件", "location"): 0.3,
        ("事件", "filter"): 0.2,
        ("事件", "measure"): 0.25,
        ("关系", "subject_id"): 0.3,
        ("关系", "relation_subject"): 0.4,
        ("关系", "subject"): 0.3,
        ("标签", "subject_id"): 0.3,
        ("标签", "filter"): 0.3,
        ("聚合", "subject_id"): 0.25,
        ("聚合", "time"): 0.3,
        ("聚合", "measure"): 0.3,
    }
    score += role_matrix.get((vt_table_type, slot_role), 0.0)

    # 共现信号
    if slot_role == "time" and has_subject_id:
        score += 0.15
    if slot_role == "location" and has_subject_id:
        score += 0.1
    if slot_role == "relation_subject" and has_subject_id:
        score += 0.15

    return min(score, 1.0)


def compute_usage_score(
    usage_count: float,
    sql_count: float,
    role_select: float,
    role_where: float,
    role_join: float,
    max_stats: dict[str, float],
) -> float:
    def norm(v: float, key: str) -> float:
        m = max_stats.get(key, 0)
        if m <= 0:
            return 0.0
        return min(1.0, math.log1p(max(0, v)) / math.log1p(m))

    return (
        0.40 * norm(role_where, "role_where")
        + 0.30 * norm(role_join, "role_join")
        + 0.20 * norm(role_select, "role_select")
        + 0.10 * norm(usage_count, "usage_count")
    )


# ============ 批量计算 ============


def _slot_semantic_text(slot: dict) -> str:
    """把 slot 转成一段语义文本，用于 TF-IDF/embedding 对比。"""
    parts = [slot.get("cn_name", ""), slot.get("description", "")]
    parts += slot.get("aliases", [])
    return " ".join(p for p in parts if p)


def compute_tfidf_semantic(field_comments: list[str], slot_texts: list[str]) -> np.ndarray:
    """返回 [N_field, N_slot] 的余弦矩阵。"""
    corpus = field_comments + slot_texts
    if not any(corpus):
        return np.zeros((len(field_comments), len(slot_texts)))
    corpus = [c if c else "<empty>" for c in corpus]
    vec = TfidfVectorizer(analyzer="char", ngram_range=(2, 4), min_df=1)
    try:
        matrix = vec.fit_transform(corpus)
    except ValueError:
        return np.zeros((len(field_comments), len(slot_texts)))
    f = matrix[: len(field_comments)]
    s = matrix[len(field_comments):]
    return cosine_similarity(f, s)


def compute_embedding_semantic(
    field_comments: list[str],
    slot_texts: list[str],
    use_cache: bool = True,
) -> np.ndarray:
    """对 field/slot 文本分别 embed 后算余弦。"""
    # 去重 + 保持顺序
    unique_field = list(dict.fromkeys(t or "<empty>" for t in field_comments))
    unique_slot = list(dict.fromkeys(t or "<empty>" for t in slot_texts))

    f_vecs = embed_client(unique_field, use_cache=use_cache)
    s_vecs = embed_client(unique_slot, use_cache=use_cache)

    f_map = dict(zip(unique_field, f_vecs))
    s_map = dict(zip(unique_slot, s_vecs))

    f_arr = np.array([f_map[t or "<empty>"] for t in field_comments])
    s_arr = np.array([s_map[t or "<empty>"] for t in slot_texts])

    # L2 normalize
    f_norm = np.linalg.norm(f_arr, axis=1, keepdims=True)
    s_norm = np.linalg.norm(s_arr, axis=1, keepdims=True)
    f_arr = f_arr / np.where(f_norm == 0, 1, f_norm)
    s_arr = s_arr / np.where(s_norm == 0, 1, s_norm)
    return f_arr @ s_arr.T


# ============ 主流程 ============


def compute_scores(
    limit_vt_id: str | None = None,
    enable_embedding: bool = True,
) -> pd.DataFrame:
    print("加载输入...")
    field_df = pd.read_parquet(FEATURES_PARQUET)
    slot_defs = load_slot_definitions()
    base_by_name = load_base_slots()
    vt_meta = load_vt_meta()

    # 过滤技术噪声
    usable = field_df[~field_df["is_technical_noise"]].copy().reset_index(drop=True)
    print(f"  字段总数 {len(field_df)} / 非噪声 {len(usable)}")

    # 构建全局使用统计用于归一化
    max_stats = {
        "usage_count": float(usable["usage_count"].max() or 1),
        "sql_count": float(usable["sql_count"].max() or 1),
        "role_select": float(usable["role_select"].max() or 1),
        "role_where": float(usable["role_where"].max() or 1),
        "role_join": float(usable["role_join"].max() or 1),
    }

    # 对每个字段展开 (field × related_vt) 行
    records: list[dict] = []
    print("展开 field × VT 组合...")

    def _to_list(v: Any) -> list:
        if v is None:
            return []
        if isinstance(v, (list, tuple)):
            return list(v)
        # numpy array 或其他可迭代
        try:
            return list(v)
        except TypeError:
            return []

    for _, row in usable.iterrows():
        vt_ids = _to_list(row.get("related_vt_ids"))
        for vt_id in vt_ids:
            if limit_vt_id and vt_id != limit_vt_id:
                continue
            if vt_id not in slot_defs:
                continue
            for slot_raw in slot_defs[vt_id]:
                slot = materialize_slot(slot_raw, base_by_name)
                records.append({
                    "table_en": row["table_en"],
                    "field_name": row["field_name"],
                    "field_comment": row["field_comment"],
                    "comment_clean": row["comment_clean"],
                    "name_tokens": _to_list(row["name_tokens"]),
                    "name_expanded": _to_list(row["name_expanded"]),
                    "sample_patterns": _to_list(row["sample_patterns"]),
                    "usage_count": float(row["usage_count"]),
                    "sql_count": float(row["sql_count"]),
                    "role_select": float(row["role_select"]),
                    "role_where": float(row["role_where"]),
                    "role_join": float(row["role_join"]),
                    "has_subject_id": bool(row["has_subject_id"]),
                    "has_time": bool(row["has_time"]),
                    "has_location": bool(row["has_location"]),
                    "vt_id": vt_id,
                    "vt_table_type": vt_meta.get(vt_id, {}).get("table_type", ""),
                    "slot_name": slot["name"],
                    "slot_from": slot["from"],
                    "slot_role": slot["role"],
                    "slot_cn_name": slot["cn_name"],
                    "slot_aliases": slot["aliases"],
                    "slot_patterns": slot["sample_patterns"],
                    "slot_applicable": slot["applicable_table_types"],
                    "slot_text": _slot_semantic_text(slot),
                })

    print(f"  展开后行数: {len(records)}")
    if not records:
        return pd.DataFrame()

    # —— 注入人工审核锚点：把已被人工归到 slot X 的字段 name + comment 加进 X 的 aliases ——
    # 这样未审的同义字段（如 xm/name1）更容易被打到 person_name
    anchors = load_reviewed_anchors()
    if anchors:
        feat_lookup = {
            (str(r["table_en"]), str(r["field_name"])): r
            for _, r in usable.iterrows()
        }
        injected = 0
        for r in records:
            key = (r["vt_id"], r["slot_name"])
            anchor_fields = anchors.get(key)
            if not anchor_fields:
                continue
            extra: list[str] = []
            for tbl, fname in anchor_fields:
                # 跳过当前记录自身（避免无意义的自循环）
                if tbl == r["table_en"] and fname == r["field_name"]:
                    continue
                extra.append(fname)
                feat = feat_lookup.get((tbl, fname))
                if feat is not None:
                    cm = str(feat.get("field_comment") or "")
                    if cm:
                        extra.append(cm)
            if not extra:
                continue
            existing = list(r["slot_aliases"]) if isinstance(r["slot_aliases"], (list, tuple)) else []
            r["slot_aliases"] = list(dict.fromkeys(existing + extra))
            injected += 1
        print(f"  锚点扩展 aliases: {injected} 条 record（来自 {sum(len(v) for v in anchors.values())} 个人工审核字段）")

    # 计算五因子分数（向量化困难，走 Python loop）
    print("计算词法 / pattern / context / usage 分数...")
    t0 = time.time()
    for r in records:
        r["lexical_score"] = compute_lexical_score(
            r["field_name"], r["comment_clean"],
            r["name_tokens"], r["name_expanded"],
            r["slot_aliases"], r["slot_cn_name"], r["slot_name"],
        )
        r["sample_pattern_score"] = compute_sample_pattern_score(
            r["sample_patterns"], r["slot_patterns"],
        )
        r["context_role_score"] = compute_context_role_score(
            r["vt_table_type"],
            r["has_subject_id"], r["has_time"], r["has_location"],
            r["slot_role"], r["slot_applicable"],
        )
        r["usage_score"] = compute_usage_score(
            r["usage_count"], r["sql_count"],
            r["role_select"], r["role_where"], r["role_join"],
            max_stats,
        )
    print(f"  耗时 {time.time()-t0:.1f}s")

    # TF-IDF 语义
    print("计算 comment_semantic_tfidf...")
    t0 = time.time()
    field_comments = [r["comment_clean"] for r in records]
    slot_texts = [r["slot_text"] for r in records]
    # 对每对做 pairwise（实际是对应 i 的 field_comments[i] 和 slot_texts[i]）
    # 为了避免构造 NxM 大矩阵，做一次对角计算
    # 但实际我们要对每对 (comment[i], slot_text[i]) 算相似度
    unique_pairs = list({(fc, st) for fc, st in zip(field_comments, slot_texts)})
    # 构造查询矩阵
    unique_comments = list({fc for fc, _ in unique_pairs})
    unique_slots = list({st for _, st in unique_pairs})
    sim_matrix = compute_tfidf_semantic(unique_comments, unique_slots)
    comment_idx = {c: i for i, c in enumerate(unique_comments)}
    slot_idx = {s: i for i, s in enumerate(unique_slots)}
    for r in records:
        i = comment_idx[r["comment_clean"]]
        j = slot_idx[r["slot_text"]]
        r["comment_semantic_tfidf"] = float(sim_matrix[i, j])
    print(f"  耗时 {time.time()-t0:.1f}s (unique fields={len(unique_comments)}, unique slots={len(unique_slots)})")

    # Embedding 语义
    if enable_embedding:
        print("计算 comment_semantic_embedding...")
        t0 = time.time()
        emb_matrix = compute_embedding_semantic(unique_comments, unique_slots)
        for r in records:
            i = comment_idx[r["comment_clean"]]
            j = slot_idx[r["slot_text"]]
            r["comment_semantic_embedding"] = float(emb_matrix[i, j])
        print(f"  耗时 {time.time()-t0:.1f}s")
    else:
        for r in records:
            r["comment_semantic_embedding"] = 0.0

    # 合成 slot_score（双通路）
    print("合成 slot_score...")
    for r in records:
        base_part = (
            0.30 * r["lexical_score"]
            + 0.20 * r["sample_pattern_score"]
            + 0.15 * r["context_role_score"]
            + 0.10 * r["usage_score"]
        )
        r["slot_score_tfidf"] = base_part + 0.25 * r["comment_semantic_tfidf"]
        r["slot_score_embedding"] = base_part + 0.25 * r["comment_semantic_embedding"]

    # 输出 DataFrame
    keep_cols = [
        "table_en", "field_name", "field_comment",
        "vt_id", "vt_table_type",
        "slot_name", "slot_from", "slot_role",
        "lexical_score", "comment_semantic_tfidf", "comment_semantic_embedding",
        "sample_pattern_score", "context_role_score", "usage_score",
        "slot_score_tfidf", "slot_score_embedding",
    ]
    return pd.DataFrame(records)[keep_cols]


# ============ top3 摘要 ============


def compute_top3(df: pd.DataFrame) -> pd.DataFrame:
    """对每个 (field, vt) 取 top3 候选槽位（按 slot_score_tfidf 排序）。"""
    if df.empty:
        return df
    df = df.sort_values(
        ["table_en", "field_name", "vt_id", "slot_score_tfidf"],
        ascending=[True, True, True, False],
    )
    rows = []
    for (t, f, v), g in df.groupby(["table_en", "field_name", "vt_id"], sort=False):
        top = g.head(3).reset_index(drop=True)
        row = {"table_en": t, "field_name": f, "vt_id": v,
               "field_comment": top["field_comment"].iloc[0]}
        for i in range(3):
            if i < len(top):
                row[f"top{i+1}_slot"] = top["slot_name"].iloc[i]
                row[f"top{i+1}_slot_from"] = top["slot_from"].iloc[i]
                row[f"top{i+1}_score_tfidf"] = round(float(top["slot_score_tfidf"].iloc[i]), 4)
                row[f"top{i+1}_score_embedding"] = round(float(top["slot_score_embedding"].iloc[i]), 4)
            else:
                row[f"top{i+1}_slot"] = None
                row[f"top{i+1}_slot_from"] = None
                row[f"top{i+1}_score_tfidf"] = None
                row[f"top{i+1}_score_embedding"] = None
        rows.append(row)
    return pd.DataFrame(rows)


def write_diagnostic(df: pd.DataFrame, top3: pd.DataFrame, total_seconds: float) -> None:
    lines: list[str] = [
        "# I-03 字段→槽位打分诊断",
        "",
        f"- 行数（field × candidate slot 对数）: {len(df)}",
        f"- 独立字段数: {len(top3) if not top3.empty else 0}",
        f"- 总耗时: {total_seconds:.1f}s",
        "",
        "## slot_score_tfidf 分布",
        "",
    ]
    buckets = [("≥0.85", 0.85, 1.01), ("0.60-0.85", 0.60, 0.85), ("0.40-0.60", 0.40, 0.60), ("<0.40", 0, 0.40)]
    for label, lo, hi in buckets:
        c = ((top3["top1_score_tfidf"] >= lo) & (top3["top1_score_tfidf"] < hi)).sum()
        pct = c / len(top3) * 100 if len(top3) else 0
        lines.append(f"- top1 {label}: {c} ({pct:.1f}%)")
    lines.append("")

    lines += ["## slot_score_embedding 分布", ""]
    for label, lo, hi in buckets:
        c = ((top3["top1_score_embedding"] >= lo) & (top3["top1_score_embedding"] < hi)).sum()
        pct = c / len(top3) * 100 if len(top3) else 0
        lines.append(f"- top1 {label}: {c} ({pct:.1f}%)")
    lines.append("")

    # 双通路 top1 一致率
    agree = (top3["top1_slot"] == top3["top1_slot"]).sum()  # 自身一致性（正确应该对比两个通路的 top1）
    # 但我们现在 top3 是按 tfidf 排的，要看 embedding 通路最高分的槽位是不是同一个
    # 需要从原 df 取
    df_sorted_emb = df.sort_values(
        ["table_en", "field_name", "vt_id", "slot_score_embedding"],
        ascending=[True, True, True, False],
    ).groupby(["table_en", "field_name", "vt_id"], sort=False).first().reset_index()
    merged = top3[["table_en", "field_name", "vt_id", "top1_slot"]].merge(
        df_sorted_emb[["table_en", "field_name", "vt_id", "slot_name"]],
        on=["table_en", "field_name", "vt_id"], how="inner",
    )
    agree_n = (merged["top1_slot"] == merged["slot_name"]).sum()
    agree_rate = agree_n / len(merged) * 100 if len(merged) else 0
    lines += [
        "## 双通路 top1 一致率",
        "",
        f"- 两通路 top1 槽位相同: {agree_n}/{len(merged)} ({agree_rate:.1f}%)",
        "",
    ]

    # 高置信命中抽样
    lines += ["## 高置信命中抽样 (top1_score_tfidf ≥ 0.85 前 15 条)", ""]
    high = top3[top3["top1_score_tfidf"] >= 0.85].head(15)
    for _, r in high.iterrows():
        lines.append(f"- `{r['field_name']}` ({r['field_comment'][:30]}) → **{r['top1_slot']}** tfidf={r['top1_score_tfidf']} emb={r['top1_score_embedding']}")
    lines.append("")

    # 低置信样本
    lines += ["## 低置信样本 (top1_score_tfidf < 0.40 前 15 条)", ""]
    low = top3[top3["top1_score_tfidf"] < 0.40].head(15)
    for _, r in low.iterrows():
        lines.append(f"- `{r['field_name']}` ({r['field_comment'][:30]}) → top1={r['top1_slot']} tfidf={r['top1_score_tfidf']}")
    lines.append("")

    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


def main(limit_vt_id: str | None = None, enable_embedding: bool = True) -> None:
    t0 = time.time()
    df = compute_scores(limit_vt_id=limit_vt_id, enable_embedding=enable_embedding)
    if df.empty:
        print("无数据，终止。")
        return
    top3 = compute_top3(df)
    total_dt = time.time() - t0

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    # limit_vt_id 模式：保留其他 VT 的行，只替换目标 VT
    if limit_vt_id and OUT_PARQUET.exists():
        existing = pd.read_parquet(OUT_PARQUET)
        kept = existing[existing["vt_id"] != limit_vt_id]
        df = pd.concat([kept, df], ignore_index=True)
        print(f"  [merge] slot_scores: 保留 {len(kept)} + 新增 {len(df) - len(kept)} 行")
    if limit_vt_id and OUT_TOP3.exists():
        existing_top3 = pd.read_parquet(OUT_TOP3)
        kept_top3 = existing_top3[existing_top3["vt_id"] != limit_vt_id]
        top3 = pd.concat([kept_top3, top3], ignore_index=True)
        print(f"  [merge] slot_scores_top3: 保留 {len(kept_top3)} + 新增 {len(top3) - len(kept_top3)} 行")

    df.to_parquet(OUT_PARQUET, index=False)
    top3.to_parquet(OUT_TOP3, index=False)
    write_diagnostic(df, top3, total_dt)

    print(f"\n=== I-03 完成 ===")
    print(f"行数: {len(df)} (field × slot 对数)")
    print(f"独立字段数: {len(top3)}")
    print(f"总耗时: {total_dt:.1f}s")
    print(f"slot_scores.parquet: {OUT_PARQUET}")
    print(f"slot_scores_top3.parquet: {OUT_TOP3}")
    print(f"诊断: {OUT_DIAG}")
