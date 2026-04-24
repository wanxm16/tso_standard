"""I-08: benchmark 评估。

输入：
  - data/benchmark/query_with_table_1.json
  - output/virtual_tables_scaffold_final.json
  - output/virtual_fields.json / virtual_field_mappings.json / field_aliases.json

输出：
  - output/evaluation.json
  - output/evaluation_details.parquet
  - output/evaluation_diagnostic.md
"""
from __future__ import annotations

import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import embed as embed_client  # noqa: E402

BENCH_JSON = REPO_ROOT / "data" / "benchmark" / "query_with_table_1.json"
BENCH_CSV = REPO_ROOT / "data" / "benchmark" / "query_sql.csv"
SCAFFOLD_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"
VFS_JSON = REPO_ROOT / "output" / "virtual_fields.json"
MAPPINGS_JSON = REPO_ROOT / "output" / "virtual_field_mappings.json"
ALIASES_JSON = REPO_ROOT / "output" / "field_aliases.json"
QUERY_INTENTS_JSON = REPO_ROOT / "output" / "query_intents.json"

OUT_JSON = REPO_ROOT / "output" / "evaluation.json"
OUT_DETAILS = REPO_ROOT / "output" / "evaluation_details.parquet"
OUT_DIAG = REPO_ROOT / "output" / "evaluation_diagnostic.md"


TOPK_LIST = [1, 3, 5, 10]


# ============ 数据加载 ============


def load_benchmark(source: str = "all", flag_filter: float | None = None) -> list[dict]:
    """加载 benchmark 查询集。

    Args:
        source: "json" / "csv" / "all"
        flag_filter: 仅 csv 有效，过滤 flag 列（如 1.0 表示高难度）
    """
    records: list[dict] = []

    if source in ("json", "all") and BENCH_JSON.exists():
        data = json.loads(BENCH_JSON.read_text(encoding="utf-8"))
        _extract_benchmark(data, records)

    if source in ("csv", "all") and BENCH_CSV.exists():
        df = pd.read_csv(BENCH_CSV, encoding="utf-8")
        if flag_filter is not None:
            df = df[df["flag"] == flag_filter]
        for _, row in df.iterrows():
            q = str(row.get("query") or "").strip()
            if not q:
                continue
            tables_raw = str(row.get("tables") or "")
            tables = [t.strip() for t in re.split(r"[;,，|]+", tables_raw) if t.strip()]
            records.append({
                "query_text": q,
                "expected_tables": tables,
                "sql": str(row.get("sql") or ""),
                "source": "csv",
                "flag": row.get("flag"),
            })

    # 去重
    seen: set[tuple[str, tuple[str, ...]]] = set()
    unique: list[dict] = []
    for r in records:
        key = (r["query_text"], tuple(sorted(r.get("expected_tables", []))))
        if key in seen or not r["query_text"]:
            continue
        seen.add(key)
        unique.append(r)
    return unique


def _extract_benchmark(node: Any, out: list[dict], depth: int = 0) -> None:
    if depth > 8:
        return
    if isinstance(node, list):
        for item in node:
            _extract_benchmark(item, out, depth + 1)
        return
    if not isinstance(node, dict):
        return

    # 识别 query / tables 键
    query_keys = {"query", "query_text", "question", "问题"}
    table_keys = {"tables", "table_list", "expected_tables", "相关表", "命中表"}

    key_map = {k.lower(): k for k in node.keys()}
    query_key = next((key_map[k] for k in key_map if k in query_keys), None)
    tables_key = next((key_map[k] for k in key_map if k in table_keys), None)

    if query_key and tables_key:
        qt = str(node.get(query_key, "") or "").strip()
        tables_raw = node.get(tables_key)
        tables: list[str] = []
        if isinstance(tables_raw, list):
            for t in tables_raw:
                if isinstance(t, dict):
                    t = t.get("table") or t.get("en") or t.get("name") or ""
                if t and isinstance(t, str):
                    tables.append(t.strip())
        elif isinstance(tables_raw, str):
            tables = [t.strip() for t in re.split(r"[,，|;\s]+", tables_raw) if t.strip()]
        if qt:
            out.append({
                "query_text": qt,
                "expected_tables": tables,
                "sql": str(node.get("sql") or node.get("query_sql") or ""),
            })

    for v in node.values():
        _extract_benchmark(v, out, depth + 1)


def load_scaffold() -> list[dict]:
    data = json.loads(SCAFFOLD_JSON.read_text(encoding="utf-8"))
    return data["virtual_tables"]


def load_virtual_fields() -> dict[str, list[dict]]:
    """vt_id → list[virtual_field]"""
    data = json.loads(VFS_JSON.read_text(encoding="utf-8"))
    by_vt: dict[str, list[dict]] = defaultdict(list)
    for v in data["virtual_fields"]:
        by_vt[v["vt_id"]].append(v)
    return dict(by_vt)


def load_aliases() -> dict[str, dict]:
    """vf_id → {seed_aliases, llm_aliases, question_words}"""
    if not ALIASES_JSON.exists():
        return {}
    data = json.loads(ALIASES_JSON.read_text(encoding="utf-8"))
    return {a["vf_id"]: a for a in data.get("field_aliases", [])}


# ============ 召回文本构建 ============


def build_vt_recall_text(vt: dict, vfs_by_vt: dict[str, list], aliases: dict[str, dict]) -> str:
    """一张 VT 的完整召回文本：
    - topic 重复 3 次（提升在 embedding 空间里的主题权重，避免被虚拟字段稀释）
    - l2_path + recall_hints（兼容旧字段）
    - recall_summary + typical_questions + topic_aliases（由 scripts/enrich_vt_recall.py 生成）
    - 虚拟字段全部 cn_name + aliases
    """
    parts: list[str] = []
    topic = vt.get("topic", "")
    if topic:
        # topic 重复 3 次加权（embedding 对重复词有累积响应；纯空格拼接，简单但有效）
        parts.extend([topic, topic, topic])
    for hint in vt.get("recall_hints", []) or []:
        parts.append(hint)
    parts.extend(vt.get("l2_path", []))

    # LLM 扩展的 VT 语义信息（新增，scripts/enrich_vt_recall.py 产出）
    summary = vt.get("recall_summary")
    if summary:
        parts.append(str(summary))
    for q in vt.get("typical_questions", []) or []:
        if q:
            parts.append(str(q))
    for a in vt.get("topic_aliases", []) or []:
        if a:
            parts.append(str(a))

    for vf in vfs_by_vt.get(vt["vt_id"], []):
        parts.append(vf["field_cn_name"])
        parts.extend(vf.get("aliases", []) or [])
        ax = aliases.get(vf["vf_id"])
        if ax:
            parts.extend(ax.get("llm_aliases", []) or [])
            parts.extend(ax.get("question_words", []) or [])
    return " ".join(p for p in parts if p)


def build_vt_vf_alias_set(vfs_by_vt: dict[str, list], aliases: dict[str, dict]) -> dict[str, set[str]]:
    """vt_id → 全部虚拟字段的 alias 小写集合，用于 virtual_field_hit 判定。"""
    by_vt: dict[str, set[str]] = {}
    for vt_id, vfs in vfs_by_vt.items():
        s: set[str] = set()
        for vf in vfs:
            if vf.get("field_cn_name"):
                s.add(vf["field_cn_name"])
            for a in vf.get("aliases", []) or []:
                if a:
                    s.add(a.lower())
            ax = aliases.get(vf["vf_id"])
            if ax:
                for a in ax.get("llm_aliases", []) or []:
                    if a:
                        s.add(a.lower())
        by_vt[vt_id] = s
    return by_vt


# ============ query 关键字段抽取 ============

# 从 query 中抽取关键业务词汇（用于 virtual_field_hit_rate）
QUERY_KEY_PATTERNS = [
    "身份证", "身份证号", "姓名", "手机号", "电话", "车牌", "号牌",
    "出生日期", "出生", "年龄", "性别", "民族", "国籍", "护照",
    "地址", "住址", "户籍", "地点", "位置", "口岸", "行政区划",
    "时间", "日期", "事件", "出入境", "加油", "住宿", "通联",
    "案件", "案号", "警情", "监区", "服刑", "刑满",
    "设备", "基站", "MAC", "IMEI", "WiFi",
    "关系", "婚姻", "亲属", "同行", "共现",
    "职业", "文化程度", "婚姻状况", "户口",
    "交通方式", "签证", "签发", "口岸",
]


def extract_key_fields_from_query(query: str) -> list[str]:
    hits: list[str] = []
    q = query or ""
    for kw in QUERY_KEY_PATTERNS:
        if kw in q:
            hits.append(kw)
    # 去重保序
    seen: set[str] = set()
    out: list[str] = []
    for h in hits:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


# ============ 主流程 ============


def compute_similarity(
    queries: list[str], vt_texts: list[str], channel: str
) -> np.ndarray:
    if channel == "tfidf":
        corpus = queries + vt_texts
        corpus = [c if c else "<empty>" for c in corpus]
        vec = TfidfVectorizer(analyzer="char", ngram_range=(2, 4), min_df=1)
        mat = vec.fit_transform(corpus)
        q_m = mat[: len(queries)]
        v_m = mat[len(queries) :]
        return cosine_similarity(q_m, v_m)
    else:
        q_emb = np.array(embed_client(queries, use_cache=True))
        v_emb = np.array(embed_client(vt_texts, use_cache=True))

        qn = np.linalg.norm(q_emb, axis=1, keepdims=True)
        vn = np.linalg.norm(v_emb, axis=1, keepdims=True)
        q_emb = q_emb / np.where(qn == 0, 1, qn)
        v_emb = v_emb / np.where(vn == 0, 1, vn)
        return q_emb @ v_emb.T


def _compute_fusion_rrf(
    queries: list[str],
    vt_texts: list[str],
    vt_ids: list[str],
    bench: list[dict],
    scaffold: list[dict],
    vfs_by_vt: dict[str, list],
    aliases: dict[str, dict],
) -> np.ndarray:
    """三路 RRF：embedding（VT 级）+ intent（v2 embedding 版）+ slot_top3。

    替代原来的 embedding+intent 两路 fusion。
    也作为 rerank 通道的候选池来源（替代单一 embedding top-10）。
    """
    sim_emb = compute_similarity(queries, vt_texts, "embedding")
    sim_intent = compute_intent_similarity(bench, scaffold, vfs_by_vt, aliases)
    sim_slot = compute_slot_max_similarity(queries, vt_ids, top_k_slots=3)
    K_RRF = 60.0
    n_q, n_vt = sim_emb.shape
    rrf = np.zeros((n_q, n_vt), dtype=np.float32)
    for i in range(n_q):
        r_emb = (-sim_emb[i]).argsort().argsort() + 1
        r_int = (-sim_intent[i]).argsort().argsort() + 1
        r_slot = (-sim_slot[i]).argsort().argsort() + 1
        rrf[i] = 1.0 / (K_RRF + r_emb) + 1.0 / (K_RRF + r_int) + 1.0 / (K_RRF + r_slot)
    return rrf


def compute_multi_topic_similarity(
    bench: list[dict],
    scaffold: list[dict],
    vfs_by_vt: dict[str, list],
    aliases: dict[str, dict],
) -> np.ndarray:
    """多主题拆分召回：每条 query 按 intent_topics 拆成 N 个子召回，用 RRF 合并。

    针对 benchmark 里 62% 的多主题 query（expected_tables ≥ 2，跨业务域）：
    单向量 embedding 会让 query 的整体语义偏向最强的一个主题，导致 top-5 被该主题占满、
    漏掉其他主题的 VT。这里把 query 在 intent 层面拆开，每个 topic 独立 rank VT，再 RRF 合并，
    让每个主题都有"代表 VT"进最终候选池。

    用现有 query_intents.json 的 intent_topics（不需要新 LLM 调用）。
    """
    if not QUERY_INTENTS_JSON.exists():
        raise RuntimeError("query_intents.json 不存在")
    intents_data = json.loads(QUERY_INTENTS_JSON.read_text(encoding="utf-8"))
    by_query = {i["query_text"]: i["intent"] for i in intents_data["query_intents"]}

    vt_texts = [build_vt_recall_text(vt, vfs_by_vt, aliases) for vt in scaffold]
    v_emb = np.array(embed_client(vt_texts, use_cache=True), dtype=np.float32)
    vn = np.linalg.norm(v_emb, axis=1, keepdims=True)
    v_emb = v_emb / np.where(vn == 0, 1, vn)

    # 收集每条 query 的 topics 列表；空的降级为 query 全文
    query_topics: list[list[str]] = []
    topic_set: set[str] = set()
    for b in bench:
        intent = by_query.get(b["query_text"], {})
        topics = [str(t).strip() for t in (intent.get("intent_topics") or []) if t and str(t).strip()]
        if not topics:
            topics = [b["query_text"]]
        query_topics.append(topics)
        topic_set.update(topics)

    topic_list = sorted(topic_set)
    t_emb = np.array(embed_client(topic_list, use_cache=True), dtype=np.float32)
    tn = np.linalg.norm(t_emb, axis=1, keepdims=True)
    t_emb = t_emb / np.where(tn == 0, 1, tn)
    topic_idx = {t: i for i, t in enumerate(topic_list)}

    K_RRF = 60.0
    n_q = len(bench)
    n_vt = len(scaffold)
    sim = np.zeros((n_q, n_vt), dtype=np.float32)

    for qi, topics in enumerate(query_topics):
        for t in topics:
            ti = topic_idx.get(t)
            if ti is None:
                continue
            scores = t_emb[ti] @ v_emb.T  # (n_vt,)
            ranks = (-scores).argsort().argsort() + 1  # 1..n_vt
            sim[qi] += 1.0 / (K_RRF + ranks)

    return sim


def compute_multi_field_similarity(
    bench: list[dict],
    vt_ids: list[str],
    hit_threshold: float = 0.5,
    idf_min: float = 0.69,  # log(2) ≈ 0.69: 命中 > 一半 VT 的字段丢弃
    per_field_top_k: int = 2,  # 每个 field 只把 top-K VT 计入 RRF；扫参最优 2
) -> np.ndarray:
    """多字段拆分召回 v2（IDF + top-K 截断）：每个 required_field 贡献自己的 top-K VT 进候选池。

    三道闸门：
    1. slot_max < hit_threshold 的 VT 不算该 field 的"命中"（只用这个算 IDF）
    2. IDF < idf_min（命中超一半 VT 的通用字段）整个 field 丢弃
    3. 每个 field 只把自己的 top-K VT 计入 RRF，其余 VT rank=∞ 贡献为 0
       → 等价于"每个 field 推荐 top-K VT，并集后再按 IDF 加权 RRF"

    融合用途：作为 fusion_mf 第四路，权重建议 0.5（三路主信号 1.0 + 本路 0.5）。
    扫参证据：全量 top10 recall +1.7pp、n>=3 子集 top10 recall +3.4pp、top10 topic_hit +3.7pp；
    代价：全量 top5 topic_hit -3.7pp（对 top-10 候选池场景净正向）。
    """
    if not QUERY_INTENTS_JSON.exists():
        raise RuntimeError("query_intents.json 不存在")
    intents_data = json.loads(QUERY_INTENTS_JSON.read_text(encoding="utf-8"))
    by_query = {i["query_text"]: i["intent"] for i in intents_data["query_intents"]}

    slot_parq = REPO_ROOT / "output" / "slot_embeddings.parquet"
    if not slot_parq.exists():
        raise RuntimeError(
            f"{slot_parq} 不存在，请先跑: python3 scripts/build_slot_embeddings.py"
        )
    import pandas as _pd
    slot_df = _pd.read_parquet(slot_parq)
    slot_df = slot_df[slot_df["vt_id"].isin(set(vt_ids))].reset_index(drop=True)
    if slot_df.empty:
        raise RuntimeError("slot_embeddings 和 scaffold 的 vt_id 无交集")
    slot_emb = np.stack([np.array(v, dtype=np.float32) for v in slot_df["embedding"].tolist()])
    slot_vt_col = slot_df["vt_id"].tolist()
    vt_to_slot_cols: dict[str, list[int]] = {}
    for i, vid in enumerate(slot_vt_col):
        vt_to_slot_cols.setdefault(vid, []).append(i)

    query_fields: list[list[str]] = []
    all_fields_set: set[str] = set()
    for b in bench:
        intent = by_query.get(b["query_text"], {})
        fields = [str(f).strip() for f in (intent.get("required_fields") or []) if f and str(f).strip()]
        if not fields:
            fields = [b["query_text"][:30]]
        query_fields.append(fields)
        all_fields_set.update(fields)

    all_fields = sorted(all_fields_set)
    f_emb = np.array(embed_client(all_fields, use_cache=True), dtype=np.float32)
    fn = np.linalg.norm(f_emb, axis=1, keepdims=True)
    f_emb = f_emb / np.where(fn == 0, 1, fn)
    field_to_idx = {f: i for i, f in enumerate(all_fields)}

    n_q = len(bench)
    n_vt = len(vt_ids)
    K_RRF = 60.0
    sim = np.zeros((n_q, n_vt), dtype=np.float32)

    # 预计算：每个 field 的 per-VT max score + IDF
    field_vt_scores: dict[int, np.ndarray] = {}
    field_idf: dict[int, float] = {}
    for fi in range(len(all_fields)):
        sim_all = slot_emb @ f_emb[fi]
        vt_scores = np.zeros(n_vt, dtype=np.float32)
        for vj, vid in enumerate(vt_ids):
            cols = vt_to_slot_cols.get(vid)
            if cols:
                vt_scores[vj] = sim_all[cols].max()
        n_hit = int((vt_scores >= hit_threshold).sum())
        field_vt_scores[fi] = vt_scores
        if n_hit == 0:
            field_idf[fi] = 0.0  # 跳过：没任何 VT 足够相关
        else:
            field_idf[fi] = float(np.log((n_vt + 1) / (n_hit + 1)))

    for qi, fields in enumerate(query_fields):
        for f in fields:
            fi = field_to_idx.get(f)
            if fi is None:
                continue
            idf = field_idf[fi]
            if idf < idf_min:
                continue
            vt_scores = field_vt_scores[fi]
            # 只让 top-K VT 参与 RRF（并集 top-K 语义）
            top_idx = np.argpartition(-vt_scores, min(per_field_top_k, n_vt - 1))[:per_field_top_k]
            top_sorted = top_idx[np.argsort(-vt_scores[top_idx])]
            for rank, vj in enumerate(top_sorted, start=1):
                sim[qi, vj] += idf * (1.0 / (K_RRF + rank))

    return sim


def compute_slot_max_similarity(
    queries: list[str],
    vt_ids: list[str],
    top_k_slots: int = 3,
) -> np.ndarray:
    """Slot 级召回：query × 每个 slot 的 embedding 余弦相似度，按 VT 聚合（top-k slot 平均）。

    为什么：VT 级整段 embedding 会被长文本稀释（尤其虚拟字段多时）。
    切换到 slot 粒度后，query "身份证号" 只需要和 certificate_no 这一个 slot 对齐，
    不会被 VT 里其他 20 个无关 slot 拉低分数。

    前提：已跑 scripts/build_slot_embeddings.py 产出 output/slot_embeddings.parquet
    """
    import pandas as _pd
    slot_parq = REPO_ROOT / "output" / "slot_embeddings.parquet"
    if not slot_parq.exists():
        raise RuntimeError(
            f"{slot_parq} 不存在，请先跑: python3 scripts/build_slot_embeddings.py"
        )
    df = _pd.read_parquet(slot_parq)
    # 只保留本次 scaffold 里的 VT（避免 slot_definitions 里有 scaffold 已删的 VT）
    df = df[df["vt_id"].isin(set(vt_ids))].reset_index(drop=True)
    if df.empty:
        raise RuntimeError("slot_embeddings 和 scaffold 的 vt_id 无交集")

    # slot embedding 已在 build 时 L2 归一化，直接 stack 即可
    slot_emb = np.stack([np.array(v, dtype=np.float32) for v in df["embedding"].tolist()])
    slot_vt = df["vt_id"].tolist()

    # query embedding + 归一化
    q_emb = np.array(embed_client(queries, use_cache=True), dtype=np.float32)
    qn = np.linalg.norm(q_emb, axis=1, keepdims=True)
    q_emb = q_emb / np.where(qn == 0, 1, qn)

    # 余弦相似矩阵: n_q × n_slot
    sim_slot = q_emb @ slot_emb.T

    # 按 vt_id 聚合：每张 VT 取该 VT 下所有 slot 相似度的 top-k 平均
    vt_id_to_cols: dict[str, list[int]] = {}
    for col_idx, vid in enumerate(slot_vt):
        vt_id_to_cols.setdefault(vid, []).append(col_idx)

    n_q = len(queries)
    n_vt = len(vt_ids)
    sim_vt = np.zeros((n_q, n_vt), dtype=np.float32)
    for j, vid in enumerate(vt_ids):
        cols = vt_id_to_cols.get(vid)
        if not cols:
            continue  # 这张 VT 没 slot（理论不会发生，因为 scaffold 里有但 slot_def 里可能空）
        slots_for_vt = sim_slot[:, cols]  # n_q × n_slot_of_vt
        k = min(top_k_slots, slots_for_vt.shape[1])
        # 对每行取 top-k 平均
        # argpartition 取无序 top-k 然后 mean，比 sort 快
        if k == slots_for_vt.shape[1]:
            sim_vt[:, j] = slots_for_vt.mean(axis=1)
        else:
            # 取 top-k 最大值的均值
            top_k = np.partition(slots_for_vt, -k, axis=1)[:, -k:]
            sim_vt[:, j] = top_k.mean(axis=1)
    return sim_vt


def compute_rerank_similarity(
    bench: list[dict],
    scaffold: list[dict],
    vfs_by_vt: dict[str, list],
    aliases: dict[str, dict],
    base_sim: np.ndarray,
    top_n_for_rerank: int = 10,
    concurrency: int = 8,
) -> np.ndarray:
    """LLM rerank：先用 embedding 拿 top N，再让 LLM 从这 N 个中重排。"""
    from src.llm_client import chat as _chat
    import concurrent.futures as _cf
    import threading as _th

    n_q, n_vt = base_sim.shape

    # 构造每个 VT 的简要描述（给 LLM 用）
    vt_briefs: list[str] = []
    for vt in scaffold:
        vfs = vfs_by_vt.get(vt["vt_id"], [])
        essential_fields = [v["field_cn_name"] for v in vfs if v.get("importance_tier") == "essential"][:10]
        vt_briefs.append(
            f"{vt['topic']} ({'/'.join(vt['l2_path'])}; {vt.get('table_type', '')}) "
            f"主要字段: {', '.join(essential_fields) if essential_fields else '(无)'}"
        )

    sys_prompt = """你是 text2sql 表召回助手。
用户给你一个自然语言查询，和一组候选虚拟表。每个虚拟表是一个业务主题，服务一组物理源表。
请从候选表中选出**最能支持这个查询**的 5 个虚拟表，按相关性降序。

判断标准：
1. 查询涉及的主题 vs 虚拟表主题是否匹配
2. 查询需要的字段（如身份证、时间、地点）是否在虚拟表典型字段里
3. 虚拟表的粒度是否匹配查询意图

严格输出 JSON: {"ranked": [候选编号, 候选编号, ...]}（5 个编号，数字）"""

    def rerank_one(qi: int, query: str, top_n_idx: list[int]) -> tuple[int, list[int]]:
        candidates = [
            f"[{k+1}] {vt_briefs[j]}" for k, j in enumerate(top_n_idx)
        ]
        user = f"""查询：{query}

候选虚拟表（{top_n_for_rerank} 个）：
{chr(10).join(candidates)}

输出严格 JSON: {{"ranked": [候选编号列表, 5 个]}}"""
        try:
            raw = _chat(
                messages=[{"role": "system", "content": sys_prompt},
                          {"role": "user", "content": user}],
                temperature=0.0, json_mode=True, use_cache=True,
            )
            result = json.loads(raw)
            ranked = result.get("ranked") or []
            # 把候选编号（1-based）翻译回 scaffold 里的 VT index
            scaffold_ranks: list[int] = []
            seen_scaffold: set[int] = set()
            for r in ranked:
                try:
                    cand_idx = int(r) - 1
                except Exception:
                    continue
                if 0 <= cand_idx < len(top_n_idx):
                    sc_idx = top_n_idx[cand_idx]
                    if sc_idx not in seen_scaffold:
                        scaffold_ranks.append(sc_idx)
                        seen_scaffold.add(sc_idx)
            # 补齐：对剩下未选的 top_n_idx 按原顺序追加
            for j in top_n_idx:
                if j not in seen_scaffold:
                    scaffold_ranks.append(j)
            return qi, scaffold_ranks
        except Exception:
            return qi, list(top_n_idx)

    # 并发调
    rerank_result: dict[int, list[int]] = {}
    queries = [b["query_text"] for b in bench]
    with _cf.ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = []
        for i in range(n_q):
            top_n_idx = (-base_sim[i]).argsort()[:top_n_for_rerank].tolist()
            futures.append(exe.submit(rerank_one, i, queries[i], top_n_idx))
        for f in _cf.as_completed(futures):
            qi, order = f.result()
            rerank_result[qi] = order

    # 把 LLM 返回的顺序反向构造成分数矩阵：越靠前分数越高
    new_sim = base_sim.copy()
    for qi, order in rerank_result.items():
        # 给 LLM 选中的 top_n 重新分配分数，保留 LLM 的相对排序
        for rank_pos, sc_idx in enumerate(order):
            new_sim[qi, sc_idx] = 2.0 - rank_pos * 0.01  # rank_pos=0 得 2.0，依次递减
    return new_sim


def compute_intent_similarity(
    bench: list[dict],
    scaffold: list[dict],
    vfs_by_vt: dict[str, list],
    aliases: dict[str, dict],
) -> np.ndarray:
    """I-08b: 基于 LLM 结构化意图的匹配（v2 升级版）。

    score(query, vt) = 0.5 * topic_sim + 0.5 * field_score
      - topic_sim: intent_topics ↔ VT 扩展主题文本（含 recall_summary + typical_questions + topic_aliases）的 embedding 余弦
      - field_score: required_fields 的 embedding × 每张 VT 下所有 slot embedding 取 max 后再对 fields 取均值
        （替代旧版的 substring 字面命中；依赖 output/slot_embeddings.parquet，缺则降级到字面匹配）
    """
    if not QUERY_INTENTS_JSON.exists():
        raise RuntimeError("query_intents.json 不存在，请先跑 query_intent_extractor")

    intents_data = json.loads(QUERY_INTENTS_JSON.read_text(encoding="utf-8"))
    intents_list = intents_data["query_intents"]
    by_query = {i["query_text"]: i["intent"] for i in intents_list}

    # VT 主题文本：加入今天 enrich 的 recall_summary / typical_questions / topic_aliases
    vt_topic_texts: list[str] = []
    for vt in scaffold:
        parts = [vt.get("topic", ""), vt.get("topic", "")]  # topic 重复一次加权
        parts.extend(vt.get("l2_path", []) or [])
        parts.extend(vt.get("recall_hints", []) or [])
        if vt.get("recall_summary"):
            parts.append(str(vt["recall_summary"]))
        for q in vt.get("typical_questions", []) or []:
            if q:
                parts.append(str(q))
        for a in vt.get("topic_aliases", []) or []:
            if a:
                parts.append(str(a))
        vt_topic_texts.append(" ".join(p for p in parts if p))

    # query 侧文本
    query_topic_texts: list[str] = []
    query_field_lists: list[list[str]] = []
    for b in bench:
        intent = by_query.get(b["query_text"], {})
        topics = intent.get("intent_topics") or []
        fields = intent.get("required_fields") or []
        query_topic_texts.append(" ".join(topics) if topics else b["query_text"][:20])
        query_field_lists.append([str(f).strip() for f in fields if f and str(f).strip()])

    # topic_sim
    q_emb = np.array(embed_client(query_topic_texts, use_cache=True))
    v_emb = np.array(embed_client(vt_topic_texts, use_cache=True))
    qn = np.linalg.norm(q_emb, axis=1, keepdims=True)
    vn = np.linalg.norm(v_emb, axis=1, keepdims=True)
    q_emb = q_emb / np.where(qn == 0, 1, qn)
    v_emb = v_emb / np.where(vn == 0, 1, vn)
    topic_sim = q_emb @ v_emb.T  # [n_query, n_vt]

    n_q, n_vt = topic_sim.shape
    field_score = np.zeros((n_q, n_vt), dtype=np.float32)

    slot_emb_parq = REPO_ROOT / "output" / "slot_embeddings.parquet"
    if slot_emb_parq.exists():
        # === 升级路径：用 required_fields embedding × slot embedding ===
        import pandas as _pd
        slot_df = _pd.read_parquet(slot_emb_parq)
        vt_ids_list = [vt["vt_id"] for vt in scaffold]
        slot_df = slot_df[slot_df["vt_id"].isin(set(vt_ids_list))].reset_index(drop=True)

        slot_emb = np.stack([np.array(v, dtype=np.float32) for v in slot_df["embedding"].tolist()])
        slot_vt_col = slot_df["vt_id"].tolist()
        vt_to_slot_cols: dict[str, list[int]] = {}
        for i, vid in enumerate(slot_vt_col):
            vt_to_slot_cols.setdefault(vid, []).append(i)

        # 全 query 所有 required_field 去重 + batch embed
        all_fields: list[str] = []
        seen: set[str] = set()
        for fs in query_field_lists:
            for f in fs:
                if f not in seen:
                    seen.add(f)
                    all_fields.append(f)
        field_idx: dict[str, int] = {}
        field_emb_arr = np.zeros((0, slot_emb.shape[1]), dtype=np.float32)
        if all_fields:
            emb = np.array(embed_client(all_fields, use_cache=True), dtype=np.float32)
            en = np.linalg.norm(emb, axis=1, keepdims=True)
            emb = emb / np.where(en == 0, 1, en)
            field_emb_arr = emb
            field_idx = {f: i for i, f in enumerate(all_fields)}

        for qi, fields in enumerate(query_field_lists):
            if not fields:
                continue
            per_field_scores = []
            for f in fields:
                fi = field_idx.get(f)
                if fi is None:
                    continue
                field_vec = field_emb_arr[fi]  # (dim,)
                # 和全局 slot_emb 做相似度 一次，再按 VT 分组 max
                sim_all = slot_emb @ field_vec  # (n_slot,)
                vt_max = np.zeros(n_vt, dtype=np.float32)
                for vj, vid in enumerate(vt_ids_list):
                    cols = vt_to_slot_cols.get(vid)
                    if cols:
                        vt_max[vj] = sim_all[cols].max()
                per_field_scores.append(vt_max)
            if per_field_scores:
                # 对 query 的所有 required_fields 取平均 → [n_vt]
                field_score[qi] = np.stack(per_field_scores).mean(axis=0)
    else:
        # === 降级：老字面匹配 ===
        vt_aliases = build_vt_vf_alias_set(vfs_by_vt, aliases)
        vt_alias_lists: list[set[str]] = []
        for vt in scaffold:
            s = vt_aliases.get(vt["vt_id"], set())
            vt_alias_lists.append({a.lower() if isinstance(a, str) else a for a in s if a})
        for qi in range(n_q):
            q_fields = {f.lower() for f in query_field_lists[qi]}
            if not q_fields:
                continue
            for vj in range(n_vt):
                va = vt_alias_lists[vj]
                if not va:
                    continue
                hits = 0
                for qf in q_fields:
                    if qf in va:
                        hits += 1
                        continue
                    if any(qf in a or a in qf for a in va if a and len(a) >= 2):
                        hits += 1
                field_score[qi, vj] = hits / len(q_fields)

    return 0.5 * topic_sim + 0.5 * field_score


def evaluate(channel: str = "embedding", benchmark_source: str = "json", flag_filter: float | None = None) -> dict:
    print(f"加载输入... channel={channel} source={benchmark_source} flag={flag_filter}")
    bench = load_benchmark(source=benchmark_source, flag_filter=flag_filter)
    scaffold = load_scaffold()
    vfs_by_vt = load_virtual_fields()
    aliases = load_aliases()
    print(f"  benchmark query: {len(bench)}")
    print(f"  virtual tables: {len(scaffold)}")

    # VT 召回文本 + metadata
    vt_texts = [build_vt_recall_text(vt, vfs_by_vt, aliases) for vt in scaffold]
    vt_ids = [vt["vt_id"] for vt in scaffold]
    vt_table_sets = [
        {t["en"] for t in (vt.get("candidate_tables") or vt.get("source_tables") or [])}
        for vt in scaffold
    ]
    vt_alias_sets = build_vt_vf_alias_set(vfs_by_vt, aliases)

    # query 文本
    queries = [r["query_text"] for r in bench]

    # 相似度
    print(f"计算 {channel} 相似度...")
    t0 = time.time()
    if channel == "intent":
        sim = compute_intent_similarity(bench, scaffold, vfs_by_vt, aliases)
    elif channel == "slot_max":
        # top_k=1 即纯 max（VT 分 = 该 VT 下最相关 slot 的相似度）
        sim = compute_slot_max_similarity(queries, vt_ids, top_k_slots=1)
    elif channel == "slot_top3":
        sim = compute_slot_max_similarity(queries, vt_ids, top_k_slots=3)
    elif channel == "multi_topic":
        sim = compute_multi_topic_similarity(bench, scaffold, vfs_by_vt, aliases)
    elif channel == "multi_field":
        sim = compute_multi_field_similarity(bench, vt_ids)
    elif channel == "fusion_mf":
        # 四路 RRF：embedding + intent + slot_top3 + multi_field；multi_field 权重 0.5
        # 目标场景：top-10 候选池（喂给下游 SQL 生成），n>=3 多表 query top10 recall +3.4pp
        sim_emb = compute_similarity(queries, vt_texts, "embedding")
        sim_intent = compute_intent_similarity(bench, scaffold, vfs_by_vt, aliases)
        sim_slot = compute_slot_max_similarity(queries, vt_ids, top_k_slots=3)
        sim_mf = compute_multi_field_similarity(bench, vt_ids)
        K_RRF = 60.0
        W_MF = 0.5
        n_q, n_vt = sim_emb.shape
        rrf = np.zeros((n_q, n_vt))
        for i in range(n_q):
            r_emb = (-sim_emb[i]).argsort().argsort() + 1
            r_int = (-sim_intent[i]).argsort().argsort() + 1
            r_slot = (-sim_slot[i]).argsort().argsort() + 1
            r_mf = (-sim_mf[i]).argsort().argsort() + 1
            rrf[i] = (
                1.0 / (K_RRF + r_emb) +
                1.0 / (K_RRF + r_int) +
                1.0 / (K_RRF + r_slot) +
                W_MF * (1.0 / (K_RRF + r_mf))
            )
        sim = rrf
    elif channel == "fusion4":
        # 四路 RRF 融合：embedding（整段）+ intent（v2）+ slot_top3 + multi_topic
        sim_emb = compute_similarity(queries, vt_texts, "embedding")
        sim_intent = compute_intent_similarity(bench, scaffold, vfs_by_vt, aliases)
        sim_slot = compute_slot_max_similarity(queries, vt_ids, top_k_slots=3)
        sim_mt = compute_multi_topic_similarity(bench, scaffold, vfs_by_vt, aliases)
        K_RRF = 60.0
        n_q, n_vt = sim_emb.shape
        rrf = np.zeros((n_q, n_vt))
        for i in range(n_q):
            r_emb = (-sim_emb[i]).argsort().argsort() + 1
            r_int = (-sim_intent[i]).argsort().argsort() + 1
            r_slot = (-sim_slot[i]).argsort().argsort() + 1
            r_mt = (-sim_mt[i]).argsort().argsort() + 1
            rrf[i] = (
                1.0 / (K_RRF + r_emb) +
                1.0 / (K_RRF + r_int) +
                1.0 / (K_RRF + r_slot) +
                1.0 / (K_RRF + r_mt)
            )
        sim = rrf
    elif channel == "rerank":
        # 二阶段：fusion（emb + intent + slot_top3）top10 → LLM rerank
        # 老实现用 embedding top10，单主题强但多主题 query 容易漏候选；
        # 换 fusion 作为候选池后，LLM 能同时看到多主题代表 VT，精排更易兼顾 recall
        sim_fusion_base = _compute_fusion_rrf(
            queries, vt_texts, vt_ids, bench, scaffold, vfs_by_vt, aliases
        )
        print(f"  fusion 候选池构建完毕，开始 LLM rerank ({len(queries)} 次调用)...")
        sim = compute_rerank_similarity(bench, scaffold, vfs_by_vt, aliases, sim_fusion_base)
    elif channel == "rerank_emb":
        # 老版 rerank：用 embedding 做候选池，保留对比
        sim_emb = compute_similarity(queries, vt_texts, "embedding")
        print(f"  embedding 初始检索完毕，开始 LLM rerank ({len(queries)} 次调用)...")
        sim = compute_rerank_similarity(bench, scaffold, vfs_by_vt, aliases, sim_emb)
    elif channel == "fusion":
        # 默认 fusion = 三路 RRF（embedding + intent + slot_top3）
        # 替代原来的 embedding+intent 两路，收益 +9.7pp k=5 topic_hit（2026-04-23 实验）
        sim = _compute_fusion_rrf(
            queries, vt_texts, vt_ids, bench, scaffold, vfs_by_vt, aliases
        )
    elif channel == "fusion_v1":
        # 旧版 fusion：只融 embedding + intent（保留对比用）
        sim_emb = compute_similarity(queries, vt_texts, "embedding")
        sim_intent = compute_intent_similarity(bench, scaffold, vfs_by_vt, aliases)
        K_RRF = 60.0
        n_q, n_vt = sim_emb.shape
        rrf = np.zeros((n_q, n_vt))
        for i in range(n_q):
            rank_emb = (-sim_emb[i]).argsort().argsort() + 1
            rank_int = (-sim_intent[i]).argsort().argsort() + 1
            rrf[i] = 1.0 / (K_RRF + rank_emb) + 1.0 / (K_RRF + rank_int)
        sim = rrf
    elif channel == "fusion_slot":
        # RRF 融合 embedding（VT 级）+ slot_top3（slot 级）
        # 用来验证 slot 级信号作为补充是否能突破 embedding 的天花板
        sim_emb = compute_similarity(queries, vt_texts, "embedding")
        sim_slot = compute_slot_max_similarity(queries, vt_ids, top_k_slots=3)
        K_RRF = 60.0
        n_q, n_vt = sim_emb.shape
        rrf = np.zeros((n_q, n_vt))
        for i in range(n_q):
            rank_emb = (-sim_emb[i]).argsort().argsort() + 1
            rank_slot = (-sim_slot[i]).argsort().argsort() + 1
            rrf[i] = 1.0 / (K_RRF + rank_emb) + 1.0 / (K_RRF + rank_slot)
        sim = rrf
    elif channel == "fusion3":
        # 三路 RRF 融合：embedding + intent + slot_top3
        sim_emb = compute_similarity(queries, vt_texts, "embedding")
        sim_intent = compute_intent_similarity(bench, scaffold, vfs_by_vt, aliases)
        sim_slot = compute_slot_max_similarity(queries, vt_ids, top_k_slots=3)
        K_RRF = 60.0
        n_q, n_vt = sim_emb.shape
        rrf = np.zeros((n_q, n_vt))
        for i in range(n_q):
            rank_emb = (-sim_emb[i]).argsort().argsort() + 1
            rank_int = (-sim_intent[i]).argsort().argsort() + 1
            rank_slot = (-sim_slot[i]).argsort().argsort() + 1
            rrf[i] = 1.0 / (K_RRF + rank_emb) + 1.0 / (K_RRF + rank_int) + 1.0 / (K_RRF + rank_slot)
        sim = rrf
    else:
        sim = compute_similarity(queries, vt_texts, channel)
    print(f"  完成 ({time.time()-t0:.1f}s)")

    # 逐 query 评估
    details: list[dict] = []
    metrics_topk: dict[int, dict[str, list[float]]] = {
        k: {"topic_hit": [], "table_recall": [], "vf_hit": [], "support": []} for k in TOPK_LIST
    }

    for i, q_record in enumerate(bench):
        query = q_record["query_text"]
        exp_tables = set(q_record.get("expected_tables", []))
        key_fields = extract_key_fields_from_query(query)

        topk_idx = np.argsort(-sim[i])  # 降序

        row: dict[str, Any] = {
            "query_text": query,
            "expected_tables": ",".join(sorted(exp_tables)),
            "expected_table_count": len(exp_tables),
            "key_fields_in_query": ",".join(key_fields),
            "channel": channel,
        }

        for k in TOPK_LIST:
            top_vts = [vt_ids[j] for j in topk_idx[:k]]
            top_vt_tables: set[str] = set()
            top_vt_aliases: set[str] = set()
            for j in topk_idx[:k]:
                top_vt_tables |= vt_table_sets[j]
                top_vt_aliases |= vt_alias_sets.get(vt_ids[j], set())

            # topic_hit: 预期表有一个 ∈ topK VT 的源表集合
            topic_hit = bool(exp_tables & top_vt_tables) if exp_tables else False

            # table_recall@k
            if exp_tables:
                recall = len(exp_tables & top_vt_tables) / len(exp_tables)
            else:
                recall = 0.0

            # virtual_field_hit
            if key_fields:
                alias_lower = {a.lower() for a in top_vt_aliases}
                hits = sum(1 for kf in key_fields if kf in top_vt_aliases or kf.lower() in alias_lower)
                vf_hit = hits / len(key_fields)
            else:
                vf_hit = 0.0  # 查询里没抽出关键字段，记为 0，避免扭曲平均

            # support: 表召回 ≥ 0.5 AND 字段命中 ≥ 0.5（若没 key_fields 则只看表）
            if key_fields:
                support = (recall >= 0.5) and (vf_hit >= 0.5)
            else:
                support = recall >= 0.5

            metrics_topk[k]["topic_hit"].append(1.0 if topic_hit else 0.0)
            metrics_topk[k]["table_recall"].append(recall)
            metrics_topk[k]["vf_hit"].append(vf_hit)
            metrics_topk[k]["support"].append(1.0 if support else 0.0)

            row[f"topK_{k}_vts"] = ",".join(top_vts)
            row[f"topK_{k}_topic_hit"] = topic_hit
            row[f"topK_{k}_table_recall"] = round(recall, 4)
            row[f"topK_{k}_vf_hit"] = round(vf_hit, 4)
            row[f"topK_{k}_support"] = support

        details.append(row)

    # 汇总
    summary: dict[str, Any] = {"channel": channel, "benchmark_count": len(bench), "by_topk": {}}
    for k in TOPK_LIST:
        m = metrics_topk[k]
        summary["by_topk"][f"k={k}"] = {
            "topic_hit_rate": round(float(np.mean(m["topic_hit"])) if m["topic_hit"] else 0, 4),
            "table_recall_rate": round(float(np.mean(m["table_recall"])) if m["table_recall"] else 0, 4),
            "virtual_field_hit_rate": round(float(np.mean(m["vf_hit"])) if m["vf_hit"] else 0, 4),
            "query_support_rate": round(float(np.mean(m["support"])) if m["support"] else 0, 4),
        }

    return {"summary": summary, "details": details}


def save_result(result: dict, channel: str) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    # 读现有 evaluation.json（支持多 channel 合并）
    all_results: dict = {}
    if OUT_JSON.exists():
        try:
            all_results = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        except Exception:
            all_results = {}
    if not isinstance(all_results, dict) or "by_channel" not in all_results:
        all_results = {"by_channel": {}}
    all_results["by_channel"][channel] = result["summary"]
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    # details parquet（每 channel 一张，append 模式）
    df = pd.DataFrame(result["details"])
    if OUT_DETAILS.exists():
        existing = pd.read_parquet(OUT_DETAILS)
        existing = existing[existing["channel"] != channel]
        df = pd.concat([existing, df], ignore_index=True)
    df.to_parquet(OUT_DETAILS, index=False)

    write_diagnostic(all_results)


def write_diagnostic(all_results: dict) -> None:
    lines = ["# I-08 评估诊断", ""]
    for channel, summary in all_results.get("by_channel", {}).items():
        lines.append(f"## {channel} 通路")
        lines.append("")
        lines.append(f"- benchmark 总数: {summary['benchmark_count']}")
        lines.append("")
        lines.append("| topK | topic_hit | table_recall | vf_hit | query_support |")
        lines.append("| --- | --- | --- | --- | --- |")
        for k_key, m in summary["by_topk"].items():
            lines.append(
                f"| {k_key} | {m['topic_hit_rate']*100:.1f}% | {m['table_recall_rate']*100:.1f}% |"
                f" {m['virtual_field_hit_rate']*100:.1f}% | {m['query_support_rate']*100:.1f}% |"
            )
        lines.append("")
    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


def main(channel: str = "embedding", benchmark_source: str = "json", flag_filter: float | None = None) -> None:
    t0 = time.time()
    result = evaluate(channel=channel, benchmark_source=benchmark_source, flag_filter=flag_filter)
    channel_tag = channel
    if benchmark_source != "json" or flag_filter is not None:
        channel_tag = f"{channel}__{benchmark_source}"
        if flag_filter is not None:
            channel_tag += f"_flag{int(flag_filter)}"
    save_result(result, channel_tag)
    total = time.time() - t0
    print(f"\n=== I-08 完成 ({channel_tag}) ===")
    for k_key, m in result["summary"]["by_topk"].items():
        print(f"  {k_key}: topic_hit={m['topic_hit_rate']*100:.1f}% / "
              f"table_recall={m['table_recall_rate']*100:.1f}% / "
              f"vf_hit={m['virtual_field_hit_rate']*100:.1f}% / "
              f"support={m['query_support_rate']*100:.1f}%")
    print(f"  总耗时: {total:.1f}s")
    print(f"  evaluation.json: {OUT_JSON}")
    print(f"  details.parquet: {OUT_DETAILS}")


if __name__ == "__main__":
    main()
