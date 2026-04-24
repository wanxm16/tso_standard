"""Review UI 后端。

提供 VT 列表 / 单 VT 详情 / base_slots / 保存 / diff / review_log 接口。
单人本地使用，无鉴权。

启动：
    uvicorn backend.app:app --reload --port 8001
"""
from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from src.naming_lint import (
    SLOT_NAMING_GUARDRAILS,
    collect_slot_name_issues,
    format_naming_retry_feedback,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SCAFFOLD_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"
SLOT_YAML = REPO_ROOT / "output" / "slot_definitions.yaml"
SLOT_BAK = REPO_ROOT / "output" / "slot_definitions.yaml.bak"
REVIEW_LOG = REPO_ROOT / "output" / "review_log.jsonl"
BASE_SLOTS_YAML = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"
DDL_CSV = REPO_ROOT / "data" / "phrase_2" / "二期_DDL_all_with_sample.csv"

# I-04 产物
NORM_PARQUET = REPO_ROOT / "output" / "field_normalization.parquet"
NORM_REVIEWED_PARQUET = REPO_ROOT / "output" / "field_normalization_reviewed.parquet"
FEATURES_PARQUET = REPO_ROOT / "output" / "field_features.parquet"
FEEDBACK_LOG = REPO_ROOT / "data" / "feedback" / "review_log.jsonl"

# I-05b 产物
SLOT_PROPOSALS_YAML = REPO_ROOT / "output" / "slot_proposals.yaml"
SLOT_PROPOSALS_LOG = REPO_ROOT / "data" / "feedback" / "slot_proposals_log.jsonl"
DOMAIN_SLOTS_YAML = REPO_ROOT / "data" / "slot_library" / "domain_slots.yaml"

# 字段黑名单
FIELD_BLACKLIST_YAML = REPO_ROOT / "data" / "slot_library" / "field_blacklist.yaml"
FIELD_BLACKLIST_LOG = REPO_ROOT / "data" / "feedback" / "field_blacklist_log.jsonl"

# 槽位库编辑审计
SLOT_LIBRARY_EDIT_LOG = REPO_ROOT / "data" / "feedback" / "slot_library_edit_log.jsonl"

# I-13 VT 合并
SCAFFOLD_YAML = REPO_ROOT / "output" / "virtual_tables_scaffold_final.yaml"
SCAFFOLD_YAML_BACKUP = REPO_ROOT / "output" / "virtual_tables_scaffold_final_before_merge.yaml"
SCAFFOLD_JSON_FINAL = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"
VT_MERGE_CANDIDATES_YAML = REPO_ROOT / "output" / "vt_merge_candidates.yaml"
VT_MERGE_LOG = REPO_ROOT / "data" / "feedback" / "vt_merge_log.jsonl"

# I-14 scaffold 全编辑
SCAFFOLD_YAML_EDIT_BACKUP = REPO_ROOT / "output" / "virtual_tables_scaffold_final_before_edit.yaml"
SCAFFOLD_EDIT_LOG = REPO_ROOT / "data" / "feedback" / "scaffold_edit_log.jsonl"

# I-15 分类树
CATEGORY_TREE_JSON = REPO_ROOT / "data" / "phrase_2" / "二期表分类树.json"
CATEGORY_TREE_BACKUP = REPO_ROOT / "data" / "phrase_2" / "二期表分类树_before_edit.json"
CATEGORIES_EDIT_LOG = REPO_ROOT / "data" / "feedback" / "categories_edit_log.jsonl"


app = FastAPI(title="TSO Standard · Slot Review UI")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========== 数据模型 ==========
# 注：为简单起见，槽位本身用 list[dict] 接收，不做强 schema。
# 校验逻辑在 update_virtual_table_slots 里手写（比 pydantic alias "from" 关键字更省事）。


class SlotsUpdate(BaseModel):
    slots: list[dict[str, Any]]
    summary: str | None = None
    # 槽位重新生成后保存时附带的"种子归一映射"：
    # [{table_en, field_name, slot_name}, ...]
    # 会写入 field_normalization_reviewed.parquet 作为下游重算种子
    seed_mappings: list[dict[str, Any]] | None = None


# ========== 数据加载 ==========


_cache: dict[str, Any] = {}


def load_slot_data() -> dict:
    """每次读最新文件，避免改文件后 UI 不刷新。"""
    if not SLOT_YAML.exists():
        raise HTTPException(500, f"slot_definitions.yaml 不存在：{SLOT_YAML}")
    with SLOT_YAML.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


# 由人工归一审核 mark_new_slot 触发的 slot 来源标记，
# 用于 generate_slot_definitions.py 重跑时识别并保留
# 所有以 "manual_" 前缀开头的 source 都会被 generate_slot_definitions 保留
MANUAL_SLOT_SOURCE = "manual_normalization_review"  # 字段归一审核里 mark_new_slot
VT_EDIT_SLOT_SOURCE = "manual_vt_edit"  # VT 详情页槽位清单手工保存


def _persist_manual_slot_to_vt(
    vt_id: str,
    new_slot_name: str,
    new_slot_cn_name: str,
    source_field_name: str,
    source_field_comment: str,
    source_data_type: str,
    reviewer_note: str,
) -> tuple[bool, str]:
    """把用户在归一审核 mark_new_slot 提议的新槽位 append 到 slot_definitions.yaml 的对应 VT。

    返回 (added, message)。已存在同名槽位 → 不动，返回 (False, '...')。
    """
    if not SLOT_YAML.exists():
        return False, "slot_definitions.yaml 不存在"
    with SLOT_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    vts = data.get("virtual_tables", []) or []
    target = next((v for v in vts if v.get("vt_id") == vt_id), None)
    if target is None:
        return False, f"vt_id={vt_id} 不在 slot_definitions.yaml 中（可能尚未生成 slots）"
    slots = target.setdefault("slots", []) or []
    # 重名直接跳过（保留原有定义，不覆盖）
    if any(s.get("name") == new_slot_name for s in slots):
        return False, "slot 已存在，跳过"
    aliases = sorted({a for a in [new_slot_cn_name, source_field_comment, source_field_name] if a})
    new_slot = {
        "name": new_slot_name,
        "from": "extended",
        "role": "filter",  # 默认；用户可在槽位清单 tab 里修改
        "cn_name": new_slot_cn_name or source_field_comment or new_slot_name,
        "logical_type": (source_data_type or "string").lower(),
        "aliases": aliases,
        "applicable_table_types": [],
        "llm_reason": reviewer_note or f"由归一审核 mark_new_slot 触发（源字段: {source_field_name}）",
        "source": MANUAL_SLOT_SOURCE,
    }
    slots.append(new_slot)
    target["slots"] = slots
    # 备份后写入
    if SLOT_YAML.exists():
        SLOT_BAK.write_bytes(SLOT_YAML.read_bytes())
    with SLOT_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
    return True, "ok"


def _append_field_to_slot_mapped_fields(
    vt_id: str,
    slot_name: str,
    table_en: str,
    field_name: str,
    field_comment: str,
) -> tuple[bool, str]:
    """把 (table_en, field_name, field_comment) 追加到指定 VT 的指定 slot 的 mapped_fields。

    - 若 slot 不存在：返回 (False, ...)
    - 若 mapped_fields 里已有同 (table_en, field_name) 项：更新 comment（保持最新），返回 (False, 'already exists')
    - 否则追加，返回 (True, 'ok')
    """
    if not SLOT_YAML.exists():
        return False, "slot_definitions.yaml 不存在"
    with SLOT_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    vts = data.get("virtual_tables", []) or []
    target = next((v for v in vts if v.get("vt_id") == vt_id), None)
    if target is None:
        return False, f"vt_id={vt_id} 不在 slot_definitions.yaml 中"
    slots = target.setdefault("slots", []) or []
    slot = next((s for s in slots if s.get("name") == slot_name), None)
    if slot is None:
        return False, f"slot={slot_name} 不在 VT={vt_id} 中"
    mf_list = slot.get("mapped_fields")
    if mf_list is None:
        mf_list = []
    # 去重：按 (table_en, field_name)
    for existing in mf_list:
        if existing.get("table_en") == table_en and existing.get("field_name") == field_name:
            # 已有 → 更新 comment（保持最新）
            if field_comment and not existing.get("field_comment"):
                existing["field_comment"] = field_comment
            slot["mapped_fields"] = mf_list
            if SLOT_YAML.exists():
                SLOT_BAK.write_bytes(SLOT_YAML.read_bytes())
            with SLOT_YAML.open("w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
            return False, "already exists (comment 更新)"
    mf_list.append({
        "table_en": table_en,
        "field_name": field_name,
        "field_comment": field_comment or "",
    })
    slot["mapped_fields"] = mf_list
    slot["source"] = slot.get("source") or "manual_normalization_review"
    if SLOT_YAML.exists():
        SLOT_BAK.write_bytes(SLOT_YAML.read_bytes())
    with SLOT_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
    return True, "ok"


def load_scaffold() -> dict:
    if "scaffold" not in _cache:
        with SCAFFOLD_JSON.open(encoding="utf-8") as f:
            _cache["scaffold"] = json.load(f)
    return _cache["scaffold"]


def load_base_slots() -> dict:
    """每次读最新文件 —— base_slots.yaml 会被 create/delete/apply 动态修改，不能缓存。"""
    if BASE_SLOTS_YAML.exists():
        with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def _legacy_load_base_slots_cached() -> dict:
    if "base_slots" not in _cache:
        with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
            _cache["base_slots"] = yaml.safe_load(f)
    return _cache["base_slots"]


def _cached_read(key: str, path: Path, reader) -> Any:
    """基于文件 mtime 的缓存：pipeline 重跑写文件后自动失效，不用重启 backend。"""
    if not path.exists():
        raise HTTPException(500, f"文件不存在：{path}")
    mtime = path.stat().st_mtime
    mtime_key = f"{key}_mtime"
    if _cache.get(mtime_key) != mtime:
        _cache[key] = reader(path)
        _cache[mtime_key] = mtime
    return _cache[key]


def load_ddl_df() -> pd.DataFrame:
    return _cached_read("ddl_df", DDL_CSV, lambda p: pd.read_csv(p, encoding="utf-8"))


def load_normalization_df() -> pd.DataFrame:
    """加载 I-04 归一结果 + 人工 review 决策（reviewed 覆盖 normalization）。

    merge 用 outer join：当 reviewed 有决策但 field_normalization.parquet 里没有对应
    (table_en, field_name, vt_id) 行时（"孤儿 reviewed"，例如新建 VT 但尚未重跑 pipeline），
    reviewed 决策仍需生效，否则 UI 会退化到 lexical 粗匹配导致显示错误的归属槽位。
    """
    base_df = _cached_read("norm_df", NORM_PARQUET, pd.read_parquet)
    df = base_df.copy()
    if NORM_REVIEWED_PARQUET.exists():
        reviewed = pd.read_parquet(NORM_REVIEWED_PARQUET)
        if not reviewed.empty:
            df = df.merge(
                reviewed[["table_en", "field_name", "vt_id", "decision",
                          "decision_slot", "reviewed_at", "reviewer_note"]],
                on=["table_en", "field_name", "vt_id"], how="outer",
            )
        else:
            df["decision"] = None
            df["decision_slot"] = None
            df["reviewed_at"] = None
            df["reviewer_note"] = None
    else:
        df["decision"] = None
        df["decision_slot"] = None
        df["reviewed_at"] = None
        df["reviewer_note"] = None
    return df


def load_features_df() -> pd.DataFrame:
    return _cached_read("features_df", FEATURES_PARQUET, pd.read_parquet)


def invalidate_norm_cache():
    _cache.pop("norm_df", None)


def append_review_log(payload: dict) -> None:
    FEEDBACK_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FEEDBACK_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"time": datetime.now().isoformat(timespec="seconds"), **payload}, ensure_ascii=False) + "\n")


# ========== API 端点 ==========


@app.get("/api/stats")
def get_stats():
    data = load_slot_data()
    return data.get("stats", {})


@app.get("/api/virtual-tables")
def list_virtual_tables():
    """返回 VT 列表摘要（用于左侧菜单）。

    数据源是 scaffold_final.yaml（权威），slot_count 从 slot_definitions.yaml 附加。
    这样新建但还没跑 LLM 生成槽位的 VT 也会出现在列表里（slot_count=0）。
    """
    scaffold = load_scaffold()
    scaffold_vts = scaffold.get("virtual_tables", []) or []

    # slot_definitions 里的 vt_id → slots 数（可能不存在）
    slot_count_by_vt: dict[str, int] = {}
    try:
        slot_data = load_slot_data()
        for v in slot_data.get("virtual_tables", []) or []:
            slot_count_by_vt[v.get("vt_id")] = len(v.get("slots", []) or [])
    except Exception:
        pass

    result = []
    for vt in scaffold_vts:
        vt_id = vt.get("vt_id")
        slot_count = slot_count_by_vt.get(vt_id, 0)
        result.append({
            "vt_id": vt_id,
            "topic": vt.get("topic", ""),
            "table_type": vt.get("table_type", ""),
            "l2_path": vt.get("l2_path", []) or [],
            "grain_desc": vt.get("grain_desc", ""),
            "source_table_count": int(vt.get("source_table_count", 0)),
            "slot_count": slot_count,
            "slot_status": "has_slots" if slot_count > 0 else "no_slots",  # I-16 Imp4
            "is_pending": vt.get("is_pending", False),
        })
    return result


def build_slot_aliases(slots: list[dict], base_by_name: dict[str, dict]) -> dict[str, list[str]]:
    """为每个槽位收集全部可匹配字符串（name + cn_name + aliases）。

    用于字段→槽位的粗匹配（lexical 层）。真正的 slot_score 由 I-03 产出。
    """
    result: dict[str, list[str]] = {}
    for s in slots:
        name = s.get("name", "")
        if not name:
            continue
        aliases_set: set[str] = set()
        aliases_set.add(name.lower())
        if s.get("from") == "base":
            base = base_by_name.get(name)
            if base:
                aliases_set.add(base.get("cn_name", ""))
                for a in base.get("aliases", []) or []:
                    if a:
                        aliases_set.add(a.lower() if a.isascii() else a)
        else:
            cn = s.get("cn_name", "")
            if cn:
                aliases_set.add(cn)
            for a in s.get("aliases", []) or []:
                if a:
                    aliases_set.add(a.lower() if a.isascii() else a)
        result[name] = [a for a in aliases_set if a]
    return result


def match_field_to_slot(
    field_name: str,
    comment: str,
    slot_aliases: dict[str, list[str]],
) -> tuple[str | None, str]:
    """粗匹配。返回 (slot_name or None, match_reason)。

    匹配策略（优先级从高到低）：
    1. 字段名小写 == 某个 alias（精确命中）
    2. 注释 trim == 某个 alias（注释精确命中）
    3. 字段名的任一 _ 切分 token == 某个 alias
    4. 注释包含某个 alias（长度 ≥2）作为子串
    """
    fn_lower = field_name.lower().strip()
    fn_tokens = [t for t in fn_lower.split("_") if t]
    cm = (comment or "").strip()

    exact_name_hits: list[str] = []
    exact_comment_hits: list[str] = []
    token_hits: list[str] = []
    substr_hits: list[str] = []

    for slot_name, aliases in slot_aliases.items():
        for a in aliases:
            if not a:
                continue
            if fn_lower and fn_lower == a:
                exact_name_hits.append(slot_name)
                break
            if cm and cm == a:
                exact_comment_hits.append(slot_name)
                break
            if fn_tokens and a in fn_tokens:
                token_hits.append(slot_name)
                break
            if cm and len(a) >= 2 and a in cm:
                substr_hits.append(slot_name)
                break

    if exact_name_hits:
        return exact_name_hits[0], "exact_name"
    if exact_comment_hits:
        return exact_comment_hits[0], "exact_comment"
    if token_hits:
        return token_hits[0], "token"
    if substr_hits:
        return substr_hits[0], "substr"
    return None, "none"


@app.get("/api/virtual-tables/{vt_id}")
def get_virtual_table(vt_id: str):
    data = load_slot_data()
    vt = next((v for v in data["virtual_tables"] if v["vt_id"] == vt_id), None)
    if vt is None:
        # VT 在 slot_definitions 里尚未登记 —— 新建未生成 slots 的 VT 走这条
        # 从 scaffold 兜底构造最小 VT 对象，slots=[]，这样前端能打开详情页做第一次生成
        scaffold = _load_scaffold_yaml()
        scaf_vt = next((v for v in scaffold.get("virtual_tables", []) if v.get("vt_id") == vt_id), None)
        if scaf_vt is None:
            raise HTTPException(404, f"vt_id={vt_id} 既不在 slot_definitions 也不在 scaffold 里")
        vt = {
            "vt_id": vt_id,
            "topic": scaf_vt.get("topic", ""),
            "table_type": scaf_vt.get("table_type", "待定"),
            "l2_path": scaf_vt.get("l2_path") or [],
            "grain_desc": scaf_vt.get("grain_desc", ""),
            "source_table_count": int(scaf_vt.get("source_table_count", 0)),
            "slots": [],
        }

    base_by_name = {b["name"]: b for b in load_base_slots().get("base_slots", [])}
    slot_aliases = build_slot_aliases(vt.get("slots", []), base_by_name)

    # 附加源表字段样本 + I-04 归一结果
    scaffold = load_scaffold()
    scaffold_vt = next((v for v in scaffold["virtual_tables"] if v["vt_id"] == vt_id), None)

    # 加载 I-04 归一结果（按 vt_id 过滤，构建 (table_en, field_name) → row 查找表）
    # outer join 后 reviewed 独有的行，norm_df 原本的列会是 NaN，全部转 None 以保证 JSON 序列化
    def _clean_nan(d: dict) -> dict:
        cleaned = {}
        for k, v in d.items():
            if isinstance(v, float) and v != v:  # NaN
                cleaned[k] = None
            else:
                cleaned[k] = v
        return cleaned

    norm_lookup: dict[tuple[str, str], dict] = {}
    try:
        norm_df = load_normalization_df()
        sub_norm = norm_df[norm_df["vt_id"] == vt_id]
        for _, nr in sub_norm.iterrows():
            key = (str(nr["table_en"]), str(nr["field_name"]))
            norm_lookup[key] = _clean_nan(nr.to_dict())
    except Exception:
        pass  # normalization 产物不存在时降级到粗匹配

    # 加载 field_features 的 usage 信息（"已用/未用"按 usage_count > 0 判定）
    usage_lookup: dict[tuple[str, str], dict] = {}
    try:
        feat_df = load_features_df()
        for _, fr in feat_df.iterrows():
            key = (str(fr["table_en"]), str(fr["field_name"]))
            usage_lookup[key] = {
                "usage_count": float(fr.get("usage_count") or 0),
                "sql_count": float(fr.get("sql_count") or 0),
                "role_select": float(fr.get("role_select") or 0),
                "role_where": float(fr.get("role_where") or 0),
                "role_join": float(fr.get("role_join") or 0),
            }
    except Exception:
        pass

    source_tables_with_fields = []
    if scaffold_vt:
        ddl_df = load_ddl_df()
        for st in scaffold_vt["candidate_tables"]:
            en = st["en"]
            sub = ddl_df[ddl_df["table"] == en]
            if sub.empty:
                sub = ddl_df[ddl_df["origin_table"] == en]
            total_fields = len(sub)
            fields = []
            used_count = 0
            for _, row in sub.head(80).iterrows():
                field_name = str(row.get("field", ""))
                comment = str(row.get("comment", "") or "")

                # "已用/未用"：看 usage_count（字段在 SQL 中被使用的次数），和槽位归一无关
                usage = usage_lookup.get((en, field_name), {})
                usage_count = usage.get("usage_count", 0)
                is_used = usage_count > 0

                # I-04 归一结果：独立维度，展示 top1 候选 + review_status 颜色
                nr = norm_lookup.get((en, field_name))
                decision = None
                if nr is not None:
                    selected_slot = nr.get("selected_slot") or None
                    review_status = nr.get("review_status") or ""
                    selected_score = float(nr.get("selected_score") or 0) if (nr.get("selected_score") is not None and nr.get("selected_score") == nr.get("selected_score")) else 0.0
                    display_slot = selected_slot
                    match_reason = f"{review_status}({selected_score:.2f})" if review_status else "-"
                    top2_slot = nr.get("top2_slot")
                    top2_score = float(nr.get("top2_score") or 0) if (nr.get("top2_score") is not None and nr.get("top2_score") == nr.get("top2_score")) else None

                    # 人工审核决策优先于自动归一结果（pandas NaN → None）
                    raw_decision = nr.get("decision")
                    decision = raw_decision if isinstance(raw_decision, str) and raw_decision else None
                    raw_decision_slot = nr.get("decision_slot")
                    decision_slot = raw_decision_slot if isinstance(raw_decision_slot, str) and raw_decision_slot else None
                    if decision in ("accept_top1", "use_top2", "use_top3", "use_slot"):
                        display_slot = decision_slot or display_slot
                        review_status = "manual"
                        match_reason = f"manual:{decision}"
                    elif decision == "mark_new_slot":
                        # 新数据：decision_slot 直接是槽位名（已立即创建）
                        # 旧数据：decision_slot 形如 __NEW__:xxx → 去前缀
                        display_slot = decision_slot[len("__NEW__:"):] if decision_slot and decision_slot.startswith("__NEW__:") else decision_slot
                        review_status = "manual_new"
                        match_reason = f"manual:{decision}"
                    elif decision == "mark_noise":
                        display_slot = None
                        review_status = "noise"
                        match_reason = "manual:mark_noise"
                    elif decision == "skip":
                        review_status = "skipped"
                        match_reason = f"manual:skip"
                else:
                    # 归一产物不存在时降级到 lexical 粗匹配
                    mapped_slot, match_reason = match_field_to_slot(field_name, comment, slot_aliases)
                    display_slot = mapped_slot
                    review_status = None
                    selected_score = None
                    top2_slot = None
                    top2_score = None

                if is_used:
                    used_count += 1
                fields.append({
                    "field": field_name,
                    "type": str(row.get("type", "")),
                    "comment": comment,
                    "sample_data": str(row.get("sample_data", "") or "")[:120],
                    "used": is_used,
                    "usage_count": int(usage_count),
                    "sql_count": int(usage.get("sql_count", 0)),
                    "mapped_slot": display_slot,
                    "match_reason": match_reason,
                    "review_status": review_status,
                    "selected_score": selected_score,
                    "top2_slot": top2_slot,
                    "top2_score": top2_score,
                    "decision": decision,
                })
            # 覆盖率只统计实际送出的字段样本
            sample_size = len(fields)
            coverage = used_count / sample_size if sample_size else 0
            # 槽位映射覆盖率：仅统计「人工已审核且确认归属」的字段
            # 即 decision ∈ {accept_top1, use_top2, use_top3, use_slot, mark_new_slot}；
            # mark_noise / skip / 未审 都不计入
            REVIEWED_MAPPED = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot"}
            mapped_count = sum(1 for f in fields if f.get("decision") in REVIEWED_MAPPED)
            mapped_ratio = mapped_count / sample_size if sample_size else 0
            # 已用字段的槽位映射率：分母=is_used=true 的字段，分子=在此基础上"人工审核映射"的字段
            # 口径和 mapped_count 一致（REVIEWED_MAPPED），只是分母换成 used_count
            used_mapped_count = sum(
                1 for f in fields
                if f.get("used") and f.get("decision") in REVIEWED_MAPPED
            )
            used_mapped_ratio = used_mapped_count / used_count if used_count else 0
            source_tables_with_fields.append({
                "en": en,
                "cn": st.get("cn", ""),
                "field_count": total_fields,
                "fields_sample": fields,
                "sample_size": sample_size,
                "used_count": used_count,
                "coverage_ratio": round(coverage, 4),
                "mapped_count": mapped_count,
                "mapped_ratio": round(mapped_ratio, 4),
                "used_mapped_count": used_mapped_count,
                "used_mapped_ratio": round(used_mapped_ratio, 4),
            })

    # 附加 scaffold 里的 review_hint（前端需要它展示 LLM 建议的 L2，并提供"确认/迁移"按钮）
    review_hint = None
    if scaffold_vt is not None and scaffold_vt.get("review_hint"):
        review_hint = scaffold_vt["review_hint"]

    return {**vt, "source_tables_with_fields": source_tables_with_fields, "review_hint": review_hint}


@app.get("/api/base-slots")
def get_base_slots():
    data = load_base_slots()
    return data.get("base_slots", [])


@app.put("/api/virtual-tables/{vt_id}/slots")
def update_virtual_table_slots(vt_id: str, payload: SlotsUpdate):
    data = load_slot_data()
    vt = next((v for v in data["virtual_tables"] if v["vt_id"] == vt_id), None)
    if vt is None:
        # VT 在 slot_definitions 里尚未登记 —— 如果 scaffold 里有，自动初始化一条新记录
        # 适用场景：新建 VT 后首次保存 slots
        scaffold = _load_scaffold_yaml()
        scaf_vt = next((v for v in scaffold.get("virtual_tables", []) if v.get("vt_id") == vt_id), None)
        if scaf_vt is None:
            raise HTTPException(404, f"vt_id={vt_id} 既不在 slot_definitions 也不在 scaffold 里")
        vt = {
            "vt_id": vt_id,
            "topic": scaf_vt.get("topic", ""),
            "table_type": scaf_vt.get("table_type", "待定"),
            "l2_path": scaf_vt.get("l2_path") or [],
            "grain_desc": scaf_vt.get("grain_desc", ""),
            "source_table_count": int(scaf_vt.get("source_table_count", 0)),
            "slots": [],
        }
        data.setdefault("virtual_tables", []).append(vt)

    base_slot_names = {s["name"] for s in load_base_slots().get("base_slots", [])}

    # 校验
    seen_names: set[str] = set()
    cleaned_slots = []
    for i, slot in enumerate(payload.slots):
        name = (slot.get("name") or "").strip()
        from_type = (slot.get("from") or slot.get("from_type") or "").strip()
        role = (slot.get("role") or "").strip()

        if not name:
            raise HTTPException(400, f"slot[{i}]: name 不能为空")
        if name in seen_names:
            raise HTTPException(400, f"slot[{i}]: name {name} 与前面重复")
        if from_type not in ("base", "extended"):
            raise HTTPException(400, f"slot[{i}]={name}: from 必须是 base 或 extended")
        if from_type == "base" and name not in base_slot_names:
            raise HTTPException(400, f"slot[{i}]={name}: 声称为 base 但不在 base_slots 中")

        cleaned = {"name": name, "from": from_type, "role": role}
        if from_type == "extended":
            cleaned["cn_name"] = (slot.get("cn_name") or name).strip()
            cleaned["logical_type"] = (slot.get("logical_type") or "custom").strip()
            cleaned["aliases"] = [a.strip() for a in (slot.get("aliases") or []) if a.strip()]
            cleaned["applicable_table_types"] = slot.get("applicable_table_types") or []
            cleaned["llm_reason"] = slot.get("llm_reason") or ""
        # mapped_fields：槽位对应的源字段（重生成契约）
        raw_mf = slot.get("mapped_fields") or []
        mf_clean: list[dict] = []
        for mf in raw_mf:
            if not isinstance(mf, dict):
                continue
            en = str(mf.get("table_en") or "").strip()
            fn = str(mf.get("field_name") or "").strip()
            if not en or not fn:
                continue
            mf_clean.append({
                "table_en": en,
                "field_name": fn,
                "field_comment": str(mf.get("field_comment") or ""),
            })
        if mf_clean:
            cleaned["mapped_fields"] = mf_clean
        # 保留 manual_ 前缀的 source（归一审核来源优先），其余统一标 manual_vt_edit
        incoming_source = (slot.get("source") or "").strip()
        if incoming_source.startswith("manual_"):
            cleaned["source"] = incoming_source
        else:
            cleaned["source"] = VT_EDIT_SLOT_SOURCE
        seen_names.add(name)
        cleaned_slots.append(cleaned)

    # 备份
    if SLOT_YAML.exists():
        shutil.copy2(SLOT_YAML, SLOT_BAK)

    # 更新内存结构
    vt["slots"] = cleaned_slots
    if payload.summary is not None:
        vt["summary"] = payload.summary

    # 更新统计
    all_vts = data["virtual_tables"]
    total_slots = sum(len(v["slots"]) for v in all_vts)
    total_base = sum(sum(1 for s in v["slots"] if s.get("from") == "base") for v in all_vts)
    stats = data.setdefault("stats", {})
    stats["total_slots"] = total_slots
    stats["total_base_refs"] = total_base
    stats["total_extended"] = total_slots - total_base
    stats["overall_base_reuse_ratio"] = round(total_base / total_slots, 4) if total_slots else 0
    stats["avg_slots_per_vt"] = round(total_slots / len(all_vts), 2) if all_vts else 0
    stats["last_edited_at"] = datetime.now().isoformat(timespec="seconds")

    # 写回
    with SLOT_YAML.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, width=200)

    # review log
    REVIEW_LOG.parent.mkdir(parents=True, exist_ok=True)
    with REVIEW_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "time": datetime.now().isoformat(timespec="seconds"),
            "vt_id": vt_id,
            "topic": vt.get("topic"),
            "slot_count_after": len(cleaned_slots),
            "summary": payload.summary,
        }, ensure_ascii=False) + "\n")

    # 种子归一映射：把每个 slot 的 mapped_fields 写入 field_normalization_reviewed.parquet
    # 让下游 field_normalization 重算时把它们当作已审决策消费
    # 同时：清理本 VT 里指向"已被删除的 slot"的孤儿 reviewed 行（保留 mark_noise / skip / 无 slot 的决策）
    seeds_written = 0
    orphans_cleaned = 0
    valid_slot_names = {s["name"] for s in cleaned_slots}

    # 是否有孤儿需要清理（即使没有 seed_mappings 也可能要清）
    need_reviewed_rewrite = bool(payload.seed_mappings) or NORM_REVIEWED_PARQUET.exists()

    if need_reviewed_rewrite and NORM_REVIEWED_PARQUET.exists():
        existing = pd.read_parquet(NORM_REVIEWED_PARQUET)

        # Step 1: 清理本 VT 的孤儿行
        # 孤儿判定：vt_id 匹配 AND decision_slot 非空 AND (decision_slot 不在有效 slot 名单 OR 以 __NEW__: 前缀开头)
        def _is_orphan(r) -> bool:
            if r.get("vt_id") != vt_id:
                return False
            ds = r.get("decision_slot")
            if ds is None or (isinstance(ds, float) and pd.isna(ds)) or str(ds).strip() == "":
                return False
            ds_str = str(ds).strip()
            if ds_str.startswith("__NEW__:"):
                return True
            return ds_str not in valid_slot_names

        orphan_mask = existing.apply(_is_orphan, axis=1) if len(existing) else pd.Series([], dtype=bool)
        orphans_cleaned = int(orphan_mask.sum()) if len(existing) else 0
        if orphans_cleaned > 0:
            existing = existing[~orphan_mask].reset_index(drop=True)

        # Step 2: 合并新种子（去重本 VT 里 (table_en, field_name) 冲突的旧行）
        seed_rows: list[dict] = []
        if payload.seed_mappings:
            now_iso = datetime.now().isoformat(timespec="seconds")
            for sm in payload.seed_mappings:
                en = str(sm.get("table_en") or "").strip()
                fn = str(sm.get("field_name") or "").strip()
                slot_name = str(sm.get("slot_name") or "").strip()
                if not en or not fn or not slot_name:
                    continue
                if slot_name not in valid_slot_names:
                    continue
                seed_rows.append({
                    "table_en": en,
                    "field_name": fn,
                    "vt_id": vt_id,
                    "decision": "use_slot",
                    "decision_slot": slot_name,
                    "reviewed_at": now_iso,
                    "reviewer_note": "from slot regeneration",
                })

        if seed_rows:
            seed_df = pd.DataFrame(seed_rows)
            key_pairs = set(zip(seed_df["table_en"], seed_df["field_name"]))
            conflict_mask = existing.apply(
                lambda r: (r["vt_id"] == vt_id)
                and ((r["table_en"], r["field_name"]) in key_pairs),
                axis=1,
            ) if len(existing) else pd.Series([], dtype=bool)
            existing = existing[~conflict_mask] if len(existing) else existing
            merged = pd.concat([existing, seed_df], ignore_index=True)
            seeds_written = len(seed_rows)
        else:
            merged = existing

        # 只有产生了变化才写回（seed 有写 或 孤儿被清）
        if seeds_written > 0 or orphans_cleaned > 0:
            merged.to_parquet(NORM_REVIEWED_PARQUET, index=False)
            _cache.pop("norm_df", None)
    elif payload.seed_mappings:
        # reviewed 文件不存在但有 seed：直接新建
        seed_rows = []
        now_iso = datetime.now().isoformat(timespec="seconds")
        for sm in payload.seed_mappings:
            en = str(sm.get("table_en") or "").strip()
            fn = str(sm.get("field_name") or "").strip()
            slot_name = str(sm.get("slot_name") or "").strip()
            if not en or not fn or not slot_name:
                continue
            if slot_name not in valid_slot_names:
                continue
            seed_rows.append({
                "table_en": en,
                "field_name": fn,
                "vt_id": vt_id,
                "decision": "use_slot",
                "decision_slot": slot_name,
                "reviewed_at": now_iso,
                "reviewer_note": "from slot regeneration",
            })
        if seed_rows:
            pd.DataFrame(seed_rows).to_parquet(NORM_REVIEWED_PARQUET, index=False)
            _cache.pop("norm_df", None)
            seeds_written = len(seed_rows)

    return {
        "ok": True,
        "vt_id": vt_id,
        "slot_count": len(cleaned_slots),
        "backup_path": str(SLOT_BAK),
        "stats": stats,
        "seeds_written": seeds_written,
        "orphans_cleaned": orphans_cleaned,
    }


@app.get("/api/review-log")
def get_review_log(limit: int = 50):
    if not REVIEW_LOG.exists():
        return []
    with REVIEW_LOG.open(encoding="utf-8") as f:
        lines = [json.loads(l) for l in f.readlines() if l.strip()]
    return lines[-limit:][::-1]  # 最新在前


@app.get("/api/health")
def health():
    return {"ok": True, "ts": time.time()}


# ============================================================
# ========== I-05 字段归一审核 端点 ==========
# ============================================================


class DecisionPayload(BaseModel):
    table_en: str
    field_name: str
    vt_id: str
    decision: str  # accept_top1 / use_top2 / use_top3 / use_slot / mark_new_slot / mark_noise / skip
    selected_slot: str | None = None
    new_slot_name: str | None = None
    new_slot_cn_name: str | None = None
    reviewer_note: str | None = None


@app.get("/api/normalization/stats")
def normalization_stats():
    df = load_normalization_df()
    if df.empty:
        return {"total_rows": 0}

    by_status = df["review_status"].value_counts().to_dict()

    conflict_counter: dict[str, int] = {}
    for cts in df["conflict_types"]:
        if isinstance(cts, (list, tuple)) or hasattr(cts, "__iter__"):
            try:
                for ct in cts:
                    if ct:
                        conflict_counter[ct] = conflict_counter.get(ct, 0) + 1
            except TypeError:
                pass

    llm_applied = int(df["applied_llm"].sum()) if "applied_llm" in df.columns else 0
    llm_new = int(
        df["llm_propose_new_slot"].notna().sum() - (df["llm_propose_new_slot"] == "null").sum()
    ) if "llm_propose_new_slot" in df.columns else 0

    reviewed = int(df["decision"].notna().sum()) if "decision" in df.columns else 0

    return {
        "total_rows": int(len(df)),
        "unique_fields": int(df["field_name"].nunique()),
        "unique_vts": int(df["vt_id"].nunique()),
        "by_status": {k: int(v) for k, v in by_status.items()},
        "by_conflict_type": conflict_counter,
        "llm_applied": llm_applied,
        "llm_propose_new_slot": llm_new,
        "reviewed": reviewed,
    }


@app.get("/api/normalization")
def normalization_list(
    status: str | None = None,
    vt_id: str | None = None,
    slot: str | None = None,
    keyword: str | None = None,
    only_unreviewed: bool = False,
    limit: int = 50,
    offset: int = 0,
):
    df = load_normalization_df()
    if df.empty:
        return {"total": 0, "items": []}

    if status:
        df = df[df["review_status"] == status]
    if vt_id:
        df = df[df["vt_id"] == vt_id]
    if slot:
        df = df[df["top1_slot"] == slot]
    if keyword:
        mask = (
            df["field_name"].str.contains(keyword, na=False, regex=False)
            | df["field_comment"].str.contains(keyword, na=False, regex=False)
            | df["table_en"].str.contains(keyword, na=False, regex=False)
        )
        df = df[mask]
    if only_unreviewed:
        df = df[df["decision"].isna()]

    # 排序：conflict > low_confidence > needs_review > auto_accepted；未审核优先
    status_order = {"conflict": 0, "low_confidence": 1, "needs_review": 2, "auto_accepted": 3}
    df = df.copy()
    df["_sort_status"] = df["review_status"].map(status_order).fillna(9)
    df["_sort_reviewed"] = df["decision"].notna().astype(int)
    df = df.sort_values(["_sort_reviewed", "_sort_status", "selected_score"], ascending=[True, True, False])

    total = int(len(df))
    page = df.iloc[offset : offset + limit].copy()
    # 全局替换 NaN → None（避免 JSON 序列化错误）
    page = page.astype(object).where(pd.notna(page), None)

    # 附加 field_features（sample_values + comment_keywords）
    features = load_features_df()
    feat_lookup = {
        (r["table_en"], r["field_name"]): r.to_dict()
        for _, r in features.iterrows()
    }

    def _safe_float(v, default: float | None = 0.0) -> float | None:
        if v is None:
            return default
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return default
        if fv != fv:  # NaN check
            return default
        return fv

    items = []
    for _, r in page.iterrows():
        feat = feat_lookup.get((r["table_en"], r["field_name"]), {})
        items.append({
            "table_en": r["table_en"],
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment", ""),
            "vt_id": r["vt_id"],
            "review_status": r["review_status"],
            "selected_slot": r["selected_slot"],
            "selected_score": _safe_float(r["selected_score"]),
            "selected_slot_from": r.get("selected_slot_from", ""),
            "top1_slot": r.get("top1_slot"),
            "top1_score": _safe_float(r.get("top1_score")),
            "top2_slot": r.get("top2_slot"),
            "top2_score": _safe_float(r.get("top2_score"), default=None),
            "top3_slot": r.get("top3_slot"),
            "top3_score": _safe_float(r.get("top3_score"), default=None),
            "score_gap_top1_top2": _safe_float(r.get("score_gap_top1_top2")),
            "conflict_types": list(r["conflict_types"]) if r.get("conflict_types") is not None else [],
            "applied_llm": bool(r.get("applied_llm", False)),
            "llm_trigger": r.get("llm_trigger"),
            "llm_suggested_slot": r.get("llm_suggested_slot"),
            "llm_propose_new_slot": r.get("llm_propose_new_slot"),
            "llm_reason": r.get("llm_reason"),
            "decision": r.get("decision"),
            "decision_slot": r.get("decision_slot"),
            "reviewed_at": r.get("reviewed_at"),
            "reviewer_note": r.get("reviewer_note"),
            # 字段元信息
            "sample_values": list(feat.get("sample_values", [])) if feat.get("sample_values") is not None else [],
            "comment_keywords": list(feat.get("comment_keywords", [])) if feat.get("comment_keywords") is not None else [],
            "sample_patterns": list(feat.get("sample_patterns", [])) if feat.get("sample_patterns") is not None else [],
            "data_type": feat.get("data_type", ""),
            "table_l1": feat.get("table_l1", ""),
            "table_l2": feat.get("table_l2", ""),
        })

    return {"total": total, "items": items}


@app.post("/api/normalization/decision")
def normalization_decision(payload: DecisionPayload):
    df = load_normalization_df()
    mask = (
        (df["table_en"] == payload.table_en)
        & (df["field_name"] == payload.field_name)
        & (df["vt_id"] == payload.vt_id)
    )
    if not mask.any():
        raise HTTPException(404, "未找到匹配字段")

    allowed = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot", "mark_noise", "skip"}
    if payload.decision not in allowed:
        raise HTTPException(400, f"decision 必须是 {allowed} 之一")

    row = df[mask].iloc[0].to_dict()
    decision_slot: str | None = None
    if payload.decision == "accept_top1":
        decision_slot = row.get("top1_slot")
    elif payload.decision == "use_top2":
        decision_slot = row.get("top2_slot")
    elif payload.decision == "use_top3":
        decision_slot = row.get("top3_slot")
    elif payload.decision == "use_slot":
        if not payload.selected_slot:
            raise HTTPException(400, "use_slot 必须提供 selected_slot")
        decision_slot = payload.selected_slot
    elif payload.decision == "mark_new_slot":
        if not payload.new_slot_name:
            raise HTTPException(400, "mark_new_slot 必须提供 new_slot_name")
        # decision_slot 直接用提议的槽位名（不再带 __NEW__: 前缀，因为槽位会立即创建）
        decision_slot = payload.new_slot_name
    # skip / mark_noise 无 decision_slot

    # 写入 reviewed parquet（追加 / 更新）
    reviewed_row = {
        "table_en": payload.table_en,
        "field_name": payload.field_name,
        "vt_id": payload.vt_id,
        "decision": payload.decision,
        "decision_slot": decision_slot,
        "reviewed_at": datetime.now().isoformat(timespec="seconds"),
        "reviewer_note": payload.reviewer_note or "",
    }

    if NORM_REVIEWED_PARQUET.exists():
        existing = pd.read_parquet(NORM_REVIEWED_PARQUET)
        # 删除已有同 key 记录
        existing = existing[
            ~(
                (existing["table_en"] == payload.table_en)
                & (existing["field_name"] == payload.field_name)
                & (existing["vt_id"] == payload.vt_id)
            )
        ]
        new_df = pd.concat([existing, pd.DataFrame([reviewed_row])], ignore_index=True)
    else:
        new_df = pd.DataFrame([reviewed_row])
    new_df.to_parquet(NORM_REVIEWED_PARQUET, index=False)

    # mark_new_slot：立即把新槽位 append 到 slot_definitions.yaml 的对应 VT
    # 标记 source=manual_normalization_review，pipeline 重跑时识别并保留
    persisted_slot = False
    mapped_field_appended = False
    if payload.decision == "mark_new_slot":
        # 拿源字段的 data_type（用于 logical_type 默认值）
        try:
            feat_df = load_features_df()
            sub = feat_df[(feat_df["table_en"] == payload.table_en) & (feat_df["field_name"] == payload.field_name)]
            data_type = str(sub.iloc[0].get("data_type") or "string") if not sub.empty else "string"
        except Exception:
            data_type = "string"
        added, msg = _persist_manual_slot_to_vt(
            vt_id=payload.vt_id,
            new_slot_name=payload.new_slot_name or "",
            new_slot_cn_name=payload.new_slot_cn_name or "",
            source_field_name=payload.field_name,
            source_field_comment=str(row.get("field_comment", "") or ""),
            source_data_type=data_type,
            reviewer_note=payload.reviewer_note or "",
        )
        persisted_slot = added
        # mark_new_slot 创建后，也要把源字段 append 到新 slot 的 mapped_fields
        if added and decision_slot:
            mf_added, _ = _append_field_to_slot_mapped_fields(
                vt_id=payload.vt_id,
                slot_name=decision_slot,
                table_en=payload.table_en,
                field_name=payload.field_name,
                field_comment=str(row.get("field_comment", "") or ""),
            )
            mapped_field_appended = mf_added

    # 归到已有 slot（accept_top1 / use_top2 / use_top3 / use_slot）：追加字段到 mapped_fields
    # 这样「槽位」Tab 的种子字段列能看到刚审核的字段
    if payload.decision in {"accept_top1", "use_top2", "use_top3", "use_slot"} and decision_slot:
        mf_added, _mf_msg = _append_field_to_slot_mapped_fields(
            vt_id=payload.vt_id,
            slot_name=decision_slot,
            table_en=payload.table_en,
            field_name=payload.field_name,
            field_comment=str(row.get("field_comment", "") or ""),
        )
        mapped_field_appended = mf_added

    # 追加 review_log
    append_review_log({
        "type": "field_normalization_decision",
        "table_en": payload.table_en,
        "field_name": payload.field_name,
        "vt_id": payload.vt_id,
        "decision": payload.decision,
        "decision_slot": decision_slot,
        "new_slot_name": payload.new_slot_name,
        "new_slot_cn_name": payload.new_slot_cn_name,
        "reviewer_note": payload.reviewer_note,
    })

    # 失效缓存（让下次 GET 读最新）
    invalidate_norm_cache()

    return {
        "ok": True,
        "decision_slot": decision_slot,
        "reviewed_count": int(len(new_df)),
        "persisted_slot": persisted_slot if payload.decision == "mark_new_slot" else None,
        "mapped_field_appended": mapped_field_appended,
    }


class ReviewedUndoPayload(BaseModel):
    table_en: str
    field_name: str
    vt_id: str


@app.delete("/api/normalization/reviewed")
def undo_reviewed(payload: ReviewedUndoPayload):
    """P1-7: 撤销某条 reviewed 决策 —— 把 reviewed.parquet 中对应行删掉。
    注意：这只是删审核记录，不会回滚已因此创建的 slot（那个要去 VTEditor 手动删）。
    下次 pipeline 跑 field_normalization 时该字段会回到自动决策流程。"""
    if not NORM_REVIEWED_PARQUET.exists():
        raise HTTPException(404, "reviewed.parquet 不存在")
    existing = pd.read_parquet(NORM_REVIEWED_PARQUET)
    mask = (
        (existing["table_en"] == payload.table_en)
        & (existing["field_name"] == payload.field_name)
        & (existing["vt_id"] == payload.vt_id)
    )
    hit = int(mask.sum())
    if hit == 0:
        raise HTTPException(404, "未找到对应 reviewed 行")
    remaining = existing[~mask]
    remaining.to_parquet(NORM_REVIEWED_PARQUET, index=False)
    append_review_log({
        "type": "field_normalization_undo",
        "table_en": payload.table_en,
        "field_name": payload.field_name,
        "vt_id": payload.vt_id,
    })
    invalidate_norm_cache()
    return {"ok": True, "removed": hit, "remaining": int(len(remaining))}


class SlotNameSuggestPayload(BaseModel):
    # 两种模式：
    # 1) 字段归一审核：传 table_en + field_name + vt_id，LLM 基于源字段 DDL 推导
    # 2) 纯新建槽位：仅传 vt_id + user_cn_name（无源字段），LLM 仅基于中文名 + VT 现有槽位推导
    table_en: str | None = None
    field_name: str | None = None
    vt_id: str | None = None
    user_cn_name: str | None = None


@app.post("/api/normalization/llm-suggest-slot-name")
def llm_suggest_slot_name(payload: SlotNameSuggestPayload):
    """LLM 根据字段元信息（注释/样例/关键词）建议一个 snake_case 槽位英文名 + 中文名 + 命名理由。"""
    import sys
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from src.llm_client import chat  # type: ignore
    except Exception as e:
        raise HTTPException(500, f"加载 llm_client 失败: {e}")

    def _to_list(v):
        if v is None:
            return []
        try:
            return list(v)
        except TypeError:
            return []

    user_cn_name = (payload.user_cn_name or "").strip()
    has_field_ctx = bool(payload.table_en and payload.field_name)
    if not has_field_ctx and not user_cn_name:
        raise HTTPException(400, "至少要提供 table_en+field_name 或 user_cn_name 之一")

    # 模式 1：基于源字段 DDL 推导
    field_comment = ""
    table_cn = ""
    sample_values: list = []
    comment_keywords: list = []
    sample_patterns: list = []
    data_type = ""
    if has_field_ctx:
        features = load_features_df()
        sub = features[(features["table_en"] == payload.table_en) & (features["field_name"] == payload.field_name)]
        if sub.empty:
            raise HTTPException(404, f"未在 field_features 找到 {payload.table_en}.{payload.field_name}")
        feat = sub.iloc[0]
        field_comment = str(feat.get("field_comment") or "")
        table_cn = str(feat.get("table_cn") or "")
        sample_values = _to_list(feat.get("sample_values"))[:8]
        comment_keywords = _to_list(feat.get("comment_keywords"))
        sample_patterns = _to_list(feat.get("sample_patterns"))
        data_type = str(feat.get("data_type") or "")

    # 收集当前 VT 现有槽位名（避免重名）
    existing_slot_names: list[str] = []
    if payload.vt_id:
        slot_data = load_slot_data()
        vt_rec = next((v for v in slot_data.get("virtual_tables", []) or [] if v.get("vt_id") == payload.vt_id), None)
        if vt_rec:
            existing_slot_names = [s.get("name", "") for s in (vt_rec.get("slots") or []) if s.get("name")]

    system_prompt = (
        "你是数据建模专家，专门为字段命名语义槽位（slot）。"
        "你需要根据可用上下文（字段 DDL 或仅中文名），提出一个清晰、业务化、不缩写的英文槽位名。"
    )
    cn_block = (
        f"\n【用户已指定的中文名（必须采用，并据此推导英文名）】\n{user_cn_name}\n"
        if user_cn_name else ""
    )
    field_block = ""
    if has_field_ctx:
        field_block = f"""
【字段信息】
- 物理表英文名: {payload.table_en}
- 物理表中文名: {table_cn or '(无)'}
- 字段名: {payload.field_name}
- 字段注释: {field_comment or '(无)'}
- 数据类型: {data_type or '(无)'}
- 样例值: {' | '.join(str(v) for v in sample_values) if sample_values else '(无)'}
- 注释关键词: {', '.join(comment_keywords) if comment_keywords else '(无)'}
- 样例 pattern: {', '.join(sample_patterns) if sample_patterns else '(无)'}
"""

    user_prompt = f"""请建议一个槽位英文名（snake_case）、中文名和别名集合。
{field_block}{cn_block}
【已有槽位（必须避免重名）】
{', '.join(existing_slot_names) if existing_slot_names else '(无)'}

【输出格式】
仅输出 JSON：
{{
  "name": "snake_case 英文名",
  "cn_name": "精炼的中文名",
  "aliases": ["...", "...", "..."],
  "reason": "命名依据，1-2 句话"
}}

【约束】
- name 必须是小写 snake_case，由 2-4 个英文单词组成，长度 ≤ 40 字符
- name 必须是业务语义（如 spouse_certificate_no），禁止用拼音缩写或物理表的字段名直译
- name 不能与「已有槽位」中的任何名字重复
- 如果给了「用户已指定的中文名」，cn_name 必须原样返回该中文，不要修改
- 否则 cn_name 输出精炼的中文名词，2-8 字
- aliases 至少 3 个、至多 8 个，要包含：中文同义词（如户籍地址 / 户口所在地）、英文同义词（如 hukou_address）、可能出现的字段名缩写（如 hjdz, hjdzm）
- reason 用中文解释为什么这样命名"""

    try:
        raw = chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            json_mode=True,
        )
        result = json.loads(raw)
    except Exception as e:
        raise HTTPException(500, f"LLM 调用失败: {e}")

    name = (result.get("name") or "").strip()
    cn_name = (result.get("cn_name") or "").strip()
    reason = (result.get("reason") or "").strip()
    raw_aliases = result.get("aliases") or []
    aliases = [str(a).strip() for a in raw_aliases if str(a).strip()] if isinstance(raw_aliases, list) else []
    if not name:
        raise HTTPException(500, "LLM 返回的 name 为空")

    return {
        "name": name,
        "cn_name": cn_name,
        "aliases": aliases,
        "reason": reason,
        "duplicate_of_existing": name in existing_slot_names,
    }


@app.get("/api/normalization/new-slot-candidates")
def new_slot_candidates(limit: int = 50):
    """聚合 LLM 建议的新槽位，按 support_count 降序。"""
    df = load_normalization_df()
    if df.empty or "llm_propose_new_slot" not in df.columns:
        return []

    llm_df = df[df["llm_propose_new_slot"].notna() & (df["llm_propose_new_slot"] != "null")].copy()

    agg: dict[str, dict] = {}
    for _, r in llm_df.iterrows():
        try:
            ns = json.loads(r["llm_propose_new_slot"])
        except Exception:
            continue
        if not ns or not isinstance(ns, dict) or not ns.get("name"):
            continue
        key = ns["name"]
        bucket = agg.setdefault(key, {
            "name": key,
            "cn_name": ns.get("cn_name", ""),
            "support_count": 0,
            "example_fields": [],
            "example_vts": set(),
            "example_comments": [],
        })
        bucket["support_count"] += 1
        if r["field_name"] not in bucket["example_fields"] and len(bucket["example_fields"]) < 6:
            bucket["example_fields"].append(r["field_name"])
        bucket["example_vts"].add(r["vt_id"])
        if r.get("field_comment") and len(bucket["example_comments"]) < 5:
            bucket["example_comments"].append(r["field_comment"])

    result = []
    for v in agg.values():
        result.append({
            "name": v["name"],
            "cn_name": v["cn_name"],
            "support_count": v["support_count"],
            "example_fields": v["example_fields"],
            "example_vts": sorted(list(v["example_vts"]))[:6],
            "example_comments": v["example_comments"],
            "vt_count": len(v["example_vts"]),
        })
    result.sort(key=lambda x: -x["support_count"])
    return result[:limit]


# ==================== I-05b 新槽位发现（proposals）====================


class ProposalApplyPayload(BaseModel):
    proposal_id: str
    decision: str  # accept | reject | rename
    renamed_to: str | None = None  # rename 时必填，覆盖 name
    renamed_cn_name: str | None = None  # rename 时可选
    target_yaml: str | None = None  # 覆盖默认 scope → yaml 的映射；可选
    reviewer_note: str | None = None


def _load_slot_proposals() -> dict:
    """读 slot_proposals.yaml（不缓存，便于 apply 后立即看到新状态）。"""
    if not SLOT_PROPOSALS_YAML.exists():
        return {"meta": {}, "proposals": []}
    with SLOT_PROPOSALS_YAML.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {"meta": {}, "proposals": []}


def _save_slot_proposals(data: dict) -> None:
    SLOT_PROPOSALS_YAML.parent.mkdir(parents=True, exist_ok=True)
    with SLOT_PROPOSALS_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, width=200)


def _append_proposals_log(entry: dict) -> None:
    SLOT_PROPOSALS_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {**entry, "ts": datetime.now().isoformat(timespec="seconds")}
    with SLOT_PROPOSALS_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _append_base_slot(proposal: dict, override_name: str | None = None, override_cn: str | None = None) -> None:
    """用 ruamel.yaml 保留 base_slots.yaml 的注释并追加新槽位。"""
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：pip install ruamel.yaml  ({e})")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200

    with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
        data = ruamel.load(f)
    if "base_slots" not in data:
        raise HTTPException(500, "base_slots.yaml 缺少 base_slots 节点")

    # 查重（name）
    existing_names = {s.get("name") for s in data["base_slots"]}
    new_name = override_name or proposal["name"]
    if new_name in existing_names:
        raise HTTPException(400, f"base_slots 中已存在同名槽位: {new_name}")

    new_slot = {
        "name": new_name,
        "cn_name": override_cn or proposal.get("cn_name", new_name),
        "logical_type": proposal.get("logical_type", "text"),
        "role": proposal.get("role", "display"),
        "description": proposal.get("description", "") or f"由 I-05b 发现，{proposal.get('support_count', 0)} 字段支持",
        "aliases": list(proposal.get("aliases", []) or []),
        "sample_patterns": list(proposal.get("sample_patterns", []) or []),
        "applicable_table_types": ["主档", "关系", "事件", "标签", "聚合"],
    }
    data["base_slots"].append(new_slot)
    with BASE_SLOTS_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(data, f)


def _append_domain_slot(proposal: dict, override_name: str | None = None, override_cn: str | None = None) -> None:
    """追加到 domain_slots.yaml（不存在则创建）。保持纯 yaml（无注释模板）。"""
    DOMAIN_SLOTS_YAML.parent.mkdir(parents=True, exist_ok=True)
    if DOMAIN_SLOTS_YAML.exists():
        with DOMAIN_SLOTS_YAML.open(encoding="utf-8") as f:
            data = yaml.safe_load(f) or {"domain_slots": []}
    else:
        data = {"domain_slots": []}

    existing_names = {s.get("name") for s in data.get("domain_slots", [])}
    new_name = override_name or proposal["name"]
    if new_name in existing_names:
        raise HTTPException(400, f"domain_slots 中已存在同名槽位: {new_name}")

    data.setdefault("domain_slots", []).append({
        "name": new_name,
        "cn_name": override_cn or proposal.get("cn_name", new_name),
        "logical_type": proposal.get("logical_type", "text"),
        "role": proposal.get("role", "display"),
        "description": proposal.get("description", "") or f"由 I-05b 发现，{proposal.get('support_count', 0)} 字段支持",
        "aliases": list(proposal.get("aliases", []) or []),
        "sample_patterns": list(proposal.get("sample_patterns", []) or []),
        "target_vt_ids": list(proposal.get("target_vt_ids", []) or []),
        "target_domain": proposal.get("target_domain"),
    })
    with DOMAIN_SLOTS_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, width=200)


@app.get("/api/slots/proposals")
def list_slot_proposals(
    scope: str | None = None,
    status: str | None = None,
    source: str | None = None,
    limit: int = 500,
):
    """读 slot_proposals.yaml，支持按 scope/status/source 过滤。"""
    data = _load_slot_proposals()
    proposals = data.get("proposals", []) or []

    def keep(p):
        if scope and p.get("scope") != scope:
            return False
        if status and p.get("status") != status:
            return False
        if source and not p.get("source", "").startswith(source):
            return False
        return True

    filtered = [p for p in proposals if keep(p)]
    return {
        "meta": data.get("meta", {}),
        "total_filtered": len(filtered),
        "proposals": filtered[:limit],
    }


@app.post("/api/slots/proposals/apply")
def apply_slot_proposal(payload: ProposalApplyPayload):
    """应用一条 proposal（accept/rename 回写 yaml；reject 只更新状态）。"""
    if payload.decision not in {"accept", "reject", "rename"}:
        raise HTTPException(400, f"未知 decision: {payload.decision}")

    data = _load_slot_proposals()
    proposals = data.get("proposals", []) or []
    target = None
    for p in proposals:
        if p.get("proposal_id") == payload.proposal_id:
            target = p
            break
    if target is None:
        raise HTTPException(404, f"proposal_id 不存在: {payload.proposal_id}")

    if target.get("status") != "pending":
        raise HTTPException(400, f"proposal 当前状态 {target.get('status')}，不是 pending，无法重复处理")

    applied_to = None
    if payload.decision in {"accept", "rename"}:
        # 决定目标 yaml
        target_yaml = payload.target_yaml or target.get("scope")  # base / domain / vt_local
        if target_yaml == "base":
            _append_base_slot(
                target,
                override_name=payload.renamed_to if payload.decision == "rename" else None,
                override_cn=payload.renamed_cn_name if payload.decision == "rename" else None,
            )
            applied_to = str(BASE_SLOTS_YAML.relative_to(REPO_ROOT))
        elif target_yaml == "domain":
            _append_domain_slot(
                target,
                override_name=payload.renamed_to if payload.decision == "rename" else None,
                override_cn=payload.renamed_cn_name if payload.decision == "rename" else None,
            )
            applied_to = str(DOMAIN_SLOTS_YAML.relative_to(REPO_ROOT))
        elif target_yaml == "vt_local":
            # vt_local 的 proposal 仅由下游 I-01 重跑时读入（我们不直接编辑 slot_definitions.yaml 的 extended）
            # 这里只更新 status + 记日志，由重跑时 generate_slot_definitions 可选读 proposals
            applied_to = "none(vt_local 由 I-01 重跑时消费)"
        else:
            raise HTTPException(400, f"未知 target_yaml: {target_yaml}")

    # 更新状态
    if payload.decision == "accept":
        target["status"] = "accepted"
    elif payload.decision == "rename":
        target["status"] = "renamed"
        target["renamed_to"] = payload.renamed_to
        target["renamed_cn_name"] = payload.renamed_cn_name
    else:
        target["status"] = "rejected"
    target["applied_at"] = datetime.now().isoformat(timespec="seconds")
    _save_slot_proposals(data)

    # 审计日志
    _append_proposals_log({
        "proposal_id": payload.proposal_id,
        "decision": payload.decision,
        "renamed_to": payload.renamed_to,
        "renamed_cn_name": payload.renamed_cn_name,
        "target_yaml": payload.target_yaml or target.get("scope"),
        "applied_to": applied_to,
        "reviewer_note": payload.reviewer_note,
        "proposal_snapshot": {
            "name": target.get("name"),
            "cn_name": target.get("cn_name"),
            "scope": target.get("scope"),
            "source": target.get("source"),
            "support_count": target.get("support_count"),
        },
    })

    next_action = None
    if payload.decision in {"accept", "rename"} and target.get("scope") in {"base", "domain"}:
        next_action = "请运行: python3 scripts/run_pipeline.py --from slot_definitions"

    return {
        "ok": True,
        "applied_to": applied_to,
        "new_status": target["status"],
        "next_action": next_action,
    }


@app.get("/api/slots/proposals/log")
def slot_proposals_log(limit: int = 200):
    """审计日志回读。"""
    if not SLOT_PROPOSALS_LOG.exists():
        return []
    lines = SLOT_PROPOSALS_LOG.read_text(encoding="utf-8").strip().splitlines()
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue
    return list(reversed(entries))


# ==================== 字段黑名单（is_technical_noise 扩展）====================


class BlacklistAddPayload(BaseModel):
    mode: str  # exact_name | pattern | pair
    value: str | None = None  # exact_name / pattern 时用（field_name 或模式串）
    table_en: str | None = None  # mode=pair 时必填
    field_name: str | None = None  # mode=pair 时必填
    reason: str | None = None


class BlacklistRemovePayload(BaseModel):
    mode: str
    value: str | None = None
    table_en: str | None = None
    field_name: str | None = None


def _load_field_blacklist() -> dict:
    if not FIELD_BLACKLIST_YAML.exists():
        return {"exact_names": [], "name_patterns": [], "table_field_pairs": []}
    with FIELD_BLACKLIST_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    data.setdefault("exact_names", [])
    data.setdefault("name_patterns", [])
    data.setdefault("table_field_pairs", [])
    return data


def _save_field_blacklist(data: dict) -> None:
    """用 ruamel.yaml 保留注释与字段顺序。"""
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200

    # 读原文件保留注释
    if FIELD_BLACKLIST_YAML.exists():
        with FIELD_BLACKLIST_YAML.open(encoding="utf-8") as f:
            doc = ruamel.load(f)
    else:
        doc = {}

    # 用新 data 覆盖字段列表（注释区在头部，不影响）
    doc["exact_names"] = data.get("exact_names", [])
    doc["name_patterns"] = data.get("name_patterns", [])
    doc["table_field_pairs"] = data.get("table_field_pairs", [])

    with FIELD_BLACKLIST_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)


def _append_blacklist_log(entry: dict) -> None:
    FIELD_BLACKLIST_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {**entry, "ts": datetime.now().isoformat(timespec="seconds")}
    with FIELD_BLACKLIST_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


@app.get("/api/fields/blacklist")
def list_field_blacklist():
    """读当前字段黑名单。"""
    data = _load_field_blacklist()
    return {
        "exact_names": data.get("exact_names") or [],
        "name_patterns": data.get("name_patterns") or [],
        "table_field_pairs": data.get("table_field_pairs") or [],
        "total": (
            len(data.get("exact_names") or [])
            + len(data.get("name_patterns") or [])
            + len(data.get("table_field_pairs") or [])
        ),
    }


@app.post("/api/fields/blacklist")
def add_field_blacklist(payload: BlacklistAddPayload):
    """追加一条到黑名单。"""
    if payload.mode not in {"exact_name", "pattern", "pair"}:
        raise HTTPException(400, f"未知 mode: {payload.mode}")

    data = _load_field_blacklist()
    added = False
    if payload.mode == "exact_name":
        if not payload.value:
            raise HTTPException(400, "exact_name 模式需要 value")
        exact = list(data.get("exact_names") or [])
        v = payload.value.strip()
        if v.lower() not in {str(x).lower() for x in exact}:
            exact.append(v)
            data["exact_names"] = exact
            added = True
    elif payload.mode == "pattern":
        if not payload.value:
            raise HTTPException(400, "pattern 模式需要 value")
        patterns = list(data.get("name_patterns") or [])
        v = payload.value.strip()
        if v not in patterns:
            patterns.append(v)
            data["name_patterns"] = patterns
            added = True
    else:  # pair
        if not (payload.table_en and payload.field_name):
            raise HTTPException(400, "pair 模式需要 table_en + field_name")
        pairs = list(data.get("table_field_pairs") or [])
        entry = {
            "table_en": payload.table_en.strip(),
            "field_name": payload.field_name.strip(),
        }
        if payload.reason:
            entry["reason"] = payload.reason.strip()
        if not any(
            isinstance(p, dict)
            and p.get("table_en") == entry["table_en"]
            and p.get("field_name") == entry["field_name"]
            for p in pairs
        ):
            pairs.append(entry)
            data["table_field_pairs"] = pairs
            added = True

    if added:
        _save_field_blacklist(data)
        _append_blacklist_log({
            "action": "add",
            "mode": payload.mode,
            "value": payload.value,
            "table_en": payload.table_en,
            "field_name": payload.field_name,
            "reason": payload.reason,
        })

    return {
        "ok": True,
        "added": added,
        "next_action": "请运行: python3 scripts/run_pipeline.py --from field_features" if added else None,
    }


@app.delete("/api/fields/blacklist")
def remove_field_blacklist(payload: BlacklistRemovePayload):
    """从黑名单删除一条。"""
    if payload.mode not in {"exact_name", "pattern", "pair"}:
        raise HTTPException(400, f"未知 mode: {payload.mode}")

    data = _load_field_blacklist()
    removed = False
    if payload.mode == "exact_name":
        exact = list(data.get("exact_names") or [])
        target = (payload.value or "").lower().strip()
        new_exact = [x for x in exact if str(x).lower().strip() != target]
        if len(new_exact) != len(exact):
            data["exact_names"] = new_exact
            removed = True
    elif payload.mode == "pattern":
        patterns = list(data.get("name_patterns") or [])
        target = (payload.value or "").strip()
        new_patterns = [x for x in patterns if str(x).strip() != target]
        if len(new_patterns) != len(patterns):
            data["name_patterns"] = new_patterns
            removed = True
    else:
        pairs = list(data.get("table_field_pairs") or [])
        new_pairs = [
            p for p in pairs
            if not (
                isinstance(p, dict)
                and p.get("table_en") == payload.table_en
                and p.get("field_name") == payload.field_name
            )
        ]
        if len(new_pairs) != len(pairs):
            data["table_field_pairs"] = new_pairs
            removed = True

    if removed:
        _save_field_blacklist(data)
        _append_blacklist_log({
            "action": "remove",
            "mode": payload.mode,
            "value": payload.value,
            "table_en": payload.table_en,
            "field_name": payload.field_name,
        })

    return {"ok": True, "removed": removed}


@app.get("/api/fields/blacklist/log")
def field_blacklist_log(limit: int = 200):
    if not FIELD_BLACKLIST_LOG.exists():
        return []
    lines = FIELD_BLACKLIST_LOG.read_text(encoding="utf-8").strip().splitlines()
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue
    return list(reversed(entries))


# ==================== VT 级黑名单扫描 + 批量踢除 ====================


@app.post("/api/virtual-tables/{vt_id}/blacklist-scan")
def blacklist_scan_for_vt(vt_id: str):
    """扫描 VT 源表 DDL 字段里匹配黑名单规则的候选。

    返回的 candidate 里会标注 match_reason（匹配了哪条规则），
    以及 already_in_exact（是否已在 exact_names 里，若已在则批量踢除会跳过）。
    """
    import fnmatch as _fnmatch

    # 硬编码噪声名单和后缀，与 feature_builder 保持一致
    HARDCODED_NAMES = {
        "pid", "id", "rn", "etl_id", "src_id", "md5", "sha1", "sha256",
        "uuid", "_uuid", "row_id", "rowid", "pk", "seq", "seq_no",
        "ordinal", "row_number", "_version", "_sys_time",
    }
    HARDCODED_SUFFIX = ("_tmp", "_new", "_old", "_bak", "_result", "_tmp2")

    # 载入 yaml 黑名单（每次读新，不用 feature_builder 的缓存）
    bl = _load_field_blacklist()
    exact_set = {str(x).lower().strip() for x in (bl.get("exact_names") or [])}
    patterns = [str(p).lower().strip() for p in (bl.get("name_patterns") or [])]
    pair_set: set[tuple[str, str]] = set()
    for entry in (bl.get("table_field_pairs") or []):
        if isinstance(entry, dict) and entry.get("table_en") and entry.get("field_name"):
            pair_set.add((str(entry["table_en"]).strip(), str(entry["field_name"]).strip()))

    # 载入已用字段集合（usage_count > 0）—— 已用字段绝不算黑名单候选
    # 即使名字像噪声（如 id / md5），只要实际在 SQL 中被用了，就保留
    used_keys: set[tuple[str, str]] = set()
    try:
        feat_df = load_features_df()
        used_keys = {
            (str(r["table_en"]), str(r["field_name"]))
            for _, r in feat_df[feat_df["usage_count"] > 0].iterrows()
        }
    except Exception as _e:
        print(f"[blacklist-scan] 读 field_features 失败（跳过 '已用' 过滤）: {_e}")

    # 找 VT
    scaffold = _load_scaffold_yaml()
    target_vt = next((v for v in scaffold.get("virtual_tables", []) if v.get("vt_id") == vt_id), None)
    if target_vt is None:
        raise HTTPException(404, f"vt_id 不存在: {vt_id}")
    candidate_tables = target_vt.get("candidate_tables") or []

    ddl_df = load_ddl_df()
    col_en = "table" if "table" in ddl_df.columns else "table_en"
    col_field = "field" if "field" in ddl_df.columns else "field_name"
    col_comment = "comment" if "comment" in ddl_df.columns else "field_comment"
    col_sample = "sample_data" if "sample_data" in ddl_df.columns else None

    candidates: list[dict] = []
    total_scanned = 0
    skipped_used = 0
    seen: set[tuple[str, str]] = set()  # 跨表同 (table, field) 去重
    for t in candidate_tables:
        en = t.get("en")
        if not en:
            continue
        sub = ddl_df[ddl_df[col_en] == en]
        for _, row in sub.iterrows():
            fn = str(row.get(col_field, "") or "")
            if not fn:
                continue
            key = (en, fn)
            if key in seen:
                continue
            seen.add(key)
            total_scanned += 1

            # 已用字段绝不进黑名单候选
            if key in used_keys:
                skipped_used += 1
                continue

            fn_lower = fn.lower().strip()
            cm = str(row.get(col_comment, "") or "")
            sample = str(row.get(col_sample, "") or "") if col_sample else ""

            match_reason = None
            match_type = None
            already_in_exact = fn_lower in exact_set

            if already_in_exact:
                match_reason = f"已在 exact_names 里"
                match_type = "exact_already"
            elif fn_lower in HARDCODED_NAMES:
                match_reason = f"hardcoded 噪声名"
                match_type = "hardcoded_name"
            else:
                for pat in patterns:
                    if _fnmatch.fnmatchcase(fn_lower, pat):
                        match_reason = f"pattern: {pat}"
                        match_type = "pattern"
                        break
                if not match_reason and key in pair_set:
                    match_reason = "pair 精确命中"
                    match_type = "pair"
                if not match_reason:
                    for suf in HARDCODED_SUFFIX:
                        if fn_lower.endswith(suf):
                            match_reason = f"hardcoded 后缀: {suf}"
                            match_type = "hardcoded_suffix"
                            break

            if match_reason:
                candidates.append({
                    "table_en": en,
                    "field_name": fn,
                    "field_comment": cm,
                    "sample": sample[:100] if sample else "",
                    "data_type": str(row.get("type", "")),
                    "match_reason": match_reason,
                    "match_type": match_type,
                    "already_in_exact": already_in_exact,
                })

    return {
        "ok": True,
        "vt_id": vt_id,
        "total_scanned": total_scanned,
        "skipped_used": skipped_used,
        "matched": len(candidates),
        "candidates": candidates,
    }


class BatchBlacklistPayload(BaseModel):
    # 批量加入 exact_names：接收一组字段名，已存在的自动跳过
    field_names: list[str]
    reason: str | None = None


@app.post("/api/fields/blacklist/batch")
def batch_add_field_blacklist(payload: BatchBlacklistPayload):
    """批量加入 exact_names（前端"批量踢除"用）。已存在的自动跳过；不触发 pipeline，前端调完统一触发一次。"""
    data = _load_field_blacklist()
    existing = list(data.get("exact_names") or [])
    existing_lower = {str(x).lower().strip() for x in existing}

    added: list[str] = []
    skipped: list[str] = []
    for raw in payload.field_names:
        v = (raw or "").strip()
        if not v:
            continue
        if v.lower() in existing_lower:
            skipped.append(v)
            continue
        existing.append(v)
        existing_lower.add(v.lower())
        added.append(v)

    if added:
        data["exact_names"] = existing
        _save_field_blacklist(data)
        for v in added:
            _append_blacklist_log({
                "action": "add",
                "mode": "exact_name",
                "value": v,
                "reason": payload.reason or "batch blacklist scan",
            })

    return {
        "ok": True,
        "added_count": len(added),
        "skipped_count": len(skipped),
        "added": added,
        "skipped": skipped,
    }


# ==================== 槽位库总览（I-12）====================


class SlotEditPayload(BaseModel):
    aliases_add: list[str] = []
    aliases_remove: list[str] = []


def _append_slot_library_edit_log(entry: dict) -> None:
    SLOT_LIBRARY_EDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {**entry, "ts": datetime.now().isoformat(timespec="seconds")}
    with SLOT_LIBRARY_EDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _compute_slot_usage_stats(slot_defs: dict, norm_df: pd.DataFrame) -> dict:
    """统计每个 slot 在 slot_definitions 被引用 + field_normalization 命中。"""
    base_used: dict[str, int] = {}            # base 槽位被多少 VT 引用（from=base）
    extended_by_vt: dict[str, list[dict]] = {}  # vt_id → [extended slot dicts]

    for vt in slot_defs.get("virtual_tables", []) or []:
        vt_id = vt.get("vt_id")
        for s in vt.get("slots", []) or []:
            nm = s.get("name")
            if not nm:
                continue
            if s.get("from") == "base":
                base_used[nm] = base_used.get(nm, 0) + 1
            else:
                extended_by_vt.setdefault(vt_id, []).append({
                    "name": nm,
                    "cn_name": s.get("cn_name", ""),
                    "logical_type": s.get("logical_type", ""),
                    "role": s.get("role", ""),
                    "aliases": s.get("aliases", []) or [],
                    "applicable_table_types": s.get("applicable_table_types", []) or [],
                })

    # 命中字段统计（按 selected_slot）
    hits: dict[str, dict] = {}
    if not norm_df.empty:
        for slot, sub in norm_df.groupby("selected_slot"):
            if slot is None or not isinstance(slot, str) or slot == "":
                continue
            status_counts = sub["review_status"].value_counts().to_dict()
            hits[slot] = {
                "field_hit_count": int(len(sub)),
                "auto_accepted_count": int(status_counts.get("auto_accepted", 0)),
                "needs_review_count": int(status_counts.get("needs_review", 0)),
                "low_confidence_count": int(status_counts.get("low_confidence", 0)),
                "conflict_count": int(status_counts.get("conflict", 0)),
            }

    return {
        "base_used_by_vt_count": base_used,
        "extended_by_vt": extended_by_vt,
        "hits": hits,
    }


@app.get("/api/slot-library")
def get_slot_library():
    """槽位库总览：base + domain + extended + 使用统计。"""
    base_slots_raw = load_base_slots().get("base_slots", []) or []

    # domain_slots 可能不存在
    domain_slots_raw: list[dict] = []
    if DOMAIN_SLOTS_YAML.exists():
        with DOMAIN_SLOTS_YAML.open(encoding="utf-8") as f:
            d = yaml.safe_load(f) or {}
        domain_slots_raw = d.get("domain_slots", []) or []

    # 读 slot_definitions + field_normalization
    try:
        with SLOT_YAML.open(encoding="utf-8") as f:
            slot_defs = yaml.safe_load(f) or {}
    except Exception:
        slot_defs = {}
    try:
        norm_df = load_normalization_df()
    except Exception:
        norm_df = pd.DataFrame()

    stats_meta = _compute_slot_usage_stats(slot_defs, norm_df)
    base_used = stats_meta["base_used_by_vt_count"]
    extended_by_vt_map = stats_meta["extended_by_vt"]
    hits = stats_meta["hits"]

    def enrich(slot: dict) -> dict:
        name = slot.get("name", "")
        h = hits.get(name, {})
        return {
            "name": name,
            "cn_name": slot.get("cn_name", ""),
            "logical_type": slot.get("logical_type", ""),
            "role": slot.get("role", ""),
            "description": slot.get("description", ""),
            "aliases": list(slot.get("aliases", []) or []),
            "sample_patterns": list(slot.get("sample_patterns", []) or []),
            "applicable_table_types": list(slot.get("applicable_table_types", []) or []),
            "used_by_vt_count": int(base_used.get(name, 0)),
            "field_hit_count": int(h.get("field_hit_count", 0)),
            "auto_accepted_count": int(h.get("auto_accepted_count", 0)),
            "needs_review_count": int(h.get("needs_review_count", 0)),
            "low_confidence_count": int(h.get("low_confidence_count", 0)),
            "conflict_count": int(h.get("conflict_count", 0)),
        }

    base_slots = [enrich(s) for s in base_slots_raw]
    domain_slots = [enrich(s) for s in domain_slots_raw]

    # extended：按 VT 分组，每个 extended slot 也补 hit count
    vt_lookup = {vt.get("vt_id"): vt for vt in slot_defs.get("virtual_tables", []) or []}
    extended_by_vt: list[dict] = []
    for vt_id, slots in extended_by_vt_map.items():
        vt = vt_lookup.get(vt_id, {})
        slots_enriched = []
        for s in slots:
            h = hits.get(s["name"], {})
            slots_enriched.append({
                **s,
                "field_hit_count": int(h.get("field_hit_count", 0)),
                "auto_accepted_count": int(h.get("auto_accepted_count", 0)),
                "needs_review_count": int(h.get("needs_review_count", 0)),
                "conflict_count": int(h.get("conflict_count", 0)),
            })
        extended_by_vt.append({
            "vt_id": vt_id,
            "topic": vt.get("topic", ""),
            "table_type": vt.get("table_type", ""),
            "l2_path": vt.get("l2_path", []) or [],
            "extended_slots": slots_enriched,
        })
    # 按 VT topic 排序
    extended_by_vt.sort(key=lambda v: v.get("topic") or "")

    return {
        "base_slots": base_slots,
        "domain_slots": domain_slots,
        "extended_by_vt": extended_by_vt,
        "stats": {
            "base_count": len(base_slots),
            "domain_count": len(domain_slots),
            "extended_total": sum(len(v["extended_slots"]) for v in extended_by_vt),
            "vt_count": len(extended_by_vt),
        },
    }


@app.get("/api/slot-library/base/{name}/fields")
def slot_library_base_fields(name: str, limit: int = 30):
    """某个 slot 命中的字段 Top N（按 selected_score 降序）。"""
    try:
        df = load_normalization_df()
    except Exception:
        return []
    if df.empty:
        return []
    sub = df[df["selected_slot"] == name].copy()
    if sub.empty:
        return []
    # 按 selected_score 降序（NaN 排后）
    sub = sub.sort_values("selected_score", ascending=False, na_position="last").head(limit)

    def _sf(v, default: float | None = 0.0) -> float | None:
        if v is None:
            return default
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return default
        return default if fv != fv else fv

    return [
        {
            "table_en": r["table_en"],
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment", "") or "",
            "vt_id": r["vt_id"],
            "review_status": r["review_status"],
            "selected_score": _sf(r.get("selected_score")),
            "top1_slot": r.get("top1_slot"),
            "top2_slot": r.get("top2_slot"),
            "top2_score": _sf(r.get("top2_score"), default=None),
        }
        for _, r in sub.iterrows()
    ]


@app.put("/api/slot-library/base/{name}")
def edit_base_slot(name: str, payload: SlotEditPayload):
    """编辑 base slot 的 aliases（增/删）。用 ruamel 保留注释。"""
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200

    with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
        doc = ruamel.load(f)
    slots = doc.get("base_slots")
    if slots is None:
        raise HTTPException(500, "base_slots.yaml 缺少 base_slots 节点")

    target = None
    for s in slots:
        if s.get("name") == name:
            target = s
            break
    if target is None:
        raise HTTPException(404, f"base slot 不存在: {name}")

    current_aliases = list(target.get("aliases", []) or [])
    before = list(current_aliases)

    added: list[str] = []
    for a in payload.aliases_add:
        a = str(a).strip()
        if not a:
            continue
        if a in current_aliases:
            continue
        current_aliases.append(a)
        added.append(a)

    removed: list[str] = []
    for a in payload.aliases_remove:
        a = str(a).strip()
        if a in current_aliases:
            current_aliases.remove(a)
            removed.append(a)

    if not added and not removed:
        return {"ok": True, "added": [], "removed": [], "next_action": None}

    target["aliases"] = current_aliases

    with BASE_SLOTS_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)

    _append_slot_library_edit_log({
        "action": "edit_base_aliases",
        "slot_name": name,
        "added": added,
        "removed": removed,
        "before_count": len(before),
        "after_count": len(current_aliases),
    })

    return {
        "ok": True,
        "added": added,
        "removed": removed,
        "next_action": "请运行: python3 scripts/run_pipeline.py --from field_features",
    }


class BaseSlotSuggestPayload(BaseModel):
    cn_name: str
    cn_aliases: list[str] = []
    role: str = "filter"


@app.post("/api/slot-library/base/suggest")
def suggest_base_slot(payload: BaseSlotSuggestPayload):
    """根据中文名 + 中文别名 + role 让 LLM 生成英文名 / 描述 / 英文 aliases / logical_type。

    - 会把已有 base_slots 清单喂给 LLM 做去重参考
    - 禁止汉字逐字拼音直译（SLOT_NAMING_GUARDRAILS + pinyin lint 检测）
    - 仅输出建议，用户审核后才会真正写库（走 /api/slot-library/base）
    """
    import sys
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from src.llm_client import chat  # type: ignore
        from src.naming_lint import (  # type: ignore
            SLOT_NAMING_GUARDRAILS,
            analyze_pinyin_tokens,
            validate_slot_name,
        )
    except Exception as e:
        raise HTTPException(500, f"加载依赖失败: {e}")

    cn_name = (payload.cn_name or "").strip()
    cn_aliases = [a.strip() for a in (payload.cn_aliases or []) if a and a.strip()]
    role = (payload.role or "filter").strip() or "filter"
    if not cn_name:
        raise HTTPException(400, "cn_name 必填")
    if not cn_aliases:
        raise HTTPException(400, "cn_aliases 至少填 1 个")

    existing_slots = load_base_slots().get("base_slots", []) or []
    existing_names = [s.get("name", "") for s in existing_slots if s.get("name")]
    existing_name_set = set(existing_names)
    existing_brief_lines = []
    for s in existing_slots:
        nm = s.get("name", "")
        cn = s.get("cn_name", "")
        al = s.get("aliases") or []
        if not nm:
            continue
        al_part = f" | aliases: {', '.join(str(x) for x in al[:6])}" if al else ""
        existing_brief_lines.append(f"- {nm} ({cn}){al_part}")
    existing_brief = "\n".join(existing_brief_lines) or "(无)"

    system_prompt = (
        "你是数据建模专家，专门为业务语义槽位（slot）起英文名。"
        "你需要根据中文名 / 中文别名 / role，给出简洁的业务语义英文 snake_case 名、英文描述性 aliases、logical_type 和一段简短 description。\n\n"
        + SLOT_NAMING_GUARDRAILS
    )

    user_prompt = f"""请为下列中文语义槽位生成英文 base_slot 定义。

【输入】
- 中文名: {cn_name}
- 中文别名: {', '.join(cn_aliases)}
- role: {role}

【已有 base_slots（必须避免重名；若语义接近直接复用它的 name）】
{existing_brief}

【输出格式】严格 JSON，不要附加解释：
{{
  "name": "snake_case 英文名（2-4 词，业务语义，不超过 40 字符）",
  "description": "1 句话中文描述该槽位的含义",
  "aliases": ["英文/拼音缩写同义词，3-8 个；不要重复输入里的中文别名"],
  "logical_type": "text | code | datetime | amount | id | boolean | region_code 等"
}}

【硬约束】
- name 禁止把中文逐字拼音直译（例如「户籍地」→ ❌ hu_ji_di，✅ household_registration）
- name 若与「已有 base_slots」里某条语义等同，直接返回该已有 name（不要新造）
- aliases 只放英文 / 拼音缩写 / 常见字段名缩写（如 clpp、hjdz、vehicle_brand）；不要重复中文别名
- logical_type 选最贴合的；不确定用 text
"""

    try:
        raw = chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            json_mode=True,
        )
        result = json.loads(raw)
    except Exception as e:
        raise HTTPException(500, f"LLM 调用失败: {e}")

    name = (result.get("name") or "").strip()
    description = (result.get("description") or "").strip()
    logical_type = (result.get("logical_type") or "text").strip() or "text"
    raw_aliases = result.get("aliases") or []
    aliases = (
        [str(a).strip() for a in raw_aliases if str(a).strip()]
        if isinstance(raw_aliases, list)
        else []
    )
    cn_alias_set = set(cn_aliases)
    aliases = [a for a in aliases if a not in cn_alias_set]

    if not name:
        raise HTTPException(500, "LLM 返回的 name 为空")

    warnings: list[str] = []
    for issue in validate_slot_name(name, source="extended", base_slot_names=existing_name_set):
        warnings.append(issue)
    pinyin_info = analyze_pinyin_tokens(name)
    if pinyin_info["pinyin_count"] > 0 and not pinyin_info["is_mostly_pinyin"]:
        warnings.append(
            f"name `{name}` 含疑似拼音 token: {', '.join(pinyin_info['pinyin_tokens'])}"
        )
    if name in existing_name_set:
        warnings.append(f"name `{name}` 已存在于 base_slots；若语义等同请直接复用，不要重复新建")

    return {
        "name": name,
        "description": description,
        "aliases": aliases,
        "logical_type": logical_type,
        "warnings": warnings,
    }


class BaseSlotCreatePayload(BaseModel):
    name: str
    cn_name: str
    role: str
    logical_type: str = "text"
    description: str = ""
    aliases: list[str] = []
    applicable_table_types: list[str] = []


@app.post("/api/slot-library/base")
def create_base_slot(payload: BaseSlotCreatePayload):
    """在 base_slots.yaml 里追加一个 base 槽位。"""
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    name = (payload.name or "").strip()
    if not name or not name.replace("_", "").isalnum() or not name[0].isalpha():
        raise HTTPException(400, "name 必须是 snake_case（字母开头，仅字母/数字/下划线）")
    if not payload.cn_name.strip():
        raise HTTPException(400, "cn_name 必填")
    if not payload.role.strip():
        raise HTTPException(400, "role 必填")

    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200
    with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
        doc = ruamel.load(f)
    slots = doc.get("base_slots") or []
    if any(s.get("name") == name for s in slots):
        raise HTTPException(409, f"base slot 已存在: {name}")

    new_slot = {
        "name": name,
        "cn_name": payload.cn_name.strip(),
        "role": payload.role.strip(),
        "logical_type": payload.logical_type.strip() or "text",
        "description": payload.description.strip(),
        "aliases": [a.strip() for a in payload.aliases if a.strip()],
        "applicable_table_types": [t.strip() for t in payload.applicable_table_types if t.strip()],
    }
    slots.append(new_slot)
    doc["base_slots"] = slots
    with BASE_SLOTS_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)
    _append_slot_library_edit_log({"action": "create_base", "slot_name": name, "slot": new_slot})
    return {"ok": True, "name": name, "next_action": "请运行: python3 scripts/run_pipeline.py --from field_features"}


@app.delete("/api/slot-library/base/{name}")
def delete_base_slot(name: str):
    """从 base_slots.yaml 删除一个 base 槽位。"""
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200
    with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
        doc = ruamel.load(f)
    slots = doc.get("base_slots") or []
    idx = next((i for i, s in enumerate(slots) if s.get("name") == name), -1)
    if idx < 0:
        raise HTTPException(404, f"base slot 不存在: {name}")
    removed = dict(slots[idx])
    del slots[idx]
    doc["base_slots"] = slots
    with BASE_SLOTS_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)
    _append_slot_library_edit_log({"action": "delete_base", "slot_name": name, "slot": removed})
    return {"ok": True, "deleted": name, "next_action": "请运行: python3 scripts/run_pipeline.py --from field_features"}


@app.get("/api/slot-library/edit-log")
def slot_library_edit_log(limit: int = 100):
    if not SLOT_LIBRARY_EDIT_LOG.exists():
        return []
    lines = SLOT_LIBRARY_EDIT_LOG.read_text(encoding="utf-8").strip().splitlines()
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue
    return list(reversed(entries))


# ==================== I-13 VT 合并 ====================


class VTMergeApplyPayload(BaseModel):
    group_id: str
    primary_vt_id: str
    absorbed_vt_ids: list[str]
    reviewer_note: str | None = None


class VTMergeRejectPayload(BaseModel):
    group_id: str
    reviewer_note: str | None = None


def _append_vt_merge_log(entry: dict) -> None:
    VT_MERGE_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {**entry, "ts": datetime.now().isoformat(timespec="seconds")}
    with VT_MERGE_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _load_merge_candidates() -> dict:
    if not VT_MERGE_CANDIDATES_YAML.exists():
        return {"meta": {}, "groups": []}
    with VT_MERGE_CANDIDATES_YAML.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {"meta": {}, "groups": []}


def _save_merge_candidates(data: dict) -> None:
    VT_MERGE_CANDIDATES_YAML.parent.mkdir(parents=True, exist_ok=True)
    with VT_MERGE_CANDIDATES_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, width=200)


@app.get("/api/vt-merge/candidates")
def list_vt_merge_candidates(status: str | None = None):
    """读 vt_merge_candidates.yaml。"""
    data = _load_merge_candidates()
    groups = data.get("groups", []) or []
    if status:
        groups = [g for g in groups if g.get("status") == status]
    return {
        "meta": data.get("meta", {}),
        "total": len(groups),
        "groups": groups,
    }


@app.post("/api/vt-merge/apply")
def apply_vt_merge(payload: VTMergeApplyPayload):
    """应用一个合并组：primary 吸收 absorbed，scaffold_final.yaml 更新。"""
    data = _load_merge_candidates()
    groups = data.get("groups", []) or []
    group = next((g for g in groups if g.get("group_id") == payload.group_id), None)
    if group is None:
        raise HTTPException(404, f"group_id 不存在: {payload.group_id}")
    if group.get("status") != "pending":
        raise HTTPException(400, f"group 当前状态 {group.get('status')}，无法重复处理")

    # 校验 vt_ids 在组内
    member_ids = {m["vt_id"] for m in group.get("members", []) or []}
    if payload.primary_vt_id not in member_ids:
        raise HTTPException(400, f"primary_vt_id 不在组内: {payload.primary_vt_id}")
    for vid in payload.absorbed_vt_ids:
        if vid not in member_ids:
            raise HTTPException(400, f"absorbed_vt_id 不在组内: {vid}")
        if vid == payload.primary_vt_id:
            raise HTTPException(400, f"primary 不能同时是 absorbed: {vid}")

    # 读 scaffold
    if not SCAFFOLD_YAML.exists():
        raise HTTPException(500, "scaffold_final.yaml 不存在")
    with SCAFFOLD_YAML.open(encoding="utf-8") as f:
        scaffold = yaml.safe_load(f)

    # 首次合并时备份
    if not SCAFFOLD_YAML_BACKUP.exists():
        import shutil
        shutil.copy(SCAFFOLD_YAML, SCAFFOLD_YAML_BACKUP)

    vts = scaffold.get("virtual_tables", []) or []
    vt_by_id = {v["vt_id"]: v for v in vts}

    if payload.primary_vt_id not in vt_by_id:
        raise HTTPException(404, f"primary VT 不在 scaffold 里: {payload.primary_vt_id}")
    primary = vt_by_id[payload.primary_vt_id]

    # 合并 candidate_tables（去重）
    primary_en_set = {t.get("en") for t in (primary.get("candidate_tables") or [])}
    merged_tables = list(primary.get("candidate_tables") or [])
    for vid in payload.absorbed_vt_ids:
        absorbed = vt_by_id.get(vid)
        if absorbed is None:
            continue
        for t in absorbed.get("candidate_tables") or []:
            en = t.get("en")
            if en and en not in primary_en_set:
                merged_tables.append(t)
                primary_en_set.add(en)

    primary["candidate_tables"] = merged_tables
    primary["source_table_count"] = len(merged_tables)
    primary.setdefault("merged_from", []).extend(payload.absorbed_vt_ids)

    # 从 scaffold 里删除 absorbed VTs
    new_vts = [v for v in vts if v["vt_id"] not in set(payload.absorbed_vt_ids)]
    scaffold["virtual_tables"] = new_vts

    # 写回 yaml + json
    with SCAFFOLD_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(scaffold, f, allow_unicode=True, sort_keys=False, width=200)
    with SCAFFOLD_JSON_FINAL.open("w", encoding="utf-8") as f:
        json.dump(scaffold, f, ensure_ascii=False, indent=2)

    # 更新候选 yaml 状态
    group["status"] = "merged"
    group["applied_at"] = datetime.now().isoformat(timespec="seconds")
    group["applied_primary"] = payload.primary_vt_id
    group["applied_absorbed"] = payload.absorbed_vt_ids
    _save_merge_candidates(data)

    # 审计
    _append_vt_merge_log({
        "action": "merge",
        "group_id": payload.group_id,
        "primary_vt_id": payload.primary_vt_id,
        "absorbed_vt_ids": payload.absorbed_vt_ids,
        "merged_source_count": len(merged_tables),
        "reviewer_note": payload.reviewer_note,
    })

    # 清缓存（scaffold 变了）
    _cache.pop("scaffold", None)

    return {
        "ok": True,
        "primary_vt_id": payload.primary_vt_id,
        "merged_source_count": len(merged_tables),
        "new_vt_total": len(new_vts),
        "next_action": "请运行: python3 scripts/run_pipeline.py --from slot_definitions",
    }


@app.post("/api/vt-merge/reject")
def reject_vt_merge(payload: VTMergeRejectPayload):
    data = _load_merge_candidates()
    groups = data.get("groups", []) or []
    group = next((g for g in groups if g.get("group_id") == payload.group_id), None)
    if group is None:
        raise HTTPException(404, f"group_id 不存在: {payload.group_id}")
    if group.get("status") != "pending":
        raise HTTPException(400, f"group 当前状态 {group.get('status')}，无法重复处理")

    group["status"] = "rejected"
    group["rejected_at"] = datetime.now().isoformat(timespec="seconds")
    _save_merge_candidates(data)

    _append_vt_merge_log({
        "action": "reject",
        "group_id": payload.group_id,
        "reviewer_note": payload.reviewer_note,
    })

    return {"ok": True}


# ==================== I-14 scaffold 全编辑 ====================


class VTMetaUpdate(BaseModel):
    topic: str | None = None
    grain_desc: str | None = None
    table_type: str | None = None
    l2_path: list[str] | None = None
    candidate_tables: list[dict] | None = None
    reason: str | None = None
    # 设为 true 时删除该 VT 的 review_hint 字段（用于"确认 L2 归属 / 已处理 pending"）
    clear_review_hint: bool = False


class VTCreate(BaseModel):
    topic: str
    grain_desc: str = ""
    table_type: str = "待定"
    l2_path: list[str]
    candidate_tables: list[dict] = []
    reason: str = ""


VALID_TABLE_TYPES = {"主档", "关系", "事件", "聚合", "标签", "待定"}


def _backup_scaffold_if_needed() -> None:
    if not SCAFFOLD_YAML_EDIT_BACKUP.exists() and SCAFFOLD_YAML.exists():
        import shutil
        shutil.copy(SCAFFOLD_YAML, SCAFFOLD_YAML_EDIT_BACKUP)


def _load_scaffold_yaml() -> dict:
    if not SCAFFOLD_YAML.exists():
        raise HTTPException(500, "scaffold_final.yaml 不存在")
    with SCAFFOLD_YAML.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def _save_scaffold_yaml(scaffold: dict) -> None:
    with SCAFFOLD_YAML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(scaffold, f, allow_unicode=True, sort_keys=False, width=200)
    with SCAFFOLD_JSON_FINAL.open("w", encoding="utf-8") as f:
        json.dump(scaffold, f, ensure_ascii=False, indent=2)
    _cache.pop("scaffold", None)


def _append_scaffold_edit_log(entry: dict) -> None:
    SCAFFOLD_EDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {**entry, "ts": datetime.now().isoformat(timespec="seconds")}
    with SCAFFOLD_EDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


@app.put("/api/virtual-tables/{vt_id}/meta")
def update_vt_meta(vt_id: str, payload: VTMetaUpdate):
    """编辑 VT 的元信息 + 源表。仅覆盖提供的字段。"""
    if payload.table_type is not None and payload.table_type not in VALID_TABLE_TYPES:
        raise HTTPException(400, f"table_type 必须是 {VALID_TABLE_TYPES}")

    scaffold = _load_scaffold_yaml()
    vts = scaffold.get("virtual_tables", []) or []
    target = next((v for v in vts if v["vt_id"] == vt_id), None)
    if target is None:
        raise HTTPException(404, f"vt_id 不存在: {vt_id}")

    _backup_scaffold_if_needed()

    before = {k: target.get(k) for k in ["topic", "grain_desc", "table_type", "l2_path", "source_table_count"]}

    if payload.topic is not None:
        target["topic"] = payload.topic
    if payload.grain_desc is not None:
        target["grain_desc"] = payload.grain_desc
    if payload.table_type is not None:
        target["table_type"] = payload.table_type
    if payload.l2_path is not None:
        if len(payload.l2_path) < 1:
            raise HTTPException(400, "l2_path 至少需要 1 级")
        target["l2_path"] = list(payload.l2_path)
    if payload.candidate_tables is not None:
        # 去重 + 校验 en 非空
        seen = set()
        cleaned = []
        for t in payload.candidate_tables:
            en = (t.get("en") if isinstance(t, dict) else None) or ""
            if not en or en in seen:
                continue
            seen.add(en)
            cleaned.append({"en": en, "cn": t.get("cn", "") if isinstance(t, dict) else ""})
        target["candidate_tables"] = cleaned
        target["source_table_count"] = len(cleaned)
    if payload.reason is not None:
        target["reason"] = payload.reason
    if payload.clear_review_hint and "review_hint" in target:
        target.pop("review_hint", None)

    _save_scaffold_yaml(scaffold)

    _append_scaffold_edit_log({
        "action": "update_meta",
        "vt_id": vt_id,
        "before": before,
        "after": {k: target.get(k) for k in ["topic", "grain_desc", "table_type", "l2_path", "source_table_count"]},
    })

    return {
        "ok": True,
        "vt_id": vt_id,
        "next_action": "请运行: python3 scripts/run_pipeline.py --from slot_definitions",
    }


@app.post("/api/virtual-tables")
def create_vt(payload: VTCreate):
    """新建 VT。vt_id 自动生成。"""
    if payload.table_type not in VALID_TABLE_TYPES:
        raise HTTPException(400, f"table_type 必须是 {VALID_TABLE_TYPES}")
    if not payload.topic.strip():
        raise HTTPException(400, "topic 不能为空")
    if len(payload.l2_path) < 1:
        raise HTTPException(400, "l2_path 至少需要 1 级")

    scaffold = _load_scaffold_yaml()
    vts = scaffold.get("virtual_tables", []) or []

    _backup_scaffold_if_needed()

    # 生成 vt_id
    import hashlib
    # 找当前最大序号
    max_seq = 0
    for v in vts:
        vid = v.get("vt_id", "")
        parts = vid.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            max_seq = max(max_seq, int(parts[1]))
    hash_part = hashlib.md5(payload.topic.encode("utf-8")).hexdigest()[:8]
    new_id = f"vt_{hash_part}_{max_seq + 1:03d}"

    # 源表清洗
    seen = set()
    cleaned_tables = []
    for t in payload.candidate_tables:
        en = (t.get("en") if isinstance(t, dict) else None) or ""
        if not en or en in seen:
            continue
        seen.add(en)
        cleaned_tables.append({"en": en, "cn": t.get("cn", "") if isinstance(t, dict) else ""})

    new_vt = {
        "vt_id": new_id,
        "topic": payload.topic.strip(),
        "table_type": payload.table_type,
        "grain_desc": payload.grain_desc.strip(),
        "l2_path": list(payload.l2_path),
        "source_table_count": len(cleaned_tables),
        "candidate_tables": cleaned_tables,
        "reason": payload.reason.strip(),
    }
    vts.append(new_vt)
    scaffold["virtual_tables"] = vts
    _save_scaffold_yaml(scaffold)

    _append_scaffold_edit_log({
        "action": "create_vt",
        "vt_id": new_id,
        "topic": new_vt["topic"],
        "table_type": new_vt["table_type"],
        "l2_path": new_vt["l2_path"],
        "source_table_count": new_vt["source_table_count"],
    })

    return {
        "ok": True,
        "vt_id": new_id,
        "new_vt_total": len(vts),
        "next_action": "请运行: python3 scripts/run_pipeline.py --from slot_definitions",
    }


@app.delete("/api/virtual-tables/{vt_id}")
def delete_vt(vt_id: str):
    """删除一个 VT。"""
    scaffold = _load_scaffold_yaml()
    vts = scaffold.get("virtual_tables", []) or []
    target = next((v for v in vts if v["vt_id"] == vt_id), None)
    if target is None:
        raise HTTPException(404, f"vt_id 不存在: {vt_id}")

    _backup_scaffold_if_needed()

    new_vts = [v for v in vts if v["vt_id"] != vt_id]
    scaffold["virtual_tables"] = new_vts
    _save_scaffold_yaml(scaffold)

    _append_scaffold_edit_log({
        "action": "delete_vt",
        "vt_id": vt_id,
        "topic": target.get("topic"),
        "table_type": target.get("table_type"),
        "l2_path": target.get("l2_path"),
    })

    return {
        "ok": True,
        "deleted_vt_id": vt_id,
        "new_vt_total": len(new_vts),
        "next_action": "请运行: python3 scripts/run_pipeline.py --from slot_definitions",
    }


@app.get("/api/tables")
def list_all_tables():
    """物理表清单，供新建 VT / 添加源表时选择。

    数据源：DDL 表 + scaffold 已出现的 candidate_tables（后者有人工维护的 cn）。
    """
    ddl_df = load_ddl_df()
    # DDL 表名列（不同版本文件列名不同，兼容 table / table_en / origin_table）
    col_en = None
    for c in ["table", "table_en", "origin_table"]:
        if c in ddl_df.columns:
            col_en = c
            break
    if col_en is None:
        return []
    col_cn = None
    for c in ["table_cn_name", "table_cn", "cn_name"]:
        if c in ddl_df.columns:
            col_cn = c
            break

    # 先收集 scaffold 里 candidate_tables 中出现的 en → cn 映射（权威）
    scaffold = load_scaffold()
    scaffold_tables: dict[str, str] = {}
    for v in scaffold.get("virtual_tables", []) or []:
        for t in v.get("candidate_tables", []) or []:
            en = t.get("en")
            cn = t.get("cn")
            if en:
                scaffold_tables[en] = cn or scaffold_tables.get(en, "")

    # 统计每表字段数 —— 仅保留：出现在 scaffold 中、或 field_count >= 2（过滤 DDL 里的脏行）
    import re
    table_stats: dict[str, dict] = {}
    groups = ddl_df.groupby(col_en)
    table_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]+$")  # 合法表名：字母开头 + 字母数字下划线
    for en, sub in groups:
        en_str = str(en)
        field_count = int(len(sub))
        in_scaffold = en_str in scaffold_tables
        if not in_scaffold and (field_count < 2 or not table_name_re.match(en_str)):
            continue  # 过滤脏行
        table_stats[en_str] = {
            "en": en_str,
            "cn": scaffold_tables.get(en_str, ""),
            "field_count": field_count,
            "in_scaffold": in_scaffold,
        }

    result = sorted(table_stats.values(), key=lambda x: (not x.get("in_scaffold", False), x["en"]))
    return result


# ==================== I-15 分类树 ====================


class CategoryL1Add(BaseModel):
    name: str


class CategoryL2Add(BaseModel):
    l1_name: str
    name: str


class CategoryRename(BaseModel):
    old_name: str
    new_name: str
    l1_name: str | None = None  # L2 重命名时必填（定位到哪个 L1 下）


def _load_category_tree() -> list:
    if not CATEGORY_TREE_JSON.exists():
        return []
    with CATEGORY_TREE_JSON.open(encoding="utf-8") as f:
        return json.load(f)


def _save_category_tree(tree: list) -> None:
    if not CATEGORY_TREE_BACKUP.exists() and CATEGORY_TREE_JSON.exists():
        import shutil
        shutil.copy(CATEGORY_TREE_JSON, CATEGORY_TREE_BACKUP)
    CATEGORY_TREE_JSON.parent.mkdir(parents=True, exist_ok=True)
    with CATEGORY_TREE_JSON.open("w", encoding="utf-8") as f:
        json.dump(tree, f, ensure_ascii=False, indent=2)


def _append_categories_log(entry: dict) -> None:
    CATEGORIES_EDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {**entry, "ts": datetime.now().isoformat(timespec="seconds")}
    with CATEGORIES_EDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _count_vts_by_l1l2(scaffold: dict) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    """返回 (l1_count, (l1,l2)_count)"""
    l1_count: dict[str, int] = {}
    l2_count: dict[tuple[str, str], int] = {}
    for v in scaffold.get("virtual_tables", []) or []:
        l2p = v.get("l2_path") or []
        l1 = l2p[0] if len(l2p) >= 1 else ""
        l2 = l2p[1] if len(l2p) >= 2 else ""
        if l1:
            l1_count[l1] = l1_count.get(l1, 0) + 1
        if l1 and l2:
            l2_count[(l1, l2)] = l2_count.get((l1, l2), 0) + 1
    return l1_count, l2_count


@app.get("/api/categories")
def get_categories():
    """返回分类树（L1 → L2）+ 每节点的物理表 + VT 列表。"""
    tree = _load_category_tree()
    scaffold = load_scaffold()
    l1_count, l2_count = _count_vts_by_l1l2(scaffold)

    # slot_definitions 里有哪些 vt_id 已有槽位
    vts_with_slots: set[str] = set()
    try:
        sd = load_slot_data()
        for v in sd.get("virtual_tables", []) or []:
            if v.get("slots"):
                vts_with_slots.add(v.get("vt_id"))
    except Exception:
        pass

    # 构建 (l1, l2) → [vt info] 索引
    l2_vts: dict[tuple[str, str], list[dict]] = {}
    for v in scaffold.get("virtual_tables", []) or []:
        l2p = v.get("l2_path") or []
        if len(l2p) >= 2:
            key = (l2p[0], l2p[1])
            candidate_tables = v.get("candidate_tables") or []
            source_tables = [
                {"en": t.get("en", ""), "cn": t.get("cn", "")}
                for t in candidate_tables
                if isinstance(t, dict) and t.get("en")
            ]
            l2_vts.setdefault(key, []).append({
                "vt_id": v.get("vt_id"),
                "topic": v.get("topic", ""),
                "table_type": v.get("table_type", ""),
                "source_table_count": int(v.get("source_table_count", 0)),
                "source_tables": source_tables,
                "slot_status": "has_slots" if v.get("vt_id") in vts_with_slots else "no_slots",
            })

    result = []
    tree_l1_names = set()
    for node in tree:
        l1_name = node.get("name") if isinstance(node, dict) else str(node)
        if not l1_name:
            continue
        tree_l1_names.add(l1_name)
        children_raw = node.get("children", []) if isinstance(node, dict) else []
        children = []
        tree_l2_names = set()
        l1_table_count = 0
        for c in children_raw or []:
            l2_name = c.get("name") if isinstance(c, dict) else str(c)
            if not l2_name:
                continue
            tree_l2_names.add(l2_name)
            tables_raw = c.get("tables", []) if isinstance(c, dict) else []
            if not isinstance(tables_raw, list):
                tables_raw = []
            tables = [
                {"en": t.get("en", ""), "cn": t.get("cn", "")}
                for t in tables_raw
                if isinstance(t, dict) and t.get("en")
            ]
            l1_table_count += len(tables)
            children.append({
                "name": l2_name,
                "vt_count": int(l2_count.get((l1_name, l2_name), 0)),
                "table_count": len(tables),
                "in_tree": True,
                "tables": tables,
                "vts": l2_vts.get((l1_name, l2_name), []),
            })
        for (l1, l2), cnt in l2_count.items():
            if l1 == l1_name and l2 not in tree_l2_names:
                children.append({
                    "name": l2, "vt_count": cnt, "table_count": 0, "in_tree": False,
                    "tables": [], "vts": l2_vts.get((l1, l2), []),
                })
        result.append({
            "name": l1_name,
            "vt_count": int(l1_count.get(l1_name, 0)),
            "table_count": l1_table_count,
            "in_tree": True,
            "children": children,
        })
    for l1 in l1_count:
        if l1 not in tree_l1_names:
            children = []
            for (_l1, l2), cnt in l2_count.items():
                if _l1 == l1:
                    children.append({
                        "name": l2, "vt_count": cnt, "table_count": 0, "in_tree": False,
                        "tables": [], "vts": l2_vts.get((l1, l2), []),
                    })
            result.append({
                "name": l1,
                "vt_count": int(l1_count.get(l1, 0)),
                "table_count": 0,
                "in_tree": False,
                "children": children,
            })
    return {"categories": result}


@app.post("/api/categories/l1")
def add_category_l1(payload: CategoryL1Add):
    name = payload.name.strip()
    if not name:
        raise HTTPException(400, "name 不能为空")
    tree = _load_category_tree()
    for node in tree:
        if (node.get("name") if isinstance(node, dict) else str(node)) == name:
            raise HTTPException(400, f"L1 已存在: {name}")
    tree.append({"name": name, "children": []})
    _save_category_tree(tree)
    _append_categories_log({"action": "add_l1", "name": name})
    return {"ok": True, "name": name}


@app.post("/api/categories/l2")
def add_category_l2(payload: CategoryL2Add):
    l1 = payload.l1_name.strip()
    name = payload.name.strip()
    if not l1 or not name:
        raise HTTPException(400, "l1_name 和 name 不能为空")
    tree = _load_category_tree()
    target = None
    for node in tree:
        if (node.get("name") if isinstance(node, dict) else str(node)) == l1:
            target = node
            break
    if target is None:
        raise HTTPException(404, f"L1 不存在: {l1}")
    children = target.setdefault("children", [])
    for c in children:
        if (c.get("name") if isinstance(c, dict) else str(c)) == name:
            raise HTTPException(400, f"L2 已存在: {l1}/{name}")
    children.append({"name": name})
    _save_category_tree(tree)
    _append_categories_log({"action": "add_l2", "l1_name": l1, "name": name})
    return {"ok": True, "l1_name": l1, "name": name}


@app.put("/api/categories/l1/rename")
def rename_category_l1(payload: CategoryRename):
    old = payload.old_name.strip()
    new = payload.new_name.strip()
    if not old or not new or old == new:
        raise HTTPException(400, "old_name 和 new_name 都必需且不相同")
    tree = _load_category_tree()
    # 不能改到已存在的 L1
    for node in tree:
        nm = node.get("name") if isinstance(node, dict) else str(node)
        if nm == new:
            raise HTTPException(400, f"L1 新名字已存在: {new}")
    target = None
    for node in tree:
        if (node.get("name") if isinstance(node, dict) else str(node)) == old:
            target = node
            break
    if target is None:
        raise HTTPException(404, f"L1 不存在: {old}")
    target["name"] = new
    _save_category_tree(tree)

    # 级联更新 scaffold
    scaffold = _load_scaffold_yaml()
    affected = 0
    for v in scaffold.get("virtual_tables", []) or []:
        l2p = v.get("l2_path") or []
        if l2p and l2p[0] == old:
            l2p[0] = new
            v["l2_path"] = l2p
            affected += 1
    if affected > 0:
        _backup_scaffold_if_needed()
        _save_scaffold_yaml(scaffold)

    _append_categories_log({
        "action": "rename_l1", "old_name": old, "new_name": new, "affected_vt_count": affected,
    })
    return {
        "ok": True,
        "affected_vt_count": affected,
        "next_action": "请运行: python3 scripts/run_pipeline.py --from slot_definitions" if affected else None,
    }


@app.put("/api/categories/l2/rename")
def rename_category_l2(payload: CategoryRename):
    if not payload.l1_name:
        raise HTTPException(400, "重命名 L2 必须提供 l1_name")
    l1 = payload.l1_name.strip()
    old = payload.old_name.strip()
    new = payload.new_name.strip()
    if not old or not new or old == new:
        raise HTTPException(400, "old_name 和 new_name 都必需且不相同")

    tree = _load_category_tree()
    target = None
    for node in tree:
        if (node.get("name") if isinstance(node, dict) else str(node)) == l1:
            target = node
            break
    if target is None:
        raise HTTPException(404, f"L1 不存在: {l1}")
    children = target.get("children", []) or []
    # 不能改到已存在的同 L1 下 L2
    for c in children:
        if (c.get("name") if isinstance(c, dict) else str(c)) == new:
            raise HTTPException(400, f"L2 新名字已存在: {l1}/{new}")
    found = False
    for c in children:
        if (c.get("name") if isinstance(c, dict) else str(c)) == old:
            c["name"] = new
            found = True
            break
    if not found:
        raise HTTPException(404, f"L2 不存在: {l1}/{old}")
    _save_category_tree(tree)

    # 级联更新 scaffold
    scaffold = _load_scaffold_yaml()
    affected = 0
    for v in scaffold.get("virtual_tables", []) or []:
        l2p = v.get("l2_path") or []
        if len(l2p) >= 2 and l2p[0] == l1 and l2p[1] == old:
            l2p[1] = new
            v["l2_path"] = l2p
            affected += 1
    if affected > 0:
        _backup_scaffold_if_needed()
        _save_scaffold_yaml(scaffold)

    _append_categories_log({
        "action": "rename_l2", "l1_name": l1, "old_name": old, "new_name": new, "affected_vt_count": affected,
    })
    return {
        "ok": True,
        "affected_vt_count": affected,
        "next_action": "请运行: python3 scripts/run_pipeline.py --from slot_definitions" if affected else None,
    }


@app.delete("/api/categories/l1/{name}")
def delete_category_l1(name: str):
    scaffold = load_scaffold()
    l1_count, _ = _count_vts_by_l1l2(scaffold)
    if l1_count.get(name, 0) > 0:
        raise HTTPException(400, f"还有 {l1_count[name]} 张 VT 引用 L1 '{name}'，请先迁移或删除")
    tree = _load_category_tree()
    new_tree = [n for n in tree if (n.get("name") if isinstance(n, dict) else str(n)) != name]
    if len(new_tree) == len(tree):
        raise HTTPException(404, f"L1 不存在: {name}")
    _save_category_tree(new_tree)
    _append_categories_log({"action": "delete_l1", "name": name})
    return {"ok": True, "deleted": name}


@app.delete("/api/categories/l2/{l1_name}/{l2_name}")
def delete_category_l2(l1_name: str, l2_name: str):
    scaffold = load_scaffold()
    _, l2_count = _count_vts_by_l1l2(scaffold)
    if l2_count.get((l1_name, l2_name), 0) > 0:
        raise HTTPException(400, f"还有 {l2_count[(l1_name, l2_name)]} 张 VT 引用 L2 '{l1_name}/{l2_name}'，请先迁移或删除")
    tree = _load_category_tree()
    target = None
    for node in tree:
        if (node.get("name") if isinstance(node, dict) else str(node)) == l1_name:
            target = node
            break
    if target is None:
        raise HTTPException(404, f"L1 不存在: {l1_name}")
    children = target.get("children", []) or []
    new_children = [c for c in children if (c.get("name") if isinstance(c, dict) else str(c)) != l2_name]
    if len(new_children) == len(children):
        raise HTTPException(404, f"L2 不存在: {l1_name}/{l2_name}")
    target["children"] = new_children
    _save_category_tree(tree)
    _append_categories_log({"action": "delete_l2", "l1_name": l1_name, "name": l2_name})
    return {"ok": True, "deleted": f"{l1_name}/{l2_name}"}


@app.get("/api/categories/edit-log")
def categories_edit_log(limit: int = 100):
    if not CATEGORIES_EDIT_LOG.exists():
        return []
    lines = CATEGORIES_EDIT_LOG.read_text(encoding="utf-8").strip().splitlines()
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue
    return list(reversed(entries))


# ==================== I-16 Gap3: 单 VT LLM 生成槽位 ====================


class L2GenerateSlotsPayload(BaseModel):
    only_missing: bool = True  # True=只生成还没有槽位的 VT；False=全部 VT（会覆盖已有）
    include_empty_source: bool = False  # False=跳过没有源表的 VT


@app.post("/api/categories/{l1}/{l2}/generate-slots")
def generate_slots_for_l2(l1: str, l2: str, payload: L2GenerateSlotsPayload):
    """批量为 L2 下的 VT 生成槽位（只返回结果，不直接写回）。"""
    import sys
    import time as _time

    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from scripts.generate_slot_definitions import (  # type: ignore
            generate_slots_for_vt,
            load_base_slots_compact,
            BASE_SLOTS_YAML as GEN_BASE_SLOTS_YAML,
            DDL_CSV as GEN_DDL_CSV,
        )
    except Exception as e:
        raise HTTPException(500, f"加载 generate_slot_definitions 失败: {e}")

    scaffold = _load_scaffold_yaml()
    l2_vts = [
        v for v in scaffold.get("virtual_tables", []) or []
        if (v.get("l2_path") or [])[:2] == [l1, l2]
    ]
    if not l2_vts:
        raise HTTPException(404, f"L1/L2 下无 VT: {l1}/{l2}")

    # 查哪些 VT 已有槽位（slot_definitions.yaml）
    existing: set[str] = set()
    try:
        sd = load_slot_data()
        for v in sd.get("virtual_tables", []) or []:
            if v.get("slots"):
                existing.add(v.get("vt_id"))
    except Exception:
        pass

    targets = []
    for v in l2_vts:
        if not v.get("candidate_tables") and not payload.include_empty_source:
            continue
        if payload.only_missing and v.get("vt_id") in existing:
            continue
        targets.append(v)
    if not targets:
        return {"ok": True, "results": [], "message": "无需要处理的 VT"}

    with GEN_BASE_SLOTS_YAML.open(encoding="utf-8") as f:
        base_slots_data = yaml.safe_load(f)
    base_slots_compact = load_base_slots_compact(base_slots_data)
    ddl_df = pd.read_csv(GEN_DDL_CSV, encoding="utf-8")

    results = []
    for vt in targets:
        t0 = _time.time()
        try:
            result, warnings = generate_slots_for_vt(vt, base_slots_data, base_slots_compact, ddl_df)
            results.append({
                "vt_id": vt["vt_id"],
                "topic": vt.get("topic", ""),
                "ok": True,
                "slots": result.get("slots", []),
                "summary": result.get("summary", ""),
                "warnings": warnings or [],
                "elapsed_sec": round(_time.time() - t0, 2),
            })
        except Exception as e:
            results.append({
                "vt_id": vt["vt_id"],
                "topic": vt.get("topic", ""),
                "ok": False,
                "error": str(e),
                "elapsed_sec": round(_time.time() - t0, 2),
            })

    return {"ok": True, "total": len(targets), "results": results}


# 扩展槽位：在现有 slots 基础上，让 LLM 根据未被覆盖的字段补充新 extended slots
class ExtendSlotsPayload(BaseModel):
    include_unconfirmed: bool = True  # 默认把"仅自动归一、未人工确认"的字段也纳入候选，让 LLM 重新判定


@app.post("/api/virtual-tables/{vt_id}/extend-slots")
def extend_slots_for_vt(vt_id: str, payload: ExtendSlotsPayload | None = None):
    """只看未被覆盖的字段，让 LLM 建议新增的 extended 槽位（不动已有）。

    过滤逻辑：
    - 空样例 / 技术字段 / 未使用字段（usage_count=0）始终 skip
    - **人工审核确认过的字段**（reviewed.parquet.decision ∈ reviewed_take）始终 skip —— 这些已经明确归属
    - **仅自动归一、未确认的字段**（selected_slot 有值但未在 reviewed）：
        - include_unconfirmed=True（默认）：**纳入 LLM 候选**（用户可能不信任自动结果，想重建槽位）
        - include_unconfirmed=False：skip（老行为，等于信任 auto_accepted）
    """
    if payload is None:
        payload = ExtendSlotsPayload()
    import sys
    import time as _time

    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from src.llm_client import chat  # type: ignore
    except Exception as e:
        raise HTTPException(500, f"加载 llm_client 失败: {e}")

    # 读 VT 的当前 slots（从 slot_definitions.yaml）
    slot_data = load_slot_data()
    vt_slots_rec = next((v for v in slot_data.get("virtual_tables", []) or [] if v.get("vt_id") == vt_id), None)
    current_slots: list[dict] = (vt_slots_rec or {}).get("slots", []) or []

    # 读 scaffold 找 VT
    scaffold = _load_scaffold_yaml()
    target_vt = next((v for v in scaffold.get("virtual_tables", []) if v.get("vt_id") == vt_id), None)
    if target_vt is None:
        raise HTTPException(404, f"vt_id 不存在: {vt_id}")
    candidate_tables = target_vt.get("candidate_tables") or []
    if not candidate_tables:
        raise HTTPException(400, "该 VT 没有源表")

    # 现有 slot 的 aliases 合集（base 槽位要从 base_slots.yaml 展开）
    base_by_name = {b["name"]: b for b in load_base_slots().get("base_slots", [])}
    covered_aliases: set[str] = set()
    covered_slot_names: set[str] = set()
    for s in current_slots:
        covered_slot_names.add(s.get("name", ""))
        name = s.get("name", "")
        covered_aliases.add(name.lower())
        if s.get("cn_name"):
            covered_aliases.add(s["cn_name"])
        for a in s.get("aliases", []) or []:
            covered_aliases.add(str(a).lower())
        # base 从 base_slots 展开 aliases
        if s.get("from") == "base" and name in base_by_name:
            for a in base_by_name[name].get("aliases", []) or []:
                covered_aliases.add(str(a).lower())
            if base_by_name[name].get("cn_name"):
                covered_aliases.add(base_by_name[name]["cn_name"])

    # 把人工已审字段（accept_top1 / use_top2 / use_top3 / use_slot / mark_new_slot）当成 seed：
    # 这些字段已经有明确归属，扩展槽位时不应再为它们建新 slot
    # 通过把它们的 field_name + field_comment 加入 covered_aliases 实现"视为已覆盖"
    reviewed_seed_fields: list[tuple[str, str]] = []  # [(field_name, comment), ...] 用于 prompt 举例
    try:
        if NORM_REVIEWED_PARQUET.exists():
            reviewed_df = pd.read_parquet(NORM_REVIEWED_PARQUET)
            rev_vt = reviewed_df[reviewed_df["vt_id"] == vt_id]
            reviewed_take = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot"}
            features_df = load_features_df()
            feat_lookup = {
                (str(r["table_en"]), str(r["field_name"])): str(r.get("field_comment") or "")
                for _, r in features_df.iterrows()
            }
            for _, r in rev_vt.iterrows():
                if str(r.get("decision") or "") not in reviewed_take:
                    continue
                fn = str(r.get("field_name") or "")
                if not fn:
                    continue
                covered_aliases.add(fn.lower())
                cm = feat_lookup.get((str(r.get("table_en") or ""), fn), "")
                if cm:
                    covered_aliases.add(cm.lower())
                reviewed_seed_fields.append((fn, cm))
    except Exception as _e:
        print(f"[extend-slots] 读 reviewed seed 失败（不影响主流程）: {_e}")

    def _is_covered(field_name: str, comment: str) -> bool:
        """粗判：field_name 或 comment 能和已有 slot 的 alias 匹配"""
        fn_lower = (field_name or "").lower()
        cm = (comment or "").strip()
        if not fn_lower and not cm:
            return False
        for a in covered_aliases:
            if not a or len(a) < 2:
                continue
            if a in fn_lower or a in cm.lower():
                return True
        return False

    # 读源表 DDL 字段
    ddl_df = load_ddl_df()
    col_en = "table" if "table" in ddl_df.columns else "table_en"
    col_field = "field" if "field" in ddl_df.columns else "field_name"
    col_comment = "comment" if "comment" in ddl_df.columns else "field_comment"
    col_sample = "sample_data" if "sample_data" in ddl_df.columns else None

    # 复用 feature_builder 的技术噪声判定（含 yaml 黑名单）
    try:
        from src.pipeline.feature_builder import is_technical_noise as _is_noise  # type: ignore
    except Exception:
        def _is_noise(field_name, comment, samples, table_en=None):
            return False, ""

    def _empty_sample(s: str) -> bool:
        if not s:
            return True
        ss = s.strip()
        if not ss:
            return True
        toks = [t.strip().lower() for t in ss.replace("|", " ").split() if t.strip()]
        if not toks:
            return True
        if all(t in {"null", "nan", "none", "", "-"} for t in toks):
            return True
        return False

    # DDL 已用字段集合：从 field_features.parquet 按 usage_count > 0 取
    used_keys: set[tuple[str, str]] = set()
    try:
        feat_df_all = load_features_df()
        used_keys = {
            (str(r["table_en"]), str(r["field_name"]))
            for _, r in feat_df_all[feat_df_all["usage_count"] > 0].iterrows()
        }
    except Exception as _e:
        print(f"[extend-slots] 读 usage 失败（跳过 '已用' 过滤）: {_e}")

    # 映射过滤（两类）：
    #   confirmed_mapped_keys: 人工审核确认过（reviewed.parquet.decision 属于 take 集合）→ 始终 skip
    #   auto_mapped_keys: 仅自动归一，selected_slot 有值但未在 reviewed.take → 视 include_unconfirmed 决定
    confirmed_mapped_keys: set[tuple[str, str]] = set()
    auto_mapped_keys: set[tuple[str, str]] = set()
    try:
        if NORM_REVIEWED_PARQUET.exists():
            reviewed_df_all = pd.read_parquet(NORM_REVIEWED_PARQUET)
            reviewed_take = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot"}
            rev_sub = reviewed_df_all[
                (reviewed_df_all["vt_id"] == vt_id)
                & (reviewed_df_all["decision"].isin(reviewed_take))
            ]
            confirmed_mapped_keys = {
                (str(r["table_en"]), str(r["field_name"]))
                for _, r in rev_sub.iterrows()
            }
    except Exception as _e:
        print(f"[extend-slots] 读 reviewed 失败（跳过 '已确认' 过滤）: {_e}")
    try:
        norm_df_all = load_normalization_df()
        sub_norm = norm_df_all[
            (norm_df_all["vt_id"] == vt_id)
            & (norm_df_all["selected_slot"].notna())
            & (norm_df_all["selected_slot"].astype(str) != "")
        ]
        all_mapped = {
            (str(r["table_en"]), str(r["field_name"]))
            for _, r in sub_norm.iterrows()
        }
        # 自动映射 = 有 selected_slot 但不在 confirmed
        auto_mapped_keys = all_mapped - confirmed_mapped_keys
    except Exception as _e:
        print(f"[extend-slots] 读 field_normalization 失败（跳过 '已映射' 过滤）: {_e}")

    uncovered: list[dict] = []
    total_fields = 0
    covered_count = 0
    skipped_empty = 0
    skipped_noise = 0
    skipped_not_used = 0
    skipped_confirmed = 0       # 人工确认的映射
    skipped_auto_mapped = 0     # 自动归一且用户选了 include_unconfirmed=False
    included_unconfirmed = 0    # 自动归一、未确认、被纳入 LLM 候选
    bypassed_tables: list[str] = []  # 全表无 usage 历史 → 回退为"视为全已用"
    for t in candidate_tables:
        en = t.get("en", "")
        if not en:
            continue
        sub = ddl_df[ddl_df[col_en] == en]

        # 全表 usage 回退：这张表所有字段都不在 used_keys → 跳过 usage 过滤
        # （新表/无 SQL 历史时不能因为"未用"就一刀切过滤掉全表）
        table_bypass_used = False
        if used_keys:
            any_used = any((en, str(r2.get(col_field, "") or "")) in used_keys for _, r2 in sub.iterrows())
            if not any_used:
                table_bypass_used = True
                bypassed_tables.append(en)

        for _, r in sub.iterrows():
            fn = str(r.get(col_field, "") or "")
            cm = str(r.get(col_comment, "") or "")
            sample = str(r.get(col_sample, "") or "") if col_sample else ""
            total_fields += 1

            # 先过滤：空样例 + 技术字段
            if _empty_sample(sample):
                skipped_empty += 1
                continue
            is_noise, _reason = _is_noise(fn, cm, [sample], table_en=en)
            if is_noise:
                skipped_noise += 1
                continue

            # 只看 DDL 使用情况为"已用"的字段（usage_count > 0）；table_bypass_used 时跳过本过滤
            if used_keys and not table_bypass_used and (en, fn) not in used_keys:
                skipped_not_used += 1
                continue

            # 人工确认的映射：始终 skip（这是用户明确过的归属）
            if (en, fn) in confirmed_mapped_keys:
                skipped_confirmed += 1
                continue

            # 仅自动归一、未经人工确认：
            # - include_unconfirmed=True（默认）：纳入 LLM 候选，让用户借此重建槽位
            # - include_unconfirmed=False：跳过（信任自动结果）
            is_auto_only = (en, fn) in auto_mapped_keys
            if is_auto_only and not payload.include_unconfirmed:
                skipped_auto_mapped += 1
                continue

            if _is_covered(fn, cm):
                covered_count += 1
            else:
                if is_auto_only:
                    included_unconfirmed += 1
                uncovered.append({
                    "table_en": en,
                    "field_name": fn,
                    "comment": cm,
                    "sample": sample[:80],
                    "_auto_only": is_auto_only,
                })

    if not uncovered:
        return {
            "ok": True,
            "vt_id": vt_id,
            "new_slots": [],
            "summary": (
                f"过滤后无需补充：空样例 {skipped_empty}，技术字段 {skipped_noise}，"
                f"未使用字段 {skipped_not_used}，已人工确认 {skipped_confirmed}，"
                f"自动映射未确认跳过 {skipped_auto_mapped}，"
                f"剩余字段都已被现有槽位覆盖（{covered_count} 命中）"
            ),
            "warnings": [],
            "total_fields": total_fields,
            "covered_count": covered_count,
            "uncovered_count": 0,
            "skipped_empty": skipped_empty,
            "skipped_noise": skipped_noise,
            "skipped_not_used": skipped_not_used,
            "skipped_confirmed": skipped_confirmed,
            "skipped_auto_mapped": skipped_auto_mapped,
            "included_unconfirmed": included_unconfirmed,
            "include_unconfirmed_mode": payload.include_unconfirmed,
            "bypassed_tables": bypassed_tables,
            "elapsed_sec": 0,
        }

    # 构造 prompt：现有 slots（base + extended） + 未覆盖字段 → LLM 只补充新的 extended
    # 小 VT 模式：源表 ≤ 2 时，要求 LLM 为每个未覆盖字段都建 slot（不合并不丢）
    small_vt_mode = len(candidate_tables) <= 2
    base_awareness = """
**先查 base_slots**：给出的 base_slot_vocabulary 是已存在的全局槽位。若某未覆盖字段在语义上等同/接近 base 槽位
（如"手机号码"→phone_no、"身份证号码"→certificate_no），**优先复用 base**，输出时 source="base" + name 用 base 的 name；
只有 base 里确实没有对应概念时才新建 extended（source="extended"）。
"""
    if small_vt_mode:
        system_prompt = f"""你是数据治理专家。任务：基于已有槽位清单 + base_slot 词表，分析"未被覆盖的字段"，为**每一个**未覆盖字段补充槽位。

⚠️ 本 VT 源表数量很少（≤2），字段不多，必须每一个都覆盖：
{base_awareness}
1. **不要重复命名**：VT 现有槽位列表中出现过的 name/cn_name/aliases 概念一律不重复
2. **优先复用 base**（见上）；新建时 source="extended"，base 不能新增，只能引用
3. **禁止合并不同业务概念**：每个未覆盖字段都应该有一个独立的 slot（除非真正同义）
4. **不要跳过任何字段**：即使是"普通属性""静态描述"也要建 slot。只有字段确实是噪声（md5/uuid/ctime/bz 等已在黑名单 filtered 的）才可以放弃
5. 每个 extended 槽位必须给 cn_name + aliases（至少 3 个，覆盖常见中/英/拼音缩写）；base 复用只给 name

{SLOT_NAMING_GUARDRAILS}

输出严格 JSON："""
    else:
        system_prompt = f"""你是数据治理专家。任务：基于已有槽位清单 + base_slot 词表，分析"未被覆盖的字段"，判断如何覆盖它们。

原则：
{base_awareness}
1. **不要重复命名**：VT 现有槽位列表中出现过的 name/cn_name/aliases 概念一律不重复
2. **优先复用 base**（见上）；新建时 source="extended"
3. **合并同类**：多个语义相同的未覆盖字段只提一个 slot
4. **放弃无语义字段**：如果字段是噪声、技术字段（md5/uuid/ctime）、自由文本（备注/摘要），**不要**为它们建槽位
5. extended 槽位必须给 cn_name + aliases（至少 3 个）；base 复用只给 name

{SLOT_NAMING_GUARDRAILS}

输出严格 JSON："""

    existing_compact = "\n".join(
        f"- {s.get('name', '')} ({s.get('cn_name', '')}) · role={s.get('role', '')} · from={s.get('from', '')}"
        for s in current_slots
    ) or "（当前 VT 暂无槽位）"

    # 截断未覆盖字段
    MAX_UNCOVERED_IN_PROMPT = 60
    uncovered_sample = uncovered[:MAX_UNCOVERED_IN_PROMPT]
    uncovered_text = "\n".join(
        f"- `{u['field_name']}` ({u['comment'] or '无注释'}) | table={u['table_en']} | sample={u['sample'][:40]}"
        for u in uncovered_sample
    )

    # base_slot 词表：给 LLM 看可复用的 base（精简字段）
    base_vocab_items = [
        {
            "name": b.get("name"),
            "cn_name": b.get("cn_name"),
            "role": b.get("role"),
            "logical_type": b.get("logical_type"),
            "aliases": (b.get("aliases") or [])[:6],
        }
        for b in load_base_slots().get("base_slots", [])
    ]
    # 过滤掉 VT 里已经引用的 base（避免 LLM 又推一遍）
    already_base_names = {s.get("name") for s in current_slots if s.get("from") == "base"}
    base_vocab_available = [b for b in base_vocab_items if b["name"] not in already_base_names]
    base_vocab_text = "\n".join(
        f"- {b['name']} ({b['cn_name']}) · role={b['role']} · lt={b['logical_type']} · aliases={b['aliases']}"
        for b in base_vocab_available
    ) or "（无可用 base）"

    user_prompt = f"""## 虚拟表
- topic: {target_vt.get('topic', '')}
- table_type: {target_vt.get('table_type', '')}
- grain_desc: {target_vt.get('grain_desc', '')}

## 已有槽位（{len(current_slots)} 个，禁止重复）

{existing_compact}

## 可复用的 base 槽位词表（本 VT 尚未引用的 {len(base_vocab_available)} 条）

{base_vocab_text}

{(
    f"## 已被人工审核归属的字段（{len(reviewed_seed_fields)} 个，seed，禁止再为其新建槽位）\n\n"
    + "\n".join(f"- {fn}（{cm}）" if cm else f"- {fn}" for fn, cm in reviewed_seed_fields[:40])
    + (f"\n… 共 {len(reviewed_seed_fields)} 条，仅展示前 40\n" if len(reviewed_seed_fields) > 40 else "\n")
) if reviewed_seed_fields else ""}
## 未被覆盖的字段（{len(uncovered)}，下面展示前 {len(uncovered_sample)} 个）

{uncovered_text}

## 输出 JSON schema（严格）

```json
{{
  "new_slots": [
    {{
      "name": "snake_case_英文名（source=base 时必须是 base 词表中的 name）",
      "source": "base | extended",
      "from": "extended",  // 兼容老字段；实际以 source 为准
      "role": "subject|subject_id|relation_subject|display|time|location|filter|measure|source|description",
      "cn_name": "中文名（source=base 可省略，会从 base_slots 读取）",
      "logical_type": "text|code|datetime|amount|region_code|status_code|...（source=base 可省略）",
      "aliases": ["至少 3 个 中/英/拼音同义词（source=base 可省略）"],
      "applicable_table_types": ["主档","关系","事件","标签","聚合"],
      "covers_fields": ["field_name_1", "field_name_2"],
      "llm_reason": "为什么需要这个槽位（复用 base / 新建 extended 的理由，简短 1 句）"
    }}
  ],
  "skipped_reason": {{
    "field_name_x": "noise | free_text | technical_id | 已被现有槽位覆盖",
    ...
  }},
  "summary": "本轮扩展的思路 1-2 句话"
}}
```
"""

    t0 = _time.time()
    warnings: list[str] = []
    base_slot_names = {b["name"] for b in load_base_slots().get("base_slots", [])}
    try:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        parsed: dict[str, Any] = {}
        for attempt in range(2):
            raw = chat(
                messages=messages,
                temperature=0.0,
                json_mode=True,
            )
            parsed = json.loads(raw)
            name_issues = collect_slot_name_issues(
                parsed.get("new_slots", []) or [],
                source_key="source",
                base_slot_names=base_slot_names,
            )
            if not name_issues:
                break
            if attempt == 0:
                messages.extend([
                    {"role": "assistant", "content": json.dumps(parsed, ensure_ascii=False)},
                    {"role": "user", "content": format_naming_retry_feedback(name_issues)},
                ])
                continue
            warnings.extend([f"命名告警：{issue}" for issue in name_issues])
    except Exception as e:
        raise HTTPException(500, f"LLM 调用或 JSON 解析失败: {e}")
    elapsed = _time.time() - t0

    # 后处理：去除重名 + 校验 role/source；source=base 时从 base_slots 填充字段
    VALID_ROLES = {"subject", "subject_id", "relation_subject", "display", "time",
                   "location", "filter", "measure", "source", "description"}
    existing_names_lower = {s.get("name", "").lower() for s in current_slots}
    base_by_name_dict = {b["name"]: b for b in load_base_slots().get("base_slots", [])}
    # covers_fields（LLM 返 field_name 数组）→ covers_mapped_fields（完整 dict）
    # 注意：同一 field_name 可能出现在多表，都纳入（让 apply 时 slot 自动绑定多表映射）
    from collections import defaultdict as _dd_local
    uncovered_by_name: dict[str, list[dict]] = _dd_local(list)
    for u in uncovered:
        uncovered_by_name[u["field_name"]].append({
            "table_en": u["table_en"],
            "field_name": u["field_name"],
            "field_comment": u.get("comment", ""),
        })
    new_slots = []
    for ns in parsed.get("new_slots", []) or []:
        if not isinstance(ns, dict):
            continue
        name = str(ns.get("name", "")).strip()
        if not name:
            continue
        if name.lower() in existing_names_lower:
            warnings.append(f"LLM 返回的 '{name}' 与现有槽位重名，已丢弃")
            continue
        source = str(ns.get("source") or "").lower()
        # 兼容：source 为 base 但 name 不在词表 → 降级为 extended
        if source == "base" and name not in base_by_name_dict:
            warnings.append(f"'{name}' 标为 source=base 但不在 base_slots.yaml，降级为 extended")
            source = "extended"
        if source not in {"base", "extended"}:
            source = "extended"  # 默认
        covers = [str(f).strip() for f in (ns.get("covers_fields") or []) if str(f).strip()]
        covers_mapped: list[dict] = []
        for cf in covers:
            # 优先从 uncovered 查；找不到时至少留个 field_name（前端可显示）
            hits = uncovered_by_name.get(cf, [])
            if hits:
                covers_mapped.extend(hits)
            else:
                covers_mapped.append({"table_en": "", "field_name": cf, "field_comment": ""})

        if source == "base":
            base_ref = base_by_name_dict[name]
            role = ns.get("role") or base_ref.get("role")
            if role not in VALID_ROLES:
                role = base_ref.get("role", "display")
            new_slots.append({
                "name": name,
                "source": "base",
                "from": "base",
                "role": role,
                "cn_name": base_ref.get("cn_name", ""),
                "logical_type": base_ref.get("logical_type", "text"),
                "aliases": list(base_ref.get("aliases", []) or [])[:10],
                "applicable_table_types": base_ref.get("applicable_table_types") or ["主档", "关系", "事件"],
                "covers_fields": covers,
                "covers_mapped_fields": covers_mapped,
                "llm_reason": str(ns.get("llm_reason", "")).strip(),
            })
        else:
            if ns.get("role") not in VALID_ROLES:
                warnings.append(f"'{name}' 的 role={ns.get('role')} 非法，已丢弃")
                continue
            new_slots.append({
                "name": name,
                "source": "extended",
                "from": "extended",
                "role": ns["role"],
                "cn_name": str(ns.get("cn_name", "")).strip(),
                "logical_type": str(ns.get("logical_type", "text")).strip(),
                "aliases": [str(a).strip() for a in ns.get("aliases", []) if str(a).strip()][:10],
                "applicable_table_types": ns.get("applicable_table_types") or ["主档", "关系", "事件"],
                "covers_fields": covers,
                "covers_mapped_fields": covers_mapped,
                "llm_reason": str(ns.get("llm_reason", "")).strip(),
            })

    return {
        "ok": True,
        "vt_id": vt_id,
        "new_slots": new_slots,
        "skipped_reason": parsed.get("skipped_reason", {}),
        "summary": parsed.get("summary", ""),
        "warnings": warnings,
        "total_fields": total_fields,
        "covered_count": covered_count,
        "uncovered_count": len(uncovered),
        "uncovered_sample_shown": len(uncovered_sample),
        "skipped_empty": skipped_empty,
        "skipped_noise": skipped_noise,
        "skipped_not_used": skipped_not_used,
        "skipped_confirmed": skipped_confirmed,
        "skipped_auto_mapped": skipped_auto_mapped,
        "included_unconfirmed": included_unconfirmed,
        "include_unconfirmed_mode": payload.include_unconfirmed,
        "bypassed_tables": bypassed_tables,
        # 人工已审 seed 信息（让前端展示"参考了 N 条人工审核字段"）
        "reviewed_seed_count": len(reviewed_seed_fields),
        "reviewed_seed_sample": [
            {"field_name": fn, "comment": cm} for fn, cm in reviewed_seed_fields[:20]
        ],
        "elapsed_sec": round(elapsed, 2),
    }


class PromoteExtendedToBasePayload(BaseModel):
    name: str
    cn_name: str
    role: str
    logical_type: str = "text"
    description: str = ""
    aliases: list[str] = []
    applicable_table_types: list[str] = []


@app.post("/api/base-slots/promote-from-extended")
def promote_extended_candidate_to_base(payload: PromoteExtendedToBasePayload):
    """把扩展槽位 Modal 里的一个 extended 候选直接升级为 base_slots 条目。

    不动任何 VT slot_definitions；仅追加 base_slots.yaml + 记审计。用户之后可以手动引用它。
    如果需要同时把该 base 注入当前 VT（作为 from: base 引用），应用端由前端调 VT 更新接口。
    """
    name = payload.name.strip()
    if not name:
        raise HTTPException(400, "name 不能为空")

    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200

    base_path = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"
    with base_path.open(encoding="utf-8") as f:
        doc = ruamel.load(f)
    if "base_slots" not in doc:
        raise HTTPException(500, "base_slots.yaml 结构异常")

    existing_names = {s.get("name") for s in doc["base_slots"]}
    if name in existing_names:
        raise HTTPException(400, f"base_slots 已存在同名槽位: {name}")

    new_entry = {
        "name": name,
        "cn_name": payload.cn_name or name,
        "logical_type": payload.logical_type or "text",
        "role": payload.role,
        "description": payload.description or "",
        "aliases": list(payload.aliases or []),
        "sample_patterns": [],
        "applicable_table_types": list(payload.applicable_table_types or ["主档", "关系", "事件", "标签", "聚合"]),
    }
    doc["base_slots"].append(new_entry)
    with base_path.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)

    # 审计
    log_path = REPO_ROOT / "data" / "feedback" / "slot_proposals_log.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ts": datetime.utcnow().isoformat(),
            "action": "promote_extended_candidate_to_base",
            "name": name,
            "cn_name": payload.cn_name,
            "role": payload.role,
            "source": "vteditor_extend_modal",
        }, ensure_ascii=False) + "\n")

    _cache.clear()
    return {"ok": True, "name": name, "base_count": len(doc["base_slots"])}


@app.post("/api/virtual-tables/{vt_id}/generate-slots")
def generate_slots_for_single_vt(vt_id: str):
    """调 LLM 为单张 VT 生成槽位（不直接写回，由前端审查后再保存）。"""
    import sys
    import time as _time

    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    # 懒加载以避免启动期触发 LLM_client 初始化
    try:
        from scripts.generate_slot_definitions import (  # type: ignore
            generate_slots_for_vt,
            load_base_slots_compact,
            SCAFFOLD_JSON as GEN_SCAFFOLD_JSON,
            BASE_SLOTS_YAML as GEN_BASE_SLOTS_YAML,
            DDL_CSV as GEN_DDL_CSV,
        )
    except Exception as e:
        raise HTTPException(500, f"加载 generate_slot_definitions 失败: {e}")

    # 读 scaffold 找目标 VT（从最新 yaml，不走缓存）
    scaffold = _load_scaffold_yaml()
    target_vt = next((v for v in scaffold.get("virtual_tables", []) if v.get("vt_id") == vt_id), None)
    if target_vt is None:
        raise HTTPException(404, f"vt_id 不存在: {vt_id}")
    if not target_vt.get("candidate_tables"):
        raise HTTPException(400, "该 VT 还没有源表，无法生成槽位")

    try:
        with GEN_BASE_SLOTS_YAML.open(encoding="utf-8") as f:
            base_slots_data = yaml.safe_load(f)
        base_slots_compact = load_base_slots_compact(base_slots_data)
        ddl_df = pd.read_csv(GEN_DDL_CSV, encoding="utf-8")

        t0 = _time.time()
        result, warnings = generate_slots_for_vt(target_vt, base_slots_data, base_slots_compact, ddl_df)
        elapsed = _time.time() - t0
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"LLM 生成失败: {e}")

    return {
        "ok": True,
        "vt_id": vt_id,
        "slots": result.get("slots", []),
        "summary": result.get("summary", ""),
        "warnings": warnings or [],
        "elapsed_sec": round(elapsed, 2),
    }


# ==================== I-16 Gap5: 触发 pipeline 重跑 ====================

import subprocess as _subprocess
import threading as _threading
import uuid as _uuid

_pipeline_jobs: dict[str, dict] = {}
_pipeline_jobs_lock = _threading.Lock()


def _run_pipeline_job(job_id: str, from_step: str) -> None:
    """后台线程跑 run_pipeline.py --from <step>，把输出累积到 job。"""
    import sys as _sys
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    try:
        proc = _subprocess.Popen(
            [
                _sys.executable,  # 用 uvicorn 自己的 python（保证 pandas 等依赖一致）
                str(REPO_ROOT / "scripts" / "run_pipeline.py"),
                "--from", from_step,
            ],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=_subprocess.PIPE,
            stderr=_subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,  # 子进程脱离父进程组，uvicorn --reload 不会连带杀子进程
        )
        # 记录 pid，便于 _reap_stale_jobs 快速识别已死的 subprocess
        with _pipeline_jobs_lock:
            if job_id in _pipeline_jobs:
                _pipeline_jobs[job_id]["pid"] = proc.pid
        lines: list[str] = []
        current_step = ""
        for line in iter(proc.stdout.readline, ''):
            if line:
                lines.append(line.rstrip())
                # 粗略解析当前 step：行里带 "[N/11] step_name"
                import re as _re
                m = _re.match(r"\[(\d+)/(\d+)\]\s+(\S+)", line.strip())
                if m:
                    current_step = f"{m.group(3)} ({m.group(1)}/{m.group(2)})"
                elif "✅" in line or "❌" in line or "🎉" in line:
                    pass
                with _pipeline_jobs_lock:
                    _pipeline_jobs[job_id]["log_tail"] = lines[-30:]
                    _pipeline_jobs[job_id]["current_step"] = current_step
        proc.wait()
        with _pipeline_jobs_lock:
            _pipeline_jobs[job_id]["state"] = "done" if proc.returncode == 0 else "failed"
            _pipeline_jobs[job_id]["return_code"] = proc.returncode
            _pipeline_jobs[job_id]["ended_at"] = datetime.now().isoformat(timespec="seconds")
        # 子进程完成后，清缓存
        _cache.pop("scaffold", None)
        _cache.pop("norm_df", None)
    except Exception as e:
        with _pipeline_jobs_lock:
            _pipeline_jobs[job_id]["state"] = "failed"
            _pipeline_jobs[job_id]["error"] = str(e)


class PipelineRunPayload(BaseModel):
    from_step: str = "slot_scores"  # slot_scores | field_normalization | evaluation | ...


@app.post("/api/pipeline/jobs")
def start_pipeline_job(payload: PipelineRunPayload):
    """启动一个 pipeline 重跑（异步），立即返回 job_id。"""
    valid_steps = {
        "scaffold_rule", "scaffold_llm", "scaffold_final", "slot_definitions",
        "field_features", "slot_scores", "field_normalization",
        "virtual_fields", "virtual_field_mappings", "query_intents", "evaluation",
    }
    if payload.from_step not in valid_steps:
        raise HTTPException(400, f"未知 from_step: {payload.from_step}")

    # 检查是否已有 running 任务
    with _pipeline_jobs_lock:
        for jid, j in _pipeline_jobs.items():
            if j.get("state") == "running":
                raise HTTPException(409, f"已有 pipeline 任务在跑: {jid}（--from {j.get('from_step')}），请等它结束")

    job_id = "job_" + _uuid.uuid4().hex[:10]
    with _pipeline_jobs_lock:
        _pipeline_jobs[job_id] = {
            "job_id": job_id,
            "from_step": payload.from_step,
            "state": "running",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "current_step": "",
            "log_tail": [],
            "return_code": None,
        }
    t = _threading.Thread(target=_run_pipeline_job, args=(job_id, payload.from_step), daemon=True)
    t.start()
    return {"ok": True, "job_id": job_id, "from_step": payload.from_step}


def _reap_stale_jobs() -> None:
    """识别已死的 pipeline job：优先用 PID 检测（秒级），PID 缺失时 fall back 到 5 分钟无日志检测。"""
    import os as _os
    now = datetime.now()
    with _pipeline_jobs_lock:
        for jid, j in _pipeline_jobs.items():
            if j.get("state") != "running":
                continue
            # PID 检测：subprocess 真实存活状态
            pid = j.get("pid")
            if pid:
                try:
                    _os.kill(pid, 0)  # signal 0 仅检测，不杀
                    continue  # 还活着
                except ProcessLookupError:
                    j["state"] = "failed"
                    j["error"] = "subprocess 已消失（可能被 uvicorn reload 或外部 kill）"
                    j["ended_at"] = now.isoformat(timespec="seconds")
                    continue
                except PermissionError:
                    continue  # 无权访问也算存活
            # fall back：无 pid（老 job）→ 5 分钟无日志判死
            try:
                started = datetime.fromisoformat(j.get("started_at", ""))
            except Exception:
                continue
            if (now - started).total_seconds() > 300 and not j.get("log_tail"):
                j["state"] = "failed"
                j["error"] = "subprocess 无日志输出超 5 分钟，判死"
                j["ended_at"] = now.isoformat(timespec="seconds")


@app.get("/api/pipeline/jobs/{job_id}")
def get_pipeline_job(job_id: str):
    _reap_stale_jobs()
    with _pipeline_jobs_lock:
        job = _pipeline_jobs.get(job_id)
    if not job:
        raise HTTPException(404, f"job_id 不存在: {job_id}")
    return job


# P1-8: Pipeline 浮窗预估剩余时间 —— 每步平均耗时（来自 scripts/run_pipeline.py 的 STEPS）
_STEP_ESTIMATES = {
    "scaffold_rule": 30,
    "scaffold_llm": 840,
    "scaffold_final": 10,
    "slot_definitions": 480,
    "field_features": 60,
    "slot_scores": 180,
    "field_normalization": 150,
    "virtual_fields": 30,
    "virtual_field_mappings": 60,
    "query_intents": 120,
    "evaluation": 60,
}
_STEP_ORDER = [
    "scaffold_rule", "scaffold_llm", "scaffold_final", "slot_definitions",
    "field_features", "slot_scores", "field_normalization",
    "virtual_fields", "virtual_field_mappings", "query_intents", "evaluation",
]


def _compute_eta(job: dict) -> int | None:
    """根据 job.current_step 和 started_at 估算剩余秒数。"""
    if job.get("state") != "running":
        return None
    cur = job.get("current_step") or ""
    # current_step 格式: "field_normalization (7/11)"
    import re as _re
    m = _re.match(r"(\S+)\s+\((\d+)/(\d+)\)", cur)
    if not m:
        # 刚启动，还没解析出当前步
        from_step = job.get("from_step") or "slot_scores"
        if from_step not in _STEP_ORDER:
            return None
        remaining_steps = _STEP_ORDER[_STEP_ORDER.index(from_step):]
        return int(sum(_STEP_ESTIMATES.get(s, 60) for s in remaining_steps))
    cur_name = m.group(1)
    if cur_name not in _STEP_ORDER:
        return None
    # 已经走过的步骤（含当前）= started_at 到 now
    try:
        started = datetime.fromisoformat(job["started_at"])
        elapsed = (datetime.now() - started).total_seconds()
    except Exception:
        return None
    cur_idx = _STEP_ORDER.index(cur_name)
    # from_step 决定起始
    from_step = job.get("from_step") or "slot_scores"
    from_idx = _STEP_ORDER.index(from_step) if from_step in _STEP_ORDER else cur_idx
    # 已完成步骤的累计预估
    done_est = sum(_STEP_ESTIMATES.get(s, 60) for s in _STEP_ORDER[from_idx:cur_idx])
    # 剩余 = 当前步 + 后续步
    remaining_est = sum(_STEP_ESTIMATES.get(s, 60) for s in _STEP_ORDER[cur_idx:])
    # 用实际已耗时校准剩余（若已超预估，说明偏慢，放大剩余）
    if done_est > 0 and elapsed > 0:
        ratio = elapsed / done_est if done_est > 0 else 1.0
        ratio = max(0.5, min(2.0, ratio))  # 校准范围 0.5x - 2x，避免极端
        # 但只校准后续未开始步，当前步不校准（否则会高估）
        next_est = sum(_STEP_ESTIMATES.get(s, 60) for s in _STEP_ORDER[cur_idx + 1:])
        cur_step_est = _STEP_ESTIMATES.get(cur_name, 60)
        # 当前步已耗时 ~= elapsed - done_est；剩余 = cur_step_est - (elapsed - done_est)（下限 0）
        cur_remaining = max(0, cur_step_est - (elapsed - done_est))
        return int(cur_remaining + next_est * ratio)
    return int(remaining_est)


@app.get("/api/pipeline/jobs")
def list_pipeline_jobs():
    _reap_stale_jobs()
    with _pipeline_jobs_lock:
        jobs = [dict(j) for j in _pipeline_jobs.values()]
    for j in jobs:
        j["eta_sec"] = _compute_eta(j)
    return jobs


@app.get("/api/pipeline/dirty-check")
def pipeline_dirty_check():
    """检查多种 source 是否比下游产物新，自动判断该从哪一步开始重跑。

    检测：
    - field_blacklist.yaml > field_features.parquet → 重跑 field_features
    - slot_definitions.yaml > field_normalization.parquet → 重跑 slot_scores

    若多个 source 同时 dirty，取最早的 step（依赖链：field_features < slot_scores）。
    """
    if not FEATURES_PARQUET.exists():
        return {
            "dirty": True,
            "dirty_sources": ["field_features_missing"],
            "from_step": "field_features",
        }
    # (source 文件, 下游产物, 标签, 推荐 step, step 依赖顺序)
    checks = [
        (FIELD_BLACKLIST_YAML, FEATURES_PARQUET, "field_blacklist", "field_features", 1),
        (SLOT_YAML, NORM_PARQUET, "slot_definitions", "slot_scores", 2),
    ]
    dirty_sources: list[str] = []
    earliest_step: str | None = None
    earliest_order = 999
    for src_path, ds_path, label, step, order in checks:
        if not src_path.exists():
            continue
        if not ds_path.exists() or src_path.stat().st_mtime > ds_path.stat().st_mtime:
            dirty_sources.append(label)
            if order < earliest_order:
                earliest_order = order
                earliest_step = step
    return {
        "dirty": bool(dirty_sources),
        "dirty_sources": dirty_sources,
        "from_step": earliest_step or "field_features",
    }


@app.delete("/api/pipeline/jobs/{job_id}")
def delete_pipeline_job(job_id: str):
    """清除一个 job（用于清僵尸状态）。不会强行杀子进程，只改内存状态。"""
    with _pipeline_jobs_lock:
        if job_id not in _pipeline_jobs:
            raise HTTPException(404, f"job_id 不存在: {job_id}")
        del _pipeline_jobs[job_id]
    return {"ok": True, "deleted": job_id}


def _run_per_vt_normalization(job_id: str, vt_id: str) -> None:
    """后台线程跑单 VT 归一（slot_scorer + decision_engine），merge 到 parquet。"""
    import sys as _sys
    import time as _time
    try:
        repo_root = str(REPO_ROOT)
        if repo_root not in _sys.path:
            _sys.path.insert(0, repo_root)
        # 懒加载
        from src.pipeline.slot_scorer import main as slot_main  # type: ignore
        from src.pipeline.decision_engine import main as dec_main  # type: ignore

        lines: list[str] = []
        def _log(s: str):
            lines.append(s)
            with _pipeline_jobs_lock:
                _pipeline_jobs[job_id]["log_tail"] = lines[-30:]

        # 1) 重新打分
        _log(f"[1/2] slot_scores for {vt_id}")
        with _pipeline_jobs_lock:
            _pipeline_jobs[job_id]["current_step"] = "slot_scores (1/2)"
        t0 = _time.time()
        slot_main(limit_vt_id=vt_id, enable_embedding=True)
        _log(f"  ✅ slot_scores 完成（{_time.time() - t0:.1f}s）")

        # 2) 重新归一决策
        _log(f"[2/2] field_normalization for {vt_id}")
        with _pipeline_jobs_lock:
            _pipeline_jobs[job_id]["current_step"] = "field_normalization (2/2)"
        t1 = _time.time()
        dec_main(enable_llm=True, limit_vt_id=vt_id)
        _log(f"  ✅ field_normalization 完成（{_time.time() - t1:.1f}s）")

        with _pipeline_jobs_lock:
            _pipeline_jobs[job_id]["state"] = "done"
            _pipeline_jobs[job_id]["ended_at"] = datetime.now().isoformat(timespec="seconds")
            _pipeline_jobs[job_id]["return_code"] = 0
        _cache.pop("scaffold", None)
        _cache.pop("norm_df", None)
    except Exception as e:
        import traceback as _tb
        with _pipeline_jobs_lock:
            _pipeline_jobs[job_id]["state"] = "failed"
            _pipeline_jobs[job_id]["error"] = f"{e}\n{_tb.format_exc()[-500:]}"
            _pipeline_jobs[job_id]["ended_at"] = datetime.now().isoformat(timespec="seconds")


@app.post("/api/virtual-tables/{vt_id}/rerun-normalization")
def rerun_per_vt_normalization(vt_id: str):
    """异步：只针对单 VT 重跑 slot_scores + field_normalization，merge 回 parquet。"""
    # 校验 VT 存在
    scaffold = _load_scaffold_yaml()
    if not any(v.get("vt_id") == vt_id for v in scaffold.get("virtual_tables", []) or []):
        raise HTTPException(404, f"vt_id 不存在: {vt_id}")

    with _pipeline_jobs_lock:
        for jid, j in _pipeline_jobs.items():
            if j.get("state") == "running":
                raise HTTPException(409, f"已有任务在跑: {jid}")

    job_id = "job_" + _uuid.uuid4().hex[:10]
    with _pipeline_jobs_lock:
        _pipeline_jobs[job_id] = {
            "job_id": job_id,
            "from_step": f"per_vt:{vt_id}",
            "state": "running",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "current_step": "",
            "log_tail": [],
            "return_code": None,
        }
    t = _threading.Thread(target=_run_per_vt_normalization, args=(job_id, vt_id), daemon=True)
    t.start()
    return {"ok": True, "job_id": job_id, "vt_id": vt_id}


@app.get("/api/tables/{table_en}/fields")
def get_table_fields(table_en: str, limit: int = 200):
    """某张物理表的字段列表（DDL + 样例值）。"""
    ddl_df = load_ddl_df()
    col_en = None
    for c in ["table", "table_en", "origin_table"]:
        if c in ddl_df.columns:
            col_en = c
            break
    if col_en is None:
        raise HTTPException(500, "DDL CSV 缺少表名列")
    sub = ddl_df[ddl_df[col_en] == table_en]
    if sub.empty:
        return {"table_en": table_en, "field_count": 0, "fields": []}

    # 字段名列、类型列、注释列、样例列（做兼容）
    col_field = "field" if "field" in sub.columns else "field_name"
    col_type = "type" if "type" in sub.columns else "data_type"
    col_comment = "comment" if "comment" in sub.columns else "field_comment"
    col_sample = "sample_data" if "sample_data" in sub.columns else None

    sub = sub.head(limit).copy()
    sub = sub.astype(object).where(pd.notna(sub), None)

    fields = []
    for _, r in sub.iterrows():
        fields.append({
            "field": str(r.get(col_field, "") or ""),
            "type": str(r.get(col_type, "") or ""),
            "comment": str(r.get(col_comment, "") or ""),
            "sample_data": (str(r.get(col_sample, "") or "")[:200]) if col_sample else "",
        })
    return {"table_en": table_en, "field_count": int(len(ddl_df[ddl_df[col_en] == table_en])), "fields": fields}


@app.get("/api/scaffold/edit-log")
def scaffold_edit_log(limit: int = 100):
    if not SCAFFOLD_EDIT_LOG.exists():
        return []
    lines = SCAFFOLD_EDIT_LOG.read_text(encoding="utf-8").strip().splitlines()
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue
    return list(reversed(entries))


@app.get("/api/vt-merge/log")
def vt_merge_log(limit: int = 100):
    if not VT_MERGE_LOG.exists():
        return []
    lines = VT_MERGE_LOG.read_text(encoding="utf-8").strip().splitlines()
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue
    return list(reversed(entries))


@app.get("/api/normalization/field-mapping")
def field_mapping(vt_id: str, table_en: str | None = None):
    """按 vt_id (+ optional table_en) 返回归一结果，供 VTEditor 升级用。"""
    df = load_normalization_df()
    df = df[df["vt_id"] == vt_id]
    if table_en:
        df = df[df["table_en"] == table_en]

    if df.empty:
        return []

    def _sf(v, default: float | None = 0.0) -> float | None:
        if v is None:
            return default
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return default
        return default if fv != fv else fv  # NaN check

    return [
        {
            "table_en": r["table_en"],
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment", ""),
            "selected_slot": r["selected_slot"],
            "selected_score": _sf(r["selected_score"]),
            "review_status": r["review_status"],
            "top1_slot": r.get("top1_slot"),
            "top1_score": _sf(r.get("top1_score")),
            "top2_slot": r.get("top2_slot"),
            "top2_score": _sf(r.get("top2_score"), default=None),
            "conflict_types": list(r["conflict_types"]) if r.get("conflict_types") is not None else [],
            "applied_llm": bool(r.get("applied_llm", False)),
            "llm_suggested_slot": r.get("llm_suggested_slot"),
            "decision": r.get("decision"),
            "decision_slot": r.get("decision_slot"),
        }
        for _, r in df.iterrows()
    ]


# ========== W5-0: 命名诊断 + 同名异义消歧 ==========

NAMING_DIAGNOSIS_YAML = REPO_ROOT / "output" / "naming_diagnosis.yaml"
HOMONYM_PROPOSALS_YAML = REPO_ROOT / "output" / "homonym_proposals.yaml"


@app.get("/api/naming/diagnosis")
def get_naming_diagnosis():
    """返回槽位命名诊断（W5-0 前置报告）。如果不存在返回 empty 结构。"""
    if not NAMING_DIAGNOSIS_YAML.exists():
        return {"exists": False, "summary": None}
    with NAMING_DIAGNOSIS_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {"exists": True, **data}


@app.post("/api/naming/diagnosis/regenerate")
def regenerate_naming_diagnosis():
    """同步跑一次诊断脚本。"""
    import subprocess
    import sys as _sys
    result = subprocess.run(
        [_sys.executable, "-m", "src.alignment.diagnose"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(500, f"诊断脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


@app.get("/api/naming/homonyms")
def get_homonym_proposals():
    if not HOMONYM_PROPOSALS_YAML.exists():
        return {"exists": False, "proposals": [], "summary": None}
    with HOMONYM_PROPOSALS_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {"exists": True, **data}


@app.post("/api/naming/homonyms/regenerate")
def regenerate_homonym_proposals():
    """同步跑一次同名异义 LLM 判断（受缓存影响，无新候选时仅命中缓存）"""
    import subprocess
    import sys as _sys
    result = subprocess.run(
        [_sys.executable, "-m", "src.alignment.homonyms"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(500, f"LLM 判断脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


class HomonymApplyPayload(BaseModel):
    proposal_name: str  # 对应 homonym_proposals.yaml 中的 name
    reviewer: str = "user"
    reason: str = ""
    # 可选：若传入则使用客户侧编辑的 member_proposals 覆盖 LLM 结果（允许用户修改建议名）
    overrides: list[dict[str, Any]] | None = None


@app.post("/api/naming/homonyms/apply")
def apply_homonym_proposal(payload: HomonymApplyPayload):
    """对一个同名异义提议执行 cascade rename。"""
    if not HOMONYM_PROPOSALS_YAML.exists():
        raise HTTPException(404, "homonym_proposals.yaml 不存在")
    with HOMONYM_PROPOSALS_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    found = next((p for p in data.get("proposals", []) if p["name"] == payload.proposal_name), None)
    if found is None:
        raise HTTPException(404, f"proposal {payload.proposal_name} 不在列表里")

    members = payload.overrides if payload.overrides is not None else found["member_proposals"]

    from src.alignment.cascade import RenameOp, apply_renames

    renames = [
        RenameOp(
            vt_id=m["vt_id"],
            before_name=m["before_name"],
            after_name=m["after_name"],
            new_cn_name=m.get("new_cn_name"),
        )
        for m in members
        if m.get("after_name") and m["after_name"] != m["before_name"]
    ]
    if not renames:
        return {"ok": True, "skipped": True, "reason": "无需 rename（全部 before==after）"}

    result = apply_renames(
        renames,
        scope="homonym",
        scope_key=payload.proposal_name,
        reviewer=payload.reviewer,
        reason=payload.reason or f"homonym resolution for {payload.proposal_name}",
    )
    _cache.clear()
    return {
        "ok": True,
        "version": result.version,
        "affected_slots": result.affected_slots,
        "affected_norm_rows": result.affected_norm_rows,
        "affected_reviewed_rows": result.affected_reviewed_rows,
        "snapshot_path": str(result.snapshot_path) if result.snapshot_path else None,
    }


@app.get("/api/alignment/log")
def get_alignment_log(limit: int = 100):
    from src.alignment.cascade import ALIGNMENT_LOG
    if not ALIGNMENT_LOG.exists():
        return {"exists": False, "rows": []}
    df = pd.read_parquet(ALIGNMENT_LOG)
    df = df.sort_values("ts", ascending=False).head(limit)
    return {
        "exists": True,
        "total": int(df.shape[0]),
        "rows": df.to_dict(orient="records"),
    }


class RevertPayload(BaseModel):
    target_version: int
    reviewer: str = "user"


@app.post("/api/alignment/revert")
def revert_alignment(payload: RevertPayload):
    from src.alignment.cascade import revert_to_version
    revert_to_version(payload.target_version, reviewer=payload.reviewer)
    _cache.clear()
    return {"ok": True}


# ========== W5-D: benchmark 归因 ==========

BENCHMARK_ATTRIBUTION_PARQUET = REPO_ROOT / "output" / "evaluation_attribution.parquet"


EVALUATION_JSON = REPO_ROOT / "output" / "evaluation.json"


EVAL_DETAILS_PARQUET = REPO_ROOT / "output" / "evaluation_details.parquet"


@app.get("/api/benchmark/channel-topk")
def get_benchmark_channel_topk(topk: int = 5):
    """每条 query × 每个通道的 topK VT 召回结果，用于前端"通道对比" tab。

    topk: 1 / 3 / 5 / 10（默认 5）
    返回：
      queries: [
        {
          query_text, expected_tables, expected_vt_ids, key_fields_in_query,
          channels: { channel_name: { top_vts: [{vt_id, topic}], hit, recall } },
          best_recall: 0..1  # 跨通道最高 recall（用于排序）
        }
      ]
      channels: ["embedding","fusion","rerank", ...]  # 有数据的 channel 列表
    """
    if not EVAL_DETAILS_PARQUET.exists():
        return {"exists": False, "queries": [], "channels": []}

    df = pd.read_parquet(EVAL_DETAILS_PARQUET)
    if df.empty:
        return {"exists": False, "queries": [], "channels": []}

    valid_k = {1, 3, 5, 10}
    k = topk if topk in valid_k else 5
    vts_col = f"topK_{k}_vts"
    hit_col = f"topK_{k}_topic_hit"
    recall_col = f"topK_{k}_table_recall"

    # vt_id → topic 映射
    try:
        slot_data = load_slot_data()
        vt_topic_map = {v["vt_id"]: v.get("topic", "") for v in slot_data.get("virtual_tables", []) or []}
    except Exception:
        vt_topic_map = {}

    # expected_tables → expected_vt_ids 反查（通过 mapped_fields）
    table_to_vts: dict[str, set] = {}
    for v in (slot_data.get("virtual_tables", []) or []):
        for s in v.get("slots", []) or []:
            for mf in s.get("mapped_fields", []) or []:
                t = mf.get("table_en")
                if t:
                    table_to_vts.setdefault(t, set()).add(v["vt_id"])

    channel_order = ["embedding", "fusion", "rerank", "intent", "tfidf"]
    channels_present = [c for c in channel_order if c in set(df["channel"].unique())]

    # 去重：同一 (query, channel) 在多切片可能多行，keep first
    df_dedup = df.drop_duplicates(subset=["query_text", "channel"], keep="first")

    # 按 query 聚合
    out_queries: list[dict] = []
    for q, grp in df_dedup.groupby("query_text"):
        first = grp.iloc[0]
        expected_tables = [t for t in str(first.get("expected_tables") or "").split(",") if t]
        expected_vt_ids: set[str] = set()
        for t in expected_tables:
            expected_vt_ids |= table_to_vts.get(t, set())

        channels_data: dict[str, dict] = {}
        best_recall = 0.0
        for ch in channels_present:
            row = grp[grp["channel"] == ch]
            if row.empty:
                continue
            r = row.iloc[0]
            vts_raw = str(r.get(vts_col) or "")
            top_vts_ids = [v for v in vts_raw.split(",") if v]
            top_vts = [
                {"vt_id": v, "topic": vt_topic_map.get(v, ""), "is_expected": v in expected_vt_ids}
                for v in top_vts_ids[:k]
            ]
            hit = bool(r.get(hit_col))
            rec = float(r.get(recall_col) or 0)
            channels_data[ch] = {
                "top_vts": top_vts,
                "hit": hit,
                "recall": round(rec, 4),
            }
            if rec > best_recall:
                best_recall = rec

        out_queries.append({
            "query_text": q,
            "expected_tables": expected_tables,
            "expected_vt_ids": sorted(expected_vt_ids),
            "n_expected": len(expected_tables),
            "channels": channels_data,
            "best_recall": round(best_recall, 4),
        })

    # 按 best_recall 升序（最糟的排最前，方便排查）
    out_queries.sort(key=lambda x: (x["best_recall"], x["query_text"]))

    return {
        "exists": True,
        "topk": k,
        "channels": channels_present,
        "queries": out_queries,
    }


@app.get("/api/benchmark/metrics")
def get_benchmark_metrics():
    """返回 evaluation.json 内容，按 benchmark 切片分组（default / all / csv / csv_flag1 等）。

    evaluation.json 的 key 格式：
      - <channel>          → 默认切片（22 条 json）
      - <channel>__all     → 82 条合并切片
      - <channel>__csv     → 60 条 csv
      - <channel>__csv_flag1 → 10 条 flag=1 的高优先级
    """
    if not EVALUATION_JSON.exists():
        return {"exists": False, "slices": {}}
    data = json.loads(EVALUATION_JSON.read_text(encoding="utf-8"))
    by = data.get("by_channel") or {}
    # 按切片重组：slice_key → { channel → metrics }
    SLICE_LABELS = {
        "": {"key": "default", "label": "default (json 22)", "order": 2},
        "__all": {"key": "all", "label": "all (82)", "order": 1},
        "__csv": {"key": "csv", "label": "csv (60)", "order": 3},
        "__csv_flag1": {"key": "csv_flag1", "label": "csv_flag1 (10)", "order": 4},
    }
    slices: dict = {}
    for key, payload in by.items():
        suffix = ""
        channel = key
        for s in ("__csv_flag1", "__csv", "__all"):
            if key.endswith(s):
                suffix = s
                channel = key[: -len(s)]
                break
        meta = SLICE_LABELS.get(suffix, {"key": suffix.strip("_"), "label": suffix, "order": 99})
        slc = slices.setdefault(meta["key"], {
            "slice_key": meta["key"],
            "slice_label": meta["label"],
            "order": meta["order"],
            "benchmark_count": payload.get("benchmark_count"),
            "channels": {},
        })
        slc["channels"][channel] = payload.get("by_topk") or {}
    # 返回按 order 排序的列表
    ordered = sorted(slices.values(), key=lambda s: s["order"])
    return {"exists": True, "slices": ordered}


def _load_benchmark_golden_map() -> dict[str, dict]:
    """读 benchmark 原始数据，构造 query_text → {sql, tables[]} 映射。
    用于 attribution 展开行里显示"正确答案"。
    """
    root = REPO_ROOT / "data" / "benchmark"
    out: dict[str, dict] = {}
    # json（22 条）
    jp = root / "query_with_table_1.json"
    if jp.exists():
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
            for e in data or []:
                q = str(e.get("query") or "").strip()
                if not q:
                    continue
                out[q] = {
                    "sql": str(e.get("sql") or ""),
                    "tables": list(e.get("tables") or []),
                }
        except Exception:
            pass
    # csv（60 条）
    cp = root / "query_sql.csv"
    if cp.exists():
        try:
            csv_df = pd.read_csv(cp)
            for _, r in csv_df.iterrows():
                q = str(r.get("query") or "").strip()
                if not q:
                    continue
                tbls_raw = str(r.get("tables") or "").strip()
                # tables 字段在 csv 里有时逗号分隔、有时单个
                tbls = [t.strip() for t in tbls_raw.replace(";", ",").split(",") if t.strip()]
                out[q] = {
                    "sql": str(r.get("sql") or ""),
                    "tables": tbls,
                }
        except Exception:
            pass
    return out


def _load_table_cn_map() -> dict[str, str]:
    """table_en → table_cn_name 映射（取自 DDL）。失败返回空 dict，不阻断。"""
    try:
        ddl = pd.read_csv(DDL_CSV, encoding="utf-8", usecols=["table", "table_cn_name"])
        return dict(zip(ddl["table"], ddl["table_cn_name"].fillna("")))
    except Exception:
        return {}


@app.get("/api/benchmark/attribution")
def get_benchmark_attribution(only_failed: bool = True, limit: int = 500, channel: str | None = None, mode: str = "fail"):
    """mode:
    - "fail"（默认，等价 only_failed=True）：只返回 top5 未命中行
    - "hit"：只返回 top5 命中行
    - "all"：全部返回
    only_failed 参数保留向后兼容，当 mode 显式传时以 mode 为准。
    """
    if not BENCHMARK_ATTRIBUTION_PARQUET.exists():
        return {"exists": False, "rows": [], "summary": None}
    full = pd.read_parquet(BENCHMARK_ATTRIBUTION_PARQUET)
    df = full.copy()
    if channel:
        df = df[df["channel"] == channel]
    if mode == "hit":
        df = df[df["top5_hit"] == True]  # noqa: E712
    elif mode == "all":
        pass
    else:  # default "fail"
        if only_failed:
            df = df[df["top5_hit"] == False]  # noqa: E712
    # 解析 json 列
    for col in ("expected_vt_ids", "required_keywords", "missing_keywords",
                "unmapped_keywords", "hit_keywords", "suggested_slot_names"):
        if col in df.columns:
            df[col] = df[col].apply(lambda s: json.loads(s) if isinstance(s, str) else s)

    golden_map = _load_benchmark_golden_map()
    cn_map = _load_table_cn_map()

    failed_full = full[full["top5_hit"] == False]  # noqa: E712
    by_type = failed_full["failure_type"].value_counts().to_dict() if not failed_full.empty else {}

    # 按 channel 汇总命中 / 失败 / hit rate / recall 均值
    channels: dict[str, dict] = {}
    if "channel" in full.columns:
        for ch, grp in full.groupby("channel"):
            hits = int(grp["top5_hit"].sum()) if "top5_hit" in grp.columns else 0
            rows_n = int(len(grp))
            recall_mean = float(grp["top5_recall"].mean()) if "top5_recall" in grp.columns and rows_n else 0.0
            channels[str(ch)] = {
                "rows": rows_n,
                "top5_hits": hits,
                "failed": rows_n - hits,
                "top5_hit_rate": round(hits / rows_n, 4) if rows_n else 0.0,
                "top5_recall": round(recall_mean, 4),
                "unique_queries": int(grp["query_text"].nunique()) if "query_text" in grp.columns else rows_n,
            }
    # 主通道 = top5_recall 最高者（recall 比 hit_rate 更有区分度；只在 rerank / embedding / fusion 里选，tfidf/intent 基线不作主）
    primary_channel = None
    candidates = [c for c in ("rerank", "embedding", "fusion") if c in channels]
    if candidates:
        primary_channel = max(candidates, key=lambda c: channels[c]["top5_recall"])

    # Top VT 缺槽位聚合（基于所有失败行）
    from collections import defaultdict as _dd
    vt_cnt: dict[str, int] = _dd(int)
    for _, r in failed_full.iterrows():
        try:
            for vt in json.loads(r["expected_vt_ids"]):
                kws = json.loads(r["missing_keywords"])
                vt_cnt[vt] += len(kws)
        except Exception:
            continue
    top_vts = sorted(vt_cnt.items(), key=lambda x: -x[1])[:20]

    rows = df.head(limit).to_dict(orient="records")
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, (pd.Timestamp,)):
                r[k] = v.isoformat()
        # 注入"正确答案"：golden SQL + expected tables 带中文名
        q = str(r.get("query_text") or "")
        golden = golden_map.get(q) or {}
        r["golden_sql"] = golden.get("sql") or ""
        # attribution 里的 expected_tables 是逗号分隔字符串；golden_map 里是 list
        exp_tables = golden.get("tables") or []
        if not exp_tables:
            raw = str(r.get("expected_tables") or "")
            exp_tables = [t.strip() for t in raw.split(",") if t.strip()]
        r["expected_tables_detail"] = [
            {"table_en": t, "table_cn": cn_map.get(t, "")} for t in exp_tables
        ]

    return {
        "exists": True,
        "summary": {
            "unique_queries": int(full["query_text"].nunique()) if "query_text" in full.columns else int(len(full)),
            "total_rows": int(len(full)),
            "top5_failed_rows": int(len(failed_full)),
            "channels": channels,
            "primary_channel": primary_channel,
            "failure_type_counts": {k: int(v) for k, v in by_type.items()},
            "top_vts_missing": [{"vt_id": v, "missing_keyword_count": c} for v, c in top_vts],
        },
        "rows": rows,
    }


@app.post("/api/benchmark/attribution/regenerate")
def regenerate_benchmark_attribution():
    import subprocess
    import sys as _sys
    result = subprocess.run(
        [_sys.executable, "-m", "src.alignment.attribution"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(500, f"归因脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


# ========== W5-A: L2 槽位对齐器 ==========

L2_ALIGNMENT_YAML = REPO_ROOT / "output" / "l2_alignment_proposals.yaml"


@app.get("/api/alignment/l2")
def get_l2_alignment_proposals():
    if not L2_ALIGNMENT_YAML.exists():
        return {"exists": False, "proposals": [], "summary": None}
    with L2_ALIGNMENT_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {"exists": True, **data}


class L2RegeneratePayload(BaseModel):
    only_l2: str | None = None
    threshold: float = 0.18


@app.post("/api/alignment/l2/regenerate")
def regenerate_l2_alignment(payload: L2RegeneratePayload):
    import subprocess
    import sys as _sys
    cmd = [_sys.executable, "-m", "src.alignment.l2_align",
           "--threshold", str(payload.threshold)]
    if payload.only_l2:
        cmd.extend(["--only-l2", payload.only_l2])
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise HTTPException(500, f"L2 对齐脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


class L2ApplyPayload(BaseModel):
    l1: str
    l2: str
    cluster_id: int
    # 允许 UI 覆盖 rename_plan（比如用户拆 cluster）
    rename_plan_override: list[dict[str, Any]] | None = None
    reviewer: str = "user"
    reason: str = ""


@app.post("/api/alignment/l2/apply")
def apply_l2_cluster(payload: L2ApplyPayload):
    if not L2_ALIGNMENT_YAML.exists():
        raise HTTPException(404, "l2_alignment_proposals.yaml 不存在")
    with L2_ALIGNMENT_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    # 查 cluster
    target_p = next((p for p in data.get("proposals", []) if p["l1"] == payload.l1 and p["l2"] == payload.l2), None)
    if target_p is None:
        raise HTTPException(404, f"proposal 不存在: {payload.l1}/{payload.l2}")
    target_c = next((c for c in target_p["clusters"] if c["cluster_id"] == payload.cluster_id), None)
    if target_c is None:
        raise HTTPException(404, f"cluster {payload.cluster_id} 不存在")

    plan = payload.rename_plan_override if payload.rename_plan_override is not None else target_c.get("rename_plan", [])

    from src.alignment.cascade import RenameOp, apply_renames

    renames = [
        RenameOp(
            vt_id=r["vt_id"],
            before_name=r["before_name"],
            after_name=r["after_name"],
            new_cn_name=r.get("new_cn_name") or target_c.get("canonical_cn_name"),
            new_description=target_c.get("canonical_description"),
            new_synonyms=target_c.get("canonical_synonyms"),
        )
        for r in plan
        if r.get("changed") and not r.get("excluded_as_outlier")
    ]
    if not renames:
        return {"ok": True, "skipped": True, "reason": "cluster 无需 rename（全部 before==after）"}

    result = apply_renames(
        renames,
        scope="l2",
        scope_key=f"{payload.l1}/{payload.l2}#cluster{payload.cluster_id}",
        reviewer=payload.reviewer,
        reason=payload.reason or f"L2 alignment cluster {payload.cluster_id}",
    )
    _cache.clear()
    return {
        "ok": True,
        "version": result.version,
        "affected_slots": result.affected_slots,
        "affected_norm_rows": result.affected_norm_rows,
        "snapshot_path": str(result.snapshot_path) if result.snapshot_path else None,
    }


# ========== W5-B: L1 槽位对齐器 ==========

L1_ALIGNMENT_YAML = REPO_ROOT / "output" / "l1_alignment_proposals.yaml"


@app.get("/api/alignment/l1")
def get_l1_alignment_proposals():
    if not L1_ALIGNMENT_YAML.exists():
        return {"exists": False, "proposals": [], "summary": None}
    with L1_ALIGNMENT_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {"exists": True, **data}


class L1RegeneratePayload(BaseModel):
    only_l1: str | None = None
    threshold: float = 0.18


@app.post("/api/alignment/l1/regenerate")
def regenerate_l1_alignment(payload: L1RegeneratePayload):
    import subprocess
    import sys as _sys
    cmd = [_sys.executable, "-m", "src.alignment.l1_align",
           "--threshold", str(payload.threshold)]
    if payload.only_l1:
        cmd.extend(["--only-l1", payload.only_l1])
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise HTTPException(500, f"L1 对齐脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


class L1ApplyPayload(BaseModel):
    l1: str
    cluster_id: int
    rename_plan_override: list[dict[str, Any]] | None = None
    reviewer: str = "user"
    reason: str = ""


@app.post("/api/alignment/l1/apply")
def apply_l1_cluster(payload: L1ApplyPayload):
    if not L1_ALIGNMENT_YAML.exists():
        raise HTTPException(404, "l1_alignment_proposals.yaml 不存在")
    with L1_ALIGNMENT_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    target_p = next((p for p in data.get("proposals", []) if p["l1"] == payload.l1), None)
    if target_p is None:
        raise HTTPException(404, f"proposal 不存在: {payload.l1}")
    target_c = next((c for c in target_p["clusters"] if c["cluster_id"] == payload.cluster_id), None)
    if target_c is None:
        raise HTTPException(404, f"cluster {payload.cluster_id} 不存在")

    plan = payload.rename_plan_override if payload.rename_plan_override is not None else target_c.get("rename_plan", [])

    from src.alignment.cascade import RenameOp, apply_renames

    renames = [
        RenameOp(
            vt_id=r["vt_id"],
            before_name=r["before_name"],
            after_name=r["after_name"],
            new_cn_name=r.get("new_cn_name") or target_c.get("canonical_cn_name"),
            new_description=target_c.get("canonical_description"),
            new_synonyms=target_c.get("canonical_synonyms"),
        )
        for r in plan
        if r.get("changed") and not r.get("excluded_as_outlier")
    ]
    if not renames:
        return {"ok": True, "skipped": True, "reason": "cluster 无需 rename（全部 before==after）"}

    result = apply_renames(
        renames,
        scope="l1",
        scope_key=f"{payload.l1}#cluster{payload.cluster_id}",
        reviewer=payload.reviewer,
        reason=payload.reason or f"L1 alignment cluster {payload.cluster_id}",
    )
    _cache.clear()
    return {
        "ok": True,
        "version": result.version,
        "affected_slots": result.affected_slots,
        "affected_norm_rows": result.affected_norm_rows,
        "snapshot_path": str(result.snapshot_path) if result.snapshot_path else None,
    }


# ========== W5-C: base 槽位提升 ==========

BASE_PROMOTION_YAML = REPO_ROOT / "output" / "base_promotion_proposals.yaml"


@app.get("/api/alignment/base")
def get_base_promotion_proposals():
    if not BASE_PROMOTION_YAML.exists():
        return {"exists": False, "proposals": [], "summary": None}
    with BASE_PROMOTION_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {"exists": True, **data}


class BasePromoteRegeneratePayload(BaseModel):
    min_l1: int = 2
    only_name: str | None = None
    no_llm: bool = False


@app.post("/api/alignment/base/regenerate")
def regenerate_base_promotion(payload: BasePromoteRegeneratePayload):
    import subprocess
    import sys as _sys
    cmd = [_sys.executable, "-m", "src.alignment.base_promote",
           "--min-l1", str(payload.min_l1)]
    if payload.only_name:
        cmd.extend(["--only-name", payload.only_name])
    if payload.no_llm:
        cmd.append("--no-llm")
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=900)
    if result.returncode != 0:
        raise HTTPException(500, f"base 提升脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


class BasePromoteApplyPayload(BaseModel):
    canonical_name: str
    base_entry_override: dict[str, Any] | None = None
    member_vt_ids: list[str] | None = None  # 只对部分 VT 生效（默认全量 members）
    reviewer: str = "user"
    reason: str = ""


@app.post("/api/alignment/base/apply")
def apply_base_promotion(payload: BasePromoteApplyPayload):
    if not BASE_PROMOTION_YAML.exists():
        raise HTTPException(404, "base_promotion_proposals.yaml 不存在")
    with BASE_PROMOTION_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    target = next((p for p in data.get("proposals", []) if p["canonical_name"] == payload.canonical_name), None)
    if target is None:
        raise HTTPException(404, f"proposal 不存在: {payload.canonical_name}")

    base_entry = payload.base_entry_override or target.get("base_entry")
    if not base_entry:
        raise HTTPException(400, f"proposal {payload.canonical_name} 无 base_entry（可能是 --no-llm 模式）")

    members_all = target.get("members", [])
    if payload.member_vt_ids is not None:
        members = [m for m in members_all if m["vt_id"] in set(payload.member_vt_ids)]
    else:
        members = members_all
    if not members:
        raise HTTPException(400, "members 为空")

    from src.alignment.cascade import PromoteOp, apply_promotions

    promo = PromoteOp(
        canonical_name=payload.canonical_name,
        base_entry=base_entry,
        members=[
            {
                "vt_id": m["vt_id"],
                "slot_index": m.get("slot_index"),
                "before_name": m.get("before_name", payload.canonical_name),
                "extended_snapshot": m.get("extended_snapshot"),
            }
            for m in members
        ],
    )
    result = apply_promotions(
        [promo],
        scope_key=f"base_promotion#{payload.canonical_name}",
        reviewer=payload.reviewer,
        reason=payload.reason or f"promote {payload.canonical_name} to base_slots",
    )
    _cache.clear()
    return {
        "ok": True,
        "version": result.version,
        "affected_slots": result.affected_slots,
        "affected_norm_rows": result.affected_norm_rows,
        "base_slots_added": result.base_slots_added,
        "snapshot_path": str(result.snapshot_path) if result.snapshot_path else None,
    }


# ========== W5-E: 技术字段自动识别 ==========

TECH_FIELD_CANDIDATES_YAML = REPO_ROOT / "output" / "technical_field_candidates.yaml"
FIELD_BLACKLIST_YAML = REPO_ROOT / "data" / "slot_library" / "field_blacklist.yaml"
FIELD_BLACKLIST_WHITELIST_YAML = REPO_ROOT / "data" / "slot_library" / "field_blacklist_whitelist.yaml"
FIELD_BLACKLIST_LOG = REPO_ROOT / "data" / "feedback" / "field_blacklist_log.jsonl"


@app.get("/api/blacklist/auto-detect")
def get_tech_field_candidates():
    if not TECH_FIELD_CANDIDATES_YAML.exists():
        return {"exists": False, "proposals": [], "summary": None}
    with TECH_FIELD_CANDIDATES_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {"exists": True, **data}


class TechDetectRegeneratePayload(BaseModel):
    no_llm: bool = False
    min_score: float = 0.3
    llm_low: float = 0.4
    llm_high: float = 0.7


@app.post("/api/blacklist/auto-detect/regenerate")
def regenerate_tech_field_candidates(payload: TechDetectRegeneratePayload):
    import subprocess
    import sys as _sys
    cmd = [_sys.executable, "scripts/detect_technical_fields.py",
           "--min-score", str(payload.min_score),
           "--llm-low", str(payload.llm_low),
           "--llm-high", str(payload.llm_high)]
    if payload.no_llm:
        cmd.append("--no-llm")
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=1800)
    if result.returncode != 0:
        raise HTTPException(500, f"技术字段识别脚本失败：{result.stderr}")
    return {"ok": True, "stdout": result.stdout}


class TechDetectApplyItem(BaseModel):
    candidate_id: str
    action: str  # exact_name / name_pattern / table_field_pair
    value: str  # 对应 action 的 payload；pair 格式 "table/field"
    reason: str = ""


class TechDetectApplyPayload(BaseModel):
    items: list[TechDetectApplyItem]
    reviewer: str = "user"


@app.post("/api/blacklist/auto-detect/apply")
def apply_tech_field_blacklist(payload: TechDetectApplyPayload):
    if not payload.items:
        raise HTTPException(400, "items 为空")
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200

    with FIELD_BLACKLIST_YAML.open(encoding="utf-8") as f:
        doc = ruamel.load(f)
    exact_list = doc.setdefault("exact_names", [])
    pattern_list = doc.setdefault("name_patterns", [])
    pair_list = doc.setdefault("table_field_pairs", [])

    exact_set = {str(n).lower().strip() for n in (exact_list or [])}
    pattern_set = {str(p).lower().strip() for p in (pattern_list or [])}
    pair_set = {
        (str(p.get("table_en")), str(p.get("field_name")))
        for p in (pair_list or []) if isinstance(p, dict)
    }

    applied = {"exact_name": 0, "name_pattern": 0, "table_field_pair": 0, "skipped_duplicate": 0}
    log_entries = []
    for it in payload.items:
        ts = datetime.utcnow().isoformat()
        if it.action == "exact_name":
            v = it.value.lower().strip()
            if v in exact_set:
                applied["skipped_duplicate"] += 1
                continue
            exact_list.append(v)
            exact_set.add(v)
            applied["exact_name"] += 1
        elif it.action == "name_pattern":
            v = it.value.lower().strip()
            if v in pattern_set:
                applied["skipped_duplicate"] += 1
                continue
            pattern_list.append(v)
            pattern_set.add(v)
            applied["name_pattern"] += 1
        elif it.action == "table_field_pair":
            if "/" not in it.value:
                raise HTTPException(400, f"pair 格式应为 table/field: {it.value}")
            table_en, field_name = it.value.split("/", 1)
            if (table_en, field_name) in pair_set:
                applied["skipped_duplicate"] += 1
                continue
            pair_list.append({
                "table_en": table_en,
                "field_name": field_name,
                "reason": it.reason or "W5-E auto-detect",
            })
            pair_set.add((table_en, field_name))
            applied["table_field_pair"] += 1
        else:
            raise HTTPException(400, f"未知 action: {it.action}")
        log_entries.append({
            "ts": ts,
            "action": it.action,
            "value": it.value,
            "reason": it.reason or "W5-E auto-detect",
            "reviewer": payload.reviewer,
            "source": "w5e_auto_detect",
        })

    with FIELD_BLACKLIST_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)

    FIELD_BLACKLIST_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FIELD_BLACKLIST_LOG.open("a", encoding="utf-8") as f:
        for e in log_entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    _cache.clear()
    return {"ok": True, **applied}


# ---- W5-E · 当前黑名单浏览 + 白名单（取消错误标记）----

@app.get("/api/blacklist/current")
def get_current_blacklist(limit: int = 5000):
    """按 L1 → L2 → 表 分组返回当前标黑字段，用于审核/取消误杀。"""
    ff_path = REPO_ROOT / "output" / "field_features.parquet"
    if not ff_path.exists():
        return {"tree": [], "total_noise": 0}
    ff = pd.read_parquet(ff_path)
    noise = ff[ff["is_technical_noise"]].copy()
    # 读 whitelist
    wl_pairs = set()
    if FIELD_BLACKLIST_WHITELIST_YAML.exists():
        with FIELD_BLACKLIST_WHITELIST_YAML.open(encoding="utf-8") as f:
            wl = yaml.safe_load(f) or {}
        for p in wl.get("whitelist_pairs") or []:
            wl_pairs.add((p.get("table_en"), p.get("field_name")))

    # table_en → table_cn 映射（用全库）
    cn_map = ff.drop_duplicates("table_en").set_index("table_en")["table_cn"].to_dict()

    # 三级聚合：l1 → l2 → table_en → [fields]
    tree: dict[str, dict[str, dict[str, list[dict]]]] = {}
    for _, r in noise.head(limit).iterrows():
        l1 = str(r.get("table_l1") or "（未分类）")
        l2 = str(r.get("table_l2") or "（未分类）")
        te = r["table_en"]
        # sample_values → list[str]
        sv_raw = r.get("sample_values")
        if sv_raw is None:
            samples = []
        elif hasattr(sv_raw, "tolist"):
            samples = [str(x) for x in sv_raw.tolist()]
        else:
            samples = [str(x) for x in list(sv_raw)]
        tree.setdefault(l1, {}).setdefault(l2, {}).setdefault(te, []).append({
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment") or "",
            "data_type": r.get("data_type") or "",
            "usage_count": int(r["usage_count"]) if pd.notna(r.get("usage_count")) else 0,
            "sample_values": samples[:5],
            "noise_reason": r.get("noise_reason") or "",
            "whitelisted": (te, r["field_name"]) in wl_pairs,
        })

    out_tree = []
    for l1 in sorted(tree.keys()):
        l2_out = []
        l1_field_count = 0
        for l2 in sorted(tree[l1].keys()):
            tables_out = []
            l2_field_count = 0
            for te in sorted(tree[l1][l2].keys()):
                fields = tree[l1][l2][te]
                tables_out.append({
                    "table_en": te,
                    "table_cn": cn_map.get(te, ""),
                    "field_count": len(fields),
                    "fields": fields,
                })
                l2_field_count += len(fields)
            l2_out.append({
                "l2": l2,
                "table_count": len(tables_out),
                "field_count": l2_field_count,
                "tables": tables_out,
            })
            l1_field_count += l2_field_count
        out_tree.append({
            "l1": l1,
            "l2_count": len(l2_out),
            "field_count": l1_field_count,
            "l2_groups": l2_out,
        })

    return {
        "tree": out_tree,
        "total_noise": int(noise.shape[0]),
        "whitelist_count": len(wl_pairs),
    }


class BlacklistWhitelistPayload(BaseModel):
    table_en: str
    field_name: str
    reason: str = ""
    remove: bool = False  # True = 取消白名单（把字段加回黑名单生效）


@app.post("/api/blacklist/whitelist")
def toggle_blacklist_whitelist(payload: BlacklistWhitelistPayload):
    """W5-E · 把 (table_en, field_name) 加进/移出白名单。白名单中的字段即便命中黑名单规则也不判技术噪声。
    变更仅改 yaml 数据；要真正生效必须跑 `run_pipeline.py --from field_features` 重建 field_features.parquet。"""
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        raise HTTPException(500, f"ruamel.yaml 未安装：{e}")
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 200

    if FIELD_BLACKLIST_WHITELIST_YAML.exists():
        with FIELD_BLACKLIST_WHITELIST_YAML.open(encoding="utf-8") as f:
            doc = ruamel.load(f) or {}
    else:
        doc = {"whitelist_pairs": []}
    wl = doc.setdefault("whitelist_pairs", [])
    existing = {(p.get("table_en"), p.get("field_name")): i for i, p in enumerate(wl) if isinstance(p, dict)}

    key = (payload.table_en, payload.field_name)
    if payload.remove:
        if key in existing:
            wl.pop(existing[key])
        else:
            return {"ok": True, "changed": False, "reason": "not in whitelist"}
    else:
        if key in existing:
            return {"ok": True, "changed": False, "reason": "already whitelisted"}
        wl.append({
            "table_en": payload.table_en,
            "field_name": payload.field_name,
            "reason": payload.reason or "manual cancel false-positive",
        })

    FIELD_BLACKLIST_WHITELIST_YAML.parent.mkdir(parents=True, exist_ok=True)
    with FIELD_BLACKLIST_WHITELIST_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)

    # 审计日志
    FIELD_BLACKLIST_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FIELD_BLACKLIST_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ts": datetime.utcnow().isoformat(),
            "action": "whitelist_remove" if payload.remove else "whitelist_add",
            "table_en": payload.table_en,
            "field_name": payload.field_name,
            "reason": payload.reason or "",
            "source": "w5e_whitelist",
        }, ensure_ascii=False) + "\n")

    _cache.clear()
    return {"ok": True, "changed": True, "action": "remove" if payload.remove else "add"}
