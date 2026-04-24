"""W5-E · 技术字段自动识别

扫 field_features.parquet 的所有"目前未标黑"字段，打分 + LLM 兜底 →
output/technical_field_candidates.yaml

用法：
  python3 scripts/detect_technical_fields.py              # 全量 + LLM 兜底
  python3 scripts/detect_technical_fields.py --no-llm     # 只规则打分
  python3 scripts/detect_technical_fields.py --min-score 0.5
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.llm_client import chat  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
FIELD_FEATURES = ROOT / "output" / "field_features.parquet"
BLACKLIST_YAML = ROOT / "data" / "slot_library" / "field_blacklist.yaml"
OUTPUT = ROOT / "output" / "technical_field_candidates.yaml"

# ---- 规则 ----

RE_SYS_TIMESTAMP = re.compile(
    r"^(create|update|modify|last_modify|add|ins|upd|tra|trans)_(time|date|dt|ts|at|by|user|uid|id|userid)$"
)
RE_BOOLEAN_PREFIX = re.compile(r"^(is_|has_|flag_|f_).+")
RE_FLAG_SUFFIX = re.compile(r".+(_flag|_flg|_bit)$")
RE_HASH_SUFFIX = re.compile(r".+(_md5|_hash|_digest|_sign|_sha|_sha1|_sha256|_crc)$")
RE_PIPELINE = re.compile(r"^(load|ingest|etl|pipe|dag|dq)_.+")
RE_RESERVED = re.compile(r"^(reserved|reserve|spare|bei_yong|by_yong)_.+")
RE_SHORT_ID = re.compile(r"^(pid|rid|sid|uid|gid|nid|oid|xid|eid|aid|tid|mid)$")
RE_PK_FK = re.compile(r".+(_pk|_fk|_pk_id|_fk_id)$")
RE_ROW_REC = re.compile(r"^(row_|rec_|record_).+")
RE_HIDDEN_TS = re.compile(r"^_?(ts|dt|ut|ct|mt)$")

# 扩展：调试/临时字段
RE_TMP_DEBUG = re.compile(r"^(tmp|temp|test|debug|dbg|trial|bak|backup|old|new)_.+|.+_(tmp|temp|bak|old|deprecated)$")
# 扩展：冗余/副本字段（xx_2 / xx_copy / xx_new 之类）
RE_DUP_SUFFIX = re.compile(r".+_([0-9]+|copy|dup|duplicate|copy[0-9])$")
# 扩展：密码/敏感字段（无业务聚合价值）
RE_SECRET = re.compile(r"^(password|passwd|pwd|pass|secret|token|api_key|access_key|mima)$|.*(_password|_pwd|_secret|_token|_mima)$")

COMMENT_KEYWORDS = [
    "技术字段", "系统字段", "预留", "审计", "内部", "保留",
    "备用", "扩展字段", "冗余", "调试", "测试",
    # 废弃/弃用
    "废弃", "弃用", "停用", "已废弃", "deprecated", "obsolete",
    # 加密
    "加密", "脱敏", "脱敏字段", "密码", "密文",
]

# 加密 / 脱敏 样本识别：以 # 开头（本项目样例里 hash_encoded 格式）或全星号/X 占位
RE_SAMPLE_MASK = re.compile(r"^(#|\*{3,}|X{6,}|x{6,}|●{3,})")

LLM_SYSTEM = """你是一个数据治理助手。给你一个物理字段（name / comment / 样例 / 数据类型 / usage_count），判断它是否该进字段黑名单（不参与虚拟表归一）。

黑名单 7 大类（命中任一即 judgement=technical）：
1) 技术字段 — 系统审计(record_create_at)、软删(is_deleted)、ETL 标记(etl_time)、管道加工(load_dt)、保留(reserved_col_1)、纯数据库主键(pid/rid/oid)
2) 空值字段 — 所有样例为 null/空 且 usage_count=0（完全没被业务使用过）
3) 废弃字段 — comment 含"废弃/弃用/停用/deprecated"
4) 调试/临时字段 — tmp_/temp_/test_/debug_/bak_ 前缀
5) 冗余副本字段 — xx_2 / xx_copy / xx_dup 这类未文档化的衍生
6) 敏感字段 — password/token/secret/api_key（无业务聚合价值）
7) 加密/脱敏字段 — 样例全是 hash（# 开头）/ 全是星号/X 占位，且无 comment

严格输出 JSON：
{
  "judgement": "technical" / "business" / "uncertain",
  "confidence": 0.0-1.0,
  "category": "technical / empty / deprecated / debug / duplicate / secret / masked / business",
  "reason": "一句话为什么"
}

判定提示：
- business：字段名/注释能看出业务语义（如 "身份证号"、"手机号"、"事件发生时间"），即使名字像 create_time 也可能是业务
- uncertain：边界案例，由人工兜底"""


def load_blacklist() -> dict:
    """返回 exact_names(set) / name_patterns(list) / table_field_pairs(set)"""
    with BLACKLIST_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {
        "exact_names": {n.lower().strip() for n in (data.get("exact_names") or [])},
        "name_patterns": list(data.get("name_patterns") or []),
        "table_field_pairs": {
            (p.get("table_en"), p.get("field_name"))
            for p in (data.get("table_field_pairs") or [])
        },
    }


def already_blacklisted(field_name: str, table_en: str, bl: dict) -> bool:
    fn = field_name.lower().strip()
    if fn in bl["exact_names"]:
        return True
    for pat in bl["name_patterns"]:
        if fnmatch.fnmatch(fn, pat.lower()):
            return True
    if (table_en, field_name) in bl["table_field_pairs"]:
        return True
    return False


def rule_score(row: pd.Series) -> tuple[float, list[str]]:
    """返回 (score 0..1, reasons list)"""
    fn = (row["field_name"] or "").lower().strip()
    comment = (row.get("field_comment") or "").strip()
    data_type = (row.get("data_type") or "").upper()
    usage_raw = row.get("usage_count", 0)
    usage = int(usage_raw) if pd.notna(usage_raw) else 0
    sample_raw = row.get("sample_values")
    if sample_raw is None:
        samples = []
    elif hasattr(sample_raw, "tolist"):
        samples = list(sample_raw.tolist())
    else:
        samples = list(sample_raw)
    non_null_samples = [s for s in samples if str(s).strip() not in {"", "nan", "null", "None"}]

    reasons: list[str] = []
    score = 0.0

    # 规则 1：名称模式（权重 0.4）
    if RE_SYS_TIMESTAMP.match(fn):
        score += 0.4
        reasons.append("sys_timestamp_pattern")
    elif RE_BOOLEAN_PREFIX.match(fn):
        score += 0.35
        reasons.append("boolean_prefix")
    elif RE_FLAG_SUFFIX.match(fn):
        score += 0.35
        reasons.append("flag_suffix")
    elif RE_HASH_SUFFIX.match(fn):
        score += 0.4
        reasons.append("hash_suffix")
    elif RE_PIPELINE.match(fn):
        score += 0.4
        reasons.append("pipeline_prefix")
    elif RE_RESERVED.match(fn):
        score += 0.5
        reasons.append("reserved_field")
    elif RE_SHORT_ID.match(fn):
        score += 0.35
        reasons.append("short_id")
    elif RE_PK_FK.match(fn):
        score += 0.35
        reasons.append("pk_fk_suffix")
    elif RE_ROW_REC.match(fn):
        score += 0.3
        reasons.append("row_rec_prefix")
    elif RE_HIDDEN_TS.match(fn):
        score += 0.35
        reasons.append("hidden_ts_name")
    elif RE_TMP_DEBUG.match(fn):
        score += 0.4
        reasons.append("tmp_debug_field")
    elif RE_DUP_SUFFIX.match(fn):
        score += 0.3
        reasons.append("dup_suffix")
    elif RE_SECRET.match(fn):
        score += 0.45
        reasons.append("secret_password")

    # 规则 2：comment 关键词（权重 0.3；含"废弃"给更高权重）
    for kw in COMMENT_KEYWORDS:
        if kw in comment:
            w = 0.5 if kw in {"废弃", "弃用", "停用", "已废弃", "deprecated", "obsolete"} else 0.3
            score += w
            reasons.append(f"comment_kw={kw}")
            break

    # 规则 3：空值/低用量（权重 0.2+）
    # —— 用户明确关注"空值的字段"
    if usage == 0 and not non_null_samples:
        score += 0.35  # 提权：完全空 + 完全不用
        reasons.append("empty_and_unused")
    elif not non_null_samples and samples:  # 有样例槽位但都是 null
        score += 0.3
        reasons.append("all_samples_null")
    elif usage == 0:
        score += 0.1
        reasons.append("no_usage")

    # 规则 4：data_type + name 组合（权重 0.1）
    if data_type in {"TIMESTAMP", "DATETIME"} and any(k in fn for k in ["etl", "sys", "load", "ingest"]):
        score += 0.1
        reasons.append("type_time_with_sys_keyword")

    # 规则 5：样例全是 hash/mask（权重 0.15）
    if non_null_samples:
        masked = sum(1 for s in non_null_samples if RE_SAMPLE_MASK.match(str(s)))
        if masked == len(non_null_samples):
            score += 0.15
            reasons.append("samples_all_masked")

    return min(score, 1.0), reasons


def decide_action(field_name: str, table_en: str, group_size: int, reasons: list[str]) -> tuple[str, str]:
    """根据规则理由决定 suggested_action：
    - pattern 级：多表共享且命名模式强 (pipeline / reserved / sys_timestamp)
    - exact_name：跨表多次出现的确切名字
    - table_field_pair：单表特例
    """
    pattern_level_reasons = {"pipeline_prefix", "reserved_field", "sys_timestamp_pattern", "tmp_debug_field"}
    has_pattern_reason = any(r in pattern_level_reasons or r.startswith("sys_timestamp") for r in reasons)
    if has_pattern_reason and group_size >= 2:
        fn = field_name.lower()
        if "pipeline_prefix" in reasons:
            prefix = fn.split("_", 1)[0]
            return "name_pattern", f"{prefix}_*"
        if "reserved_field" in reasons:
            prefix = fn.split("_", 1)[0]
            return "name_pattern", f"{prefix}_*"
        if "tmp_debug_field" in reasons:
            prefix = fn.split("_", 1)[0]
            return "name_pattern", f"{prefix}_*"
        if "sys_timestamp_pattern" in reasons:
            return "exact_name", fn
    if group_size >= 2:
        return "exact_name", field_name.lower()
    return "table_field_pair", f"{table_en}/{field_name}"


def llm_tiebreak(row: pd.Series, reasons: list[str]) -> dict:
    sample_raw = row.get("sample_values")
    if sample_raw is None:
        samples = []
    elif hasattr(sample_raw, "tolist"):
        samples = list(sample_raw.tolist())
    else:
        samples = list(sample_raw)
    info = {
        "field_name": row["field_name"],
        "field_comment": row.get("field_comment") or "",
        "data_type": row.get("data_type") or "",
        "sample_values": samples[:5],
        "usage_count": int(row.get("usage_count", 0) or 0),
        "rule_hit_reasons": reasons,
    }
    user = "字段信息：\n" + json.dumps(info, ensure_ascii=False, indent=2) + "\n\n请判断。"
    resp = chat(
        messages=[{"role": "system", "content": LLM_SYSTEM}, {"role": "user", "content": user}],
        json_mode=True,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        return {"judgement": "uncertain", "confidence": 0.0, "reason": "parse_error"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-llm", action="store_true", help="跳过 LLM 兜底（只规则）")
    parser.add_argument("--min-score", type=float, default=0.3, help="最低入候选分数（默认 0.3）")
    parser.add_argument("--llm-low", type=float, default=0.4, help="LLM 兜底区间下界（默认 0.4）")
    parser.add_argument("--llm-high", type=float, default=0.7, help="LLM 兜底区间上界（默认 0.7）")
    args = parser.parse_args()

    ff = pd.read_parquet(FIELD_FEATURES)
    bl = load_blacklist()
    already_noise_parquet = int(ff["is_technical_noise"].sum())

    # 实时用 yaml 过滤：apply 后 yaml 立即变，但 parquet 要等 pipeline 重跑才更新
    # 所以这里两层过滤都用，确保刚 apply 的 78 条立即不再出现在候选里
    ff["_yaml_blacklisted"] = ff.apply(
        lambda r: already_blacklisted(str(r["field_name"]), str(r["table_en"]), bl),
        axis=1,
    )
    yaml_noise_count = int(ff["_yaml_blacklisted"].sum())
    extra_from_yaml = int((ff["_yaml_blacklisted"] & ~ff["is_technical_noise"]).sum())
    print(f"扫 {len(ff)} 字段 · parquet 已标黑 {already_noise_parquet}（{already_noise_parquet/len(ff)*100:.1f}%）"
          f" · yaml 现在命中 {yaml_noise_count}（其中 {extra_from_yaml} 条是 apply 后新加、parquet 未刷新）")
    if extra_from_yaml > 0:
        print(f"  ⚠️  提示：跑 `run_pipeline.py --from field_features` 可以让 parquet 同步，让这些项永久生效")

    # 过滤：parquet 已标黑 OR yaml 已命中 OR 在白名单
    to_scan = ff[~ff["is_technical_noise"] & ~ff["_yaml_blacklisted"]].copy()
    to_scan = to_scan.drop(columns=["_yaml_blacklisted"], errors="ignore")
    print(f"待扫 {len(to_scan)}（已排除 parquet + yaml 双重标黑）")

    # 规则打分
    scores = []
    reasons_list = []
    for _, r in to_scan.iterrows():
        score, reasons = rule_score(r)
        scores.append(score)
        reasons_list.append(reasons)
    to_scan["_score"] = scores
    to_scan["_reasons"] = reasons_list

    # 过滤低分
    cand = to_scan[to_scan["_score"] >= args.min_score].copy()
    print(f"score ≥ {args.min_score}: {len(cand)} 候选")

    # 跨表分组：同名字段出现多表
    group_sizes = cand.groupby(cand["field_name"].str.lower())["table_en"].nunique()
    cand["_group_size"] = cand["field_name"].str.lower().map(group_sizes)

    # LLM 兜底（只跑边界分数段）
    llm_judged = 0
    llm_cache: dict[str, dict] = {}
    proposals = []
    per_field_group: dict[str, dict] = defaultdict(lambda: {"tables": [], "samples": [], "fields": []})

    # 归并同 (suggested_action, suggested_value)：同 exact_name / pattern 合并，pair 独立
    # 先逐行定 action，再按 key 聚合
    for _, r in cand.iterrows():
        fn = r["field_name"].lower()
        action, value = decide_action(r["field_name"], r["table_en"], int(r["_group_size"]), r["_reasons"])
        key = f"{action}:{value}"
        per_field_group[key]["tables"].append(r["table_en"])
        per_field_group[key]["fields"].append({
            "table_en": r["table_en"],
            "field_name": r["field_name"],
            "field_comment": r.get("field_comment") or "",
            "data_type": r.get("data_type") or "",
            "usage_count": int(r["usage_count"]) if pd.notna(r.get("usage_count")) else 0,
            "score": float(r["_score"]),
            "reasons": r["_reasons"],
        })

    cid = 0
    for key, g in per_field_group.items():
        action, value = key.split(":", 1)
        # 代表字段：分最高的
        g_sorted = sorted(g["fields"], key=lambda x: -x["score"])
        rep = g_sorted[0]
        reasons_all = sorted({r for f in g["fields"] for r in f["reasons"]})
        avg_score = sum(f["score"] for f in g["fields"]) / len(g["fields"])
        entry = {
            "candidate_id": f"c{cid:04d}",
            "suggested_action": action,
            "suggested_value": value,
            "field_name": rep["field_name"],
            "field_comment": rep["field_comment"],
            "data_type": rep["data_type"],
            "usage_count": rep["usage_count"],
            "example_tables": list(dict.fromkeys(g["tables"]))[:10],
            "affected_field_count": len(g["fields"]),
            "score": round(avg_score, 3),
            "reasons": reasons_all,
        }

        # LLM 兜底：在边界分数段
        if not args.no_llm and args.llm_low <= avg_score <= args.llm_high:
            if rep["field_name"] not in llm_cache:
                # 构造代表 row 给 LLM
                rep_row = pd.Series({
                    "field_name": rep["field_name"],
                    "field_comment": rep["field_comment"],
                    "data_type": rep["data_type"],
                    "sample_values": [],
                    "usage_count": rep["usage_count"],
                })
                llm_cache[rep["field_name"]] = llm_tiebreak(rep_row, reasons_all)
                llm_judged += 1
            lj = llm_cache[rep["field_name"]]
            entry["llm_judgement"] = lj.get("judgement")
            entry["llm_confidence"] = lj.get("confidence")
            entry["llm_category"] = lj.get("category")
            entry["llm_reason"] = lj.get("reason")
        else:
            entry["llm_judgement"] = None

        proposals.append(entry)
        cid += 1

    # 按最终分数排序
    proposals.sort(key=lambda x: -x["score"])

    bands = {"high": 0, "medium": 0, "low": 0}
    for p in proposals:
        if p["score"] >= 0.7: bands["high"] += 1
        elif p["score"] >= 0.5: bands["medium"] += 1
        else: bands["low"] += 1

    out = {
        "generated_at": datetime.utcnow().isoformat(),
        "rule_version": "v1",
        "llm_band": [args.llm_low, args.llm_high],
        "summary": {
            "total_scanned": int(len(ff)),
            "already_blacklisted_parquet": already_noise_parquet,
            "already_blacklisted_yaml": yaml_noise_count,
            "yaml_extra_pending_pipeline": extra_from_yaml,
            "new_candidates": len(proposals),
            "llm_judged": llm_judged,
            "by_confidence": bands,
            "by_action": dict(Counter(p["suggested_action"] for p in proposals)),
        },
        "proposals": proposals,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True, sort_keys=False, width=160)
    print(f"✅ → {OUTPUT}")
    print(f"   summary: {out['summary']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
