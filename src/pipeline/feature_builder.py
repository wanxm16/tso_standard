"""I-02: 字段特征提取。

对 10,324 个物理字段各抽取 6 类特征（§ 10.8.2）：
- 名称特征（token / pinyin / 缩写展开 / 后缀）
- 注释特征（清洗 / 分词 / 关键词命中）
- 样例特征（pattern 命中 / 长度分布 / 字符构成）
- 使用特征（usage_count / sql_count / role_select/where/join）
- 上下文特征（同表字段共现）
- 表语义特征（L1/L2 / related_vt_ids）

输出：output/field_features.parquet
"""
from __future__ import annotations

import json
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.pipeline.patterns import (  # noqa: E402
    NULL_TOKENS,
    match_patterns_multi,
    sample_char_type,
    sample_length_stats,
)


DDL_CSV = REPO_ROOT / "data" / "phrase_2" / "二期_DDL_all_with_sample.csv"
USAGE_CSV = REPO_ROOT / "data" / "phrase_2" / "二期DDL字段使用情况.csv"
CATEGORY_JSON = REPO_ROOT / "data" / "phrase_2" / "二期表分类树.json"
SCAFFOLD_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"
ABBREV_YAML = REPO_ROOT / "data" / "slot_library" / "abbreviation_dict.yaml"
BLACKLIST_YAML = REPO_ROOT / "data" / "slot_library" / "field_blacklist.yaml"
BLACKLIST_WHITELIST_YAML = REPO_ROOT / "data" / "slot_library" / "field_blacklist_whitelist.yaml"

OUT_PARQUET = REPO_ROOT / "output" / "field_features.parquet"
OUT_DIAG = REPO_ROOT / "output" / "field_features_diagnostic.md"


# ============ 技术噪声过滤规则 ============

# 基础硬编码黑名单（和 field_blacklist.yaml 合并使用；yaml 是主要扩展位）
TECH_NOISE_NAMES = {
    "rn", "dt", "ds", "pt", "etl_time", "etl_date", "batch_id",
    "created_at", "updated_at", "create_time_tech", "_etl_ts",
    "ordinal", "row_number", "_version", "_sys_time",
}
TECH_NOISE_SUFFIX = ("_tmp", "_new", "_old", "_bak", "_result", "_tmp2")


def _load_field_blacklist() -> dict:
    """读 data/slot_library/field_blacklist.yaml，返回结构化黑名单。"""
    if not BLACKLIST_YAML.exists():
        return {"exact_names": set(), "name_patterns": [], "table_field_pairs": set()}
    with BLACKLIST_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    exact = {str(n).lower().strip() for n in (data.get("exact_names") or []) if n}
    patterns = [str(p).lower().strip() for p in (data.get("name_patterns") or []) if p]
    pairs: set[tuple[str, str]] = set()
    for entry in data.get("table_field_pairs") or []:
        if isinstance(entry, dict) and entry.get("table_en") and entry.get("field_name"):
            pairs.add((str(entry["table_en"]).strip(), str(entry["field_name"]).strip()))
    return {"exact_names": exact, "name_patterns": patterns, "table_field_pairs": pairs}


def _load_whitelist() -> set[tuple[str, str]]:
    """W5-E: 读 field_blacklist_whitelist.yaml —— 被黑名单规则命中但人工判业务的 (table, field) 对。"""
    if not BLACKLIST_WHITELIST_YAML.exists():
        return set()
    with BLACKLIST_WHITELIST_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    out: set[tuple[str, str]] = set()
    for entry in data.get("whitelist_pairs") or []:
        if isinstance(entry, dict) and entry.get("table_en") and entry.get("field_name"):
            out.add((str(entry["table_en"]).strip(), str(entry["field_name"]).strip()))
    return out


# 模块加载时读一次（改 yaml 必须重跑脚本才生效，不在进程内动态 reload）
_BLACKLIST = _load_field_blacklist()
_WHITELIST = _load_whitelist()


def _match_pattern(name: str, pattern: str) -> bool:
    """fnmatch 风格大小写不敏感匹配。"""
    import fnmatch
    return fnmatch.fnmatchcase(name.lower(), pattern.lower())


def is_technical_noise(
    field_name: str,
    comment: str,
    sample_values: list[str],
    table_en: str | None = None,
) -> tuple[bool, str]:
    fn_lower = field_name.lower().strip()

    # 0) W5-E 白名单优先：(table_en, field_name) 明确排除则永不判技术噪声
    if table_en is not None and (table_en, field_name) in _WHITELIST:
        return False, "whitelisted"

    # 1) 硬编码黑名单
    if fn_lower in TECH_NOISE_NAMES:
        return True, "blacklist_name"
    for suf in TECH_NOISE_SUFFIX:
        if fn_lower.endswith(suf):
            return True, f"suffix={suf}"

    # 2) yaml 黑名单：exact_names / name_patterns / table_field_pairs
    if fn_lower in _BLACKLIST["exact_names"]:
        return True, "blacklist_yaml_name"
    for pat in _BLACKLIST["name_patterns"]:
        if _match_pattern(fn_lower, pat):
            return True, f"blacklist_yaml_pattern={pat}"
    if table_en is not None:
        if (table_en, field_name) in _BLACKLIST["table_field_pairs"]:
            return True, "blacklist_yaml_pair"

    # 3) 兜底：空注释 + 无意义名字
    cm = (comment or "").strip()
    non_null_samples = [s for s in sample_values if str(s).strip() not in NULL_TOKENS]
    if not cm and (not fn_lower or not re.search(r"[a-zA-Z一-鿿]", fn_lower)):
        return True, "no_comment_and_meaningless_name"
    if not non_null_samples and not cm:
        return True, "all_null_and_no_comment"
    return False, ""


# ============ 名称特征 ============


def tokenize_name(name: str) -> list[str]:
    """对字段名做分词：下划线 / 连字符 / 驼峰 分隔，全部小写。"""
    if not name:
        return []
    # 驼峰转下划线
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
    s = s.lower().replace("-", "_")
    return [t for t in s.split("_") if t]


def extract_suffix(name: str) -> str:
    """识别常见字段后缀：_id / _no / _bh / _dm / _mc / _sj / _rq 等。"""
    if not name:
        return ""
    lower = name.lower()
    for suf in ["_id", "_no", "_bh", "_dm", "_mc", "_sj", "_rq", "_time", "_date", "_name", "_code", "_type"]:
        if lower.endswith(suf):
            return suf.lstrip("_")
    return ""


def load_abbrev() -> tuple[dict[str, list[str]], set[str]]:
    data = yaml.safe_load(ABBREV_YAML.read_text(encoding="utf-8"))
    abbrev = data.get("abbreviations", {}) or {}
    ambiguous = set(data.get("ambiguous_keys", []) or [])
    return abbrev, ambiguous


def expand_abbreviation(tokens: list[str], abbrev: dict[str, list[str]]) -> list[str]:
    """对每个 token 查询缩写字典，展开成中文全称候选。

    对于非缩写的 token（纯 ASCII 无命中），保留原 token；中文 token 直接保留。
    """
    expanded: list[str] = []
    for t in tokens:
        key_candidates = [t, t + "_"]  # 处理 id_ / bm 这种带下划线的 key
        matched = False
        for k in key_candidates:
            if k in abbrev:
                expanded.extend(abbrev[k])
                matched = True
                break
        if not matched:
            # 中文直接保留
            if re.search(r"[一-鿿]", t):
                expanded.append(t)
            # 非缩写英文（超过 2 字符且不是 a-z 拼音）也保留
            elif len(t) > 2 and not re.fullmatch(r"[a-z]+", t):
                expanded.append(t)
            # 其他短拼音保留原形，供后续 lexical 匹配
            else:
                expanded.append(t)
    # 去重保序
    seen: set[str] = set()
    result: list[str] = []
    for e in expanded:
        if e not in seen:
            seen.add(e)
            result.append(e)
    return result


# ============ 注释特征 ============

COMMENT_KEYWORDS = {
    "person_name": ["姓名", "名字", "人名"],
    "id_card": ["身份证", "身份证号", "公民身份"],
    "phone": ["手机号", "电话", "联系方式"],
    "vehicle_plate": ["车牌", "号牌"],
    "passport": ["护照"],
    "certificate": ["证件"],
    "time": ["时间", "日期", "时刻"],
    "birth": ["出生"],
    "address": ["地址", "住址", "住所", "地点"],
    "region": ["行政区划", "省", "市", "县", "区"],
    "status": ["状态", "标记", "是否"],
    "type": ["类型", "类别", "种类"],
    "code": ["代码", "编码", "编号"],
    "case": ["案件", "案由", "警情", "立案"],
    "amount": ["金额", "费用"],
    "count": ["次数", "数量", "频次"],
    "device": ["设备", "终端"],
    "source": ["来源"],
    "gender": ["性别"],
    "nationality": ["民族", "国籍"],
    "marital": ["婚姻"],
    "education": ["文化程度", "学历"],
    "occupation": ["职业", "职务"],
    "household": ["户口", "户籍"],
    "organization": ["机构", "单位", "企业", "组织"],
    "relation": ["关系", "关联", "亲属"],
    "latitude": ["纬度"],
    "longitude": ["经度"],
    "coordinate": ["坐标"],
}


def clean_comment(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    # 去除 | 后的 remark 说明部分的第一层括号
    s = re.sub(r"[(（][^)）]*[)）]", "", s)  # 去括号内容
    # 多行/竖线只取第一段
    s = s.split("|")[0].split("\n")[0]
    return s.strip()


def extract_comment_keywords(comment_clean: str) -> list[str]:
    hits: list[str] = []
    for concept, kws in COMMENT_KEYWORDS.items():
        if any(kw in comment_clean for kw in kws):
            hits.append(concept)
    return hits


def tokenize_comment(comment_clean: str) -> list[str]:
    """轻量中文 token 化：先按标点切，再逐字符生成 bigram。"""
    if not comment_clean:
        return []
    segments = re.split(r"[，,。.\s/、\-_:：()（）]+", comment_clean)
    segments = [s for s in segments if s]
    # 生成 bigram（中文 2 字概念）
    bigrams: list[str] = []
    for seg in segments:
        if len(seg) <= 3:
            bigrams.append(seg)
        else:
            for i in range(len(seg) - 1):
                bigrams.append(seg[i : i + 2])
    # 去重
    return list(dict.fromkeys(bigrams))


# ============ 样例特征 ============


def parse_samples(sample_data: str) -> list[str]:
    if not sample_data:
        return []
    s = str(sample_data)
    # CSV 里样例用 " | " 分隔
    parts = [p.strip() for p in s.split("|")]
    return parts[:5]  # 最多取 5 条


# ============ 使用情况 ============


def load_usage(path: Path) -> pd.DataFrame:
    """加载字段使用情况表。精确列名匹配（避免 "table" vs "table_cn_name" 冲突）。"""
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8")

    rename_map: dict[str, str] = {}
    for c in df.columns:
        if c == "table":
            rename_map[c] = "table_en"
        elif c == "field":
            rename_map[c] = "field_name"
        elif c in ("usage_count", "sql_count", "etl_count", "query_count",
                   "role_select", "role_where", "role_join",
                   "role_group_by", "role_order_by", "role_having",
                   "role_function", "role_target"):
            rename_map[c] = c  # 保持原名

    df = df.rename(columns=rename_map)
    keep_cols = [
        "table_en", "field_name",
        "usage_count", "query_count", "etl_count",
        "role_select", "role_where", "role_join",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()
    # sql_count 是下游期望的列；若不存在就用 query_count 作为等价代理
    if "sql_count" not in df.columns:
        if "query_count" in df.columns:
            df["sql_count"] = df["query_count"]
        else:
            df["sql_count"] = 0
    for col in ["usage_count", "sql_count", "role_select", "role_where", "role_join"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


# ============ 表语义：L1/L2 + related_vt_ids ============


def build_table_category_map(category_json: Path) -> dict[str, tuple[str, str]]:
    """返回 en → (L1, L2) 映射。"""
    tree = json.loads(category_json.read_text(encoding="utf-8"))
    mapping: dict[str, tuple[str, str]] = {}
    for l1 in tree:
        l1_name = l1.get("name", "")
        for l2 in l1.get("children", []) or []:
            l2_name = l2.get("name", "")
            for t in l2.get("tables", []) or []:
                en = t.get("en", "")
                if en:
                    mapping[en] = (l1_name, l2_name)
    return mapping


def build_table_to_vt_map(scaffold_json: Path) -> dict[str, list[str]]:
    """表 en → [vt_id] 映射。"""
    data = json.loads(scaffold_json.read_text(encoding="utf-8"))
    mapping: dict[str, list[str]] = defaultdict(list)
    for vt in data.get("virtual_tables", []):
        for t in vt.get("candidate_tables", []):
            en = t.get("en", "")
            if en:
                mapping[en].append(vt["vt_id"])
    return dict(mapping)


# ============ 上下文特征 ============


def compute_table_context(ddl_df: pd.DataFrame) -> dict[str, dict]:
    """按 table 分组，计算表级上下文信号。

    返回：{table_en: {has_subject_id, has_time, has_location, sibling_count}}
    """
    ctx: dict[str, dict] = {}
    for table_en, sub in ddl_df.groupby("table"):
        has_subject_id = False
        has_time = False
        has_location = False
        for _, row in sub.iterrows():
            comment = str(row.get("comment", "") or "")
            field = str(row.get("field", "") or "").lower()
            kws = extract_comment_keywords(clean_comment(comment))
            if any(k in kws for k in ["id_card", "phone", "vehicle_plate", "passport", "certificate", "device"]):
                has_subject_id = True
            if "time" in kws or "birth" in kws:
                has_time = True
            if "address" in kws or "region" in kws or "coordinate" in kws:
                has_location = True
            # 字段名也看
            for token in ["sfzh", "sjh", "cph", "hzhm", "mac", "imei"]:
                if token in field:
                    has_subject_id = True
            for token in ["sj", "sjc", "rq"]:
                if field.endswith(token) or field == token:
                    has_time = True
            for token in ["dz", "xzqh", "kymc", "dwmc"]:
                if token in field:
                    has_location = True
        ctx[table_en] = {
            "sibling_count": int(len(sub)),
            "has_subject_id": has_subject_id,
            "has_time": has_time,
            "has_location": has_location,
        }
    return ctx


# ============ 主流程 ============


def build_features() -> pd.DataFrame:
    print("加载数据...")
    ddl_df = pd.read_csv(DDL_CSV, encoding="utf-8")
    usage_df = load_usage(USAGE_CSV)
    category_map = build_table_category_map(CATEGORY_JSON)
    table_to_vt = build_table_to_vt_map(SCAFFOLD_JSON)
    abbrev, ambiguous = load_abbrev()
    table_ctx = compute_table_context(ddl_df)

    print(f"  DDL: {len(ddl_df)} 行")
    print(f"  usage: {len(usage_df)} 行")
    print(f"  category: {len(category_map)} 表")
    print(f"  scaffold→VT: {len(table_to_vt)} 表有 VT")

    if not usage_df.empty:
        ddl_df = ddl_df.merge(
            usage_df, how="left",
            left_on=["table", "field"], right_on=["table_en", "field_name"],
            suffixes=("", "_u"),
        )
    else:
        for col in ["usage_count", "sql_count", "role_select", "role_where", "role_join"]:
            ddl_df[col] = 0.0

    print("抽取特征...")
    rows: list[dict[str, Any]] = []
    t0 = time.time()
    for i, row in ddl_df.iterrows():
        if i and i % 2000 == 0:
            print(f"  处理 {i}/{len(ddl_df)} ... ({time.time()-t0:.1f}s)")

        table_en = str(row.get("table", "") or "")
        table_cn = str(row.get("table_cn_name", "") or "")
        field_name = str(row.get("field", "") or "")
        field_comment = str(row.get("comment", "") or "")
        data_type = str(row.get("type", "") or "")
        sample_raw = str(row.get("sample_data", "") or "")

        sample_values = parse_samples(sample_raw)
        is_noise, noise_reason = is_technical_noise(field_name, field_comment, sample_values, table_en=table_en)

        comment_clean = clean_comment(field_comment)
        name_tokens = tokenize_name(field_name)
        name_expanded = expand_abbreviation(name_tokens, abbrev)
        suffix = extract_suffix(field_name)

        patterns = [] if is_noise else match_patterns_multi(sample_values)
        length_stats = sample_length_stats(sample_values)
        char_type = sample_char_type(sample_values)

        comment_keywords = extract_comment_keywords(comment_clean)
        comment_tokens = tokenize_comment(comment_clean)

        l1, l2 = category_map.get(table_en, ("未知", "未知"))
        vt_ids = table_to_vt.get(table_en, [])
        ctx = table_ctx.get(table_en, {})

        rows.append({
            "table_en": table_en,
            "table_cn": table_cn,
            "field_name": field_name,
            "field_comment": field_comment,
            "data_type": data_type,
            # 名称
            "name_tokens": name_tokens,
            "name_expanded": name_expanded,
            "name_suffix": suffix,
            # 注释
            "comment_clean": comment_clean,
            "comment_tokens": comment_tokens,
            "comment_keywords": comment_keywords,
            # 样例
            "sample_values": sample_values,
            "sample_patterns": patterns,
            "sample_length_min": length_stats["min"],
            "sample_length_max": length_stats["max"],
            "sample_length_avg": length_stats["avg"],
            "sample_char_type": char_type,
            # 使用
            "usage_count": float(row.get("usage_count", 0) or 0),
            "sql_count": float(row.get("sql_count", 0) or 0),
            "role_select": float(row.get("role_select", 0) or 0),
            "role_where": float(row.get("role_where", 0) or 0),
            "role_join": float(row.get("role_join", 0) or 0),
            # 上下文
            "sibling_count": ctx.get("sibling_count", 0),
            "has_subject_id": ctx.get("has_subject_id", False),
            "has_time": ctx.get("has_time", False),
            "has_location": ctx.get("has_location", False),
            # 表语义
            "table_l1": l1,
            "table_l2": l2,
            "related_vt_ids": vt_ids,
            # 元信息
            "is_technical_noise": is_noise,
            "noise_reason": noise_reason,
        })

    df = pd.DataFrame(rows)
    print(f"全部处理完成: {len(df)} 行，耗时 {time.time()-t0:.1f}s")
    return df


def write_diagnostic(df: pd.DataFrame) -> None:
    lines: list[str] = [
        "# I-02 字段特征提取诊断",
        "",
        "## 总体",
        "",
        f"- 字段总数: {len(df)}",
        f"- 技术噪声字段: {df['is_technical_noise'].sum()} ({df['is_technical_noise'].mean()*100:.1f}%)",
        f"- 有 related VT 的字段: {(df['related_vt_ids'].str.len() > 0).sum()} ({(df['related_vt_ids'].str.len()>0).mean()*100:.1f}%)",
        f"- 有注释的字段: {(df['comment_clean'].str.len() > 0).sum()}",
        f"- 有样例 pattern 命中的字段: {(df['sample_patterns'].str.len() > 0).sum()}",
        "",
        "## 技术噪声理由分布",
        "",
    ]
    noise_reasons = df[df["is_technical_noise"]]["noise_reason"].value_counts()
    for reason, c in noise_reasons.items():
        lines.append(f"- {reason}: {c}")
    lines.append("")

    lines += ["## 样例 pattern 命中 Top 20", ""]
    pattern_counter: Counter[str] = Counter()
    for patterns in df["sample_patterns"]:
        for p in patterns:
            pattern_counter[p] += 1
    for p, c in pattern_counter.most_common(20):
        lines.append(f"- `{p}`: {c}")
    lines.append("")

    lines += ["## 注释关键词命中 Top 20", ""]
    kw_counter: Counter[str] = Counter()
    for kws in df["comment_keywords"]:
        for k in kws:
            kw_counter[k] += 1
    for k, c in kw_counter.most_common(20):
        lines.append(f"- `{k}`: {c}")
    lines.append("")

    lines += ["## 按 L1 分类的字段分布", ""]
    l1_counter = df["table_l1"].value_counts().head(15)
    for l1, c in l1_counter.items():
        lines.append(f"- {l1}: {c}")
    lines.append("")

    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    df = build_features()
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PARQUET, index=False)
    write_diagnostic(df)
    print(f"\nParquet: {OUT_PARQUET}")
    print(f"诊断: {OUT_DIAG}")
    print(f"\n总结:")
    print(f"  字段总数: {len(df)}")
    print(f"  技术噪声: {df['is_technical_noise'].sum()} ({df['is_technical_noise'].mean()*100:.1f}%)")
    print(f"  有 VT 关联: {(df['related_vt_ids'].str.len() > 0).sum()}")
    print(f"  样例 pattern 命中: {(df['sample_patterns'].str.len() > 0).sum()}")


if __name__ == "__main__":
    main()
