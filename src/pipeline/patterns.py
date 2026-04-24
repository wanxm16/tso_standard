"""样例值正则模式库（I-02 特征提取使用）。

模式来自 data/slot_library/base_slots.yaml 的 sample_patterns +
若干通用扩展（技术噪声识别 / 短枚举 / 中文名候选 等）。

match_patterns(value) 返回命中的 pattern 名列表（按语义优先级）。
"""
from __future__ import annotations

import re
from typing import Pattern


PATTERN_SOURCES: list[tuple[str, str]] = [
    # ====== 强特征（直接决定 logical_type）======
    ("id_card_18", r"^[1-9]\d{5}(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx]$"),
    ("id_card_15", r"^[1-9]\d{5}\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}$"),
    ("cn_mobile", r"^1[3-9]\d{9}$"),
    ("cn_plate_blue", r"^[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][A-HJ-NP-Z0-9]{4,5}$"),
    ("cn_plate_green", r"^[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][DF][A-HJ-NP-Z0-9]{5}$"),
    ("cn_passport", r"^[EeGgPpSsDdMm]\d{8}$"),
    ("generic_passport", r"^[A-Z]\d{7,9}$"),
    ("vin", r"^[A-HJ-NPR-Z0-9]{17}$"),
    ("imei", r"^\d{14,16}$"),
    ("mac", r"^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$"),
    ("social_credit", r"^[0-9A-HJ-NPQRTUWXY]{2}\d{6}[0-9A-HJ-NPQRTUWXY]{10}$"),

    # ====== 时间 ======
    ("datetime_iso", r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"),
    ("datetime_compact", r"^\d{14}$"),
    ("timestamp_ms", r"^\d{13}$"),
    ("timestamp_s", r"^\d{10}$"),
    ("date_iso", r"^\d{4}-\d{2}-\d{2}$"),
    ("date_compact", r"^\d{8}$"),

    # ====== 行政区划 ======
    ("region_code_6", r"^\d{6}$"),
    ("region_code_9", r"^\d{9}$"),
    ("region_code_12", r"^\d{12}$"),

    # ====== 坐标 ======
    ("longitude_decimal", r"^-?(1[0-7]\d|180|\d{1,2})(\.\d+)?$"),
    ("latitude_decimal", r"^-?([0-8]\d|90)(\.\d+)?$"),

    # ====== 金额 / 数值 ======
    ("decimal_money", r"^-?\d{1,10}\.\d{1,4}$"),
    ("integer_short", r"^\d{1,8}$"),
    ("integer_long", r"^\d{9,}$"),

    # ====== 哈希 / 密文（常见于脱敏或主键 MD5）======
    ("md5_hex", r"^[a-f0-9]{32}$"),
    ("hash_encoded", r"^#[A-Za-z0-9+/=]{10,}$"),  # 观察到 data 里用 #... 前缀表脱敏

    # ====== 通用格式 ======
    ("all_digit", r"^\d+$"),
    ("all_lower_letter", r"^[a-z]+$"),
    ("all_upper_letter", r"^[A-Z]+$"),
    ("all_alnum", r"^[A-Za-z0-9]+$"),

    # ====== 中文相关 ======
    ("all_chinese_2_4", r"^[一-鿿]{2,4}$"),
    ("all_chinese", r"^[一-鿿]+$"),
    ("chinese_with_punct", r"^[一-鿿\w\s，,。.（）()：:、·\-/]+$"),
]


_COMPILED: list[tuple[str, Pattern]] = [(n, re.compile(r)) for n, r in PATTERN_SOURCES]


# 空值标记
NULL_TOKENS = {"", "null", "Null", "NULL", "NaN", "nan", "None", "\\N", "-", "--"}


def match_patterns_single(value: str) -> list[str]:
    """对一个样例值，返回所有命中的 pattern 名（按定义顺序）。"""
    if value is None:
        return []
    v = str(value).strip()
    if v in NULL_TOKENS:
        return ["null_or_empty"]
    hits: list[str] = []
    for name, pat in _COMPILED:
        if pat.match(v):
            hits.append(name)
    return hits


def match_patterns_multi(values: list[str], *, top_k_vote: int = 2) -> list[str]:
    """对多个样例值，返回"多数命中"的 pattern 名。

    规则：
    - 对每个非空样例值命中的 pattern 投票
    - 取得票数 >= ceil(非空样例数 * 0.5) 的 pattern
    - 最多返回 top_k_vote 个（按票数降序）
    """
    from collections import Counter
    import math

    non_null_count = 0
    counter: Counter[str] = Counter()
    for v in values:
        hits = match_patterns_single(v)
        if hits and hits != ["null_or_empty"]:
            non_null_count += 1
            for h in hits:
                counter[h] += 1

    if non_null_count == 0:
        return ["all_null_or_empty"]

    threshold = max(1, math.ceil(non_null_count * 0.5))
    winners = [(name, c) for name, c in counter.items() if c >= threshold]
    winners.sort(key=lambda x: -x[1])

    # 去除过泛的 pattern（如果已有更强 pattern 命中）
    strong_patterns = {
        "id_card_18", "id_card_15", "cn_mobile", "cn_plate_blue", "cn_plate_green",
        "cn_passport", "generic_passport", "vin", "imei", "mac", "social_credit",
        "datetime_iso", "datetime_compact", "timestamp_ms", "timestamp_s",
        "date_iso", "date_compact", "md5_hex", "hash_encoded", "all_chinese_2_4",
    }
    weak_patterns = {
        "all_digit", "all_alnum", "all_lower_letter", "all_upper_letter",
        "integer_short", "integer_long", "all_chinese", "chinese_with_punct",
        "region_code_6", "region_code_9", "region_code_12",  # 这些和 all_digit 有重叠
    }
    winner_names = {n for n, _ in winners}
    if winner_names & strong_patterns:
        winners = [w for w in winners if w[0] in strong_patterns or w[0] not in weak_patterns]

    return [n for n, _ in winners[:top_k_vote]]


# 样例值长度分布统计


def sample_length_stats(values: list[str]) -> dict:
    lens = [len(str(v)) for v in values if str(v).strip() not in NULL_TOKENS]
    if not lens:
        return {"min": 0, "max": 0, "avg": 0.0}
    return {"min": min(lens), "max": max(lens), "avg": round(sum(lens) / len(lens), 2)}


def sample_char_type(values: list[str]) -> str:
    import re as _re
    non_null = [str(v) for v in values if str(v).strip() not in NULL_TOKENS]
    if not non_null:
        return "unknown"
    joined = "".join(non_null)
    has_digit = bool(_re.search(r"\d", joined))
    has_cn = bool(_re.search(r"[一-鿿]", joined))
    has_letter = bool(_re.search(r"[A-Za-z]", joined))
    count = sum([has_digit, has_cn, has_letter])
    if count == 0:
        return "unknown"
    if count >= 2:
        return "mixed"
    if has_digit:
        return "digit"
    if has_cn:
        return "chinese"
    if has_letter:
        return "alnum"
    return "unknown"
