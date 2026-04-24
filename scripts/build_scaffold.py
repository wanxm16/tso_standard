"""Generate virtual-table scaffold from the L1/L2 category tree.

Implements the splitting rules defined in 设计文档 11.6.1.
Output: output/virtual_tables_scaffold.yaml + .json
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
TREE_PATH = REPO_ROOT / "data" / "phrase_2" / "二期表分类树.json"
OUT_DIR = REPO_ROOT / "output"
OUT_YAML = OUT_DIR / "virtual_tables_scaffold.yaml"
OUT_JSON = OUT_DIR / "virtual_tables_scaffold.json"


TABLE_TYPE_KEYWORDS: dict[str, list[str]] = {
    "字典": ["字典", "行政区划"],
    "标签": ["名单", "标签", "管控", "重点人员", "关注对象", "出境未归", "走访"],
    "关系": ["关系", "图谱", "亲属", "婚姻", "同行同住", "共现", "圈层", "扩圈", "关联"],
    "主档": ["主档", "基本信息"],
}

AGGREGATE_TABLE_HINTS = [
    "最近", "最新", "最后", "整合", "汇总",
    "zui_hou", "zui_xin", "last", "latest", "final",
    "zhenghe", "huizong", "_zh_", "_df_",
]

GRAIN_DESC = {
    "主档": "一主体一行",
    "关系": "一对关系一行",
    "事件": "一次行为/事件一行",
    "标签": "一主体一标签一行",
    "聚合": "一主体最近一次/聚合一行",
    "字典": "一行一字典项",
}


def infer_table_type(l1_name: str, l2_name: str) -> str:
    text = f"{l2_name} {l1_name}"
    for ttype, kws in TABLE_TYPE_KEYWORDS.items():
        if any(kw in text for kw in kws):
            return ttype
    return "事件"


def is_aggregate_table(table_cn: str, table_en: str) -> bool:
    text = f"{table_cn or ''} {table_en or ''}".lower()
    return any(hint.lower() in text for hint in AGGREGATE_TABLE_HINTS)


def split_l2_into_vts(l1_name: str, l2_name: str, tables: list[dict], table_type: str) -> list[dict[str, Any]]:
    """Apply 11.6.1 splitting rules."""
    n = len(tables)

    if table_type == "字典":
        return []

    if n == 0:
        return []

    if n <= 2 or table_type in {"关系", "标签", "主档"}:
        return [_make_vt(l2_name, table_type, tables)]

    # 事件类（含其他默认走事件分支的）：检查是否需要拆"明细 + 聚合/最近一次"
    agg_tables = [t for t in tables if is_aggregate_table(t.get("cn", ""), t.get("en", ""))]
    detail_tables = [t for t in tables if t not in agg_tables]

    if agg_tables and detail_tables:
        return [
            _make_vt(f"{l2_name}-明细", "事件", detail_tables),
            _make_vt(f"{l2_name}-最近/聚合", "聚合", agg_tables),
        ]

    return [_make_vt(l2_name, table_type, tables)]


def _make_vt(topic: str, table_type: str, tables: list[dict]) -> dict[str, Any]:
    return {
        "topic": topic,
        "table_type": table_type,
        "grain_desc": GRAIN_DESC.get(table_type, "待定"),
        "candidate_tables": [
            {"cn": t.get("cn", ""), "en": t.get("en", "")} for t in tables
        ],
    }


def make_vt_id(topic: str, idx: int) -> str:
    h = hashlib.md5(topic.encode("utf-8")).hexdigest()[:8]
    return f"vt_{h}_{idx:03d}"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with TREE_PATH.open(encoding="utf-8") as f:
        tree = json.load(f)

    scaffold: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    idx = 0

    for l1 in tree:
        l1_name = l1.get("name", "")
        for l2 in l1.get("children", []) or []:
            l2_name = l2.get("name", "")
            tables = l2.get("tables", []) or []
            table_type = infer_table_type(l1_name, l2_name)
            vts = split_l2_into_vts(l1_name, l2_name, tables, table_type)
            if not vts:
                excluded.append({
                    "l1": l1_name,
                    "l2": l2_name,
                    "table_count": len(tables),
                    "reason": "字典维表暂不生成召回 VT" if table_type == "字典" else "无源表",
                })
                continue
            for vt in vts:
                idx += 1
                vt["vt_id"] = make_vt_id(vt["topic"], idx)
                vt["l2_path"] = [l1_name, l2_name]
                vt["source_table_count"] = len(vt["candidate_tables"])
                scaffold.append(vt)

    # 排序：按类型、L1、topic
    type_order = {"主档": 0, "关系": 1, "事件": 2, "聚合": 3, "标签": 4, "字典": 5}
    scaffold.sort(key=lambda x: (type_order.get(x["table_type"], 9), x["l2_path"][0], x["topic"]))

    by_type: dict[str, int] = {}
    by_domain: dict[str, int] = {}
    for vt in scaffold:
        by_type[vt["table_type"]] = by_type.get(vt["table_type"], 0) + 1
        by_domain[vt["l2_path"][0]] = by_domain.get(vt["l2_path"][0], 0) + 1

    output = {
        "stats": {
            "virtual_table_count": len(scaffold),
            "by_table_type": by_type,
            "by_domain": by_domain,
            "excluded_l2_count": len(excluded),
        },
        "excluded_l2": excluded,
        "virtual_tables": [
            {
                "vt_id": vt["vt_id"],
                "topic": vt["topic"],
                "table_type": vt["table_type"],
                "grain_desc": vt["grain_desc"],
                "l2_path": vt["l2_path"],
                "source_table_count": vt["source_table_count"],
                "candidate_tables": vt["candidate_tables"],
            }
            for vt in scaffold
        ],
    }

    with OUT_YAML.open("w", encoding="utf-8") as f:
        yaml.dump(output, f, allow_unicode=True, sort_keys=False, width=200)

    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(json.dumps({
        "virtual_table_count": len(scaffold),
        "by_table_type": by_type,
        "by_domain": by_domain,
        "excluded_l2_count": len(excluded),
        "yaml_path": str(OUT_YAML),
        "json_path": str(OUT_JSON),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
