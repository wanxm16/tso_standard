"""融合规则版 + LLM 版，产出定稿脚手架。

融合规则（由用户确认，2026-04-21）：
1. LLM 识别的 misplaced 表不做物理移动，在当前 L2 下按 suggested_l2 分组生成新 VT
2. 采纳 LLM 的跨主体拆分（车辆主档、场所与机构、落脚点分析等）
3. 采纳 LLM 的按人群主题拆分（出入境按人群类型）
4. 拒绝明显过拆的几处，合并回合理粒度

过拆合并白名单（key=L1/L2）：
- 关系扩圈 / 共现关系：7 张合 1 张
- 设备感知与网安 / 小区门禁与感知采集：3 张合 1 张（同源表不拆）
- 出入境与境外 / 签证信息：2 张合 1 张（都是外籍人签证）
- 主体主档 / 证件与照片：3 张合 1 张
- 时空轨迹 / 地感轨迹：按主体合并（人轨迹 / 车轨迹 2 张，而非 4 张）
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
LLM_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_llm.json"
OUT_YAML = REPO_ROOT / "output" / "virtual_tables_scaffold_final.yaml"
OUT_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"
OUT_REPORT = REPO_ROOT / "output" / "scaffold_final_report.md"


# === 过拆合并规则 ===

MERGE_ALL_RULES: dict[tuple[str, str], dict[str, str]] = {
    ("关系扩圈", "共现关系"): {
        "topic": "人员共现关系",
        "table_type": "关系",
        "grain_desc": "两主体在同一场景（WiFi/加油/暂住/地感/设备/违章/同学等）共现一行",
        "merge_reason": "各场景共现语义同源，召回层不必拆 7 张；字段差异在下钻层解决",
    },
    ("设备感知与网安", "小区门禁与感知采集"): {
        "topic": "三张网小区感知",
        "table_type": "事件",
        "grain_desc": "一次小区进出/感知采集一行",
        "merge_reason": "LLM 把同一张源表拆成了 3 张 VT，同源表不能拆成多张 VT",
    },
    ("出入境与境外", "签证信息"): {
        "topic": "外籍人签证信息",
        "table_type": "主档",
        "grain_desc": "每位外籍/港澳台人员一条签证记录",
        "merge_reason": "仅'是否含港澳台'就拆过细，主体都是外籍签证",
    },
    ("主体主档", "证件与照片"): {
        "topic": "人员证件与照片",
        "table_type": "主档",
        "grain_desc": "一人一行，含证件信息与照片链接",
        "merge_reason": "证件主档与照片聚合可合为一张人员扩展主档",
    },
}

# 地感轨迹按主体合并（custom）：机动车 1 张、人员 1 张
DGZ_KEY = ("时空轨迹", "地感轨迹")
DGZ_VEHICLE_HINTS = ["cl", "机动车", "vehicle", "车"]
DGZ_PERSON_HINTS = ["ry", "人员", "person", "ren", "htdg"]


# === 工具函数 ===


def make_vt_id(topic: str, idx: int) -> str:
    h = hashlib.md5(topic.encode("utf-8")).hexdigest()[:8]
    return f"vt_{h}_{idx:03d}"


def merge_vts(vts: list[dict], topic: str, table_type: str, grain_desc: str, merge_reason: str) -> list[dict]:
    """把多张 VT 合并成一张，保留所有 source_tables 去重。"""
    all_sources: list[dict] = []
    seen_en: set[str] = set()
    all_reasons: list[str] = []
    for vt in vts:
        for s in vt.get("source_tables", []):
            if s["en"] not in seen_en:
                all_sources.append(s)
                seen_en.add(s["en"])
        if vt.get("reason"):
            all_reasons.append(f"原『{vt['topic']}』: {vt['reason']}")
    if not all_sources:
        return []
    return [{
        "topic": topic,
        "table_type": table_type,
        "grain_desc": grain_desc,
        "source_tables": all_sources,
        "reason": merge_reason + " | 合并前 LLM 拆分理由：" + " || ".join(all_reasons),
        "merge_note": f"由 LLM 版 {len(vts)} 张 VT 合并而来",
    }]


def split_dgz_by_subject(vts: list[dict]) -> list[dict]:
    """地感轨迹按主体（车/人）合并成 2 张。"""
    vehicle_sources: list[dict] = []
    person_sources: list[dict] = []
    seen_en: set[str] = set()

    for vt in vts:
        topic = vt.get("topic", "")
        for s in vt.get("source_tables", []):
            if s["en"] in seen_en:
                continue
            seen_en.add(s["en"])
            combined = f"{topic} {s.get('cn', '')} {s.get('en', '')}".lower()
            is_vehicle = any(h.lower() in combined for h in DGZ_VEHICLE_HINTS) and not any(
                h.lower() in combined for h in ["ry", "人员", "person"]
            )
            if is_vehicle:
                vehicle_sources.append(s)
            else:
                person_sources.append(s)

    result = []
    if vehicle_sources:
        result.append({
            "topic": "机动车地感轨迹",
            "table_type": "事件",
            "grain_desc": "每次地感设备捕获的机动车轨迹记录一行",
            "source_tables": vehicle_sources,
            "reason": "按主体合并：地感轨迹下的机动车事件归一张 VT",
            "merge_note": "地感轨迹按主体（车）合并",
        })
    if person_sources:
        result.append({
            "topic": "人员地感轨迹",
            "table_type": "事件",
            "grain_desc": "每次地感设备捕获的人员轨迹记录一行（含实时/历史/模型）",
            "source_tables": person_sources,
            "reason": "按主体合并：地感轨迹下的人员事件（实时/历史/模型）归一张 VT，时效性在字段层区分",
            "merge_note": "地感轨迹按主体（人）合并，不按时效拆",
        })
    return result


def build_misplaced_vts(misplaced: list[dict], l1: str, l2: str) -> list[dict]:
    """按 suggested_l2 分组生成 VT，完全停留在原 L2 下（l2_path 不变）。

    按用户决策（2026-04-21）：
    - 不做物理迁移，l2_path 严格保持当前 L2
    - topic 直接用 suggested_l2 作主题名，不加"暂留于 XX"措辞（避免误解为已迁移）
    - LLM 的建议归属保留在 review_hint 字段，后续 review 时可读
    """
    if not misplaced:
        return []
    grouped: dict[str, list[dict]] = {}
    for m in misplaced:
        key = m.get("suggested_l2", "未指定")
        grouped.setdefault(key, []).append(m)

    result = []
    for suggested_l2, items in grouped.items():
        sources = [{"en": m["en"], "cn": m["cn"]} for m in items]
        reasons = [f"{m['cn']}: {m.get('reason', '')}" for m in items]
        result.append({
            "topic": suggested_l2,
            "table_type": "待定",
            "grain_desc": "待定（后续 review 决定粒度与类型）",
            "source_tables": sources,
            "reason": f"LLM 建议归属『{suggested_l2}』，按用户决策不迁移，保留在 L2『{l2}』下。LLM 原因：" + " | ".join(reasons),
            "review_hint": {
                "llm_suggested_l2": suggested_l2,
                "current_l2": l2,
                "items": items,
            },
        })
    return result


# === 主流程 ===


def main() -> None:
    if not LLM_JSON.exists():
        print(f"未找到 {LLM_JSON}，请先运行 build_scaffold_llm.py")
        sys.exit(1)

    with LLM_JSON.open(encoding="utf-8") as f:
        llm_data = json.load(f)

    per_l2 = llm_data["per_l2_analysis"]

    final_scaffold: list[dict] = []
    merge_events: list[dict] = []
    misplaced_events: list[dict] = []
    global_idx = 0

    for record in per_l2:
        l1 = record["l1"]
        l2 = record["l2"]
        key = (l1, l2)
        raw_vts = record["virtual_tables"]
        misplaced = record["misplaced_tables"]

        # 1. 过拆合并
        if key in MERGE_ALL_RULES:
            rule = MERGE_ALL_RULES[key]
            merged = merge_vts(
                raw_vts,
                topic=rule["topic"],
                table_type=rule["table_type"],
                grain_desc=rule["grain_desc"],
                merge_reason=rule["merge_reason"],
            )
            merge_events.append({
                "l1": l1, "l2": l2,
                "llm_vt_count": len(raw_vts),
                "final_vt_count": len(merged),
                "rule": "merge_all",
                "topic": rule["topic"],
            })
            vts = merged
        elif key == DGZ_KEY:
            merged = split_dgz_by_subject(raw_vts)
            merge_events.append({
                "l1": l1, "l2": l2,
                "llm_vt_count": len(raw_vts),
                "final_vt_count": len(merged),
                "rule": "merge_by_subject",
                "topic": "人/车分离",
            })
            vts = merged
        else:
            vts = raw_vts

        # 2. misplaced → 生成当前 L2 下的新 VT
        misplaced_vts = build_misplaced_vts(misplaced, l1, l2)
        if misplaced_vts:
            misplaced_events.append({
                "l1": l1, "l2": l2,
                "misplaced_table_count": len(misplaced),
                "new_vt_count": len(misplaced_vts),
            })

        all_vts = vts + misplaced_vts

        for vt in all_vts:
            global_idx += 1
            final_scaffold.append({
                "vt_id": make_vt_id(vt["topic"], global_idx),
                "topic": vt["topic"],
                "table_type": vt["table_type"],
                "grain_desc": vt["grain_desc"],
                "l2_path": [l1, l2],
                "source_table_count": len(vt["source_tables"]),
                "candidate_tables": vt["source_tables"],
                "reason": vt.get("reason", ""),
                **({"merge_note": vt["merge_note"]} if "merge_note" in vt else {}),
                **({"review_hint": vt["review_hint"]} if "review_hint" in vt else {}),
            })

    # 排除不适合做 VT 的 L1 类别（字典/维表是参考数据，不应作为虚拟表）
    EXCLUDED_L1 = {"字典维表"}
    before_count = len(final_scaffold)
    final_scaffold = [
        vt for vt in final_scaffold
        if not (vt.get("l2_path") and vt["l2_path"][0] in EXCLUDED_L1)
    ]
    if before_count != len(final_scaffold):
        print(f"[prune] 排除 L1={EXCLUDED_L1} 共 {before_count - len(final_scaffold)} 张 VT，剩 {len(final_scaffold)} 张")

    # 排序
    type_order = {"主档": 0, "关系": 1, "事件": 2, "聚合": 3, "标签": 4, "待定": 8, "字典": 9}
    final_scaffold.sort(key=lambda x: (type_order.get(x["table_type"], 9), x["l2_path"][0], x["topic"]))

    # 统计
    by_type: dict[str, int] = {}
    by_domain: dict[str, int] = {}
    for vt in final_scaffold:
        by_type[vt["table_type"]] = by_type.get(vt["table_type"], 0) + 1
        by_domain[vt["l2_path"][0]] = by_domain.get(vt["l2_path"][0], 0) + 1

    output = {
        "stats": {
            "virtual_table_count": len(final_scaffold),
            "by_table_type": by_type,
            "by_domain": by_domain,
            "merge_event_count": len(merge_events),
            "misplaced_event_count": len(misplaced_events),
        },
        "merge_events": merge_events,
        "misplaced_events": misplaced_events,
        "virtual_tables": final_scaffold,
    }

    with OUT_YAML.open("w", encoding="utf-8") as f:
        yaml.dump(output, f, allow_unicode=True, sort_keys=False, width=200)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 生成最终报告
    write_final_report(output)

    print(json.dumps({
        "final_virtual_table_count": len(final_scaffold),
        "by_table_type": by_type,
        "by_domain": by_domain,
        "merge_events": len(merge_events),
        "misplaced_events": len(misplaced_events),
        "yaml_path": str(OUT_YAML),
        "report_path": str(OUT_REPORT),
    }, ensure_ascii=False, indent=2))


def write_final_report(output: dict) -> None:
    lines: list[str] = [
        "# 虚拟表脚手架定稿报告",
        "",
        f"- 虚拟表总数: **{output['stats']['virtual_table_count']}**",
        f"- 按类型: {output['stats']['by_table_type']}",
        f"- 按 domain: {output['stats']['by_domain']}",
        f"- 合并事件: {output['stats']['merge_event_count']}",
        f"- misplaced 转生成事件: {output['stats']['misplaced_event_count']}",
        "",
        "## 过拆合并记录（LLM 判断过细，已合并）",
        "",
    ]
    for e in output["merge_events"]:
        lines.append(f"- **{e['l1']} / {e['l2']}**：LLM {e['llm_vt_count']} 张 → 定稿 {e['final_vt_count']} 张（{e['rule']}）")
    lines.append("")

    lines += [
        "## misplaced 转本地 VT 记录（暂不迁移，保留在原 L2 下）",
        "",
    ]
    for e in output["misplaced_events"]:
        lines.append(f"- **{e['l1']} / {e['l2']}**：{e['misplaced_table_count']} 张 misplaced 表 → 在本 L2 下新增 {e['new_vt_count']} 张 VT")
    lines.append("")

    lines += [
        "## 全量虚拟表清单（按类型分组）",
        "",
    ]
    type_order = ["主档", "关系", "事件", "聚合", "标签", "待定"]
    vts_by_type: dict[str, list] = {}
    for vt in output["virtual_tables"]:
        vts_by_type.setdefault(vt["table_type"], []).append(vt)

    for t in type_order:
        if t not in vts_by_type:
            continue
        lines.append(f"### {t}（{len(vts_by_type[t])} 张）")
        lines.append("")
        for vt in vts_by_type[t]:
            tag = ""
            if "merge_note" in vt:
                tag = " 🔗"
            elif "review_hint" in vt:
                tag = " ⚠️"
            lines.append(f"- `{vt['vt_id']}`{tag} **{vt['topic']}** · {' / '.join(vt['l2_path'])} · {vt['source_table_count']} 源表")
            lines.append(f"  - 粒度: {vt['grain_desc']}")
        lines.append("")

    lines.append("> 🔗 = 由 LLM 拆分合并而来；⚠️ = 由 misplaced 表生成的待重归类 VT")

    with OUT_REPORT.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
