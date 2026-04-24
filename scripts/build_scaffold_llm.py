"""对每个 L2 节点调 qwen3-max，让 LLM 判断应生成几张虚拟表，如何拆分。

- 输入：data/phrase_2/二期表分类树.json
- 每个 L2 调一次 LLM（json_mode），有缓存，重跑免费
- 输出：
  - output/virtual_tables_scaffold_llm.yaml  (LLM 版脚手架，人类可读)
  - output/virtual_tables_scaffold_llm.json  (机器可读)
  - output/scaffold_comparison.md            (LLM 版 vs 规则版的差异报告)
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat  # noqa: E402


TREE_PATH = REPO_ROOT / "data" / "phrase_2" / "二期表分类树.json"
OUT_DIR = REPO_ROOT / "output"
OUT_YAML = OUT_DIR / "virtual_tables_scaffold_llm.yaml"
OUT_JSON = OUT_DIR / "virtual_tables_scaffold_llm.json"
OUT_COMPARE = OUT_DIR / "scaffold_comparison.md"
RULE_YAML = OUT_DIR / "virtual_tables_scaffold.yaml"  # 规则版，用于对比


SYSTEM_PROMPT = """你是一位数据治理专家，任务是为一个 text2sql 表召回系统设计"虚拟表"。

核心定义：
- 虚拟表 = 一个业务主题 + 一个稳定的查询粒度
- 虚拟表面向自然语言问题的召回，不是物理合并后的执行表
- 一张虚拟表可以对应多张物理源表（多源合并）

五种虚拟表类型：
- 主档：一个主体一行（人员主档、机动车主档）
- 关系：两个主体的关系（婚姻、亲属、同行）
- 事件：一次行为/一条记录（出入境事件、加油事件、轨迹点）
- 标签：名单/重点对象（重点人员库、出境未归）
- 聚合：最近一次/最新状态/汇总（每人最新一次出入境、每车牌最后一条记录）
- 字典：行政区划、代码表等，通常不生成召回 VT

拆分原则（严格遵守）：
1. 同一主体 + 同一粒度的多张表 → 合并成 1 张 VT
2. 主体类型不同（如机动车 vs 电动自行车 vs 驾驶人） → 必须拆开
3. 粒度不同（明细 vs 最近一次/聚合） → 必须拆开
4. 如果某张表明显不属于当前 L2（例如驾驶人表被放到了"车辆主档"下），
   必须标注为 misplaced_tables，并建议归属到哪个分类

输出要求：
- 严格输出 JSON，字段与 schema 完全一致
- 每张 VT 的 source_tables 必须是输入表的 en（英文名）子集
- topic 要简洁、含粒度信息（如"机动车主档"、"机动车车牌最新一次"）
- reason 要说明为什么这几张表合并成一张（共同主体 + 共同粒度）"""


USER_PROMPT_TEMPLATE = """请评估以下分类节点应生成几张虚拟表。

L1 分类: {l1}
L2 分类: {l2}

该 L2 下的源表：
{tables}

请输出 JSON，schema 如下：
{{
  "virtual_tables": [
    {{
      "topic": "字符串",
      "table_type": "主档|关系|事件|标签|聚合|字典",
      "grain_desc": "字符串（粒度描述）",
      "source_tables": ["表的英文名", ...],
      "reason": "字符串"
    }}
  ],
  "misplaced_tables": [
    {{
      "en": "英文表名",
      "cn": "中文表名",
      "suggested_l2": "建议归属的 L2 名（可自由命名，优先用已有分类）",
      "reason": "字符串"
    }}
  ],
  "overall_note": "字符串（对本 L2 拆分判断的一句话总结）"
}}

若 L2 为字典维表类（不生成召回 VT），请返回 virtual_tables=[]，并在 overall_note 说明原因。"""


def format_tables_for_prompt(tables: list[dict]) -> str:
    lines = []
    for i, t in enumerate(tables, 1):
        lines.append(f"{i}. {t.get('cn', '')} | {t.get('en', '')}")
    return "\n".join(lines)


def ask_llm_for_l2(l1: str, l2: str, tables: list[dict]) -> dict[str, Any]:
    user_prompt = USER_PROMPT_TEMPLATE.format(
        l1=l1,
        l2=l2,
        tables=format_tables_for_prompt(tables),
    )
    raw = chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
        json_mode=True,
        use_cache=True,
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # 兜底：尝试剥 markdown 代码块
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)[1]
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip("`").strip()
        return json.loads(stripped)


def make_vt_id(topic: str, idx: int) -> str:
    h = hashlib.md5(topic.encode("utf-8")).hexdigest()[:8]
    return f"vt_{h}_{idx:03d}"


def validate_and_normalize(llm_output: dict[str, Any], l1: str, l2: str, tables: list[dict]) -> dict[str, Any]:
    valid_en = {t.get("en", "") for t in tables if t.get("en")}
    en_to_cn = {t.get("en", ""): t.get("cn", "") for t in tables}

    vts = llm_output.get("virtual_tables", []) or []
    misplaced = llm_output.get("misplaced_tables", []) or []

    # 过滤 LLM 可能编造的表名；附上中文名
    cleaned_vts = []
    all_covered_en = set()
    for vt in vts:
        sources = vt.get("source_tables", []) or []
        kept = [en for en in sources if en in valid_en]
        if not kept:
            continue
        cleaned_vts.append({
            "topic": vt.get("topic", ""),
            "table_type": vt.get("table_type", ""),
            "grain_desc": vt.get("grain_desc", ""),
            "source_tables": [{"en": en, "cn": en_to_cn.get(en, "")} for en in kept],
            "reason": vt.get("reason", ""),
        })
        all_covered_en.update(kept)

    cleaned_misplaced = []
    for m in misplaced:
        en = m.get("en", "")
        if en and en in valid_en:
            cleaned_misplaced.append({
                "en": en,
                "cn": en_to_cn.get(en, m.get("cn", "")),
                "suggested_l2": m.get("suggested_l2", ""),
                "reason": m.get("reason", ""),
            })

    # 诊断：是否有表既没被分配到 VT 也没标为 misplaced
    leftover = [
        {"en": t.get("en", ""), "cn": t.get("cn", "")}
        for t in tables
        if t.get("en") and t.get("en") not in all_covered_en
        and t.get("en") not in {m["en"] for m in cleaned_misplaced}
    ]

    return {
        "virtual_tables": cleaned_vts,
        "misplaced_tables": cleaned_misplaced,
        "leftover_tables": leftover,
        "overall_note": llm_output.get("overall_note", ""),
    }


def run_smoke_test() -> None:
    """先用"车辆主档"跑一个 smoke。"""
    with TREE_PATH.open(encoding="utf-8") as f:
        tree = json.load(f)

    target_l1, target_l2 = "主体主档", "车辆主档"
    tables = []
    for l1 in tree:
        if l1.get("name") != target_l1:
            continue
        for l2 in l1.get("children", []) or []:
            if l2.get("name") == target_l2:
                tables = l2.get("tables", []) or []
                break

    if not tables:
        print(f"找不到 {target_l1}/{target_l2}")
        return

    print(f"=== Smoke Test: {target_l1} / {target_l2}（{len(tables)} 张源表）===\n")
    raw = ask_llm_for_l2(target_l1, target_l2, tables)
    normalized = validate_and_normalize(raw, target_l1, target_l2, tables)
    print(json.dumps(normalized, ensure_ascii=False, indent=2))


def run_full(limit: int | None = None) -> None:
    with TREE_PATH.open(encoding="utf-8") as f:
        tree = json.load(f)

    # 收集所有 (l1, l2, tables)
    l2_list: list[tuple[str, str, list[dict]]] = []
    for l1 in tree:
        l1_name = l1.get("name", "")
        for l2 in l1.get("children", []) or []:
            l2_name = l2.get("name", "")
            tables = l2.get("tables", []) or []
            if tables:
                l2_list.append((l1_name, l2_name, tables))

    if limit:
        l2_list = l2_list[:limit]

    print(f"共 {len(l2_list)} 个 L2 节点需要调 LLM")

    results: list[dict[str, Any]] = []
    scaffold: list[dict[str, Any]] = []
    global_idx = 0

    t0 = time.time()
    for i, (l1_name, l2_name, tables) in enumerate(l2_list, 1):
        print(f"  [{i}/{len(l2_list)}] {l1_name} / {l2_name} ({len(tables)} 表) ...", end=" ", flush=True)
        t_start = time.time()
        try:
            raw = ask_llm_for_l2(l1_name, l2_name, tables)
            normalized = validate_and_normalize(raw, l1_name, l2_name, tables)
            dt = time.time() - t_start
            print(f"→ {len(normalized['virtual_tables'])} VT, {len(normalized['misplaced_tables'])} misplaced ({dt:.1f}s)")
        except Exception as exc:
            print(f"FAILED: {exc}")
            normalized = {
                "virtual_tables": [],
                "misplaced_tables": [],
                "leftover_tables": [{"en": t.get("en", ""), "cn": t.get("cn", "")} for t in tables],
                "overall_note": f"LLM 调用失败: {exc}",
            }

        results.append({
            "l1": l1_name,
            "l2": l2_name,
            "source_table_count": len(tables),
            **normalized,
        })

        for vt in normalized["virtual_tables"]:
            global_idx += 1
            scaffold.append({
                "vt_id": make_vt_id(vt["topic"], global_idx),
                "topic": vt["topic"],
                "table_type": vt["table_type"],
                "grain_desc": vt["grain_desc"],
                "l2_path": [l1_name, l2_name],
                "source_table_count": len(vt["source_tables"]),
                "candidate_tables": vt["source_tables"],
                "llm_reason": vt["reason"],
            })

    total_dt = time.time() - t0

    # 排序
    type_order = {"主档": 0, "关系": 1, "事件": 2, "聚合": 3, "标签": 4, "字典": 5}
    scaffold.sort(key=lambda x: (type_order.get(x["table_type"], 9), x["l2_path"][0], x["topic"]))

    by_type: dict[str, int] = {}
    by_domain: dict[str, int] = {}
    misplaced_total = 0
    for vt in scaffold:
        by_type[vt["table_type"]] = by_type.get(vt["table_type"], 0) + 1
        by_domain[vt["l2_path"][0]] = by_domain.get(vt["l2_path"][0], 0) + 1
    for r in results:
        misplaced_total += len(r["misplaced_tables"])

    output = {
        "stats": {
            "virtual_table_count": len(scaffold),
            "by_table_type": by_type,
            "by_domain": by_domain,
            "misplaced_count": misplaced_total,
            "llm_total_seconds": round(total_dt, 1),
        },
        "per_l2_analysis": results,
        "virtual_tables": scaffold,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_YAML.open("w", encoding="utf-8") as f:
        yaml.dump(output, f, allow_unicode=True, sort_keys=False, width=200)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 生成对比报告
    write_comparison_report(scaffold, results)

    print(f"\n=== LLM 版脚手架生成完成 ===")
    print(f"  虚拟表数量: {len(scaffold)}")
    print(f"  按类型分布: {by_type}")
    print(f"  misplaced 表: {misplaced_total}")
    print(f"  总耗时: {total_dt:.1f}s")
    print(f"  输出: {OUT_YAML}")
    print(f"  对比报告: {OUT_COMPARE}")


def write_comparison_report(llm_scaffold: list[dict], per_l2_results: list[dict]) -> None:
    # 读取规则版
    rule_data = None
    if RULE_YAML.exists():
        with RULE_YAML.open(encoding="utf-8") as f:
            rule_data = yaml.safe_load(f)

    lines: list[str] = [
        "# 脚手架对比报告：规则版 vs LLM 版",
        "",
        "## 总体对比",
        "",
    ]
    if rule_data:
        rule_count = rule_data["stats"]["virtual_table_count"]
        rule_by_type = rule_data["stats"]["by_table_type"]
        lines += [
            f"| 维度 | 规则版 | LLM 版 |",
            f"| --- | --- | --- |",
            f"| 虚拟表总数 | {rule_count} | {len(llm_scaffold)} |",
        ]
        all_types = set(rule_by_type.keys()) | {vt["table_type"] for vt in llm_scaffold}
        llm_by_type: dict[str, int] = {}
        for vt in llm_scaffold:
            llm_by_type[vt["table_type"]] = llm_by_type.get(vt["table_type"], 0) + 1
        for t in sorted(all_types):
            lines.append(f"| {t} | {rule_by_type.get(t, 0)} | {llm_by_type.get(t, 0)} |")
    else:
        lines.append(f"LLM 版虚拟表总数: {len(llm_scaffold)}")

    lines += [
        "",
        "## LLM 识别出的 misplaced 表（应从当前 L2 移出）",
        "",
    ]
    any_misplaced = False
    for r in per_l2_results:
        if r["misplaced_tables"]:
            any_misplaced = True
            lines.append(f"### {r['l1']} / {r['l2']}")
            lines.append("")
            for m in r["misplaced_tables"]:
                lines.append(f"- `{m['en']}` ({m['cn']}) → 建议归属: **{m['suggested_l2']}**")
                lines.append(f"  - 理由: {m['reason']}")
            lines.append("")
    if not any_misplaced:
        lines.append("（无）\n")

    lines += [
        "## 拆分差异显著的 L2（LLM 拆出 ≥2 张，规则版 1 张）",
        "",
    ]
    for r in per_l2_results:
        vts = r["virtual_tables"]
        if len(vts) >= 2:
            lines.append(f"### {r['l1']} / {r['l2']}（{r['source_table_count']} 表 → LLM 拆 {len(vts)} 张）")
            lines.append("")
            for vt in vts:
                src = ", ".join([s["en"] for s in vt["source_tables"]])
                lines.append(f"- **{vt['topic']}** [{vt['table_type']}] — {vt['grain_desc']}")
                lines.append(f"  - 源表: {src}")
                lines.append(f"  - 理由: {vt['reason']}")
            if r["overall_note"]:
                lines.append(f"- _总结_: {r['overall_note']}")
            lines.append("")

    lines += [
        "## LLM 判定不生成 VT 的 L2（字典维表等）",
        "",
    ]
    for r in per_l2_results:
        if not r["virtual_tables"] and not r["misplaced_tables"]:
            lines.append(f"- {r['l1']} / {r['l2']}（{r['source_table_count']} 表）: {r['overall_note']}")
    lines.append("")

    with OUT_COMPARE.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "smoke":
        run_smoke_test()
    elif len(sys.argv) > 1 and sys.argv[1].startswith("limit="):
        n = int(sys.argv[1].split("=", 1)[1])
        run_full(limit=n)
    else:
        run_full()
