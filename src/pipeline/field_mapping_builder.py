"""I-07: 源字段映射 + 别名扩展（对应设计文档 § 5.3 + § 10.4 + § 13.4）。

两个产物：
  1. virtual_field_mappings.json：虚拟字段 → 物理字段 1:N 映射
  2. field_aliases.json：虚拟字段的同义词 + 问题表达（LLM 扩展）
"""
from __future__ import annotations

import concurrent.futures
import json
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat  # noqa: E402


VFS_PARQUET = REPO_ROOT / "output" / "virtual_fields.parquet"
NORM_PARQUET = REPO_ROOT / "output" / "field_normalization.parquet"

OUT_MAPPINGS = REPO_ROOT / "output" / "virtual_field_mappings.json"
OUT_MAPPINGS_PARQUET = REPO_ROOT / "output" / "virtual_field_mappings.parquet"
OUT_ALIASES = REPO_ROOT / "output" / "field_aliases.json"
OUT_DIAG = REPO_ROOT / "output" / "virtual_field_mappings_diagnostic.md"


# ============ Mapping 提取 ============


def build_mappings(vfs_df: pd.DataFrame, norm_df: pd.DataFrame) -> list[dict]:
    """从归一结果提取每个 virtual_field 的源字段映射。"""
    # 仅使用 auto_accepted + needs_review（conflict / low_confidence 不进稳定映射）
    eligible = norm_df[norm_df["review_status"].isin(["auto_accepted", "needs_review"])].copy()

    mappings: list[dict] = []
    # 按 (vt_id, selected_slot) 分组生成 mappings
    for (vt_id, slot_name), grp in eligible.groupby(["vt_id", "selected_slot"]):
        vf_row = vfs_df[(vfs_df["vt_id"] == vt_id) & (vfs_df["field_name"] == slot_name)]
        if vf_row.empty:
            continue
        vf_id = vf_row.iloc[0]["vf_id"]

        # 按 score 降序排
        ordered = grp.sort_values("selected_score", ascending=False)
        for priority, (_, row) in enumerate(ordered.iterrows(), start=1):
            score = float(row["selected_score"])
            applied_llm = bool(row.get("applied_llm", False))
            llm_trigger = row.get("llm_trigger")
            status = row["review_status"]

            if applied_llm and llm_trigger == "B":
                mapping_type = "llm_adjusted"
            elif score >= 0.65:
                mapping_type = "direct"
            else:
                mapping_type = "synonym_best"

            mappings.append({
                "vf_id": vf_id,
                "vt_id": vt_id,
                "slot_name": slot_name,
                "source_table": row["table_en"],
                "source_field": row["field_name"],
                "source_comment": row.get("field_comment", ""),
                "mapping_type": mapping_type,
                "transform_rule": "",
                "priority": priority,
                "confidence": round(score, 4),
                "mapping_scope": status,
                "evidence": {
                    "review_status": status,
                    "top1_score": score,
                    "applied_llm": applied_llm,
                    "llm_trigger": llm_trigger,
                },
            })
    return mappings


# ============ LLM 别名扩展 ============


ALIAS_SYSTEM = """你是数据治理专家。任务：为一个语义字段槽位生成"常用中文表达"和"用户可能的提问方式"。

要求：
- 3-5 个自然表达（aliases）：人说话时会用的同义表述，不含拼音缩写
- 2-3 个"问题词"（question_words）：普通用户在提问时可能的问法，完整短句
- 严格输出 JSON，无任何多余文字
"""


ALIAS_USER_TEMPLATE = """## 槽位信息
- 字段名（英文 slot）：{name}
- 中文名：{cn_name}
- 逻辑类型：{logical_type}
- 角色：{role}
- 描述：{description}
- 已有 aliases：{seed_aliases}
- 所属虚拟表主题：{vt_topic}

请输出 JSON：
{{
  "llm_aliases": ["...", ...],
  "question_words": ["...", ...]
}}"""


def build_alias_prompt(vf: dict) -> tuple[str, str]:
    user = ALIAS_USER_TEMPLATE.format(
        name=vf["field_name"],
        cn_name=vf["field_cn_name"],
        logical_type=vf.get("logical_type", ""),
        role=vf.get("field_role", ""),
        description=vf.get("description", "") or "(无)",
        seed_aliases=", ".join(list(vf.get("aliases", []))[:8]),
        vt_topic=vf["vt_topic"],
    )
    return ALIAS_SYSTEM, user


def call_alias_llm(vf: dict) -> dict:
    sys_p, user_p = build_alias_prompt(vf)
    raw = chat(
        messages=[{"role": "system", "content": sys_p}, {"role": "user", "content": user_p}],
        temperature=0.0,
        json_mode=True,
        use_cache=True,
    )
    try:
        result = json.loads(raw)
    except Exception:
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)[1]
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip("`").strip()
        try:
            result = json.loads(stripped)
        except Exception:
            result = {"llm_aliases": [], "question_words": []}
    return {
        "llm_aliases": [a for a in (result.get("llm_aliases") or []) if a],
        "question_words": [q for q in (result.get("question_words") or []) if q],
    }


def build_aliases_concurrent(vfs_df: pd.DataFrame, concurrency: int = 10) -> list[dict]:
    """对 essential + frequent 虚拟字段调 LLM 扩展别名。"""
    candidates = vfs_df[vfs_df["importance_tier"].isin(["essential", "frequent"])].to_dict("records")
    print(f"  LLM 扩展候选: {len(candidates)} 个虚拟字段（essential+frequent）")

    results: dict[int, dict] = {}
    lock = threading.Lock()
    counter = {"done": 0}
    t0 = time.time()

    def worker(idx: int, vf: dict) -> tuple[int, dict]:
        try:
            r = call_alias_llm(vf)
        except Exception as exc:
            r = {"llm_aliases": [], "question_words": [], "_error": str(exc)}
        with lock:
            counter["done"] += 1
            if counter["done"] % 50 == 0 or counter["done"] == len(candidates):
                print(f"  LLM 别名 {counter['done']}/{len(candidates)} ({time.time()-t0:.1f}s)", flush=True)
        return idx, r

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = [exe.submit(worker, i, vf) for i, vf in enumerate(candidates)]
        for f in concurrent.futures.as_completed(futures):
            idx, r = f.result()
            results[idx] = r

    # 组装输出
    out: list[dict] = []
    for i, vf in enumerate(candidates):
        r = results.get(i, {})
        out.append({
            "vf_id": vf["vf_id"],
            "vt_id": vf["vt_id"],
            "vt_topic": vf["vt_topic"],
            "field_name": vf["field_name"],
            "field_cn_name": vf["field_cn_name"],
            "importance_tier": vf["importance_tier"],
            "seed_aliases": list(vf.get("aliases", [])),
            "llm_aliases": r.get("llm_aliases", []),
            "question_words": r.get("question_words", []),
        })
    return out


# ============ Diagnostic ============


def write_diagnostic(mappings: list[dict], aliases: list[dict], vfs_df: pd.DataFrame) -> None:
    lines = [
        "# I-07 虚拟字段映射 + 别名扩展诊断",
        "",
        f"- 虚拟字段总数: {len(vfs_df)}",
        f"- mapping 总数: {len(mappings)}",
        f"- 扩展 aliases 的虚拟字段数: {len(aliases)}",
        "",
        "## mapping_type 分布",
        "",
    ]
    mt = Counter(m["mapping_type"] for m in mappings)
    for k, c in mt.items():
        lines.append(f"- {k}: {c}")
    lines.append("")

    lines += ["## mapping_scope 分布（review_status）", ""]
    ms = Counter(m["mapping_scope"] for m in mappings)
    for k, c in ms.items():
        lines.append(f"- {k}: {c}")
    lines.append("")

    # 每个 vf 平均几个 source
    df_m = pd.DataFrame(mappings)
    if not df_m.empty:
        per_vf = df_m.groupby("vf_id").size()
        lines += [
            "## 每个 vf 的 source 数分布",
            "",
            f"- 中位数: {int(per_vf.median())}",
            f"- 平均: {per_vf.mean():.2f}",
            f"- 最大: {int(per_vf.max())}",
            f"- 仅 1 source 的 vf 数: {int((per_vf == 1).sum())}",
            f"- ≥10 source 的 vf 数: {int((per_vf >= 10).sum())}",
            "",
        ]

    # essential 覆盖
    essential_vfs = vfs_df[vfs_df["importance_tier"] == "essential"]["vf_id"].tolist()
    if mappings:
        covered = set(df_m["vf_id"])
        not_covered = [vf for vf in essential_vfs if vf not in covered]
        lines += [
            "## essential 虚拟字段覆盖检查",
            "",
            f"- essential 总数: {len(essential_vfs)}",
            f"- 有 mapping 的 essential: {len(essential_vfs) - len(not_covered)}",
            f"- 无 mapping 的 essential: {len(not_covered)}",
            "",
        ]
        if not_covered:
            lines.append("### 无 mapping 的 essential（前 15）")
            for vf_id in not_covered[:15]:
                lines.append(f"- {vf_id}")
            lines.append("")

    # aliases 扩展抽样
    lines += ["## LLM 扩展 aliases 抽样（前 10）", ""]
    for a in aliases[:10]:
        lines.append(
            f"- **{a['field_cn_name']}** ({a['field_name']}) @ {a['vt_topic']}"
            f" → aliases: {a['llm_aliases']} / questions: {a['question_words']}"
        )
    lines.append("")

    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


# ============ 主流程 ============


def main(enable_llm: bool = True, concurrency: int = 10) -> None:
    print("加载输入...")
    vfs_df = pd.read_parquet(VFS_PARQUET)
    norm_df = pd.read_parquet(NORM_PARQUET)
    print(f"  virtual_fields: {len(vfs_df)}")
    print(f"  field_normalization: {len(norm_df)}")

    # 1. 提取 mappings
    print("构建 mappings...")
    t0 = time.time()
    mappings = build_mappings(vfs_df, norm_df)
    print(f"  mappings: {len(mappings)} ({time.time()-t0:.1f}s)")

    # 2. 写 mappings
    OUT_MAPPINGS.parent.mkdir(parents=True, exist_ok=True)
    with OUT_MAPPINGS.open("w", encoding="utf-8") as f:
        json.dump({"virtual_field_mappings": mappings}, f, ensure_ascii=False, indent=2)
    pd.DataFrame([{**m, "evidence": json.dumps(m["evidence"], ensure_ascii=False)} for m in mappings]).to_parquet(OUT_MAPPINGS_PARQUET, index=False)

    # 3. LLM 别名扩展
    if enable_llm:
        print("LLM 扩展别名...")
        aliases = build_aliases_concurrent(vfs_df, concurrency=concurrency)
    else:
        print("跳过 LLM 别名扩展")
        aliases = [
            {
                "vf_id": r["vf_id"],
                "vt_id": r["vt_id"],
                "vt_topic": r["vt_topic"],
                "field_name": r["field_name"],
                "field_cn_name": r["field_cn_name"],
                "importance_tier": r["importance_tier"],
                "seed_aliases": list(r.get("aliases", [])),
                "llm_aliases": [],
                "question_words": [],
            }
            for _, r in vfs_df[vfs_df["importance_tier"].isin(["essential", "frequent"])].iterrows()
        ]

    with OUT_ALIASES.open("w", encoding="utf-8") as f:
        json.dump({"field_aliases": aliases}, f, ensure_ascii=False, indent=2)

    write_diagnostic(mappings, aliases, vfs_df)

    print(f"\n=== I-07 完成 ===")
    print(f"mapping 数: {len(mappings)}")
    print(f"扩展别名的虚拟字段数: {len(aliases)}")
    print(f"JSON: {OUT_MAPPINGS}")
    print(f"Aliases: {OUT_ALIASES}")
    print(f"诊断: {OUT_DIAG}")


if __name__ == "__main__":
    main()
