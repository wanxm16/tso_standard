"""I-06: 生成虚拟字段清单（对应设计文档 § 5.2 virtual_field）。

输入：
  - output/slot_definitions.yaml
  - output/field_normalization.parquet
  - data/slot_library/base_slots.yaml

输出：
  - output/virtual_fields.json
  - output/virtual_fields.parquet
  - output/virtual_fields_diagnostic.md
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

SLOT_YAML = REPO_ROOT / "output" / "slot_definitions.yaml"
NORM_PARQUET = REPO_ROOT / "output" / "field_normalization.parquet"
BASE_SLOTS_YAML = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"
SCAFFOLD_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"

OUT_JSON = REPO_ROOT / "output" / "virtual_fields.json"
OUT_PARQUET = REPO_ROOT / "output" / "virtual_fields.parquet"
OUT_DIAG = REPO_ROOT / "output" / "virtual_fields_diagnostic.md"


GEO_LOGICAL_TYPES = {"address", "coordinate", "region_code", "track_point_name"}


def load_base_slots() -> dict[str, dict]:
    data = yaml.safe_load(BASE_SLOTS_YAML.read_text(encoding="utf-8"))
    return {s["name"]: s for s in data["base_slots"]}


def load_slot_definitions() -> dict:
    return yaml.safe_load(SLOT_YAML.read_text(encoding="utf-8"))


def load_scaffold_meta() -> dict[str, dict]:
    data = json.loads(SCAFFOLD_JSON.read_text(encoding="utf-8"))
    return {vt["vt_id"]: vt for vt in data["virtual_tables"]}


def materialize_slot(slot: dict, base_by_name: dict) -> dict:
    """统一 base/extended 槽位的展开结构。"""
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
            "applicable_table_types": list(b.get("applicable_table_types", [])),
        }
    return {
        "name": name,
        "from": "extended",
        "cn_name": slot.get("cn_name", name),
        "logical_type": slot.get("logical_type", "custom"),
        "role": slot.get("role", ""),
        "description": slot.get("llm_reason", ""),
        "aliases": list(slot.get("aliases", []) or []),
        "applicable_table_types": list(slot.get("applicable_table_types", []) or []),
    }


def compute_slot_source_count(norm_df: pd.DataFrame) -> dict[tuple[str, str], int]:
    """统计 (vt_id, slot_name) 被多少物理字段归一到它（auto + needs_review，排除冲突/低置信）。"""
    eligible = norm_df[norm_df["review_status"].isin(["auto_accepted", "needs_review"])]
    grouped = eligible.groupby(["vt_id", "selected_slot"]).size()
    return {(vt, slot): int(c) for (vt, slot), c in grouped.items()}


def build_virtual_fields() -> list[dict[str, Any]]:
    base_by_name = load_base_slots()
    slot_data = load_slot_definitions()
    scaffold_meta = load_scaffold_meta()

    if NORM_PARQUET.exists():
        norm_df = pd.read_parquet(NORM_PARQUET)
        slot_field_count = compute_slot_source_count(norm_df)
    else:
        slot_field_count = {}

    vfs: list[dict] = []

    for vt in slot_data.get("virtual_tables", []):
        vt_id = vt["vt_id"]
        topic = vt.get("topic", "")
        table_type = vt.get("table_type", "")
        meta = scaffold_meta.get(vt_id, {})
        slots_raw = vt.get("slots", []) or []

        # 记录该 VT 内第一个 time 和 location 槽位（用于 essential 判定）
        first_time_idx: int | None = None
        first_location_idx: int | None = None
        for i, s in enumerate(slots_raw):
            role = s.get("role", "")
            if role == "time" and first_time_idx is None:
                first_time_idx = i
            if role == "location" and first_location_idx is None:
                first_location_idx = i

        for idx, slot_raw in enumerate(slots_raw):
            slot = materialize_slot(slot_raw, base_by_name)
            name = slot["name"]
            role = slot["role"]
            logical_type = slot["logical_type"]

            source_count = slot_field_count.get((vt_id, name), 0)

            # importance_tier 判定（§ 13.1-13.3）
            if (
                role in ("subject_id", "relation_subject")
                or idx == first_time_idx
                or idx == first_location_idx
                or name == "source_system"
            ):
                tier = "essential"
            elif source_count >= 3:
                tier = "frequent"
            else:
                tier = "optional"

            vf = {
                "vf_id": f"{vt_id}__{name}",
                "vt_id": vt_id,
                "vt_topic": topic,
                "vt_table_type": table_type,
                "field_name": name,
                "field_cn_name": slot["cn_name"],
                "logical_type": logical_type,
                "field_role": role,
                "slot_from": slot["from"],
                "is_nullable": True,  # 默认都允许为空；真实可空性交给 I-07 源映射时判定
                "is_subject_key": role == "subject_id",
                "is_time_field": role == "time",
                "is_geo_field": role == "location" or logical_type in GEO_LOGICAL_TYPES,
                "aliases": slot["aliases"],
                "description": slot["description"],
                "importance_tier": tier,
                "source_field_count": source_count,
                "applicable_table_types": slot["applicable_table_types"],
                "vt_l2_path": list(meta.get("l2_path", [])),
            }
            vfs.append(vf)

    return vfs


def write_diagnostic(vfs: list[dict]) -> None:
    lines = [
        "# I-06 虚拟字段清单诊断",
        "",
        f"- 虚拟字段总数: {len(vfs)}",
        f"- 独立 VT 数: {len({v['vt_id'] for v in vfs})}",
        "",
        "## 按 importance_tier 分布",
        "",
    ]
    tier_counter = Counter(v["importance_tier"] for v in vfs)
    for t in ("essential", "frequent", "optional"):
        c = tier_counter.get(t, 0)
        pct = c / len(vfs) * 100 if vfs else 0
        lines.append(f"- {t}: {c} ({pct:.1f}%)")
    lines.append("")

    # 按 field_role 分布
    lines += ["## 按 field_role 分布", ""]
    role_counter = Counter(v["field_role"] for v in vfs)
    for r, c in role_counter.most_common():
        lines.append(f"- {r}: {c}")
    lines.append("")

    # 是否所有 VT 都有 essential
    by_vt = {}
    for v in vfs:
        by_vt.setdefault(v["vt_id"], []).append(v)
    missing_essential = [vt for vt, fields in by_vt.items() if not any(f["importance_tier"] == "essential" for f in fields)]
    lines += ["## 每张 VT essential 覆盖检查", ""]
    lines.append(f"- 缺 essential 的 VT 数: {len(missing_essential)}")
    if missing_essential:
        lines.append(f"- 问题 VT: {missing_essential[:10]}")
    lines.append("")

    # source_field_count 分布（非 0 的）
    lines += ["## slot 的 source_field_count 分布（排除 0）", ""]
    counts = [v["source_field_count"] for v in vfs if v["source_field_count"] > 0]
    if counts:
        import statistics
        lines.append(f"- 有字段归入的 slot 数: {len(counts)} / {len(vfs)}")
        lines.append(f"- 最小/中位数/平均/最大: {min(counts)} / {int(statistics.median(counts))} / {sum(counts)/len(counts):.1f} / {max(counts)}")
    lines.append("")

    # 抽样
    lines += ["## essential 虚拟字段抽样（前 15）", ""]
    essential = [v for v in vfs if v["importance_tier"] == "essential"][:15]
    for v in essential:
        lines.append(
            f"- `{v['field_name']}` ({v['field_cn_name']}) · role={v['field_role']} · "
            f"logical={v['logical_type']} · vt={v['vt_topic']} · sources={v['source_field_count']}"
        )
    lines.append("")

    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    vfs = build_virtual_fields()
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump({"virtual_fields": vfs}, f, ensure_ascii=False, indent=2)

    df = pd.DataFrame(vfs)
    df.to_parquet(OUT_PARQUET, index=False)

    write_diagnostic(vfs)

    tiers = Counter(v["importance_tier"] for v in vfs)
    print(f"\n=== I-06 完成 ===")
    print(f"虚拟字段总数: {len(vfs)}")
    print(f"tier 分布: {dict(tiers)}")
    print(f"subject_key: {sum(1 for v in vfs if v['is_subject_key'])}")
    print(f"time_field: {sum(1 for v in vfs if v['is_time_field'])}")
    print(f"geo_field: {sum(1 for v in vfs if v['is_geo_field'])}")
    print(f"JSON: {OUT_JSON}")
    print(f"诊断: {OUT_DIAG}")


if __name__ == "__main__":
    main()
