"""扫 reviewed.parquet，把每条 decision_slot 非空的决策同步到 slot_definitions.yaml 的 slot.mapped_fields。

修复之前 bug：/api/normalization/decision 只在 mark_new_slot 时才创建 slot 但不写 mapped_fields，
其他决策（accept_top1/use_top2/use_top3/use_slot）完全没写。现在修代码后，一次性追补历史。

用法：
  python3 scripts/backfill_reviewed_to_mapped_fields.py           # 干跑只打印
  python3 scripts/backfill_reviewed_to_mapped_fields.py --write   # 实际写入
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
SLOT_DEF = ROOT / "output" / "slot_definitions.yaml"
REVIEWED = ROOT / "output" / "field_normalization_reviewed.parquet"
FEATURES = ROOT / "output" / "field_features.parquet"

TAKE_DECISIONS = {"accept_top1", "use_top2", "use_top3", "use_slot", "mark_new_slot"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="实际写回 slot_definitions.yaml（默认干跑）")
    args = parser.parse_args()

    if not REVIEWED.exists():
        print("reviewed.parquet 不存在"); return 1
    rev = pd.read_parquet(REVIEWED)
    # 过滤有意义的决策
    rev = rev[rev["decision"].isin(TAKE_DECISIONS) & rev["decision_slot"].notna() & (rev["decision_slot"].astype(str) != "")]
    print(f"reviewed 有意义决策: {len(rev)} 条")

    # 查 field_comment
    feat = pd.read_parquet(FEATURES)
    comment_lookup = {
        (str(r["table_en"]), str(r["field_name"])): str(r.get("field_comment") or "")
        for _, r in feat.iterrows()
    }

    with SLOT_DEF.open(encoding="utf-8") as f:
        sd = yaml.safe_load(f)

    # vt_id → name → slot（原地修改）
    vt_lookup: dict[str, dict] = {vt["vt_id"]: vt for vt in sd.get("virtual_tables", []) or []}

    added_count = 0
    updated_comment = 0
    missing_vt = 0
    missing_slot = 0
    already_mapped = 0

    plan: list[dict] = []
    for _, r in rev.iterrows():
        vt_id = str(r["vt_id"])
        slot_name = str(r["decision_slot"])
        table_en = str(r["table_en"])
        field_name = str(r["field_name"])
        comment = comment_lookup.get((table_en, field_name), "")

        vt = vt_lookup.get(vt_id)
        if vt is None:
            missing_vt += 1
            continue
        slots = vt.setdefault("slots", []) or []
        slot = next((s for s in slots if s.get("name") == slot_name), None)
        if slot is None:
            missing_slot += 1
            continue
        mfs = slot.get("mapped_fields")
        if mfs is None:
            mfs = []
        found = None
        for m in mfs:
            if m.get("table_en") == table_en and m.get("field_name") == field_name:
                found = m
                break
        if found:
            if comment and not found.get("field_comment"):
                found["field_comment"] = comment
                updated_comment += 1
                plan.append({"type": "update_comment", "vt": vt_id, "slot": slot_name, "field": field_name})
            else:
                already_mapped += 1
            slot["mapped_fields"] = mfs
            continue
        mfs.append({
            "table_en": table_en,
            "field_name": field_name,
            "field_comment": comment,
        })
        slot["mapped_fields"] = mfs
        if not slot.get("source"):
            slot["source"] = "backfill_from_reviewed"
        added_count += 1
        plan.append({"type": "add", "vt": vt_id, "slot": slot_name, "field": field_name, "comment": comment})

    print(f"追加 mapped_fields: {added_count}")
    print(f"更新 comment: {updated_comment}")
    print(f"已存在（跳过）: {already_mapped}")
    print(f"找不到 VT: {missing_vt}")
    print(f"找不到 slot: {missing_slot}")
    print()
    print("前 20 条操作:")
    for p in plan[:20]:
        print(f"  {p}")

    if not args.write:
        print()
        print("（干跑模式；加 --write 实际写回）")
        return 0

    # 写回（备份）
    bak = SLOT_DEF.with_suffix(SLOT_DEF.suffix + ".backfill_bak")
    bak.write_bytes(SLOT_DEF.read_bytes())
    with SLOT_DEF.open("w", encoding="utf-8") as f:
        yaml.safe_dump(sd, f, allow_unicode=True, sort_keys=False)
    print(f"✅ 已写回 {SLOT_DEF}（备份在 {bak.name}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
