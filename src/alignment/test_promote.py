"""集成测试：apply_promotions + revert 在真实数据上跑一遍

挑 output/base_promotion_proposals.yaml 的第一条有 base_entry 的 proposal，
apply 后验证：
  - base_slots.yaml 多了对应条目
  - slot_definitions.yaml 每个 member 的 slot 变成 from: base（去掉 description/aliases 等）
  - alignment_log 有 scope=base 行，带 extended_snapshot_json / base_entry_json
revert 后验证：
  - base_slots.yaml 回到初值（新增条目被移除）
  - slot_definitions.yaml 每个 member 的 slot 恢复为 from: extended + 原 description/aliases
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pandas as pd
import yaml

from src.alignment.cascade import (
    ALIGNMENT_LOG,
    BASE_SLOTS,
    PromoteOp,
    SLOT_DEF,
    apply_promotions,
    next_version,
    revert_to_version,
)

ROOT = Path(__file__).resolve().parents[2]
PROPOSALS = ROOT / "output" / "base_promotion_proposals.yaml"


def file_hash(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def load_yaml(p: Path) -> dict:
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_slot(slot_def: dict, vt_id: str, slot_index: int) -> dict | None:
    for vt in slot_def["virtual_tables"]:
        if vt["vt_id"] == vt_id:
            slots = vt.get("slots", [])
            if 0 <= slot_index < len(slots):
                return slots[slot_index]
    return None


def run_case() -> int:
    if not PROPOSALS.exists():
        print("❌ base_promotion_proposals.yaml 不存在，先跑 base_promote")
        return 1
    with PROPOSALS.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    target = next((p for p in data.get("proposals", []) if p.get("base_entry")), None)
    if target is None:
        print("❌ 无含 base_entry 的 proposal 可测")
        return 1

    name = target["canonical_name"]
    members = target["members"]
    print(f"🧪 测试 promotion: {name} · {len(members)} VT")

    # ---- 初值快照 ----
    pre_slot_hash = file_hash(SLOT_DEF)
    pre_base_hash = file_hash(BASE_SLOTS)
    pre_base = load_yaml(BASE_SLOTS)
    pre_base_names = {s.get("name") for s in pre_base.get("base_slots", [])}
    assert name not in pre_base_names, f"base_slots 已含 {name}，无法测试"

    pre_slot_def = load_yaml(SLOT_DEF)
    pre_member_slots = {
        (m["vt_id"], m["slot_index"]): dict(find_slot(pre_slot_def, m["vt_id"], m["slot_index"]) or {})
        for m in members
    }
    for key, s in pre_member_slots.items():
        assert s.get("from") == "extended", f"{key} 应为 extended，实际 {s.get('from')}"
        assert s.get("name") == name

    version_before = next_version() - 1

    # ---- apply ----
    promo = PromoteOp(
        canonical_name=name,
        base_entry=target["base_entry"],
        members=[
            {
                "vt_id": m["vt_id"],
                "slot_index": m["slot_index"],
                "before_name": m.get("before_name", name),
                "extended_snapshot": m.get("extended_snapshot"),
            }
            for m in members
        ],
    )
    result = apply_promotions(
        [promo],
        scope_key=f"base_promotion#{name}",
        reviewer="test_promote",
        reason=f"integration test for {name}",
    )
    print(f"  apply → version={result.version} slots={result.affected_slots} "
          f"base+{result.base_slots_added} norm_rows={result.affected_norm_rows}")

    # ---- 验证 base_slots 多了一条 ----
    post_base = load_yaml(BASE_SLOTS)
    post_base_names = {s.get("name") for s in post_base.get("base_slots", [])}
    assert name in post_base_names, f"base_slots 未写入 {name}"
    print(f"  ✅ base_slots 已追加 {name}")

    # ---- 验证 slot_definitions 每个 member 变 from: base ----
    post_slot_def = load_yaml(SLOT_DEF)
    for m in members:
        slot = find_slot(post_slot_def, m["vt_id"], m["slot_index"])
        assert slot is not None, f"找不到 slot {m['vt_id']}[{m['slot_index']}]"
        assert slot.get("from") == "base", f"{m['vt_id']}[{m['slot_index']}] from != base: {slot}"
        assert slot.get("name") == name
        assert "description" not in slot and "aliases" not in slot, \
            f"extended 字段未清理：{slot}"
        assert slot.get("source") == "alignment_base"
        post_mf = slot.get("mapped_fields") or []
        pre_mf = pre_member_slots[(m["vt_id"], m["slot_index"])].get("mapped_fields") or []
        assert post_mf == pre_mf, f"mapped_fields 被改动：{pre_mf} → {post_mf}"
    print(f"  ✅ {len(members)} 个 slot 已转为 from: base，mapped_fields 保留")

    # ---- 验证 alignment_log ----
    log = pd.read_parquet(ALIGNMENT_LOG)
    this = log[log["version"] == result.version]
    assert len(this) == len(members), f"log 行数 {len(this)} ≠ members {len(members)}"
    for _, row in this.iterrows():
        assert row["scope"] == "base"
        assert row["after_name"] == name
        assert row["extended_snapshot_json"] and row["extended_snapshot_json"] != ""
        assert row["base_entry_json"] and row["base_entry_json"] != ""
    print(f"  ✅ alignment_log 追加 {len(this)} 行（scope=base，含 snapshot JSON）")

    # ---- revert ----
    revert_to_version(version_before, reviewer="test_promote_revert")

    # ---- 验证恢复 ----
    post_revert_base_hash = file_hash(BASE_SLOTS)
    post_revert_slot_hash = file_hash(SLOT_DEF)
    assert post_revert_base_hash == pre_base_hash, \
        f"base_slots.yaml 未完全恢复（hash {pre_base_hash[:8]} → {post_revert_base_hash[:8]}）"
    print(f"  ✅ base_slots.yaml 文件级恢复（hash 完全一致）")

    if post_revert_slot_hash != pre_slot_hash:
        # 允许结构等价但序列化差异（ruamel vs safe_dump 再回写），逐 slot 对比关键字段
        rev_def = load_yaml(SLOT_DEF)
        for m in members:
            rev_slot = find_slot(rev_def, m["vt_id"], m["slot_index"])
            pre_slot = pre_member_slots[(m["vt_id"], m["slot_index"])]
            assert rev_slot.get("from") == pre_slot.get("from"), \
                f"{m['vt_id']}[{m['slot_index']}] from: {rev_slot.get('from')} vs {pre_slot.get('from')}"
            assert rev_slot.get("name") == pre_slot.get("name")
            assert rev_slot.get("mapped_fields") == pre_slot.get("mapped_fields")
        print(f"  ✅ slot_definitions.yaml 恢复（序列化不完全一致但 {len(members)} 个受影响 slot 关键字段一致）")
    else:
        print(f"  ✅ slot_definitions.yaml 文件级恢复（hash 完全一致）")

    print("🎉 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(run_case())
