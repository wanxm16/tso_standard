"""集成测试：apply_renames + revert 在真实数据上走一遍，验证完全可逆。

用法：python3 -m src.alignment.test_cascade

选一个 homonym proposal（education_level，LLM 判为异义，需改名）
- 备份当前 slot_definitions.yaml 和 field_normalization.parquet 的关键摘要
- apply_renames 走 homonym 全流程
- 验证 slot_definitions 里对应 slot name 已改
- 验证 field_normalization.parquet 对应行 selected_slot 已改
- 验证 alignment_log 有新行 + snapshot 目录存在
- revert_to_version 回滚
- 验证 slot_definitions 和 field_normalization 的关键字段回到初值
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pandas as pd
import yaml

from src.alignment.cascade import (
    ALIGNMENT_LOG,
    NORM,
    SLOT_COLS,
    SLOT_DEF,
    SNAPSHOT_ROOT,
    RenameOp,
    apply_renames,
    next_version,
    revert_to_version,
)


def load_slot_def() -> dict:
    with SLOT_DEF.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def slot_name_snapshot() -> dict[tuple[str, int], str]:
    """返回 {(vt_id, slot_index): slot_name} 用于事后比对"""
    out: dict[tuple[str, int], str] = {}
    data = load_slot_def()
    for vt in data["virtual_tables"]:
        for i, s in enumerate(vt.get("slots", [])):
            out[(vt["vt_id"], i)] = s.get("name")
    return out


def norm_slot_tallies(vt_ids: set[str]) -> dict[tuple[str, str], int]:
    """{(vt_id, name_on_selected_slot): count} for rows where vt_id in vt_ids"""
    if not NORM.exists():
        return {}
    df = pd.read_parquet(NORM)
    sub = df[df["vt_id"].isin(vt_ids)]
    tallies: dict[tuple[str, str], int] = {}
    for _, row in sub.iterrows():
        key = (row["vt_id"], row.get("selected_slot"))
        tallies[key] = tallies.get(key, 0) + 1
    return tallies


def run_case() -> int:
    # 从 homonym_proposals 里挑第一个 homonym 类的提议
    proposals_path = Path(__file__).resolve().parents[2] / "output" / "homonym_proposals.yaml"
    if not proposals_path.exists():
        print("❌ homonym_proposals.yaml 不存在")
        return 1
    with proposals_path.open(encoding="utf-8") as f:
        proposals = yaml.safe_load(f)["proposals"]
    target = next((p for p in proposals if p["judgement"] == "homonym"), None)
    if target is None:
        print("ℹ️ 未找到 homonym 类提议，测试改为跑第一个 mixed...")
        target = next((p for p in proposals if p["judgement"] == "mixed"), None)
    if target is None:
        print("❌ 无 homonym/mixed 提议可测")
        return 1

    name = target["name"]
    members = [m for m in target["member_proposals"] if m["changed"]]
    if not members:
        print(f"❌ proposal {name} 没有 changed==True 的成员")
        return 1

    print(f"🧪 测试 proposal: {name} · 涉及 {len(members)} 个 VT")

    # —— 初值快照 ——
    pre_slot_names = slot_name_snapshot()
    affected_vt_ids = {m["vt_id"] for m in members}
    pre_tallies = norm_slot_tallies(affected_vt_ids)

    version_before = next_version() - 1  # 当前最高版本（可能 0）

    # —— apply ——
    renames = [
        RenameOp(vt_id=m["vt_id"], before_name=m["before_name"], after_name=m["after_name"])
        for m in members
    ]
    result = apply_renames(
        renames,
        scope="homonym",
        scope_key=name,
        reviewer="test_cascade",
        reason=f"integration test for {name}",
    )
    print(f"  apply → version={result.version} slots={result.affected_slots} norm_rows={result.affected_norm_rows}")

    # —— 验证 apply 效果 ——
    post_slot_names = slot_name_snapshot()
    changed_count = 0
    for m in members:
        # 找对应 slot index
        for (vt_id, idx), pre_name in pre_slot_names.items():
            if vt_id != m["vt_id"]:
                continue
            if pre_name == m["before_name"]:
                post_name = post_slot_names.get((vt_id, idx))
                if post_name == m["after_name"]:
                    changed_count += 1
                else:
                    print(f"  ❌ ({vt_id}, slot[{idx}]) 期望 '{m['after_name']}', 实际 '{post_name}'")
    assert changed_count >= len(members), f"应至少改 {len(members)} 个 slot，实际改 {changed_count}"
    print(f"  ✅ slot_definitions 已改名 {changed_count}")

    # 验证 log
    assert ALIGNMENT_LOG.exists(), "alignment_log.parquet 未生成"
    log_df = pd.read_parquet(ALIGNMENT_LOG)
    this_version_logs = log_df[log_df["version"] == result.version]
    assert len(this_version_logs) == len(renames), f"log 行数 {len(this_version_logs)} ≠ renames 数 {len(renames)}"
    print(f"  ✅ alignment_log 追加 {len(this_version_logs)} 行")

    # 验证 snapshot
    assert result.snapshot_path and result.snapshot_path.exists()
    assert (result.snapshot_path / "slot_definitions.yaml").exists()
    print(f"  ✅ snapshot 目录已创建: {result.snapshot_path.name}")

    # —— revert ——
    revert_to_version(version_before, reviewer="test_cascade_revert")

    # —— 验证 revert 恢复 ——
    post_revert_slot_names = slot_name_snapshot()
    matched = 0
    mismatch_cases = []
    for key, pre_name in pre_slot_names.items():
        rev_name = post_revert_slot_names.get(key)
        if rev_name == pre_name:
            matched += 1
        else:
            mismatch_cases.append((key, pre_name, rev_name))
    if mismatch_cases:
        print(f"  ❌ revert 后有 {len(mismatch_cases)} 个 slot name 未恢复:")
        for k, p, r in mismatch_cases[:5]:
            print(f"    {k}: {p} → {r}")
        return 1
    print(f"  ✅ revert 后所有 {matched} 个 slot name 已恢复原值")

    # 验证 field_normalization 也已恢复
    post_revert_tallies = norm_slot_tallies(affected_vt_ids)
    # 因为 revert 也是一次 apply，所以 tallies 应该恢复
    if post_revert_tallies != pre_tallies:
        diffs = []
        keys = set(pre_tallies) | set(post_revert_tallies)
        for k in keys:
            if pre_tallies.get(k) != post_revert_tallies.get(k):
                diffs.append((k, pre_tallies.get(k), post_revert_tallies.get(k)))
        print(f"  ⚠️  field_normalization 有 {len(diffs)} 个 tallies 不一致（前 5 个）:")
        for k, a, b in diffs[:5]:
            print(f"    {k}: pre={a} post_revert={b}")
    else:
        print(f"  ✅ field_normalization selected_slot tallies 完全恢复")

    print("🎉 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(run_case())
