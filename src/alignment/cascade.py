"""W5 建模统一层 · cascade 核心

一次对齐 = 一批 RenameOp（vt_id, before_name, after_name）
原子地修改 4 个产物：
  1) output/slot_definitions.yaml         slot.name
  2) output/field_normalization.parquet   selected_slot / top1..3_slot / llm_suggested_slot
  3) data/feedback/field_normalization_reviewed.parquet  decision_slot（若存在）
  4) data/feedback/alignment_log.parquet  追加日志行

同步写 snapshot 到 .alignment_snapshots/<version>/，支持 revert_alignment.py --version N。
"""
from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[2]
SLOT_DEF = ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS = ROOT / "data" / "slot_library" / "base_slots.yaml"
NORM = ROOT / "output" / "field_normalization.parquet"
REVIEWED = ROOT / "output" / "field_normalization_reviewed.parquet"
ALIGNMENT_LOG = ROOT / "data" / "feedback" / "alignment_log.parquet"
SNAPSHOT_ROOT = ROOT / ".alignment_snapshots"
LOCK_FILE = ROOT / ".alignment.lock"

SLOT_COLS = ("selected_slot", "top1_slot", "top2_slot", "top3_slot", "llm_suggested_slot")

Scope = Literal["homonym", "l2", "l1", "base", "upgrade", "manual"]

# alignment_log 扩展列：extended_snapshot_json / base_entry_json（scope=base 时用）
LOG_COLUMNS_EXT = ("extended_snapshot_json", "base_entry_json")


def _ensure_log_columns(df: pd.DataFrame) -> pd.DataFrame:
    """旧版本 parquet 可能没有 LOG_COLUMNS_EXT 列，读后补齐。"""
    for col in LOG_COLUMNS_EXT:
        if col not in df.columns:
            df[col] = ""
    return df


@dataclass
class RenameOp:
    vt_id: str
    before_name: str
    after_name: str
    # 可选：同步更新描述/synonyms（同义合并时用）
    new_description: str | None = None
    new_synonyms: list[str] | None = None
    new_cn_name: str | None = None
    # 精确 slot 定位（revert 场景用；apply 时若传入则不靠 before_name 匹配）
    slot_index: int | None = None


@dataclass
class PromoteOp:
    """W5-C · 把一个 extended slot 提升为 base_slots（一个 canonical_name → N VT members）"""
    canonical_name: str
    base_entry: dict  # 追加到 base_slots.yaml 的完整条目
    members: list[dict]  # 每项 {vt_id, slot_index, before_name, extended_snapshot}


@dataclass
class UpgradeOp:
    """W5-F · 为小表 VT 增加一个新 slot（extended 或 base 复用），并把物理字段 mapped 进去"""
    vt_id: str
    slot_name: str
    slot_kind: str  # "extended" 或 "base"
    field: dict  # {table_en, field_name, field_comment}
    # 新 slot 的业务字段（仅 extended 需要；base 复用时以 base_slots.yaml 为准）
    cn_name: str | None = None
    role: str | None = None
    logical_type: str | None = None
    description: str | None = None
    aliases: list[str] | None = None


@dataclass
class CascadeResult:
    version: int
    log_ids: list[str] = field(default_factory=list)
    snapshot_path: Path | None = None
    affected_slots: int = 0
    affected_norm_rows: int = 0
    affected_reviewed_rows: int = 0
    base_slots_added: int = 0
    upgrades_applied: int = 0


# ---------- lock ----------

def _acquire_lock() -> None:
    if LOCK_FILE.exists():
        raise RuntimeError(f"另一次对齐正在进行：{LOCK_FILE}")
    LOCK_FILE.write_text(str(datetime.now(timezone.utc)), encoding="utf-8")


def _release_lock() -> None:
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()


# ---------- version ----------

def next_version() -> int:
    if not ALIGNMENT_LOG.exists():
        return 1
    try:
        df = pd.read_parquet(ALIGNMENT_LOG)
        if df.empty or "version" not in df.columns:
            return 1
        return int(df["version"].max()) + 1
    except Exception:
        return 1


# ---------- snapshot ----------

def _snapshot_slot_def(snap_dir: Path) -> None:
    shutil.copy2(SLOT_DEF, snap_dir / "slot_definitions.yaml")


def _snapshot_base_slots(snap_dir: Path) -> None:
    if BASE_SLOTS.exists():
        shutil.copy2(BASE_SLOTS, snap_dir / "base_slots.yaml")


def _snapshot_norm_subset(snap_dir: Path, affected_mask: pd.Series, df: pd.DataFrame) -> None:
    if affected_mask.any():
        sub = df[affected_mask].copy()
        sub.to_parquet(snap_dir / "field_normalization_subset.parquet")


def _snapshot_reviewed_subset(snap_dir: Path, affected_mask: pd.Series, df: pd.DataFrame) -> None:
    if affected_mask.any():
        sub = df[affected_mask].copy()
        sub.to_parquet(snap_dir / "field_normalization_reviewed_subset.parquet")


# ---------- apply ----------

def apply_renames(
    renames: Iterable[RenameOp],
    *,
    scope: Scope,
    scope_key: str | None,
    reviewer: str,
    reason: str,
) -> CascadeResult:
    """原子地执行一批 rename。成功返回 version。失败抛异常并尝试从 snapshot 恢复。"""
    renames = list(renames)
    if not renames:
        raise ValueError("renames 为空")

    _acquire_lock()
    try:
        version = next_version()
        snap_dir = SNAPSHOT_ROOT / f"v{version:05d}"
        snap_dir.mkdir(parents=True, exist_ok=True)
        result = CascadeResult(version=version, snapshot_path=snap_dir)

        # ---- 1. 读 + snapshot slot_definitions ----
        _snapshot_slot_def(snap_dir)
        with SLOT_DEF.open(encoding="utf-8") as f:
            slot_def = yaml.safe_load(f)

        # vt_id → slots list（原地修改）
        vt_lookup = {vt["vt_id"]: vt for vt in slot_def["virtual_tables"]}

        # 处理过的 slot 索引，防止同 (vt_id, before_name) 的多个 op 把同一个 slot 改两次
        processed: set[tuple[str, int]] = set()
        # op → 落地到的 slot_index（用于 log 记账，revert 精确定位）
        op_to_slot_idx: list[int | None] = []
        for op in renames:
            vt = vt_lookup.get(op.vt_id)
            if vt is None:
                op_to_slot_idx.append(None)
                continue
            # 如果 op 指定了 slot_index，直接用（revert 精确恢复场景）
            if op.slot_index is not None:
                target_i = op.slot_index
                if target_i < len(vt.get("slots", [])) and (op.vt_id, target_i) not in processed:
                    slot = vt["slots"][target_i]
                    if slot.get("from") == "extended":
                        slot["name"] = op.after_name
                        if op.new_cn_name:
                            slot["cn_name"] = op.new_cn_name
                        if op.new_description:
                            slot["description"] = op.new_description
                        if op.new_synonyms:
                            slot["aliases"] = op.new_synonyms
                        slot["source"] = f"alignment_{scope}"
                        result.affected_slots += 1
                        processed.add((op.vt_id, target_i))
                        op_to_slot_idx.append(target_i)
                        continue
                op_to_slot_idx.append(None)
                continue
            # 否则按 before_name 扫描第一个未处理的 match
            matched_i: int | None = None
            for i, slot in enumerate(vt.get("slots", [])):
                if (op.vt_id, i) in processed:
                    continue
                if slot.get("name") == op.before_name and slot.get("from") == "extended":
                    slot["name"] = op.after_name
                    if op.new_cn_name:
                        slot["cn_name"] = op.new_cn_name
                    if op.new_description:
                        slot["description"] = op.new_description
                    if op.new_synonyms:
                        slot["aliases"] = op.new_synonyms
                    slot["source"] = f"alignment_{scope}"
                    result.affected_slots += 1
                    processed.add((op.vt_id, i))
                    matched_i = i
                    break
            op_to_slot_idx.append(matched_i)

        # ---- 2. field_normalization.parquet ----
        if NORM.exists():
            norm = pd.read_parquet(NORM)
            # 计算受影响 mask（用于 snapshot 前置）
            affected_mask = pd.Series(False, index=norm.index)
            for op in renames:
                for col in SLOT_COLS:
                    if col not in norm.columns:
                        continue
                    affected_mask |= (norm["vt_id"] == op.vt_id) & (norm[col] == op.before_name)
            _snapshot_norm_subset(snap_dir, affected_mask, norm)

            # 执行替换
            for op in renames:
                for col in SLOT_COLS:
                    if col not in norm.columns:
                        continue
                    m = (norm["vt_id"] == op.vt_id) & (norm[col] == op.before_name)
                    norm.loc[m, col] = op.after_name
            result.affected_norm_rows = int(affected_mask.sum())
            norm.to_parquet(NORM)

        # ---- 3. reviewed.parquet（若存在）----
        if REVIEWED.exists():
            rev = pd.read_parquet(REVIEWED)
            if "decision_slot" in rev.columns and "vt_id" in rev.columns:
                rev_mask = pd.Series(False, index=rev.index)
                for op in renames:
                    rev_mask |= (rev["vt_id"] == op.vt_id) & (rev["decision_slot"] == op.before_name)
                _snapshot_reviewed_subset(snap_dir, rev_mask, rev)
                for op in renames:
                    m = (rev["vt_id"] == op.vt_id) & (rev["decision_slot"] == op.before_name)
                    rev.loc[m, "decision_slot"] = op.after_name
                result.affected_reviewed_rows = int(rev_mask.sum())
                rev.to_parquet(REVIEWED)

        # ---- 4. 写 slot_definitions（最后，先改 parquet 再改 yaml 风险较低，但此处先 yaml 也可）----
        with SLOT_DEF.open("w", encoding="utf-8") as f:
            yaml.safe_dump(slot_def, f, allow_unicode=True, sort_keys=False)

        # ---- 5. 写 alignment_log ----
        ts = datetime.now(timezone.utc).isoformat()
        log_rows = []
        for op, slot_idx in zip(renames, op_to_slot_idx):
            log_id = str(uuid.uuid4())
            result.log_ids.append(log_id)
            log_rows.append({
                "log_id": log_id,
                "version": version,
                "scope": scope,
                "scope_key": scope_key or "",
                "vt_id": op.vt_id,
                "slot_index": -1 if slot_idx is None else slot_idx,
                "before_name": op.before_name,
                "after_name": op.after_name,
                "new_cn_name": op.new_cn_name or "",
                "new_description": op.new_description or "",
                "new_synonyms_json": json.dumps(op.new_synonyms or [], ensure_ascii=False),
                "extended_snapshot_json": "",
                "base_entry_json": "",
                "decision": "accept",
                "reviewer": reviewer,
                "ts": ts,
                "reason": reason,
                "rollback_of": "",
                "snapshot_path": str(snap_dir.relative_to(ROOT)),
            })
        new_log = pd.DataFrame(log_rows)
        if ALIGNMENT_LOG.exists():
            old = _ensure_log_columns(pd.read_parquet(ALIGNMENT_LOG))
            combined = pd.concat([old, new_log], ignore_index=True)
        else:
            ALIGNMENT_LOG.parent.mkdir(parents=True, exist_ok=True)
            combined = new_log
        combined.to_parquet(ALIGNMENT_LOG)

        return result
    except Exception:
        # 粗糙回滚：从 snapshot 恢复 slot_definitions.yaml（最常用的，先保 yaml 一致）
        try:
            if (snap_dir / "slot_definitions.yaml").exists():
                shutil.copy2(snap_dir / "slot_definitions.yaml", SLOT_DEF)
        except Exception:
            pass
        raise
    finally:
        _release_lock()


def apply_promotions(
    promotions: Iterable[PromoteOp],
    *,
    scope_key: str | None,
    reviewer: str,
    reason: str,
) -> CascadeResult:
    """W5-C · 把一批 extended slot 提升为 base_slots。

    每个 PromoteOp：
      - canonical_name：在 base_slots 里的最终 name
      - base_entry：追加进 base_slots.yaml 的完整 dict（若 name 已存在则跳过追加，只转 from）
      - members：[{vt_id, slot_index, before_name, extended_snapshot}]

    原子动作：
      1. 快照 slot_definitions.yaml / base_slots.yaml / field_normalization 子集
      2. 用 ruamel 追加 base_entry（若尚未存在）
      3. 把每个 VT 的对应 slot 改写为 {name=canonical_name, from=base, role, mapped_fields, source=alignment_base}
      4. 若 before_name != canonical_name，同步改 field_normalization / reviewed.parquet
      5. 追加 alignment_log 行（scope=base, extended_snapshot_json, base_entry_json）

    失败则从 snapshot 文件回滚 slot_definitions + base_slots。
    """
    promotions = list(promotions)
    if not promotions:
        raise ValueError("promotions 为空")

    _acquire_lock()
    snap_dir: Path | None = None
    try:
        version = next_version()
        snap_dir = SNAPSHOT_ROOT / f"v{version:05d}"
        snap_dir.mkdir(parents=True, exist_ok=True)
        result = CascadeResult(version=version, snapshot_path=snap_dir)

        # ---- 1. 快照 ----
        _snapshot_slot_def(snap_dir)
        _snapshot_base_slots(snap_dir)

        # ---- 2. 追加 base_slots ----
        try:
            from ruamel.yaml import YAML
        except ImportError as e:
            raise RuntimeError(f"ruamel.yaml 未安装：{e}") from e
        ruamel = YAML()
        ruamel.preserve_quotes = True
        ruamel.indent(mapping=2, sequence=4, offset=2)
        ruamel.width = 200
        with BASE_SLOTS.open(encoding="utf-8") as f:
            base_doc = ruamel.load(f)
        if base_doc is None or "base_slots" not in base_doc:
            raise RuntimeError("base_slots.yaml 结构异常")
        existing_base_names = {s.get("name") for s in base_doc["base_slots"]}
        for p in promotions:
            if p.canonical_name in existing_base_names:
                continue
            base_doc["base_slots"].append(dict(p.base_entry))
            existing_base_names.add(p.canonical_name)
            result.base_slots_added += 1
        with BASE_SLOTS.open("w", encoding="utf-8") as f:
            ruamel.dump(base_doc, f)

        # ---- 3. 改 slot_definitions ----
        with SLOT_DEF.open(encoding="utf-8") as f:
            slot_def = yaml.safe_load(f)
        vt_lookup = {vt["vt_id"]: vt for vt in slot_def["virtual_tables"]}

        # 收集"改名"场景（before_name != canonical_name）的 rename 对，用于 field_normalization 级联
        renames_for_parquet: list[tuple[str, str, str]] = []  # (vt_id, before, after)
        for p in promotions:
            for m in p.members:
                vt = vt_lookup.get(m["vt_id"])
                if vt is None:
                    continue
                slot_index = m.get("slot_index")
                slot = None
                if slot_index is not None and 0 <= slot_index < len(vt.get("slots", [])):
                    cand = vt["slots"][slot_index]
                    if cand.get("from") == "extended":
                        slot = cand
                if slot is None:
                    # fallback: 按 before_name 找第一个 extended
                    for i, s in enumerate(vt.get("slots", [])):
                        if s.get("name") == m["before_name"] and s.get("from") == "extended":
                            slot = s
                            slot_index = i
                            break
                if slot is None:
                    continue

                before_name = slot.get("name")
                after_name = p.canonical_name

                # 改写：只留 name / from / role / mapped_fields / source
                mapped_fields = slot.get("mapped_fields", [])
                role = slot.get("role")
                new_slot: dict = {
                    "name": after_name,
                    "from": "base",
                }
                if role is not None:
                    new_slot["role"] = role
                if mapped_fields:
                    new_slot["mapped_fields"] = mapped_fields
                new_slot["source"] = "alignment_base"
                vt["slots"][slot_index] = new_slot

                result.affected_slots += 1
                if before_name != after_name:
                    renames_for_parquet.append((m["vt_id"], before_name, after_name))

        # ---- 4. field_normalization 级联（仅有 rename 的成员）----
        if renames_for_parquet and NORM.exists():
            norm = pd.read_parquet(NORM)
            affected_mask = pd.Series(False, index=norm.index)
            for vt_id, before, _ in renames_for_parquet:
                for col in SLOT_COLS:
                    if col not in norm.columns:
                        continue
                    affected_mask |= (norm["vt_id"] == vt_id) & (norm[col] == before)
            _snapshot_norm_subset(snap_dir, affected_mask, norm)
            for vt_id, before, after in renames_for_parquet:
                for col in SLOT_COLS:
                    if col not in norm.columns:
                        continue
                    m = (norm["vt_id"] == vt_id) & (norm[col] == before)
                    norm.loc[m, col] = after
            result.affected_norm_rows = int(affected_mask.sum())
            norm.to_parquet(NORM)

        if renames_for_parquet and REVIEWED.exists():
            rev = pd.read_parquet(REVIEWED)
            if "decision_slot" in rev.columns and "vt_id" in rev.columns:
                rev_mask = pd.Series(False, index=rev.index)
                for vt_id, before, _ in renames_for_parquet:
                    rev_mask |= (rev["vt_id"] == vt_id) & (rev["decision_slot"] == before)
                _snapshot_reviewed_subset(snap_dir, rev_mask, rev)
                for vt_id, before, after in renames_for_parquet:
                    m = (rev["vt_id"] == vt_id) & (rev["decision_slot"] == before)
                    rev.loc[m, "decision_slot"] = after
                result.affected_reviewed_rows = int(rev_mask.sum())
                rev.to_parquet(REVIEWED)

        # ---- 5. 写回 slot_definitions ----
        with SLOT_DEF.open("w", encoding="utf-8") as f:
            yaml.safe_dump(slot_def, f, allow_unicode=True, sort_keys=False)

        # ---- 6. alignment_log ----
        ts = datetime.now(timezone.utc).isoformat()
        log_rows = []
        for p in promotions:
            base_entry_json = json.dumps(p.base_entry, ensure_ascii=False)
            for m in p.members:
                log_id = str(uuid.uuid4())
                result.log_ids.append(log_id)
                log_rows.append({
                    "log_id": log_id,
                    "version": version,
                    "scope": "base",
                    "scope_key": scope_key or f"base_promotion#{p.canonical_name}",
                    "vt_id": m["vt_id"],
                    "slot_index": int(m.get("slot_index", -1)) if m.get("slot_index") is not None else -1,
                    "before_name": m["before_name"],
                    "after_name": p.canonical_name,
                    "new_cn_name": p.base_entry.get("cn_name", ""),
                    "new_description": p.base_entry.get("description", ""),
                    "new_synonyms_json": json.dumps(p.base_entry.get("aliases", []) or [], ensure_ascii=False),
                    "extended_snapshot_json": json.dumps(m.get("extended_snapshot") or {}, ensure_ascii=False),
                    "base_entry_json": base_entry_json,
                    "decision": "accept",
                    "reviewer": reviewer,
                    "ts": ts,
                    "reason": reason,
                    "rollback_of": "",
                    "snapshot_path": str(snap_dir.relative_to(ROOT)),
                })
        new_log = pd.DataFrame(log_rows)
        if ALIGNMENT_LOG.exists():
            old = _ensure_log_columns(pd.read_parquet(ALIGNMENT_LOG))
            combined = pd.concat([old, new_log], ignore_index=True)
        else:
            ALIGNMENT_LOG.parent.mkdir(parents=True, exist_ok=True)
            combined = new_log
        combined.to_parquet(ALIGNMENT_LOG)

        return result
    except Exception:
        try:
            if snap_dir and (snap_dir / "slot_definitions.yaml").exists():
                shutil.copy2(snap_dir / "slot_definitions.yaml", SLOT_DEF)
            if snap_dir and (snap_dir / "base_slots.yaml").exists():
                shutil.copy2(snap_dir / "base_slots.yaml", BASE_SLOTS)
        except Exception:
            pass
        raise
    finally:
        _release_lock()


def apply_upgrades(
    ops: Iterable[UpgradeOp],
    *,
    scope_key: str | None,
    reviewer: str,
    reason: str,
) -> CascadeResult:
    """W5-F · 为小表 VT 追加新 slot，并绑定物理字段。

    每个 UpgradeOp：
      - vt_id + slot_name + slot_kind("extended" / "base")
      - field：{table_en, field_name, field_comment} —— 要被 mapped 的物理字段
      - extended 模式下需要给 cn_name/role/logical_type/description/aliases；base 模式下忽略

    原子动作（加锁）：
      1. 快照 slot_definitions.yaml
      2. 对每个 op：
         - 如果 vt 已有同名 slot（from 对应）→ 追加 mapped_fields（不新增 slot）
         - 否则新增 slot 并写 mapped_fields=[field]
      3. 追加 alignment_log 行：scope=upgrade, before_name="", after_name=slot_name,
         extended_snapshot_json=新 slot 完整 dict, slot_index=新 slot 的 index

    revert：按 slot_index 删除该 slot（或移除刚追加的 mapped_fields 项）。
    """
    ops = list(ops)
    if not ops:
        raise ValueError("ops 为空")

    _acquire_lock()
    snap_dir: Path | None = None
    try:
        version = next_version()
        snap_dir = SNAPSHOT_ROOT / f"v{version:05d}"
        snap_dir.mkdir(parents=True, exist_ok=True)
        result = CascadeResult(version=version, snapshot_path=snap_dir)

        _snapshot_slot_def(snap_dir)
        with SLOT_DEF.open(encoding="utf-8") as f:
            slot_def = yaml.safe_load(f)
        vt_lookup = {vt["vt_id"]: vt for vt in slot_def["virtual_tables"]}

        # 每个 op 记录落地 slot_index 和 "是否新增 slot"（用于 revert）
        log_rows = []
        ts = datetime.now(timezone.utc).isoformat()
        for op in ops:
            vt = vt_lookup.get(op.vt_id)
            if vt is None:
                raise RuntimeError(f"VT 不存在：{op.vt_id}")
            slots = vt.setdefault("slots", [])

            # 查是否已有同 slot_name
            existing_idx = None
            for i, s in enumerate(slots):
                if s.get("name") == op.slot_name and s.get("from") == op.slot_kind:
                    existing_idx = i
                    break

            field_entry = {
                "table_en": op.field["table_en"],
                "field_name": op.field["field_name"],
                "field_comment": op.field.get("field_comment", "") or "",
            }

            if existing_idx is not None:
                slot = slots[existing_idx]
                mapped = slot.setdefault("mapped_fields", []) or []
                # 去重
                already = any(
                    m.get("table_en") == field_entry["table_en"]
                    and m.get("field_name") == field_entry["field_name"]
                    for m in mapped
                )
                if not already:
                    mapped.append(field_entry)
                    slot["mapped_fields"] = mapped
                    slot["source"] = "alignment_upgrade"
                op_added_new_slot = False
                slot_index = existing_idx
                snapshot_json = json.dumps(
                    {"merged_into_existing": True, "added_mapped_field": field_entry},
                    ensure_ascii=False,
                )
            else:
                # 新增 slot
                new_slot: dict = {"name": op.slot_name, "from": op.slot_kind}
                if op.role:
                    new_slot["role"] = op.role
                if op.slot_kind == "extended":
                    if op.cn_name:
                        new_slot["cn_name"] = op.cn_name
                    if op.logical_type:
                        new_slot["logical_type"] = op.logical_type
                    if op.description:
                        new_slot["description"] = op.description
                    if op.aliases:
                        new_slot["aliases"] = list(op.aliases)
                new_slot["mapped_fields"] = [field_entry]
                new_slot["source"] = "alignment_upgrade"
                slots.append(new_slot)
                op_added_new_slot = True
                slot_index = len(slots) - 1
                snapshot_json = json.dumps(
                    {"added_new_slot": True, "slot": new_slot},
                    ensure_ascii=False,
                )

            result.upgrades_applied += 1
            log_rows.append({
                "log_id": str(uuid.uuid4()),
                "version": version,
                "scope": "upgrade",
                "scope_key": scope_key or f"upgrade#{op.vt_id}/{op.slot_name}",
                "vt_id": op.vt_id,
                "slot_index": slot_index,
                "before_name": "" if op_added_new_slot else op.slot_name,
                "after_name": op.slot_name,
                "new_cn_name": op.cn_name or "",
                "new_description": op.description or "",
                "new_synonyms_json": json.dumps(op.aliases or [], ensure_ascii=False),
                "extended_snapshot_json": snapshot_json,
                "base_entry_json": "",
                "decision": "accept",
                "reviewer": reviewer,
                "ts": ts,
                "reason": reason,
                "rollback_of": "",
                "snapshot_path": str(snap_dir.relative_to(ROOT)),
            })

        with SLOT_DEF.open("w", encoding="utf-8") as f:
            yaml.safe_dump(slot_def, f, allow_unicode=True, sort_keys=False)

        new_log = pd.DataFrame(log_rows)
        result.log_ids = [r["log_id"] for r in log_rows]
        if ALIGNMENT_LOG.exists():
            old = _ensure_log_columns(pd.read_parquet(ALIGNMENT_LOG))
            combined = pd.concat([old, new_log], ignore_index=True)
        else:
            ALIGNMENT_LOG.parent.mkdir(parents=True, exist_ok=True)
            combined = new_log
        combined.to_parquet(ALIGNMENT_LOG)
        return result
    except Exception:
        try:
            if snap_dir and (snap_dir / "slot_definitions.yaml").exists():
                shutil.copy2(snap_dir / "slot_definitions.yaml", SLOT_DEF)
        except Exception:
            pass
        raise
    finally:
        _release_lock()


def _snapshot_file_restore(target_version: int, to_undo: pd.DataFrame, reviewer: str) -> None:
    """文件级回滚：找 to_undo 中最早 version 的 snapshot，用里面的 yaml 覆盖当前。

    适用场景：涉及 scope=base 的 promotion 无法按行反向，只能文件级恢复。
    parquet 子集尽力恢复（若存在）。
    """
    earliest_version = int(to_undo["version"].min())
    snap_dir = SNAPSHOT_ROOT / f"v{earliest_version:05d}"
    if not snap_dir.exists():
        raise RuntimeError(f"snapshot {snap_dir} 不存在，无法文件级回滚")

    _acquire_lock()
    try:
        # 恢复 slot_definitions.yaml
        snap_slot = snap_dir / "slot_definitions.yaml"
        if snap_slot.exists():
            shutil.copy2(snap_slot, SLOT_DEF)
        # 恢复 base_slots.yaml（如果快照里有）
        snap_base = snap_dir / "base_slots.yaml"
        if snap_base.exists():
            shutil.copy2(snap_base, BASE_SLOTS)
        # 恢复 field_normalization 子集（各 version 的子集叠加撤销）
        # 为了简化：只用"最早 version"的 subset（它代表对应行在 apply 前的值）
        # 注意：若中间还有其他 version 修改了这些行，简单恢复可能覆盖后续合法修改
        # —— 接受这一限制：revert 通常用于 POC 阶段的顺序撤销，而非任意跳点
        if NORM.exists():
            subset_path = snap_dir / "field_normalization_subset.parquet"
            if subset_path.exists():
                sub = pd.read_parquet(subset_path)
                cur = pd.read_parquet(NORM)
                # 按 index 对齐恢复
                common = cur.index.intersection(sub.index)
                if len(common) > 0:
                    cur.loc[common, sub.columns] = sub.loc[common].values
                    cur.to_parquet(NORM)
        if REVIEWED.exists():
            subset_path = snap_dir / "field_normalization_reviewed_subset.parquet"
            if subset_path.exists():
                sub = pd.read_parquet(subset_path)
                cur = pd.read_parquet(REVIEWED)
                common = cur.index.intersection(sub.index)
                if len(common) > 0:
                    cur.loc[common, sub.columns] = sub.loc[common].values
                    cur.to_parquet(REVIEWED)

        # 写一个"revert 标记"版本到 alignment_log：不新建 parquet 行（因为我们撤销了多行），
        # 而是标记 rollback_of，并追加一条 meta 行
        version = next_version()
        ts = datetime.now(timezone.utc).isoformat()
        meta_row = {
            "log_id": str(uuid.uuid4()),
            "version": version,
            "scope": "manual",
            "scope_key": f"revert_to_v{target_version}",
            "vt_id": "",
            "slot_index": -1,
            "before_name": "",
            "after_name": "",
            "new_cn_name": "",
            "new_description": "",
            "new_synonyms_json": "",
            "extended_snapshot_json": "",
            "base_entry_json": "",
            "decision": "revert_snapshot",
            "reviewer": reviewer,
            "ts": ts,
            "reason": f"snapshot restore from v{earliest_version:05d} (undoing {len(to_undo)} rows)",
            "rollback_of": ",".join(to_undo["log_id"].astype(str).tolist()),
            "snapshot_path": str(snap_dir.relative_to(ROOT)),
        }
        log = _ensure_log_columns(pd.read_parquet(ALIGNMENT_LOG))
        combined = pd.concat([log, pd.DataFrame([meta_row])], ignore_index=True)
        combined.to_parquet(ALIGNMENT_LOG)
        print(f"✅ 已文件级回滚到 v{earliest_version:05d} 起点（当前 version {version}）")
    finally:
        _release_lock()


def revert_to_version(target_version: int, reviewer: str = "revert_script") -> None:
    """回滚到 target_version 之前的状态（即撤销 version > target_version 的所有 apply）

    策略：
      - 纯 rename（无 scope=base）：逐行反向 apply_renames
      - 含 scope=base 的区间：走 snapshot 文件级恢复（因 promotion 结构变换无法按行反向）
    """
    if not ALIGNMENT_LOG.exists():
        raise RuntimeError("alignment_log 不存在，无历史可回滚")
    log = _ensure_log_columns(pd.read_parquet(ALIGNMENT_LOG))
    to_undo = log[log["version"] > target_version].sort_values("version", ascending=False)
    if to_undo.empty:
        print(f"已经是 version {target_version}，无需回滚")
        return

    has_structural = to_undo["scope"].isin(["base", "upgrade"]).any()
    if has_structural:
        _snapshot_file_restore(target_version, to_undo, reviewer)
        return

    reverse_renames: list[RenameOp] = []
    for _, row in to_undo.iterrows():
        slot_idx = row.get("slot_index")
        if pd.notna(slot_idx) and int(slot_idx) >= 0:
            reverse_renames.append(RenameOp(
                vt_id=row["vt_id"],
                before_name=row["after_name"],
                after_name=row["before_name"],
                slot_index=int(slot_idx),
            ))
        else:
            reverse_renames.append(RenameOp(
                vt_id=row["vt_id"],
                before_name=row["after_name"],
                after_name=row["before_name"],
            ))

    result = apply_renames(
        reverse_renames,
        scope="manual",
        scope_key=f"revert_to_v{target_version}",
        reviewer=reviewer,
        reason=f"revert to version {target_version}",
    )
    log = _ensure_log_columns(pd.read_parquet(ALIGNMENT_LOG))
    for _, row in to_undo.iterrows():
        ln = log[(log["version"] == result.version) & (log["vt_id"] == row["vt_id"]) & (log["before_name"] == row["after_name"])]
        if not ln.empty:
            log.loc[ln.index[0], "rollback_of"] = row["log_id"]
    log.to_parquet(ALIGNMENT_LOG)
    print(f"✅ 已回滚至 version {target_version}（新版本 {result.version} 记录反向操作）")
