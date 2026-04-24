"""I-01: 为 119 张虚拟表生成语义槽位清单。

流程：
1. 加载 base_slots.yaml（~30 个跨域通用槽位）
2. 加载脚手架定稿（virtual_tables_scaffold_final.json）
3. 加载 DDL with sample（字段样例）
4. 对每张 VT：
   - 收集候选源表的字段样本
   - 调 qwen3-max：基于 base_slots + 字段样本生成槽位清单（base 复用 + extended 新建）
   - 严格校验 LLM 输出 schema
5. 产出：
   - output/slot_definitions.yaml
   - output/slot_definitions.json
   - output/slot_definitions_diagnostic.md

用法：
    python3 scripts/generate_slot_definitions.py smoke <vt_id>   # 单张 smoke
    python3 scripts/generate_slot_definitions.py limit=5         # 前 5 张
    python3 scripts/generate_slot_definitions.py                 # 全量

设计文档：§ 10.8.4
TASK: tasks/TASK-I-01-语义槽位定义.md
"""
from __future__ import annotations

import concurrent.futures
import json
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.llm_client import chat  # noqa: E402
from src.naming_lint import (  # noqa: E402
    SLOT_NAMING_GUARDRAILS,
    collect_slot_name_issues,
    format_naming_retry_feedback,
    validate_slot_name,
)


SCAFFOLD_JSON = REPO_ROOT / "output" / "virtual_tables_scaffold_final.json"
BASE_SLOTS_YAML = REPO_ROOT / "data" / "slot_library" / "base_slots.yaml"
DDL_CSV = REPO_ROOT / "data" / "phrase_2" / "二期_DDL_all_with_sample.csv"
FIELD_FEATURES_PARQUET = REPO_ROOT / "output" / "field_features.parquet"

OUT_YAML = REPO_ROOT / "output" / "slot_definitions.yaml"
OUT_JSON = REPO_ROOT / "output" / "slot_definitions.json"
OUT_DIAG = REPO_ROOT / "output" / "slot_definitions_diagnostic.md"
NORM_REVIEWED_PARQUET = REPO_ROOT / "output" / "field_normalization_reviewed.parquet"

# 种子来源标签（reviewer_note 前缀）—— 用来区分"LLM 种子"vs"人工决策"
# 种子标签可被新种子覆盖；人工决策永远优先
LLM_SEED_NOTE = "llm_seed"
LEGACY_SEED_NOTE = "from slot regeneration"  # 兼容 VTEditor 旧写入


def _is_seed_note(note) -> bool:
    if note is None:
        return False
    s = str(note)
    return s.startswith(LLM_SEED_NOTE) or s == LEGACY_SEED_NOTE


MAX_FIELDS_PER_TABLE_IN_PROMPT = 25
MAX_SAMPLE_LEN = 60


SYSTEM_PROMPT = f"""你是一位数据治理专家。任务是为一张虚拟表（text2sql 表召回层的主题表）定义完整的"语义字段槽位"清单。

核心概念：
- 一张虚拟表 = 一个主题 + 一个稳定粒度
- 虚拟表不直接由物理字段组成，而是由一组"语义槽位"定义；每个槽位抽象表达一个业务概念
- 每个槽位后续会被一批物理字段归一到一起（例如槽位 certificate_no ← sfzh/gmsfhm/zheng_jian_hao）

槽位来源（两类）：
- base：从预定义的"基础跨域槽位库"复用（姓名、证件号、时间、地点等跨主题通用概念）
- extended：该 VT 独有的业务细节槽位（如出入境方向、加油升数、签证类型）

严格原则：
1. **base 优先**：输入字段中能对应到 base_slots 的，必须用 base，绝对不要重复造同义槽位
2. **extended 必须是 base 覆盖不到的真实业务概念**，不能是 base 的同义词
3. **每张 VT 的槽位数控制在 10-30 个**：**鼓励细粒度，不要过度合并**。
   - 同一个业务概念的多个具名变体（如"出生地 / 出生地详址 / 出生地代码"、"联系方式 / 手机号 / 固定电话"）**应拆成不同槽位**，不要合并到一个宽泛的 slot
   - 典型反例：把"身高 / 血型 / 专长技能 / 宗教信仰 / 婚姻状况"全塞进一个"personal_attributes" —— 错
   - 典型正例：分别建 `height` / `blood_type` / `specialty_skill` / `religion` / `marital_status` 五个槽位
4. **必须覆盖主题涉及的关键维度**：参考 § 10.8.4 的"9 类槽位"——主语/标识/时间/地点/行为或关系/状态/来源/描述/统计
5. **extended 槽位必须给 cn_name + aliases**，aliases 至少 3 个（中文全称+常见缩写/拼音）
6. **每个 slot 必须至少关联 1 个源字段（mapped_fields）**：这是"槽位就是一批语义等价字段的归一"的落地证据。未被任何字段触发的 slot 不应该存在。
7. **mapped_fields 只能从 "候选源表字段样本" 中选**：不要发明字段、不要换名、不要改写 table_en/field_name，原样引用

{SLOT_NAMING_GUARDRAILS}

字段角色（role）枚举：
subject（主语） / subject_id（主体标识） / relation_subject（关系另一方）
display（展示） / time（时间） / location（地点）
filter（过滤） / measure（统计） / source（来源） / description（描述）

输出严格 JSON，字段与 schema 完全一致。"""


USER_PROMPT_TEMPLATE = """请为以下虚拟表生成完整槽位清单。

## 虚拟表信息

- topic: {topic}
- table_type: {table_type}
- grain_desc: {grain_desc}
- l2_path: {l2_path}
{review_hint_block}

## 可用的基础槽位（base_slots，优先复用这些）

{base_slots_compact}

## 候选源表字段样本

下列字段来自本 VT 的源表，**已过滤**：保留 DDL 使用情况为"已用"、非技术噪声、样例非空的字段。
判断时请综合 table/field 名、注释、样例值。每个字段都用 `table=... field=...` 明确标识，**你在 mapped_fields 里必须原样引用**。

{field_samples}

## 输出 JSON schema

```json
{{
  "slots": [
    {{
      "name": "英文名（base 复用时必须与 base_slots name 一致；extended 新建时 snake_case）",
      "from": "base" 或 "extended",
      "role": "subject|subject_id|relation_subject|display|time|location|filter|measure|source|description",
      "mapped_fields": [
        {{"table_en": "表名（原样引用）", "field_name": "字段名（原样引用）", "field_comment": "字段注释（原样）"}}
      ],
      // 以下字段仅 extended 需要（base 引用时可省略）
      "cn_name": "中文名",
      "logical_type": "逻辑类型（优先复用 base_slots 已有的 logical_type；新类型可用 custom_xxx）",
      "aliases": ["至少 3 个中文/英文/拼音同义词"],
      "applicable_table_types": ["主档", "关系", "事件", ...],
      "llm_reason": "为什么新建这个 extended 而不是用 base"
    }}
  ],
  "summary": "本 VT 槽位设计思路的一句话总结"
}}
```

要求：
- **槽位数 10-30**（鼓励细粒度；如果源表字段多且业务细节丰富，**优先出 25-30 而不是 10-15**，不要因为嫌多而合并不同业务概念）
- 同一 VT 内 name 不重复
- base 引用时 name 必须在 base_slots 名称列表内
- extended 新建时 aliases 至少 3 个
- **每个 slot 至少有 1 个 mapped_fields**，且 table_en/field_name 必须来自上面的字段样本清单（不是从 base_slots.yaml 里猜）
- 同一字段可映射到多个 slot，但应当尽量唯一归属（归到最能代表其业务含义的 slot）"""


def load_base_slots_compact(base_slots_data: dict) -> str:
    """把 base_slots 精简成 prompt 用的表格。"""
    lines = ["| name | cn_name | logical_type | role | 核心 aliases |",
             "| --- | --- | --- | --- | --- |"]
    for slot in base_slots_data["base_slots"]:
        aliases = ", ".join(slot.get("aliases", [])[:5])
        lines.append(f"| {slot['name']} | {slot['cn_name']} | {slot['logical_type']} | {slot['role']} | {aliases} |")
    return "\n".join(lines)


def _is_empty_sample(sample: str) -> bool:
    """判断 sample 是否为空：空串、nan/null/空格/只有分隔符。"""
    if not sample:
        return True
    s = sample.strip()
    if not s:
        return True
    # 剥掉分隔符看剩余
    tokens = [t.strip() for t in s.replace("|", " ").split() if t.strip()]
    if not tokens:
        return True
    # 全是 null/nan/空白变体
    null_like = {"null", "nan", "none", "", "-"}
    if all(t.lower() in null_like for t in tokens):
        return True
    return False


def build_field_samples_text(
    ddl_df: pd.DataFrame,
    source_tables: list[dict],
    used_keys: set[tuple[str, str]] | None = None,
) -> tuple[str, list[tuple[str, str, str]]]:
    """把 VT 的所有源表字段样本组织成 prompt 文本。

    过滤：
    - sample 为空（空串 / 全 null / nan）
    - 技术字段（字段黑名单 + is_technical_noise 硬编码规则）
    - 若 used_keys 非空：只保留 (table_en, field_name) 在 used_keys 里的字段（DDL usage_count > 0）

    返回：
    - prompt 文本（每行用 table=`x` field=`y` 便于 LLM 引用）
    - 可用的 (table_en, field_name, field_comment) 三元组列表，用于后续 mapped_fields 校验
    """
    # 懒加载 feature_builder 的过滤函数（它已经读了 yaml 黑名单）
    try:
        from src.pipeline.feature_builder import is_technical_noise  # type: ignore
    except Exception:
        def is_technical_noise(field_name: str, comment: str, samples: list, table_en: str | None = None):  # type: ignore
            return False, ""

    valid_fields: list[tuple[str, str, str]] = []  # (table_en, field_name, comment)
    blocks = []
    for st in source_tables:
        en = st["en"]
        cn = st.get("cn", "")
        sub = ddl_df[ddl_df["table"] == en]
        if sub.empty:
            sub = ddl_df[ddl_df["origin_table"] == en]
        if sub.empty:
            blocks.append(f"### 表 {en} ({cn})\n  (未在 DDL 中找到)")
            continue

        # 全表字段 usage 回退：若本表所有字段都不在 used_keys 里（可能是新表/无 SQL 历史），
        # 则这张表绕过 "已用" 过滤（把全部字段当作已用处理）。
        table_bypass_used = False
        if used_keys is not None:
            any_used = any((en, str(r.get("field", ""))) in used_keys for _, r in sub.iterrows())
            if not any_used:
                table_bypass_used = True

        lines = [f"### 表 {en} ({cn})"]
        if table_bypass_used:
            lines.append(f"  _注：本表无 SQL usage 历史，全部字段视为已用_")
        kept = 0
        skipped_empty = 0
        skipped_noise = 0
        skipped_not_used = 0
        for _, row in sub.iterrows():
            if kept >= MAX_FIELDS_PER_TABLE_IN_PROMPT:
                break
            field_name = str(row.get("field", ""))
            comment = str(row.get("comment", "") or "")
            sample = str(row.get("sample_data", "") or "")

            if _is_empty_sample(sample):
                skipped_empty += 1
                continue
            is_noise, _reason = is_technical_noise(field_name, comment, [sample], table_en=en)
            if is_noise:
                skipped_noise += 1
                continue
            if used_keys is not None and not table_bypass_used and (en, field_name) not in used_keys:
                skipped_not_used += 1
                continue

            sample_clean = sample
            if len(sample_clean) > MAX_SAMPLE_LEN:
                sample_clean = sample_clean[:MAX_SAMPLE_LEN] + "..."
            # 关键变化：table=`x` field=`y` 便于 LLM 在 mapped_fields 里精确引用
            lines.append(
                f"- table=`{en}` field=`{field_name}` [{row.get('type','')}] · "
                f"{comment or '(无注释)'} · 样例: {sample_clean}"
            )
            valid_fields.append((en, field_name, comment))
            kept += 1

        if kept == 0:
            lines.append(
                f"  (过滤后无有效字段：空样例 {skipped_empty}，技术字段 {skipped_noise}，未用 {skipped_not_used})"
            )
        elif skipped_empty + skipped_noise + skipped_not_used > 0:
            lines.append(
                f"  _过滤：空样例 {skipped_empty}，技术字段 {skipped_noise}，未用 {skipped_not_used}_"
            )
        blocks.append("\n".join(lines))

    return "\n\n".join(blocks), valid_fields


def build_review_hint_block(vt: dict) -> str:
    if "review_hint" not in vt:
        return ""
    rh = vt["review_hint"]
    return (
        f"\n- **注意**：本 VT 为脚手架定稿中的『待定』类型，"
        f"LLM 曾建议归属到『{rh.get('llm_suggested_l2')}』分类。"
        f"生成槽位时，请参照 suggested_l2 所属主题的典型槽位。\n"
    )


def build_coverage_hint_block(source_table_count: int, valid_field_count: int) -> str:
    """当 VT 源表数少（≤2）时，强制要求 LLM 为每一个已用字段都建 slot，不合并不丢。"""
    if source_table_count > 2:
        return ""
    return (
        f"\n## ⚠️ 小 VT 覆盖模式（本 VT 只有 {source_table_count} 张源表 / {valid_field_count} 个候选字段）\n\n"
        f"**必须为每一个候选字段都建一个槽位**（要么复用 base_slots，要么建 extended）：\n"
        f"- 不要为了槽位数限制而合并不同概念的字段\n"
        f"- 不要丢字段\n"
        f"- 预期槽位数 ≈ 候选字段数 ({valid_field_count})\n"
        f"- 超过 30 是正常的，不要因为 prompt 里 '10-30' 约束而砍字段\n"
    )


def build_prompt(vt: dict, base_slots_compact: str, field_samples: str, source_table_count: int = 0, valid_field_count: int = 0) -> tuple[str, str]:
    coverage_hint = build_coverage_hint_block(source_table_count, valid_field_count)
    user = USER_PROMPT_TEMPLATE.format(
        topic=vt["topic"],
        table_type=vt["table_type"],
        grain_desc=vt["grain_desc"],
        l2_path=" / ".join(vt["l2_path"]),
        review_hint_block=build_review_hint_block(vt),
        base_slots_compact=base_slots_compact,
        field_samples=field_samples,
    )
    if coverage_hint:
        # 把覆盖提示追加到 user prompt 尾部（最后出现的指令权重最高）
        user = user + coverage_hint
    return SYSTEM_PROMPT, user


def validate_llm_output(
    raw: dict,
    vt: dict,
    base_slot_names: set[str],
    valid_fields: list[tuple[str, str, str]] | None = None,
    small_vt_mode: bool = False,
) -> tuple[dict, list[str]]:
    """校验 LLM 输出，返回清洗后的结果和 warnings 列表。

    valid_fields：prompt 里实际提供过的 (table_en, field_name, comment) 列表。
    用于校验 LLM 在 mapped_fields 里引用的字段是否真实存在（防止幻觉）。
    """
    warnings: list[str] = []
    slots_raw = raw.get("slots", []) or []

    # 建立合法字段查找表（含 comment 回填）
    valid_field_set: set[tuple[str, str]] = set()
    field_comment_lookup: dict[tuple[str, str], str] = {}
    if valid_fields:
        for en, fn, cm in valid_fields:
            valid_field_set.add((en, fn))
            field_comment_lookup[(en, fn)] = cm

    cleaned_slots = []
    seen_names: set[str] = set()

    for i, slot in enumerate(slots_raw):
        name = slot.get("name", "").strip()
        from_type = slot.get("from", "").strip()
        role = slot.get("role", "").strip()

        if not name:
            warnings.append(f"slot[{i}]: missing name, dropped")
            continue
        if name in seen_names:
            warnings.append(f"slot[{i}]={name}: duplicate name in this VT, dropped")
            continue
        if from_type not in ("base", "extended"):
            warnings.append(f"slot[{i}]={name}: invalid from={from_type}, dropped")
            continue

        # base 引用必须在 base_slots 中真实存在
        if from_type == "base" and name not in base_slot_names:
            warnings.append(f"slot[{i}]={name}: claimed base but not in base_slots, treated as extended")
            from_type = "extended"

        for issue in validate_slot_name(name, source=from_type, base_slot_names=base_slot_names):
            warnings.append(f"slot[{i}]={name}: {issue}")

        # mapped_fields 校验：必须非空，字段必须在候选池里
        raw_mapped = slot.get("mapped_fields") or []
        mapped_fields: list[dict] = []
        for mf in raw_mapped:
            if not isinstance(mf, dict):
                continue
            en = str(mf.get("table_en") or "").strip()
            fn = str(mf.get("field_name") or "").strip()
            if not en or not fn:
                continue
            if valid_field_set and (en, fn) not in valid_field_set:
                warnings.append(f"slot[{i}]={name}: mapped_fields 引用了不存在的字段 {en}.{fn}，丢弃该引用")
                continue
            mapped_fields.append({
                "table_en": en,
                "field_name": fn,
                "field_comment": str(mf.get("field_comment") or field_comment_lookup.get((en, fn), "")),
            })

        # 强制：每个 slot 必须至少有 1 个合法 mapped_fields
        if not mapped_fields:
            warnings.append(f"slot[{i}]={name}: mapped_fields 为空或全部非法，整个 slot 被丢弃")
            continue

        cleaned = {
            "name": name,
            "from": from_type,
            "role": role,
            "mapped_fields": mapped_fields,
        }

        if from_type == "extended":
            cn_name = slot.get("cn_name", "").strip()
            aliases = [a.strip() for a in (slot.get("aliases") or []) if a.strip()]
            logical_type = slot.get("logical_type", "").strip()
            applicable = slot.get("applicable_table_types") or []

            if not cn_name:
                warnings.append(f"slot[{i}]={name}: extended missing cn_name, using name as cn_name")
                cn_name = name
            if len(aliases) < 3:
                warnings.append(f"slot[{i}]={name}: extended has only {len(aliases)} aliases (<3)")
            if not logical_type:
                warnings.append(f"slot[{i}]={name}: extended missing logical_type, using 'custom'")
                logical_type = "custom"

            cleaned.update({
                "cn_name": cn_name,
                "logical_type": logical_type,
                "aliases": aliases,
                "applicable_table_types": applicable,
                "llm_reason": slot.get("llm_reason", ""),
            })

        seen_names.add(name)
        cleaned_slots.append(cleaned)

    # 数量检查：默认 10-30 上限；小 VT 模式（源表 ≤2）放宽到 max(30, valid_field_count)
    valid_field_count = len(valid_fields or [])
    if small_vt_mode:
        # 上限 = max(30, 候选字段数)，最多不超过 100（再多就是 LLM 疯了）
        cap = max(30, min(valid_field_count, 100))
    else:
        cap = 30
    if len(cleaned_slots) < 10 and not small_vt_mode:
        warnings.append(f"slot count={len(cleaned_slots)} < 10 (recommended minimum)")
    if len(cleaned_slots) > cap:
        warnings.append(f"slot count={len(cleaned_slots)} > {cap}, trimming to top {cap} (by role priority)")
        role_priority = {
            "subject_id": 0, "subject": 1, "relation_subject": 2,
            "time": 3, "location": 4, "filter": 5, "measure": 6,
            "display": 7, "source": 8, "description": 9,
        }
        cleaned_slots.sort(key=lambda s: role_priority.get(s.get("role", ""), 10))
        cleaned_slots = cleaned_slots[:cap]

    # 小 VT 覆盖率硬约束（W5-F2 内联）：
    # 候选字段全集应几乎都被至少一个 slot mapped_fields 引用；漏字段记 warnings，不阻断
    # 非小 VT 也算 coverage 但只作诊断，不发 warning
    mapped_set: set[tuple[str, str]] = set()
    for s in cleaned_slots:
        for mf in s.get("mapped_fields", []) or []:
            en = str(mf.get("table_en") or "").strip()
            fn = str(mf.get("field_name") or "").strip()
            if en and fn:
                mapped_set.add((en, fn))
    missing_fields: list[tuple[str, str]] = []
    if valid_fields:
        valid_pairs = {(en, fn) for en, fn, _ in valid_fields}
        missing_fields = sorted(valid_pairs - mapped_set)
        coverage = (len(valid_pairs) - len(missing_fields)) / max(1, len(valid_pairs))
        if small_vt_mode and coverage < 0.8:
            preview = ", ".join(f"{en}.{fn}" for en, fn in missing_fields[:8])
            more = f" (+{len(missing_fields)-8} more)" if len(missing_fields) > 8 else ""
            warnings.append(
                f"small_vt coverage={coverage:.0%} < 80%"
                f"，未覆盖 {len(missing_fields)}/{len(valid_pairs)} 字段: {preview}{more}"
            )

    return {
        "slots": cleaned_slots,
        "summary": raw.get("summary", ""),
        "coverage_report": {
            "total_business_fields": valid_field_count,
            "mapped_count": len(mapped_set & {(en, fn) for en, fn, _ in (valid_fields or [])}),
            "missing_fields": [{"table_en": en, "field_name": fn} for en, fn in missing_fields],
            "small_vt_mode": small_vt_mode,
        },
    }, warnings


_USED_KEYS_CACHE: set[tuple[str, str]] | None = None
_USED_KEYS_LOCK = threading.Lock()


def _parse_json_response(raw_content: str) -> dict:
    try:
        return json.loads(raw_content)
    except json.JSONDecodeError:
        stripped = raw_content.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)[1]
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip("`").strip()
        return json.loads(stripped)


def load_used_keys() -> set[tuple[str, str]]:
    """从 field_features.parquet 构造 (table_en, field_name) → usage_count>0 集合，模块级缓存。"""
    global _USED_KEYS_CACHE
    if _USED_KEYS_CACHE is not None:
        return _USED_KEYS_CACHE
    with _USED_KEYS_LOCK:
        if _USED_KEYS_CACHE is not None:
            return _USED_KEYS_CACHE
        keys: set[tuple[str, str]] = set()
        try:
            if FIELD_FEATURES_PARQUET.exists():
                feat_df = pd.read_parquet(FIELD_FEATURES_PARQUET)
                for _, r in feat_df[feat_df["usage_count"] > 0].iterrows():
                    keys.add((str(r["table_en"]), str(r["field_name"])))
        except Exception as e:
            print(f"⚠️  加载 field_features 失败（跳过 'used' 过滤）: {e}")
        _USED_KEYS_CACHE = keys
        return keys


def generate_slots_for_vt(
    vt: dict,
    base_slots_data: dict,
    base_slots_compact: str,
    ddl_df: pd.DataFrame,
) -> tuple[dict, list[str]]:
    base_slot_names = {s["name"] for s in base_slots_data["base_slots"]}
    source_table_count = len(vt.get("candidate_tables") or [])
    small_vt_mode = source_table_count <= 2
    # small_vt_mode 下不走 used_keys 过滤：小表的承诺是"覆盖全部业务字段"，
    # 与 benchmark SQL 的 usage_count 解耦（某些业务表可能从未出现在 benchmark 里，
    # 但其业务字段仍需建 slot）。由 is_technical_noise 负责剔除真正的技术字段。
    if small_vt_mode:
        effective_used_keys = None
    else:
        effective_used_keys = load_used_keys() or None
    field_samples, valid_fields = build_field_samples_text(
        ddl_df, vt["candidate_tables"], used_keys=effective_used_keys
    )
    system, user = build_prompt(
        vt, base_slots_compact, field_samples,
        source_table_count=source_table_count,
        valid_field_count=len(valid_fields),
    )

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]
    retry_warnings: list[str] = []
    raw: dict = {}
    for attempt in range(2):
        raw_content = chat(
            messages=messages,
            temperature=0.0,
            json_mode=True,
            use_cache=(attempt == 0),
        )
        raw = _parse_json_response(raw_content)
        name_issues = collect_slot_name_issues(
            raw.get("slots", []) or [],
            source_key="from",
            base_slot_names=base_slot_names,
        )
        if not name_issues:
            break
        if attempt == 0:
            messages.extend([
                {"role": "assistant", "content": json.dumps(raw, ensure_ascii=False)},
                {"role": "user", "content": format_naming_retry_feedback(name_issues)},
            ])
            continue
        retry_warnings.extend([f"命名告警：{issue}" for issue in name_issues])

    cleaned, warnings = validate_llm_output(
        raw, vt, base_slot_names,
        valid_fields=valid_fields,
        small_vt_mode=small_vt_mode,
    )
    return cleaned, retry_warnings + warnings


def run_smoke(vt_id: str) -> None:
    scaffold = json.loads(SCAFFOLD_JSON.read_text(encoding="utf-8"))
    target_vt = next((vt for vt in scaffold["virtual_tables"] if vt["vt_id"] == vt_id), None)
    if target_vt is None:
        # 也支持传 topic
        target_vt = next((vt for vt in scaffold["virtual_tables"] if vt["topic"] == vt_id), None)
    if target_vt is None:
        print(f"找不到 vt_id/topic = {vt_id}")
        print("前 10 个 VT:")
        for vt in scaffold["virtual_tables"][:10]:
            print(f"  {vt['vt_id']} | {vt['topic']}")
        return

    base_slots_data = yaml.safe_load(BASE_SLOTS_YAML.read_text(encoding="utf-8"))
    base_slots_compact = load_base_slots_compact(base_slots_data)
    ddl_df = pd.read_csv(DDL_CSV, encoding="utf-8")

    print(f"=== Smoke: {target_vt['topic']} ({target_vt['vt_id']}) ===")
    print(f"  table_type: {target_vt['table_type']}")
    print(f"  source_tables: {target_vt['source_table_count']} 张\n")
    t0 = time.time()
    result, warnings = generate_slots_for_vt(target_vt, base_slots_data, base_slots_compact, ddl_df)
    dt = time.time() - t0

    print(f"=== LLM 输出 ({dt:.1f}s) ===")
    print(f"\nSummary: {result['summary']}\n")

    base_count = sum(1 for s in result["slots"] if s["from"] == "base")
    ext_count = sum(1 for s in result["slots"] if s["from"] == "extended")
    print(f"槽位总数: {len(result['slots'])} (base={base_count}, extended={ext_count})")
    print(f"base 复用率: {base_count/len(result['slots'])*100:.1f}%\n" if result["slots"] else "")

    print("=== base 复用 ===")
    for s in result["slots"]:
        if s["from"] == "base":
            print(f"  [{s['role']:<17}] {s['name']}")

    print("\n=== extended 新建 ===")
    for s in result["slots"]:
        if s["from"] == "extended":
            print(f"  [{s['role']:<17}] {s['name']} ({s['cn_name']}) | {s['logical_type']}")
            print(f"      aliases: {s.get('aliases', [])}")
            print(f"      reason: {s.get('llm_reason', '')[:120]}")

    if warnings:
        print(f"\n=== Warnings ({len(warnings)}) ===")
        for w in warnings:
            print(f"  - {w}")
    else:
        print("\n✅ 无 warnings")


def run_full(limit: int | None = None, concurrency: int = 10) -> None:
    scaffold = json.loads(SCAFFOLD_JSON.read_text(encoding="utf-8"))
    vts = scaffold["virtual_tables"]
    if limit:
        vts = vts[:limit]

    base_slots_data = yaml.safe_load(BASE_SLOTS_YAML.read_text(encoding="utf-8"))
    base_slots_compact = load_base_slots_compact(base_slots_data)
    ddl_df = pd.read_csv(DDL_CSV, encoding="utf-8")

    print(f"共 {len(vts)} 张 VT 需要生成槽位，并发 {concurrency}")

    results_by_index: dict[int, dict] = {}
    warnings_by_index: dict[int, list[str]] = {}
    exceptions_by_index: dict[int, Exception] = {}
    print_lock = threading.Lock()
    counter = {"done": 0}

    def process_one(i: int, vt: dict) -> tuple[int, dict, list[str]]:
        t_start = time.time()
        result, warnings = generate_slots_for_vt(vt, base_slots_data, base_slots_compact, ddl_df)
        dt = time.time() - t_start
        n = len(result["slots"])
        n_base = sum(1 for s in result["slots"] if s["from"] == "base")
        n_ext = n - n_base
        with print_lock:
            counter["done"] += 1
            msg = f"  [{counter['done']:>3}/{len(vts)}] {vt['topic']} ({vt['vt_id']}) → {n} slots (base={n_base}, ext={n_ext}) {dt:.1f}s"
            if warnings:
                msg += f" [{len(warnings)} warn]"
            print(msg, flush=True)
        return i, result, warnings

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_one, i, vt): (i, vt) for i, vt in enumerate(vts)}
        for future in concurrent.futures.as_completed(futures):
            i, vt = futures[future]
            try:
                idx, result, warnings = future.result()
                results_by_index[idx] = result
                warnings_by_index[idx] = warnings
            except Exception as exc:
                exceptions_by_index[i] = exc
                with print_lock:
                    counter["done"] += 1
                    print(f"  [{counter['done']:>3}/{len(vts)}] {vt['topic']} ({vt['vt_id']}) FAILED: {exc}", flush=True)

    # 按原始顺序重组
    all_vt_results: list[dict] = []
    all_warnings: list[dict] = []
    base_reuse_stats: list[tuple[str, int, int, float]] = []

    for i, vt in enumerate(vts):
        topic = vt["topic"]
        if i in exceptions_by_index:
            result = {"slots": [], "summary": f"生成失败: {exceptions_by_index[i]}"}
        else:
            result = results_by_index[i]
            warnings = warnings_by_index[i]
            if warnings:
                all_warnings.append({"vt_id": vt["vt_id"], "topic": topic, "warnings": warnings})
            n = len(result["slots"])
            n_base = sum(1 for s in result["slots"] if s["from"] == "base")
            ratio = n_base / n if n else 0
            base_reuse_stats.append((vt["vt_id"], n_base, n, ratio))

        all_vt_results.append({
            "vt_id": vt["vt_id"],
            "topic": topic,
            "table_type": vt["table_type"],
            "l2_path": vt["l2_path"],
            "grain_desc": vt["grain_desc"],
            "source_table_count": vt["source_table_count"],
            "slots": result["slots"],
            "summary": result["summary"],
            "is_pending": vt["table_type"] == "待定",
        })

    total_dt = time.time() - t0

    # 统计
    total_slots = sum(len(r["slots"]) for r in all_vt_results)
    total_base = sum(sum(1 for s in r["slots"] if s["from"] == "base") for r in all_vt_results)
    total_ext = total_slots - total_base
    overall_ratio = total_base / total_slots if total_slots else 0

    stats = {
        "vt_count": len(all_vt_results),
        "total_slots": total_slots,
        "total_base_refs": total_base,
        "total_extended": total_ext,
        "overall_base_reuse_ratio": round(overall_ratio, 4),
        "avg_slots_per_vt": round(total_slots / len(all_vt_results), 2) if all_vt_results else 0,
        "llm_total_seconds": round(total_dt, 1),
        "vt_with_warnings_count": len(all_warnings),
    }

    output = {
        "stats": stats,
        "base_slot_count": len(base_slots_data["base_slots"]),
        "virtual_tables": all_vt_results,
    }

    # 保留所有 source 以 "manual_" 开头的 slots（归一审核 / VT 详情页等手工来源）
    # 这些 slot 不是 pipeline 生成的，覆盖会丢失用户操作
    # 已知 manual 来源: manual_normalization_review, manual_vt_edit
    manual_slots_by_vt: dict[str, list[dict]] = {}
    if OUT_YAML.exists():
        try:
            with OUT_YAML.open(encoding="utf-8") as f:
                prev = yaml.safe_load(f) or {}
            for vt in prev.get("virtual_tables", []) or []:
                manual = [
                    s for s in (vt.get("slots") or [])
                    if (s.get("source") or "").startswith("manual_")
                ]
                if manual:
                    manual_slots_by_vt[vt["vt_id"]] = manual
        except Exception as e:
            print(f"⚠️  读取旧 slot_definitions 失败（manual slots 保留跳过）: {e}")

    if manual_slots_by_vt:
        merged_count = 0
        for vt in output["virtual_tables"]:
            saved = manual_slots_by_vt.get(vt["vt_id"], [])
            if not saved:
                continue
            existing_names = {s.get("name") for s in vt.get("slots", []) or []}
            for ms in saved:
                if ms.get("name") not in existing_names:
                    vt.setdefault("slots", []).append(ms)
                    merged_count += 1
        print(f"✅ 保留人工添加的 slots: {merged_count} 条（{len(manual_slots_by_vt)} 个 VT）")

    OUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    with OUT_YAML.open("w", encoding="utf-8") as f:
        yaml.dump(output, f, allow_unicode=True, sort_keys=False, width=200)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    write_diagnostic(output, all_warnings, base_reuse_stats)

    print(f"\n=== I-01 槽位生成完成 ===")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"YAML: {OUT_YAML}")
    print(f"JSON: {OUT_JSON}")
    print(f"诊断: {OUT_DIAG}")

    # 把每个 slot 的 mapped_fields 作为 LLM 种子落到 reviewed.parquet
    # 下游 slot_scorer / field_normalization 重跑时会识别为 use_slot（manual），
    # 从而：① UI mapped_count 计入种子；② slot_scorer 把种子字段当 anchor 扩展 aliases 帮其他字段打分
    seed_stats = sync_llm_seeds_to_reviewed(output["virtual_tables"])
    print(
        f"\n种子同步 → reviewed.parquet: "
        f"added={seed_stats['added']} updated={seed_stats['updated']} "
        f"removed_orphans={seed_stats['removed']} kept_manual={seed_stats['kept_manual']}"
    )


def sync_llm_seeds_to_reviewed(
    virtual_tables: list[dict],
    limit_vt_ids: set[str] | None = None,
) -> dict[str, int]:
    """扫 slot_definitions 的 slot.mapped_fields，作为 LLM 种子同步到 reviewed.parquet。

    幂等规则：
    - (table_en, field_name, vt_id) 唯一键
    - 已存在且是"人工决策"（reviewer_note 非种子标签）→ 保留不动（人工优先）
    - 已存在且是"种子"→ 用新种子覆盖（slot 名变了会更新）
    - 不存在 → 新增 decision=use_slot, reviewer_note="llm_seed"
    - 同一 VT 内如果 reviewed 里存在孤儿种子（slot_definitions 不再提到的 field）→ 删除

    limit_vt_ids 非空时只处理这些 VT（单 VT 场景），其他 VT 的 reviewed 行不动。
    """
    import pandas as pd
    from datetime import datetime

    # 1. 从 virtual_tables 收集所有种子 {(vt_id, table_en, field_name): slot_name}
    new_seeds: dict[tuple[str, str, str], str] = {}
    touched_vts: set[str] = set()
    for vt in virtual_tables or []:
        vt_id = str(vt.get("vt_id") or "")
        if not vt_id:
            continue
        if limit_vt_ids is not None and vt_id not in limit_vt_ids:
            continue
        touched_vts.add(vt_id)
        for s in vt.get("slots", []) or []:
            slot_name = str(s.get("name") or "").strip()
            if not slot_name:
                continue
            for mf in s.get("mapped_fields", []) or []:
                en = str(mf.get("table_en") or "").strip()
                fn = str(mf.get("field_name") or "").strip()
                if en and fn:
                    # 同一字段在 VT 里出现多次（多 slot 引用）以第一个为准；极少见
                    new_seeds.setdefault((vt_id, en, fn), slot_name)

    if not touched_vts:
        return {"added": 0, "updated": 0, "removed": 0, "kept_manual": 0}

    # 2. 读取现有 reviewed.parquet
    cols = ["table_en", "field_name", "vt_id", "decision", "decision_slot",
            "reviewed_at", "reviewer_note"]
    if NORM_REVIEWED_PARQUET.exists():
        existing = pd.read_parquet(NORM_REVIEWED_PARQUET)
    else:
        existing = pd.DataFrame(columns=cols)

    # 3. 本次涉及 VT 的 rows 分两堆：人工保留 vs 种子候删
    if len(existing):
        in_scope = existing[existing["vt_id"].isin(touched_vts)].copy()
        out_of_scope = existing[~existing["vt_id"].isin(touched_vts)].copy()
    else:
        in_scope = existing.copy()
        out_of_scope = existing.copy()

    kept_manual = 0
    rows_to_keep: list[dict] = []
    if len(in_scope):
        for _, r in in_scope.iterrows():
            note = r.get("reviewer_note")
            if not _is_seed_note(note):
                # 人工决策：保留，新种子不会覆盖这里的 (vt_id, en, fn)
                rows_to_keep.append(r.to_dict())
                kept_manual += 1
            # 种子行不保留（会被下面重新写入；孤儿种子自然就没了）

    # 4. 把新种子里的"人工冲突 key"去掉（人工优先）
    manual_keys = {(r["vt_id"], r["table_en"], r["field_name"]) for r in rows_to_keep}
    added = 0
    now_iso = datetime.now().isoformat(timespec="seconds")
    for key, slot_name in new_seeds.items():
        if key in manual_keys:
            continue
        vt_id, en, fn = key
        rows_to_keep.append({
            "table_en": en,
            "field_name": fn,
            "vt_id": vt_id,
            "decision": "use_slot",
            "decision_slot": slot_name,
            "reviewed_at": now_iso,
            "reviewer_note": LLM_SEED_NOTE,
        })
        added += 1

    # 5. 组装 + 落盘
    new_df = pd.DataFrame(rows_to_keep, columns=cols) if rows_to_keep else pd.DataFrame(columns=cols)
    merged = pd.concat([out_of_scope, new_df], ignore_index=True) if len(out_of_scope) else new_df

    # 统计 "updated"：新种子里 key 已存在于旧种子但 slot 变了
    # 粗略算：原 in_scope 种子数 - 新写入时 key 还在 manual_keys 的 - added 里是新 key 不在旧里的
    # 这里为简洁，updated 近似为"种子覆盖的行数"—— 不精算
    old_seed_count = len(in_scope) - kept_manual if len(in_scope) else 0
    removed_orphans = max(0, old_seed_count - (added - max(0, added - old_seed_count)))
    # 精确统计太绕，换个算法：
    added_new_keys = 0
    updated_keys = 0
    if len(in_scope):
        old_seed_rows = in_scope[in_scope["reviewer_note"].apply(_is_seed_note)]
        old_seed_keys = {(str(r["vt_id"]), str(r["table_en"]), str(r["field_name"])): str(r["decision_slot"])
                         for _, r in old_seed_rows.iterrows()}
    else:
        old_seed_keys = {}
    for key, slot_name in new_seeds.items():
        if key in manual_keys:
            continue
        if key in old_seed_keys:
            if old_seed_keys[key] != slot_name:
                updated_keys += 1
            # 相同 slot 的种子不算 updated
        else:
            added_new_keys += 1
    removed = max(0, len(old_seed_keys) - (updated_keys + sum(1 for k in old_seed_keys if k in new_seeds)))

    merged.to_parquet(NORM_REVIEWED_PARQUET, index=False)
    return {
        "added": added_new_keys,
        "updated": updated_keys,
        "removed": removed,
        "kept_manual": kept_manual,
    }


def write_diagnostic(output: dict, warnings: list[dict], base_reuse_stats: list) -> None:
    lines: list[str] = [
        "# I-01 槽位定义诊断报告",
        "",
        "## 总体统计",
        "",
        "```json",
        json.dumps(output["stats"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## 槽位数分布",
        "",
    ]
    distribution: dict[str, int] = {}
    for r in output["virtual_tables"]:
        n = len(r["slots"])
        bucket = "<5" if n < 5 else "5-10" if n <= 10 else "11-15" if n <= 15 else "16-20" if n <= 20 else ">20"
        distribution[bucket] = distribution.get(bucket, 0) + 1
    lines.append("| 槽位数区间 | VT 数 |")
    lines.append("| --- | --- |")
    for bucket in ["<5", "5-10", "11-15", "16-20", ">20"]:
        lines.append(f"| {bucket} | {distribution.get(bucket, 0)} |")
    lines.append("")

    lines.append("## base 复用率分布（低复用率优先 review）")
    lines.append("")
    lines.append("| VT | base | total | 复用率 |")
    lines.append("| --- | --- | --- | --- |")
    low_reuse = [x for x in base_reuse_stats if x[2] > 0 and x[3] < 0.4]
    for vt_id, n_base, n_total, ratio in sorted(low_reuse, key=lambda x: x[3])[:20]:
        topic = next((r["topic"] for r in output["virtual_tables"] if r["vt_id"] == vt_id), "")
        lines.append(f"| {topic} | {n_base} | {n_total} | {ratio*100:.0f}% |")
    lines.append("")

    pending = [r for r in output["virtual_tables"] if r.get("is_pending")]
    lines.append(f"## 『待定』类型 VT 的槽位（共 {len(pending)} 张，需重点 review）")
    lines.append("")
    for r in pending:
        lines.append(f"### {r['topic']} · {' / '.join(r['l2_path'])}")
        lines.append(f"- 粒度: {r['grain_desc']}")
        lines.append(f"- 槽位: {len(r['slots'])} 个")
        slot_names = [s["name"] for s in r["slots"]]
        lines.append(f"- 槽位清单: {slot_names}")
        lines.append("")

    lines.append("## Warnings")
    lines.append("")
    if warnings:
        for w in warnings:
            lines.append(f"### {w['topic']} ({w['vt_id']})")
            for msg in w["warnings"]:
                lines.append(f"- {msg}")
            lines.append("")
    else:
        lines.append("（无）")

    OUT_DIAG.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    concurrency = 10
    limit = None

    args = sys.argv[1:]
    if args and args[0] == "smoke":
        if len(args) < 2:
            run_smoke("人员主档")
        else:
            run_smoke(args[1])
        sys.exit(0)
    if args and args[0] == "sync-seeds":
        # 基于现有 slot_definitions.yaml 补跑一次种子同步（不重生成槽位）
        # 用于历史数据补齐场景，例如 pipeline 在加入 sync 逻辑之前产出的槽位
        with OUT_YAML.open(encoding="utf-8") as f:
            existing = yaml.safe_load(f) or {}
        vts = existing.get("virtual_tables", []) or []
        limit_set = None
        if len(args) >= 2:
            limit_set = set(args[1:])
            vts = [v for v in vts if v.get("vt_id") in limit_set]
            print(f"只同步 {len(vts)} 个 VT: {sorted(limit_set)}")
        stats = sync_llm_seeds_to_reviewed(vts, limit_vt_ids=limit_set)
        print(f"种子同步完成: {stats}")
        sys.exit(0)

    for arg in args:
        if arg.startswith("limit="):
            limit = int(arg.split("=", 1)[1])
        elif arg.startswith("concurrency="):
            concurrency = int(arg.split("=", 1)[1])

    run_full(limit=limit, concurrency=concurrency)
