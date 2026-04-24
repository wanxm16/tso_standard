"""为每张 VT 用 qwen3-max 生成召回扩展字段并写回 scaffold_final.yaml。

新增字段：
- recall_summary:    1-2 句业务语义概述（"这张 VT 承载什么业务数据 / 典型用途"）
- typical_questions: 3-5 条典型业务问法（对标 benchmark query 的常见句式）
- topic_aliases:     3-6 个主题同义词 / 上下位词

用法：
    python3 scripts/enrich_vt_recall.py                  # 全量（有缓存的秒过）
    python3 scripts/enrich_vt_recall.py --only-vt vt_xx  # 单张 VT 调试
    python3 scripts/enrich_vt_recall.py --force          # 忽略已有字段，重新生成
    python3 scripts/enrich_vt_recall.py --concurrency 10 # 并发度
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml
from ruamel.yaml import YAML

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.llm_client import chat  # noqa: E402

SCAFFOLD_YAML = ROOT / "output" / "virtual_tables_scaffold_final.yaml"
SCAFFOLD_JSON = ROOT / "output" / "virtual_tables_scaffold_final.json"
VIRTUAL_FIELDS_JSON = ROOT / "output" / "virtual_fields.json"


SYSTEM_PROMPT = """你是数据建模专家，为"虚拟表"（Virtual Table, VT）生成召回扩展信息，用于 text2sql 表召回层。

一张 VT 是一个业务主题数据集合，由 topic（主题名）、l2_path（层级分类）、若干候选源表、若干虚拟字段组成。
召回时，系统用用户自然语言 query 和 VT 的语义文本做 embedding 余弦相似度排序。

你的任务是为 VT 生成三类信息，让语义 embedding 能更准确匹配用户业务问法：
1. recall_summary：1-2 句业务语义概述，说清"这张 VT 承载什么数据、典型用途是什么"
2. typical_questions：3-5 条用户可能的业务问法（中文自然句），贴近 benchmark 实际 query 的句式
3. topic_aliases：3-6 个主题同义词/上下位词/业务别名（用来覆盖 query 里的不同叫法）

严格输出 JSON，字段名固定：
{
  "recall_summary": "...",
  "typical_questions": ["...", "...", "..."],
  "topic_aliases": ["...", "..."]
}

约束：
- 每条 typical_question 15-40 字，自然业务问法（如"X 名下有几套房产？"、"某人近 6 个月的出行记录？"）
- topic_aliases 只放同义词，不放字段名
- 不要重复 topic 字面量 —— 重复词在下游会被稀释
"""


def build_user_prompt(vt: dict, vfs: list[dict]) -> str:
    tables = "\n".join(
        f"  - {t.get('en','')} ({t.get('cn','')})" for t in (vt.get("candidate_tables") or [])[:6]
    ) or "  (无)"

    vf_preview = "\n".join(
        f"  - {v.get('field_cn_name','')}" for v in (vfs or [])[:15]
    ) or "  (无)"
    if len(vfs or []) > 15:
        vf_preview += f"\n  ... 共 {len(vfs)} 个虚拟字段"

    return f"""请为以下 VT 生成召回扩展信息。

【VT 元信息】
- topic: {vt.get('topic','')}
- table_type: {vt.get('table_type','')}
- grain_desc: {vt.get('grain_desc','')}
- l2_path: {' / '.join(vt.get('l2_path') or [])}

【候选源表】
{tables}

【本 VT 已定义的虚拟字段（节选）】
{vf_preview}

请仅输出 JSON。"""


def enrich_one(vt: dict, vfs_by_vt: dict[str, list[dict]]) -> dict:
    vfs = vfs_by_vt.get(vt["vt_id"], [])
    user = build_user_prompt(vt, vfs)
    raw = chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        json_mode=True,
        use_cache=True,
    )
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)[1]
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip("`").strip()
        obj = json.loads(stripped)

    summary = str(obj.get("recall_summary") or "").strip()
    tq_raw = obj.get("typical_questions") or []
    tq = [str(q).strip() for q in tq_raw if str(q).strip()][:6]
    al_raw = obj.get("topic_aliases") or []
    al = [str(a).strip() for a in al_raw if str(a).strip()][:8]

    return {
        "recall_summary": summary,
        "typical_questions": tq,
        "topic_aliases": al,
    }


def load_vfs_by_vt() -> dict[str, list[dict]]:
    if not VIRTUAL_FIELDS_JSON.exists():
        return {}
    with VIRTUAL_FIELDS_JSON.open(encoding="utf-8") as f:
        data = json.load(f)
    vfs = data.get("virtual_fields") if isinstance(data, dict) else data
    if not vfs:
        return {}
    out: dict[str, list[dict]] = {}
    for v in vfs:
        out.setdefault(v["vt_id"], []).append(v)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-vt", default=None, help="只处理指定 VT（调试用）")
    parser.add_argument("--force", action="store_true",
                        help="忽略已有字段强制重生成（默认跳过已有 recall_summary 的 VT）")
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()

    # ruamel 保留注释 + 顺序
    ruamel = YAML()
    ruamel.preserve_quotes = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    ruamel.width = 2000

    with SCAFFOLD_YAML.open(encoding="utf-8") as f:
        doc = ruamel.load(f)
    vts = doc.get("virtual_tables", []) or []

    vfs_by_vt = load_vfs_by_vt()

    # 筛出要跑的 VT
    targets = []
    for vt in vts:
        vt_id = vt.get("vt_id", "")
        if args.only_vt and vt_id != args.only_vt:
            continue
        if not args.force and vt.get("recall_summary"):
            continue
        targets.append(vt)

    print(f"共 {len(vts)} 张 VT，本次处理 {len(targets)} 张")
    if not targets:
        print("（无需要处理的 VT，可用 --force 强制重跑）")
        return

    results: dict[str, dict] = {}
    failures: list[tuple[str, str]] = []

    def _task(vt):
        try:
            return vt["vt_id"], enrich_one(vt, vfs_by_vt), None
        except Exception as e:
            return vt["vt_id"], None, str(e)

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [ex.submit(_task, vt) for vt in targets]
        for i, f in enumerate(as_completed(futs), 1):
            vt_id, obj, err = f.result()
            if err:
                failures.append((vt_id, err))
                print(f"  [{i}/{len(targets)}] ❌ {vt_id}: {err[:80]}")
            else:
                results[vt_id] = obj
                tq_n = len(obj["typical_questions"])
                al_n = len(obj["topic_aliases"])
                print(f"  [{i}/{len(targets)}] ✅ {vt_id}: summary={len(obj['recall_summary'])}字 tq={tq_n} al={al_n}")

    # 回写 yaml
    for vt in vts:
        vt_id = vt.get("vt_id")
        if vt_id in results:
            vt["recall_summary"] = results[vt_id]["recall_summary"]
            vt["typical_questions"] = results[vt_id]["typical_questions"]
            vt["topic_aliases"] = results[vt_id]["topic_aliases"]

    with SCAFFOLD_YAML.open("w", encoding="utf-8") as f:
        ruamel.dump(doc, f)
    print(f"\nYAML 已更新: {SCAFFOLD_YAML}")

    # 同步更新 json（下游 pipeline 有读 json 的）
    with SCAFFOLD_YAML.open(encoding="utf-8") as f:
        plain = yaml.safe_load(f)
    with SCAFFOLD_JSON.open("w", encoding="utf-8") as f:
        json.dump(plain, f, ensure_ascii=False, indent=2)
    print(f"JSON 已同步: {SCAFFOLD_JSON}")

    print(f"\n完成 ✅ 成功 {len(results)} / 失败 {len(failures)}")
    if failures:
        print("失败列表:")
        for vt_id, err in failures[:10]:
            print(f"  {vt_id}: {err[:120]}")


if __name__ == "__main__":
    main()
