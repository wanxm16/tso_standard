"""为每个 slot 生成语义 embedding，供 slot 级召回通道使用。

产物：output/slot_embeddings.parquet
列：vt_id, slot_name, slot_from, slot_role, slot_text, embedding (list[float], 1024 维)

用法：
    python3 scripts/build_slot_embeddings.py         # 全量（有缓存秒级）
    python3 scripts/build_slot_embeddings.py --force # 忽略缓存重新 embed
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.llm_client import embed  # noqa: E402

SLOT_DEF_YAML = ROOT / "output" / "slot_definitions.yaml"
BASE_SLOTS_YAML = ROOT / "data" / "slot_library" / "base_slots.yaml"
OUT_PARQUET = ROOT / "output" / "slot_embeddings.parquet"


def load_base_slots_map() -> dict[str, dict]:
    with BASE_SLOTS_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {s["name"]: s for s in data.get("base_slots", [])}


def materialize_slot(slot: dict, base_map: dict[str, dict]) -> dict:
    """把 base 引用的 slot 展开成完整字段；extended 原样返回。"""
    name = slot.get("name", "")
    from_type = slot.get("from", "extended")
    out = dict(slot)
    if from_type == "base":
        b = base_map.get(name) or {}
        for key in ("cn_name", "description", "aliases", "logical_type"):
            if not out.get(key) and b.get(key):
                out[key] = b[key]
        if not out.get("role"):
            out["role"] = b.get("role", "")
    return out


def build_slot_text(slot: dict) -> str:
    """聚焦单 slot 语义的文本。

    策略：cn_name 权重最大（重复 2 次）+ aliases + description + name。
    不放 mapped_fields 里的具体字段名（那是底层物理信息，对 query 语义无增益且会稀释）。
    """
    parts: list[str] = []
    cn_name = str(slot.get("cn_name") or "").strip()
    if cn_name:
        parts.extend([cn_name, cn_name])  # cn_name 重复 2 次加权
    description = str(slot.get("description") or "").strip()
    if description:
        parts.append(description)
    aliases = slot.get("aliases") or []
    for a in aliases:
        if a and str(a).strip():
            parts.append(str(a).strip())
    name = str(slot.get("name") or "").strip()
    if name:
        parts.append(name)
    return " ".join(p for p in parts if p)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="忽略 LLM 缓存强制重算")
    args = parser.parse_args()

    with SLOT_DEF_YAML.open(encoding="utf-8") as f:
        sd = yaml.safe_load(f)
    base_map = load_base_slots_map()

    rows: list[dict] = []
    for vt in sd.get("virtual_tables", []) or []:
        vt_id = vt.get("vt_id", "")
        if not vt_id:
            continue
        for slot in vt.get("slots", []) or []:
            slot_name = slot.get("name", "")
            if not slot_name:
                continue
            mat = materialize_slot(slot, base_map)
            slot_text = build_slot_text(mat)
            if not slot_text:
                continue
            rows.append({
                "vt_id": vt_id,
                "slot_name": slot_name,
                "slot_from": slot.get("from", "extended"),
                "slot_role": mat.get("role", ""),
                "slot_text": slot_text,
            })

    if not rows:
        print("⚠️ 无 slot，退出")
        return

    print(f"共 {len(rows)} 个 slot，准备 embed...")
    texts = [r["slot_text"] for r in rows]
    # batch embed（src.llm_client.embed 内部已批次处理，接受 list[str]）
    vecs = embed(texts, use_cache=(not args.force))
    # 归一化到单位长度（后续余弦直接用点积）
    arr = np.array(vecs, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    arr_normed = arr / norms
    for i, r in enumerate(rows):
        r["embedding"] = arr_normed[i].tolist()

    df = pd.DataFrame(rows)
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PARQUET, index=False)
    print(f"✅ 已产出 {OUT_PARQUET}")
    print(f"   rows={len(df)}  vt={df['vt_id'].nunique()}  dim={len(rows[0]['embedding'])}")


if __name__ == "__main__":
    main()
