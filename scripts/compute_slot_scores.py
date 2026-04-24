"""I-03 入口：字段→槽位打分。

用法：
    python3 scripts/compute_slot_scores.py                     # 全量（含 embedding）
    python3 scripts/compute_slot_scores.py no-embedding        # 只跑 TF-IDF
    python3 scripts/compute_slot_scores.py smoke <vt_id>       # 单张 VT smoke
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.slot_scorer import main


if __name__ == "__main__":
    args = sys.argv[1:]
    limit_vt_id = None
    enable_emb = True
    i = 0
    while i < len(args):
        a = args[i]
        if a == "smoke":
            if i + 1 < len(args):
                limit_vt_id = args[i + 1]
                i += 1
        elif a == "no-embedding":
            enable_emb = False
        i += 1
    main(limit_vt_id=limit_vt_id, enable_embedding=enable_emb)
