"""I-04 入口：归槽决策 + 冲突检测 + LLM 兜底。

用法：
  python3 scripts/make_field_normalization.py                 # 全量 + LLM 兜底
  python3 scripts/make_field_normalization.py no-llm          # 禁用 LLM，纯规则
  python3 scripts/make_field_normalization.py smoke <vt_id>   # 单 VT smoke（含 LLM）
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.decision_engine import main


if __name__ == "__main__":
    args = sys.argv[1:]
    enable_llm = True
    vt_id = None

    i = 0
    while i < len(args):
        a = args[i]
        if a == "no-llm":
            enable_llm = False
        elif a == "smoke":
            if i + 1 < len(args):
                vt_id = args[i + 1]
                i += 1
        i += 1
    main(enable_llm=enable_llm, limit_vt_id=vt_id)
