"""I-08b 入口：用 LLM 把 benchmark query 结构化成意图。

用法：
  python3 scripts/extract_query_intents.py                         # 22 条 JSON
  python3 scripts/extract_query_intents.py source=csv              # 60 条 CSV
  python3 scripts/extract_query_intents.py source=csv flag=1       # 10 条 flag=1
  python3 scripts/extract_query_intents.py source=all              # 全部 82 条
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.query_intent_extractor import main

if __name__ == "__main__":
    args = sys.argv[1:]
    source = "json"
    flag: float | None = None
    for a in args:
        if a.startswith("source="):
            source = a.split("=", 1)[1]
        elif a.startswith("flag="):
            flag = float(a.split("=", 1)[1])
    main(benchmark_source=source, flag_filter=flag)
