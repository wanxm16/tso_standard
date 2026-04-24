"""I-08 入口：benchmark 评估。

用法：
  python3 scripts/run_evaluation.py                                   # embedding 通路，22 JSON
  python3 scripts/run_evaluation.py channel=tfidf                     # TF-IDF 通路
  python3 scripts/run_evaluation.py channel=both                      # tfidf + embedding
  python3 scripts/run_evaluation.py channel=all                       # 全部通路（tfidf/embedding/intent/rerank/fusion）
  python3 scripts/run_evaluation.py channel=rerank source=csv flag=1  # 60 CSV 中 flag=1 的 10 条
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.evaluator import main


if __name__ == "__main__":
    args = sys.argv[1:]
    channel = "embedding"
    source = "json"
    flag: float | None = None
    for a in args:
        if a.startswith("channel="):
            channel = a.split("=", 1)[1]
        elif a.startswith("source="):
            source = a.split("=", 1)[1]
        elif a.startswith("flag="):
            flag = float(a.split("=", 1)[1])

    if channel == "both":
        main(channel="tfidf", benchmark_source=source, flag_filter=flag)
        main(channel="embedding", benchmark_source=source, flag_filter=flag)
    elif channel == "all":
        for ch in ("tfidf", "embedding", "intent", "slot_max", "fusion", "rerank"):
            main(channel=ch, benchmark_source=source, flag_filter=flag)
    else:
        main(channel=channel, benchmark_source=source, flag_filter=flag)
