"""I-07 入口：源字段映射 + 别名扩展。

用法：
  python3 scripts/build_virtual_field_mappings.py               # 全量（含 LLM 别名扩展）
  python3 scripts/build_virtual_field_mappings.py no-llm        # 只做 mappings，跳过别名扩展
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.field_mapping_builder import main


if __name__ == "__main__":
    args = sys.argv[1:]
    enable_llm = True
    concurrency = 10
    for a in args:
        if a == "no-llm":
            enable_llm = False
        elif a.startswith("concurrency="):
            concurrency = int(a.split("=", 1)[1])
    main(enable_llm=enable_llm, concurrency=concurrency)
