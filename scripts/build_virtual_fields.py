"""I-06 入口：生成虚拟字段清单。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.virtual_field_builder import main

if __name__ == "__main__":
    main()
