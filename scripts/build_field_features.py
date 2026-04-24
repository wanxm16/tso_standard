"""I-02 入口：字段特征提取。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.feature_builder import main

if __name__ == "__main__":
    main()
