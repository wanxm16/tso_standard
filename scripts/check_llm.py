"""连通性自检：验证 .env 配置 + LLM chat + Embedding 都能跑通。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.llm_client import chat, embed  # noqa: E402


def main() -> int:
    print("=== 配置检查 ===")
    print(f"LLM_BASE_URL    : {os.getenv('LLM_BASE_URL')}")
    print(f"TEXT2SQL_LLM_MODEL: {os.getenv('TEXT2SQL_LLM_MODEL')}")
    print(f"EMBEDDING_MODEL : {os.getenv('EMBEDDING_MODEL')}")
    api_key = os.getenv("OPENAI_API_KEY") or ""
    print(f"OPENAI_API_KEY  : {'已配置 (***' + api_key[-4:] + ')' if api_key else '未配置'}")

    print("\n=== LLM chat 测试 ===")
    content = chat(
        messages=[
            {"role": "system", "content": "你是一个简洁的助手，只回答一个词。"},
            {"role": "user", "content": "中国的首都是？"},
        ],
        temperature=0.0,
        use_cache=False,
    )
    print(f"模型回复: {content!r}")

    print("\n=== Embedding 测试 ===")
    texts = ["身份证号", "公民身份号码", "姓名", "出入境时间"]
    embeddings = embed(texts, use_cache=False)
    print(f"输入 {len(texts)} 条文本，得到 {len(embeddings)} 个向量")
    if embeddings and embeddings[0]:
        print(f"向量维度: {len(embeddings[0])}")
        # 简单算一下"身份证号"和"公民身份号码"的余弦相似度
        import math
        a, b = embeddings[0], embeddings[1]
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(x * x for x in b))
        sim = dot / (na * nb) if na and nb else 0.0
        print(f'\"身份证号\" vs \"公民身份号码\" 余弦相似度: {sim:.4f}')
        a, c = embeddings[0], embeddings[2]
        dot = sum(x * y for x, y in zip(a, c))
        nc = math.sqrt(sum(x * x for x in c))
        sim2 = dot / (na * nc) if na and nc else 0.0
        print(f'\"身份证号\" vs \"姓名\"           余弦相似度: {sim2:.4f}')

    print("\n✅ 全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
