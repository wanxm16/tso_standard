"""统一的 LLM / Embedding 客户端。

封装阿里云 DashScope（OpenAI 兼容模式）的 chat 和 embedding 接口，
后续字段归一、别名扩展、benchmark 召回评估等都通过这里统一调用。

特性：
- 从 .env 加载配置
- 自带 retry（指数退避）
- 自带文件级缓存，避免字段归一阶段重复调用
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential


load_dotenv(Path(__file__).resolve().parent.parent / ".env")


_DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent / ".llm_cache"
_lock = threading.Lock()


def _client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("LLM_BASE_URL")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY 未配置，请检查 .env")
    if not base_url:
        raise RuntimeError("LLM_BASE_URL 未配置，请检查 .env")
    return OpenAI(api_key=api_key, base_url=base_url)


def _llm_model() -> str:
    return os.getenv("TEXT2SQL_LLM_MODEL", "qwen3-max")


def _embedding_model() -> str:
    return os.getenv("EMBEDDING_MODEL", "text-embedding-v3")


def _cache_path(namespace: str, key: str, cache_dir: Path) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    sub = cache_dir / namespace / digest[:2]
    sub.mkdir(parents=True, exist_ok=True)
    return sub / f"{digest}.json"


def _read_cache(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_cache(path: Path, value: Any) -> None:
    with _lock:
        with path.open("w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False)


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=20))
def _chat_raw(messages: list[dict[str, str]], model: str, temperature: float, response_format: dict | None) -> str:
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    if response_format is not None:
        kwargs["response_format"] = response_format
    resp = _client().chat.completions.create(**kwargs)
    return resp.choices[0].message.content or ""


def chat(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    temperature: float = 0.0,
    json_mode: bool = False,
    use_cache: bool = True,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> str:
    """调用 LLM chat。messages = [{'role': 'system'|'user'|'assistant', 'content': str}, ...]"""
    model = model or _llm_model()
    response_format = {"type": "json_object"} if json_mode else None
    cache_key = json.dumps({
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "json_mode": json_mode,
    }, ensure_ascii=False, sort_keys=True)

    if use_cache:
        path = _cache_path("chat", cache_key, cache_dir)
        cached = _read_cache(path)
        if cached is not None:
            return cached["content"]
    else:
        path = None

    content = _chat_raw(messages, model, temperature, response_format)

    if use_cache and path is not None:
        _write_cache(path, {"content": content})
    return content


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=20))
def _embed_raw(texts: list[str], model: str) -> list[list[float]]:
    resp = _client().embeddings.create(model=model, input=texts)
    return [item.embedding for item in resp.data]


def embed(
    texts: Iterable[str],
    *,
    model: str | None = None,
    batch_size: int = 10,  # DashScope text-embedding-v3 上限是 10
    use_cache: bool = True,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> list[list[float]]:
    """批量 embedding。命中 cache 的项不会再发请求。"""
    model = model or _embedding_model()
    text_list = [str(t) if t is not None else "" for t in texts]
    results: list[list[float] | None] = [None] * len(text_list)
    pending: list[tuple[int, str]] = []

    for i, text in enumerate(text_list):
        if use_cache:
            cache_key = json.dumps({"model": model, "text": text}, ensure_ascii=False, sort_keys=True)
            path = _cache_path("embed", cache_key, cache_dir)
            cached = _read_cache(path)
            if cached is not None:
                results[i] = cached["embedding"]
                continue
        pending.append((i, text))

    for start in range(0, len(pending), batch_size):
        batch = pending[start : start + batch_size]
        batch_texts = [t for _, t in batch]
        embeddings = _embed_raw(batch_texts, model)
        for (i, text), emb in zip(batch, embeddings):
            results[i] = emb
            if use_cache:
                cache_key = json.dumps({"model": model, "text": text}, ensure_ascii=False, sort_keys=True)
                path = _cache_path("embed", cache_key, cache_dir)
                _write_cache(path, {"embedding": emb})

    return [r if r is not None else [] for r in results]
