# tso_standard 虚拟表自动生成（当前数据适配版 V1.0）

`wanxm16/tso_standard` 的离线配套实现包。从 119 张虚拟表脚手架出发，经字段归一、虚拟字段生成、源字段映射，到 benchmark 评估闭环，产出面向 Text2SQL **表召回层**的虚拟表语义层。

> ✋ 本仓库不负责 SQL 生成 / JOIN 编排 / 执行计划；那是独立的下游模块。详见 `tasks/decisions.md` 2026-04-21 条 3。

## 快速开始

```bash
# 1. 依赖
pip install -r requirements.txt

# 2. 配置 LLM（阿里云 DashScope，OpenAI 兼容模式）
cp .env.example .env
# 编辑 .env 填入 OPENAI_API_KEY / LLM_BASE_URL / TEXT2SQL_LLM_MODEL=qwen3-max / EMBEDDING_MODEL=text-embedding-v3

# 3. 自检
python3 scripts/check_llm.py

# 4. 一键跑完整 pipeline（有缓存时很快）
python3 scripts/run_pipeline.py

# 5. 起 Review UI（前后端）
bash scripts/dev_review_ui.sh
```

## 关键入口命令

### 端到端 pipeline（I-10）

一条命令跑完 11 步，带断点续跑：

```bash
python3 scripts/run_pipeline.py --list-steps          # 列出所有步骤
python3 scripts/run_pipeline.py --dry-run             # 看计划不动
python3 scripts/run_pipeline.py                       # 有输出则跳过，补缺失
python3 scripts/run_pipeline.py --from slot_definitions   # I-05b 回写 base_slots 后重跑下游
python3 scripts/run_pipeline.py --from evaluation --to evaluation  # 只重跑 eval
python3 scripts/run_pipeline.py --force-step field_normalization
python3 scripts/run_pipeline.py --force-all           # 全量重跑（LLM 有缓存时依旧很快）
```

状态文件：`output/.pipeline_state.json`（每步上次成功时间戳 + 产物 mtime）

### 11 步顺序（不可乱）

| # | step | 说明 |
|---|---|---|
| 1 | scaffold_rule | 规则版脚手架 |
| 2 | scaffold_llm | LLM 版脚手架（73 L2 × qwen3-max；首次 ~14 min，缓存后秒级）|
| 3 | scaffold_final | 融合定稿 119 张 VT |
| 4 | slot_definitions | I-01 每张 VT 的槽位清单（base + extended）|
| 5 | field_features | I-02 物理字段特征 |
| 6 | slot_scores | I-03 字段→槽位打分（TF-IDF + Embedding 并行）|
| 7 | field_normalization | I-04 归槽决策 + 置信度分层 + 冲突 + LLM 兜底 |
| 8 | virtual_fields | I-06 虚拟字段清单 |
| 9 | virtual_field_mappings | I-07 源字段映射 + 别名扩展 |
| 10 | query_intents | I-08 query 意图结构化抽取 |
| 11 | evaluation | I-08/I-09 评估指标 |

### 反馈回路（I-05b，不在主 pipeline 内）

```bash
# 1. 对归一结果中低置信 / 冲突字段做聚类 + LLM 命名
python3 scripts/discover_new_slots.py
# → output/slot_proposals.yaml + output/slot_proposals_diagnostic.md

# 2. 在 UI 中人工审核 proposal：/normalization/new-slots
#    → 接受的 proposal 回写 data/slot_library/base_slots.yaml（或 domain_slots.yaml）
#    → 审计日志 data/feedback/slot_proposals_log.jsonl

# 3. UI 会提示重跑归一：
python3 scripts/run_pipeline.py --from slot_definitions
```

### 单步入口（也可单独跑，便于调试）

```bash
python3 scripts/build_scaffold.py              # 1
python3 scripts/build_scaffold_llm.py          # 2
python3 scripts/build_scaffold_final.py        # 3
python3 scripts/generate_slot_definitions.py   # 4
python3 scripts/build_field_features.py        # 5
python3 scripts/compute_slot_scores.py         # 6
python3 scripts/make_field_normalization.py    # 7
python3 scripts/build_virtual_fields.py        # 8
python3 scripts/build_virtual_field_mappings.py # 9
python3 scripts/extract_query_intents.py       # 10
python3 scripts/run_evaluation.py              # 11
```

## 目录结构

```
tso_standard/
├── requirement/            # 详细设计说明（V1.0）
├── data/
│   ├── phrase_2/           # DDL / 样例 / 使用情况 / 分类树（中文文件名硬编码）
│   ├── benchmark/          # query_with_table.{json,csv} + query_sql.csv
│   ├── slot_library/       # base_slots.yaml（+ domain_slots.yaml by I-05b）
│   └── feedback/           # review_log.jsonl / slot_proposals_log.jsonl
├── scripts/
│   ├── run_pipeline.py     # ★ I-10 端到端入口
│   ├── discover_new_slots.py   # I-05b 新槽位发现（反馈回路）
│   └── build_*.py / compute_*.py / make_*.py / run_*.py   # 11 个单步脚本
├── src/
│   ├── llm_client.py       # 统一 chat/embed 接口（DashScope OpenAI 兼容 + 缓存 + retry）
│   └── pipeline/           # I-02~I-08 的实现模块
├── backend/                # FastAPI Review UI 后端
├── frontend/               # React + antd Review UI 前端
├── tasks/                  # todo.md / decisions.md / TASK-I-xx-*.md
├── output/                 # 所有中间/最终产物（gitignore）
└── .llm_cache/             # 文件级缓存（gitignore）
```

## 主要输出

| 文件 | 说明 | 对应设计章节 |
|---|---|---|
| `output/virtual_tables_scaffold_final.yaml` | 119 张 VT 定稿 | 5.1 |
| `output/slot_definitions.yaml` | 每张 VT 的槽位清单 | 10.8.4 |
| `output/field_normalization.parquet` | 字段归一结果（含置信度 + 冲突）| 10.8.12 |
| `output/slot_proposals.yaml` | I-05b 新槽位候选 | 10.8.11 |
| `output/virtual_fields.json` | 虚拟字段清单 | 5.2 / 13 |
| `output/virtual_field_mappings.json` | 源字段映射 | 5.3 / 13.4 |
| `output/evaluation.json` | 4 项指标（topic_hit / table_recall / vf_hit / query_support）| 5.5 / 15 |

## LLM / Embedding

- 模型：`qwen3-max`（chat）+ `text-embedding-v3`（1024 维）
- 统一入口：`src/llm_client.chat()` / `embed()`（不要直接用 openai SDK）
- 缓存：`.llm_cache/`，同输入零成本重跑；prompt 或 model 变化自动失效
- LLM 职责：**建议不做决策**。归槽决策走规则（§ 10.8.5-8），LLM 仅在 § 10.8.9 的 4 种边缘场景兜底

## 项目阶段

**当前是正式开发阶段**。不走捷径、不 mock、不跳测试。每个 I-xx 开工前建 `tasks/TASK-I-xx-*.md`，实现后对照设计验证。详见 `CLAUDE.md`。

## 设计文档锚点

- `requirement/虚拟表自动生成系统详细设计说明_当前数据适配版V1.0.md`
- `tasks/todo.md` 四个 Wave 的任务清单
- `tasks/decisions.md` 跨阶段决策日志
