"""I-10: 端到端 pipeline 整合入口。

把现有 11 个 I-xx 脚本串成一个固定顺序、可断点续跑的入口。

用法：
    python3 scripts/run_pipeline.py --list-steps
    python3 scripts/run_pipeline.py --dry-run
    python3 scripts/run_pipeline.py                         # 有输出则跳过，从缺失的一步开始
    python3 scripts/run_pipeline.py --from slot_definitions # I-05b 回写 base_slots 后
    python3 scripts/run_pipeline.py --from evaluation       # benchmark 调整后
    python3 scripts/run_pipeline.py --to slot_scores        # 只跑到打分
    python3 scripts/run_pipeline.py --force-step field_normalization
    python3 scripts/run_pipeline.py --force-all

规则：
- 每步都可作为 --from / --to 的起点
- 脚手架三段式（rule → llm → final）顺序不可跳（本 pipeline 内置该顺序）
- 子脚本用 subprocess.run 调用，继承父进程 env
- 任一步失败立即停，不继续下游
- 状态写 output/.pipeline_state.json

TASK: tasks/TASK-I-10-端到端pipeline整合.md
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
OUT = REPO_ROOT / "output"
STATE_PATH = OUT / ".pipeline_state.json"


@dataclass
class Step:
    name: str
    script: Path
    outputs: list[Path]
    description: str
    estimated_sec: float  # 仅用于打印预估


STEPS: list[Step] = [
    Step(
        name="scaffold_rule",
        script=REPO_ROOT / "scripts" / "build_scaffold.py",
        outputs=[OUT / "virtual_tables_scaffold.yaml", OUT / "virtual_tables_scaffold.json"],
        description="规则版脚手架（基于分类树 + DDL 结构规则）",
        estimated_sec=30,
    ),
    Step(
        name="scaffold_llm",
        script=REPO_ROOT / "scripts" / "build_scaffold_llm.py",
        outputs=[OUT / "virtual_tables_scaffold_llm.yaml", OUT / "virtual_tables_scaffold_llm.json"],
        description="LLM 版脚手架（73 个 L2 逐个调 qwen3-max；首次 ~14 分钟，缓存后秒级）",
        estimated_sec=840,
    ),
    Step(
        name="scaffold_final",
        script=REPO_ROOT / "scripts" / "build_scaffold_final.py",
        outputs=[OUT / "virtual_tables_scaffold_final.yaml", OUT / "virtual_tables_scaffold_final.json"],
        description="融合定稿版（rule + llm 合并为 119 张权威 VT）",
        estimated_sec=10,
    ),
    Step(
        name="slot_definitions",
        script=REPO_ROOT / "scripts" / "generate_slot_definitions.py",
        outputs=[OUT / "slot_definitions.yaml", OUT / "slot_definitions.json"],
        description="I-01 每张 VT 的候选槽位清单（base 复用 + extended 新建）",
        estimated_sec=480,
    ),
    Step(
        name="field_features",
        script=REPO_ROOT / "scripts" / "build_field_features.py",
        outputs=[OUT / "field_features.parquet"],
        description="I-02 每个物理字段的 6 类特征",
        estimated_sec=60,
    ),
    Step(
        name="slot_scores",
        script=REPO_ROOT / "scripts" / "compute_slot_scores.py",
        outputs=[OUT / "slot_scores.parquet", OUT / "slot_scores_top3.parquet"],
        description="I-03 字段→槽位打分（TF-IDF + Embedding 并行）",
        estimated_sec=180,
    ),
    Step(
        name="field_normalization",
        script=REPO_ROOT / "scripts" / "make_field_normalization.py",
        outputs=[OUT / "field_normalization.parquet"],
        description="I-04 归槽决策 + 置信度分层 + 冲突检测 + LLM 兜底",
        estimated_sec=150,
    ),
    Step(
        name="virtual_fields",
        script=REPO_ROOT / "scripts" / "build_virtual_fields.py",
        outputs=[OUT / "virtual_fields.json", OUT / "virtual_fields.parquet"],
        description="I-06 每张 VT 的虚拟字段清单",
        estimated_sec=30,
    ),
    Step(
        name="virtual_field_mappings",
        script=REPO_ROOT / "scripts" / "build_virtual_field_mappings.py",
        outputs=[OUT / "virtual_field_mappings.json", OUT / "virtual_field_mappings.parquet"],
        description="I-07 源字段映射 + 别名扩展",
        estimated_sec=60,
    ),
    Step(
        name="query_intents",
        script=REPO_ROOT / "scripts" / "extract_query_intents.py",
        outputs=[OUT / "query_intents.json"],
        description="I-08 benchmark query 意图结构化抽取",
        estimated_sec=120,
    ),
    Step(
        name="evaluation",
        script=REPO_ROOT / "scripts" / "run_evaluation.py",
        outputs=[OUT / "evaluation.json", OUT / "evaluation_details.parquet"],
        description="I-08/I-09 评估指标计算（topic_hit / table_recall / virtual_field_hit / query_support）",
        estimated_sec=60,
    ),
]


STEP_NAMES = [s.name for s in STEPS]


def step_by_name(name: str) -> Step:
    for s in STEPS:
        if s.name == name:
            return s
    raise ValueError(f"未知步骤: {name}；可选: {STEP_NAMES}")


def is_output_present(step: Step) -> bool:
    return all(p.exists() and p.stat().st_size > 0 for p in step.outputs)


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def fmt_duration(sec: float) -> str:
    if sec < 60:
        return f"{sec:.1f}s"
    m, s = divmod(int(sec), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def banner(msg: str) -> None:
    print("\n" + "=" * 78)
    print(msg)
    print("=" * 78, flush=True)


def run_step(step: Step) -> tuple[bool, float]:
    """返回 (是否成功, 耗时秒)。"""
    print(f"\n▶ running: {step.script.relative_to(REPO_ROOT)}", flush=True)
    t0 = time.time()
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    try:
        completed = subprocess.run(
            [sys.executable, str(step.script)],
            cwd=str(REPO_ROOT),
            env=env,
            check=False,
        )
        ok = completed.returncode == 0
    except Exception as e:
        print(f"\n❌ 子进程异常: {e}", flush=True)
        ok = False
    elapsed = time.time() - t0
    # 校验输出
    if ok:
        missing = [p for p in step.outputs if not (p.exists() and p.stat().st_size > 0)]
        if missing:
            print(f"\n❌ 步骤退出 0 但产物缺失: {[str(p.relative_to(REPO_ROOT)) for p in missing]}", flush=True)
            ok = False
    return ok, elapsed


def plan_steps(
    from_step: str | None,
    to_step: str | None,
    force_steps: set[str],
    force_all: bool,
) -> list[tuple[Step, str]]:
    """返回 [(step, action)]，action ∈ {'run', 'skip(cached)', 'skip(out-of-range)'}"""
    start_idx = 0
    end_idx = len(STEPS) - 1
    if from_step:
        start_idx = STEP_NAMES.index(from_step)
    if to_step:
        end_idx = STEP_NAMES.index(to_step)
    if start_idx > end_idx:
        raise ValueError(f"--from {from_step} 在 --to {to_step} 之后，范围无效")

    plan: list[tuple[Step, str]] = []
    for i, step in enumerate(STEPS):
        if i < start_idx or i > end_idx:
            plan.append((step, "skip(out-of-range)"))
            continue
        # --from 起点：无条件跑
        # --from 之后的步骤：上游变了，也必须跑（不走 cached 跳过）
        if from_step and i >= start_idx:
            plan.append((step, "run(--from)"))
            continue
        if force_all or step.name in force_steps:
            plan.append((step, "run(--force)"))
            continue
        if is_output_present(step):
            plan.append((step, "skip(cached)"))
            continue
        plan.append((step, "run"))
    return plan


def print_plan(plan: list[tuple[Step, str]]) -> None:
    print("\n计划：")
    print(f"  {'#':>2}  {'step':<24} {'action':<20} {'预估':<8} outputs")
    print("  " + "-" * 96)
    total_est = 0.0
    for i, (step, action) in enumerate(plan, 1):
        out_str = ", ".join(p.name for p in step.outputs)
        marker = "▶" if action.startswith("run") else " "
        print(f"  {i:>2}. {step.name:<24} {action:<20} {fmt_duration(step.estimated_sec):<8} {out_str} {marker}")
        if action.startswith("run"):
            total_est += step.estimated_sec
    print(f"\n  预计总耗时（仅待跑步骤，首次；有缓存会大幅缩短）: {fmt_duration(total_est)}")


def cmd_list_steps() -> None:
    print("Pipeline steps (11)：\n")
    for i, step in enumerate(STEPS, 1):
        out_str = ", ".join(p.name for p in step.outputs)
        print(f"  {i:>2}. {step.name:<24} — {step.description}")
        print(f"      script:  {step.script.relative_to(REPO_ROOT)}")
        print(f"      outputs: {out_str}")
        print(f"      预估:    {fmt_duration(step.estimated_sec)}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="端到端 pipeline 整合（I-10）")
    parser.add_argument("--list-steps", action="store_true", help="列出所有步骤并退出")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划不执行")
    parser.add_argument("--from", dest="from_step", default=None, choices=STEP_NAMES, help="从该步开始（该步无条件跑）")
    parser.add_argument("--to", dest="to_step", default=None, choices=STEP_NAMES, help="跑到该步停")
    parser.add_argument("--force-step", dest="force_step", default=None, help="强制重跑指定步（逗号分隔）")
    parser.add_argument("--force-all", action="store_true", help="强制重跑所有步")
    args = parser.parse_args()

    if args.list_steps:
        cmd_list_steps()
        return

    force_steps: set[str] = set()
    if args.force_step:
        force_steps = {s.strip() for s in args.force_step.split(",") if s.strip()}
        unknown = force_steps - set(STEP_NAMES)
        if unknown:
            print(f"❌ --force-step 包含未知步骤: {unknown}")
            sys.exit(2)

    plan = plan_steps(args.from_step, args.to_step, force_steps, args.force_all)

    banner(f"Pipeline run @ {datetime.now().isoformat(timespec='seconds')}")
    if args.from_step:
        print(f"--from = {args.from_step}")
    if args.to_step:
        print(f"--to   = {args.to_step}")
    if force_steps:
        print(f"--force-step = {sorted(force_steps)}")
    if args.force_all:
        print("--force-all")

    print_plan(plan)

    if args.dry_run:
        print("\n(--dry-run：不执行)")
        return

    state = load_state()
    results: list[dict] = []
    overall_t0 = time.time()

    for i, (step, action) in enumerate(plan, 1):
        if not action.startswith("run"):
            continue
        banner(f"[{i}/{len(plan)}] {step.name} — {step.description}")
        ok, elapsed = run_step(step)
        entry = {
            "step": step.name,
            "action": action,
            "status": "ok" if ok else "failed",
            "duration_sec": round(elapsed, 1),
            "ended_at": datetime.now().isoformat(timespec="seconds"),
            "outputs": [str(p.relative_to(REPO_ROOT)) for p in step.outputs],
        }
        results.append(entry)
        # 更新状态文件
        state[step.name] = {
            "last_run": entry["ended_at"],
            "duration_sec": entry["duration_sec"],
            "status": entry["status"],
            "output_mtimes": {
                str(p.relative_to(REPO_ROOT)): (p.stat().st_mtime if p.exists() else None)
                for p in step.outputs
            },
        }
        save_state(state)

        if not ok:
            banner(f"❌ 步骤 {step.name} 失败，pipeline 停止。请修复后用 --from {step.name} 重跑。")
            print_summary(results, time.time() - overall_t0)
            sys.exit(1)
        print(f"\n✅ {step.name} 完成（耗时 {fmt_duration(elapsed)}）", flush=True)

    banner(f"🎉 Pipeline 全部完成，总耗时 {fmt_duration(time.time() - overall_t0)}")
    print_summary(results, time.time() - overall_t0)


def print_summary(results: list[dict], total_sec: float) -> None:
    print("\n耗时汇总：")
    print(f"  {'step':<24} {'status':<8} {'duration':<10}")
    print("  " + "-" * 48)
    for r in results:
        print(f"  {r['step']:<24} {r['status']:<8} {fmt_duration(r['duration_sec']):<10}")
    print(f"  {'TOTAL':<24} {'':<8} {fmt_duration(total_sec):<10}")


if __name__ == "__main__":
    main()
