"""Automation Composer Action Module.

Provides visual workflow composition with branching, loops,
parallel sections, and error handling blocks.
"""

from __future__ import annotations

import sys
import os
import time
import threading
import hashlib
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BlockType(Enum):
    """Types of composition blocks."""
    SEQUENCE = "sequence"
    PARALLEL = "parallel"
    BRANCH = "branch"
    LOOP = "loop"
    TRY_EXCEPT = "try_except"
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    RACE = "race"


class BlockStatus(Enum):
    """Execution status of a block."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ComposerBlock:
    """A composable automation block."""
    block_id: str
    block_type: BlockType
    name: str
    steps: List[Dict[str, Any]] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    children: List["ComposerBlock"] = field(default_factory=list)
    on_error: str = "stop"
    max_iterations: int = 1
    condition: Optional[str] = None
    status: BlockStatus = BlockStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    iterations: int = 0


@dataclass
class ExecutionContext:
    """Shared execution context for a composition."""
    variables: Dict[str, Any] = field(default_factory=dict)
    results: List[Dict] = field(default_factory=list)
    errors: List[Dict] = field(default_factory=list)
    timing: Dict[str, float] = field(default_factory=dict)


class AutomationComposerAction(BaseAction):
    """Compose automation workflows from reusable blocks.

    Provides structured composition patterns including parallel execution,
    conditional branching, iteration, error handling, and mapping.
    """
    action_type = "automation_composer"
    display_name = "自动化编排"
    description = "可视化工作流编排，支持分支、循环和并行执行"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute composition operation.

        Args:
            context: Execution context.
            params: Dict with keys: action, blocks, etc.

        Returns:
            ActionResult with composition execution result.
        """
        action = params.get("action", "compose")

        if action == "compose":
            return self._compose_workflow(context, params)
        elif action == "run":
            return self._run_workflow(context, params)
        elif action == "validate":
            return self._validate_blocks(params)
        elif action == "optimize":
            return self._optimize_workflow(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )

    def _validate_blocks(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a block structure."""
        blocks = params.get("blocks", [])
        errors = []

        for i, block in enumerate(blocks):
            block_type = block.get("type", "")
            if not block_type:
                errors.append(f"Block {i}: missing type")

            if block_type == "branch":
                if "branches" not in block and "condition" not in block:
                    errors.append(f"Block {i}: branch missing branches or condition")
            elif block_type == "loop":
                if "iterations" not in block and "while" not in block:
                    errors.append(f"Block {i}: loop missing iterations or while")
            elif block_type == "map":
                if "source" not in block:
                    errors.append(f"Block {i}: map missing source")

        result_data = {
            "valid": len(errors) == 0,
            "errors": errors,
            "block_count": len(blocks)
        }

        return ActionResult(
            success=len(errors) == 0,
            message=f"Validation: {'passed' if not errors else f'{len(errors)} errors'}",
            data=result_data
        )

    def _optimize_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Optimize a workflow by combining sequential blocks."""
        blocks = params.get("blocks", [])
        save_to_var = params.get("save_to_var", None)

        optimized = []
        i = 0
        while i < len(blocks):
            block = blocks[i]

            if block.get("type") == "sequence" and i + 1 < len(blocks):
                next_block = blocks[i + 1]
                if next_block.get("type") == "sequence":
                    merged_steps = block.get("steps", []) + next_block.get("steps", [])
                    merged_block = {**block, "steps": merged_steps}
                    optimized.append(merged_block)
                    i += 2
                    continue

            optimized.append(block)
            i += 1

        result_data = {
            "original_count": len(blocks),
            "optimized_count": len(optimized),
            "blocks": optimized
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Optimized {len(blocks)} blocks to {len(optimized)}",
            data=result_data
        )

    def _compose_workflow(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Build a composed workflow from blocks."""
        blocks_config = params.get("blocks", [])
        name = params.get("name", "Composed Workflow")
        save_to_var = params.get("save_to_var", None)

        blocks = []
        for i, bc in enumerate(blocks_config):
            try:
                block_type = BlockType[bc.get("type", "sequence").upper()]
            except KeyError:
                block_type = BlockType.SEQUENCE

            block = ComposerBlock(
                block_id=bc.get("id", f"block_{i}"),
                block_type=block_type,
                name=bc.get("name", f"Block {i}"),
                steps=bc.get("steps", []),
                config=bc.get("config", {}),
                on_error=bc.get("on_error", "stop"),
                max_iterations=bc.get("max_iterations", 1),
                condition=bc.get("condition")
            )
            blocks.append(block)

        result_data = {
            "name": name,
            "blocks": [
                {"id": b.block_id, "type": b.block_type.value, "name": b.name}
                for b in blocks
            ],
            "total_blocks": len(blocks)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Workflow '{name}' composed with {len(blocks)} blocks",
            data=result_data
        )

    def _run_workflow(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a composed workflow."""
        blocks_config = params.get("blocks", [])
        initial_vars = params.get("variables", {})
        save_to_var = params.get("save_to_var", None)
        stop_on_error = params.get("stop_on_error", True)

        exec_ctx = ExecutionContext(variables=dict(initial_vars))

        blocks = []
        for i, bc in enumerate(blocks_config):
            try:
                block_type = BlockType[bc.get("type", "sequence").upper()]
            except KeyError:
                block_type = BlockType.SEQUENCE

            block = ComposerBlock(
                block_id=bc.get("id", f"block_{i}"),
                block_type=block_type,
                name=bc.get("name", f"Block {i}"),
                steps=bc.get("steps", []),
                config=bc.get("config", {}),
                on_error=bc.get("on_error", "stop" if stop_on_error else "continue"),
                max_iterations=bc.get("max_iterations", 1),
                condition=bc.get("condition")
            )
            blocks.append(block)

        start_time = time.time()
        all_results = []
        failed = False

        for block in blocks:
            if block.condition:
                try:
                    cond_result = eval(
                        block.condition,
                        {"__builtins__": {}},
                        exec_ctx.variables
                    )
                    if not cond_result:
                        block.status = BlockStatus.SKIPPED
                        continue
                except Exception as e:
                    block.error = f"Condition eval error: {str(e)}"
                    block.status = BlockStatus.FAILED
                    exec_ctx.errors.append({"block": block.block_id, "error": str(e)})
                    if block.on_error == "stop":
                        failed = True
                        break
                    continue

            block.status = BlockStatus.RUNNING
            result = self._execute_block(block, exec_ctx)
            block.result = result
            all_results.append({"block_id": block.block_id, "result": result})

            if isinstance(result, dict) and not result.get("success", True):
                block.status = BlockStatus.FAILED
                block.error = result.get("error")
                exec_ctx.errors.append({"block": block.block_id, "error": block.error})
                if block.on_error == "stop":
                    failed = True
                    break
            else:
                block.status = BlockStatus.COMPLETED

        elapsed = time.time() - start_time
        exec_ctx.timing["total"] = elapsed

        completed = [b for b in blocks if b.status == BlockStatus.COMPLETED]
        skipped = [b for b in blocks if b.status == BlockStatus.SKIPPED]
        failed_blocks = [b for b in blocks if b.status == BlockStatus.FAILED]

        result_data = {
            "total_blocks": len(blocks),
            "completed": len(completed),
            "skipped": len(skipped),
            "failed": len(failed_blocks),
            "elapsed": elapsed,
            "variables": exec_ctx.variables,
            "block_results": all_results
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=not failed,
            message=f"Workflow: {len(completed)}/{len(blocks)} completed "
                    f"in {elapsed:.2f}s",
            data=result_data
        )

    def _execute_block(self, block: ComposerBlock,
                      exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute a single composition block."""
        block_start = time.time()

        try:
            if block.block_type == BlockType.SEQUENCE:
                return self._run_sequence(block, exec_ctx)
            elif block.block_type == BlockType.PARALLEL:
                return self._run_parallel(block, exec_ctx)
            elif block.block_type == BlockType.BRANCH:
                return self._run_branch(block, exec_ctx)
            elif block.block_type == BlockType.LOOP:
                return self._run_loop(block, exec_ctx)
            elif block.block_type == BlockType.MAP:
                return self._run_map(block, exec_ctx)
            elif block.block_type == BlockType.FILTER:
                return self._run_filter(block, exec_ctx)
            elif block.block_type == BlockType.RACE:
                return self._run_race(block, exec_ctx)
            elif block.block_type == BlockType.TRY_EXCEPT:
                return self._run_try_except(block, exec_ctx)
            else:
                return {"success": True, "skipped": True, "block_type": block.block_type.value}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            exec_ctx.timing[block.block_id] = time.time() - block_start

    def _run_sequence(self, block: ComposerBlock,
                      exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute steps in sequence."""
        results = []
        for step in block.steps:
            step_result = self._execute_step(step, exec_ctx)
            results.append(step_result)
            if isinstance(step_result, dict) and not step_result.get("success", True):
                return {"success": False, "step_results": results, "failed_at": step.get("id")}
        return {"success": True, "step_results": results}

    def _run_parallel(self, block: ComposerBlock,
                     exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute steps in parallel threads."""
        threads = []
        step_results = []

        def run_step(step):
            result = self._execute_step(step, exec_ctx)
            step_results.append(result)

        for step in block.steps:
            t = threading.Thread(target=run_step, args=(step,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=block.config.get("timeout", 60.0))

        return {"success": True, "step_results": step_results, "parallel": True}

    def _run_branch(self, block: ComposerBlock,
                    exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute conditional branches."""
        condition = block.condition or block.config.get("condition", "False")
        try:
            cond_result = eval(condition, {"__builtins__": {}}, exec_ctx.variables)
        except Exception:
            cond_result = False

        branches = block.config.get("branches", [])
        true_branch = block.config.get("true_branch")
        false_branch = block.config.get("false_branch")

        if cond_result and true_branch:
            return self._execute_step(true_branch, exec_ctx)
        elif not cond_result and false_branch:
            return self._execute_step(false_branch, exec_ctx)
        elif branches:
            for branch in branches:
                try:
                    if eval(branch.get("condition", "False"),
                            {"__builtins__": {}}, exec_ctx.variables):
                        return self._execute_step(branch, exec_ctx)
                except Exception:
                    continue

        return {"success": True, "branch_executed": cond_result}

    def _run_loop(self, block: ComposerBlock,
                  exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute loop iterations."""
        max_iter = block.max_iterations or block.config.get("iterations", 1)
        while_cond = block.config.get("while")
        results = []
        iterations = 0

        while iterations < max_iter:
            if while_cond:
                try:
                    if not eval(while_cond, {"__builtins__": {}}, exec_ctx.variables):
                        break
                except Exception:
                    break

            for step in block.steps:
                step_result = self._execute_step(step, exec_ctx)
                results.append(step_result)
                if isinstance(step_result, dict) and not step_result.get("success", True):
                    return {"success": False, "iterations": iterations,
                            "results": results, "failed_at": step.get("id")}
            iterations += 1

        return {"success": True, "iterations": iterations, "results": results}

    def _run_map(self, block: ComposerBlock,
                 exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute map operation over a collection."""
        source = block.config.get("source", [])
        if isinstance(source, str):
            try:
                source = exec_ctx.variables.get(source, [])
            except Exception:
                source = []

        results = []
        for item in source:
            exec_ctx.variables["_item"] = item
            for step in block.steps:
                result = self._execute_step(step, exec_ctx)
                results.append(result)
        exec_ctx.variables.pop("_item", None)

        return {"success": True, "mapped": len(results), "results": results}

    def _run_filter(self, block: ComposerBlock,
                    exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute filter operation over a collection."""
        source = block.config.get("source", [])
        filter_expr = block.config.get("filter")
        if not filter_expr:
            return {"success": True, "filtered": 0, "results": []}

        if isinstance(source, str):
            source = exec_ctx.variables.get(source, [])

        results = []
        for item in source:
            exec_ctx.variables["_item"] = item
            try:
                if eval(filter_expr, {"__builtins__": {}}, exec_ctx.variables):
                    results.append(item)
            except Exception:
                pass
        exec_ctx.variables.pop("_item", None)

        return {"success": True, "filtered": len(results), "results": results}

    def _run_race(self, block: ComposerBlock,
                  exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute branches in race, return first to complete."""
        branches = block.config.get("branches", [])
        results = []
        completed = threading.Event()

        def run_branch(branch):
            result = self._execute_step(branch, exec_ctx)
            results.append(result)
            completed.set()

        threads = []
        for branch in branches:
            t = threading.Thread(target=run_branch, args=(branch,))
            threads.append(t)
            t.start()

        timeout = block.config.get("timeout", 60.0)
        finished = completed.wait(timeout=timeout)

        return {
            "success": bool(results),
            "winner": results[0] if results else None,
            "total": len(branches),
            "finished": finished
        }

    def _run_try_except(self, block: ComposerBlock,
                        exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute try/except block."""
        try:
            for step in block.steps:
                result = self._execute_step(step, exec_ctx)
                if isinstance(result, dict) and not result.get("success", True):
                    return result
            return {"success": True}
        except Exception as e:
            except_handler = block.config.get("except")
            if except_handler:
                return self._execute_step(except_handler, exec_ctx)
            return {"success": False, "error": str(e), "handled": False}

    def _execute_step(self, step: Dict[str, Any],
                      exec_ctx: ExecutionContext) -> Dict[str, Any]:
        """Execute a single step."""
        step_type = step.get("type", "log")
        step_id = step.get("id", "step")
        step_config = step.get("config", {})

        try:
            if step_type == "log":
                message = step_config.get("message", f"Step {step_id}")
                exec_ctx.results.append({"step": step_id, "log": message})
                return {"success": True, "logged": message}

            elif step_type == "delay":
                duration = float(step_config.get("duration", 1.0))
                time.sleep(min(duration, 10.0))
                return {"success": True, "delayed": duration}

            elif step_type == "set":
                var_name = step_config.get("name")
                var_value = step_config.get("value")
                if var_name:
                    exec_ctx.variables[var_name] = var_value
                return {"success": True, "set": {var_name: var_value}}

            elif step_type == "get":
                var_name = step_config.get("name")
                value = exec_ctx.variables.get(var_name)
                return {"success": True, "value": value}

            elif step_type == "increment":
                var_name = step_config.get("name", "counter")
                exec_ctx.variables[var_name] = exec_ctx.variables.get(var_name, 0) + 1
                return {"success": True, "incremented": var_name}

            elif step_type == "decrement":
                var_name = step_config.get("name", "counter")
                exec_ctx.variables[var_name] = exec_ctx.variables.get(var_name, 0) - 1
                return {"success": True, "decremented": var_name}

            else:
                return {"success": True, "skipped": True, "type": step_type}

        except Exception as e:
            return {"success": False, "step": step_id, "error": str(e)}

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "blocks": [],
            "name": "Workflow",
            "variables": {},
            "stop_on_error": True,
            "save_to_var": None
        }
