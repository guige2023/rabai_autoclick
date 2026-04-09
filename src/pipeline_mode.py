"""
CLI 管道集成模式 v23
P0级功能 - 支持 Unix 管道风格的工作流集成
"""
from __future__ import annotations

import sys
import json
import os
import subprocess
import traceback
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import shlex
from concurrent.futures import ThreadPoolExecutor, Future


class PipeMode(Enum):
    """管道模式"""
    LINEAR = "linear"           # 线性管道 (A | B | C)
    BRANCH = "branch"           # 分支管道 (A -> [B, C])
    MERGE = "merge"            # 合并管道 ([A, B] -> C)
    CONDITIONAL = "conditional"  # 条件管道
    PARALLEL = "parallel"      # 并行管道


class DataFormat(Enum):
    """数据格式"""
    JSON = "json"
    TEXT = "text"
    CSV = "csv"
    YAML = "yaml"


@dataclass
class PipeStep:
    """管道步骤"""
    step_id: str
    name: str
    command: str              # CLI 命令或工作流名
    input_mapping: Dict[str, str] = field(default_factory=dict)  # 输入映射
    output_mapping: Dict[str, str] = field(default_factory=dict)  # 输出映射
    enabled: bool = True
    timeout: int = 300        # 超时秒数
    retry: int = 0           # 重试次数
    on_error: str = "stop"   # stop, continue, fallback
    breakpoint: bool = False  # 断点标记


@dataclass
class PipeChain:
    """管道链"""
    chain_id: str
    name: str
    mode: PipeMode
    steps: List[PipeStep]
    input_schema: Dict = field(default_factory=dict)
    output_schema: Dict = field(default_factory=dict)
    description: str = ""
    breakpoints: List[str] = field(default_factory=list)  # 断点step_id列表


@dataclass
class PipeResult:
    """管道执行结果"""
    success: bool
    step_id: str
    step_name: str = ""
    output: Any = None
    error: Optional[str] = None
    error_context: Optional[Dict] = None
    duration: float = 0.0
    retry_count: int = 0


@dataclass
class ChainResult:
    """管道链执行结果"""
    success: bool
    chain_id: str
    results: List[PipeResult]
    final_output: Any
    total_duration: float
    errors: List[str] = field(default_factory=list)
    error_trace: Optional[str] = None
    breakpoints_hit: List[str] = field(default_factory=list)


class PipelineValidationError(Exception):
    """管道验证错误"""
    def __init__(self, message: str, errors: List[str] = None):
        super().__init__(message)
        self.errors = errors or [message]


class PipelineExecuteError(Exception):
    """管道执行错误"""
    def __init__(self, message: str, step_id: str = None, context: Dict = None):
        super().__init__(message)
        self.step_id = step_id
        self.context = context or {}


class PipeRunner:
    """管道运行器 (别名: PipelineRunner)"""
    
    def __init__(self, data_dir: str = "./data", workflow_executor: Any = None,
                 dry_run: bool = False, step_by_step: bool = False,
                 max_workers: int = 4):
        self.data_dir = data_dir
        self.workflow_executor = workflow_executor
        self.chains: Dict[str, PipeChain] = {}
        self._dry_run = dry_run
        self._step_by_step = step_by_step
        self._max_workers = max_workers
        self._breakpoints_active: List[str] = []
        self._load_chains()
        
    @property
    def dry_run(self) -> bool:
        return self._dry_run
    
    @dry_run.setter
    def dry_run(self, value: bool):
        self._dry_run = value
        
    @property
    def step_by_step(self) -> bool:
        return self._step_by_step
    
    @step_by_step.setter
    def step_by_step(self, value: bool):
        self._step_by_step = value

    def _load_chains(self) -> None:
        """加载管道链"""
        try:
            chains_file = f"{self.data_dir}/pipe_chains.json"
            if os.path.exists(chains_file):
                with open(chains_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for chain_id, chain_data in data.items():
                        chain_data["mode"] = PipeMode(chain_data["mode"])
                        steps = [PipeStep(**s) for s in chain_data["steps"]]
                        chain_data["steps"] = steps
                        self.chains[chain_id] = PipeChain(**chain_data)
        except FileNotFoundError:
            pass
        except json.JSONDecodeError as e:
            print(f"警告: 管道链文件格式错误: {e}", file=sys.stderr)
    
    def _save_chains(self) -> None:
        """保存管道链"""
        data = {}
        for chain_id, chain in self.chains.items():
            d = {
                "chain_id": chain.chain_id,
                "name": chain.name,
                "mode": chain.mode.value,
                "steps": [asdict(s) for s in chain.steps],
                "input_schema": chain.input_schema,
                "output_schema": chain.output_schema,
                "description": chain.description,
                "breakpoints": chain.breakpoints
            }
            data[chain_id] = d
            
        with open(f"{self.data_dir}/pipe_chains.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def validate_chain(self, chain_id: str, input_data: Any = None) -> tuple[bool, List[str]]:
        """验证管道链配置是否正确"""
        errors = []
        chain = self.chains.get(chain_id)
        
        if not chain:
            errors.append(f"管道链不存在: {chain_id}")
            return False, errors
        
        # 验证步骤
        if not chain.steps:
            errors.append("管道链没有定义任何步骤")
        
        step_ids = set()
        for i, step in enumerate(chain.steps):
            if not step.step_id:
                errors.append(f"步骤 {i} 缺少 step_id")
            elif step.step_id in step_ids:
                errors.append(f"步骤 ID 重复: {step.step_id}")
            step_ids.add(step.step_id)
            
            if not step.command:
                errors.append(f"步骤 '{step.name}' 缺少命令")
                
            # 验证断点配置
            if step.breakpoint and step.step_id not in chain.breakpoints:
                chain.breakpoints.append(step.step_id)
        
        # 验证输入映射引用
        for step in chain.steps:
            for target_key in step.input_mapping.values():
                if target_key.startswith("${") and target_key.endswith("}"):
                    var_name = target_key[2:-1]
                    # 检查变量是否在之前的步骤输出中存在
                    found = False
                    for prev_step in chain.steps:
                        if prev_step.output_mapping.get(var_name) or var_name in prev_step.output_mapping.values():
                            found = True
                            break
                    if not found and var_name not in ("*", "input", "data"):
                        errors.append(f"步骤 '{step.name}' 引用的变量 ${{{var_name}}} 未定义")
        
        # 验证模式兼容性
        if chain.mode == PipeMode.PARALLEL and len(chain.steps) < 2:
            errors.append("并行模式需要至少2个步骤")
        
        return len(errors) == 0, errors
    
    def validate_chain_safe(self, chain_id: str, input_data: Any = None) -> Dict[str, Any]:
        """安全的验证接口，返回结构化结果"""
        try:
            valid, errors = self.validate_chain(chain_id, input_data)
            return {
                "valid": valid,
                "chain_id": chain_id,
                "errors": errors,
                "step_count": len(self.chains.get(chain_id, PipeChain("", "", PipeMode.LINEAR, [])).steps)
            }
        except Exception as e:
            return {
                "valid": False,
                "chain_id": chain_id,
                "errors": [f"验证过程出错: {str(e)}"],
                "step_count": 0
            }

    def create_chain(self, name: str, mode: PipeMode = PipeMode.LINEAR,
                    description: str = "") -> PipeChain:
        """创建管道链"""
        chain_id = f"chain_{len(self.chains) + 1}"
        chain = PipeChain(
            chain_id=chain_id,
            name=name,
            mode=mode,
            steps=[],
            description=description
        )
        self.chains[chain_id] = chain
        self._save_chains()
        return chain
    
    def add_step(self, chain_id: str, name: str, command: str,
                input_mapping: Dict[str, str] = None,
                output_mapping: Dict[str, str] = None,
                breakpoint: bool = False) -> Optional[PipeStep]:
        """添加步骤到管道链"""
        chain = self.chains.get(chain_id)
        if not chain:
            return None
            
        step_id = f"{chain_id}_step_{len(chain.steps) + 1}"
        step = PipeStep(
            step_id=step_id,
            name=name,
            command=command,
            input_mapping=input_mapping or {},
            output_mapping=output_mapping or {},
            breakpoint=breakpoint
        )
        chain.steps.append(step)
        if breakpoint:
            chain.breakpoints.append(step_id)
        self._save_chains()
        return step
    
    def set_breakpoint(self, chain_id: str, step_id: str) -> bool:
        """设置断点"""
        chain = self.chains.get(chain_id)
        if not chain:
            return False
        for step in chain.steps:
            if step.step_id == step_id:
                step.breakpoint = True
                if step_id not in chain.breakpoints:
                    chain.breakpoints.append(step_id)
                self._save_chains()
                return True
        return False
    
    def clear_breakpoint(self, chain_id: str, step_id: str) -> bool:
        """清除断点"""
        chain = self.chains.get(chain_id)
        if not chain:
            return False
        for step in chain.steps:
            if step.step_id == step_id:
                step.breakpoint = False
                if step_id in chain.breakpoints:
                    chain.breakpoints.remove(step_id)
                self._save_chains()
                return True
        return False
    
    def execute_step(self, step: PipeStep, input_data: Any, 
                    context: Dict = None) -> PipeResult:
        """执行单个管道步骤"""
        context = context or {}
        start_time = time.time()
        retry_count = 0
        
        try:
            # 干运行模式
            if self._dry_run:
                print(f"[DRY-RUN] 步骤 '{step.name}' (跳过实际执行)")
                return PipeResult(
                    success=True,
                    step_id=step.step_id,
                    step_name=step.name,
                    output={"dry_run": True, "command": step.command},
                    duration=0.0,
                    retry_count=0
                )
            
            # 解析命令
            command = self._parse_command(step.command, input_data)
            
            # 执行命令
            if command.startswith("rabai "):
                result = self._execute_workflow(command, input_data)
            else:
                result = self._execute_command(command, step.timeout)
            
            # 处理输出映射
            output = self._map_output(result, step.output_mapping)
            
            return PipeResult(
                success=True,
                step_id=step.step_id,
                step_name=step.name,
                output=output,
                duration=time.time() - start_time,
                retry_count=retry_count
            )
            
        except Exception as e:
            error_trace = traceback.format_exc()
            return PipeResult(
                success=False,
                step_id=step.step_id,
                step_name=step.name,
                output=None,
                error=str(e),
                error_context={
                    "command": step.command,
                    "input_type": type(input_data).__name__,
                    "traceback": error_trace,
                    "context": context
                },
                duration=time.time() - start_time,
                retry_count=retry_count
            )
    
    def _parse_command(self, command: str, input_data: Any) -> str:
        """解析命令，替换变量"""
        if isinstance(input_data, dict):
            for key, value in input_data.items():
                placeholder = f"${{{key}}}"
                if placeholder in command:
                    command = command.replace(placeholder, str(value))
                if f"${key}" in command:
                    command = command.replace(f"${key}", str(value))
                    
        return command
    
    def _execute_command(self, command: str, timeout: int) -> str:
        """执行外部命令"""
        try:
            result = subprocess.run(
                shlex.split(command),
                capture_output=True,
                text=True,
                timeout=timeout
            )
            if result.returncode == 0:
                return result.stdout
            else:
                raise PipelineExecuteError(
                    f"命令执行失败: {result.stderr}",
                    context={"returncode": result.returncode, "stdout": result.stdout}
                )
        except subprocess.TimeoutExpired:
            raise PipelineExecuteError(f"命令执行超时 ({timeout}秒)")
    
    def _execute_workflow(self, command: str, input_data: Any) -> str:
        """执行 RabAI 工作流"""
        parts = command.split()
        if len(parts) < 2:
            raise PipelineExecuteError("无效的工作流命令")
            
        workflow_name = parts[1]
        
        if self.workflow_executor:
            result = self.workflow_executor.run(workflow_name, input_data)
            return json.dumps(result)
        else:
            return json.dumps({
                "workflow": workflow_name,
                "status": "executed",
                "input": input_data
            })
    
    def _map_output(self, output: Any, mapping: Dict[str, str]) -> Any:
        """映射输出"""
        if not mapping or not output:
            return output
            
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except json.JSONDecodeError:
                pass
        
        if not isinstance(output, dict):
            return output
            
        result = {}
        for new_key, old_key in mapping.items():
            if old_key in output:
                result[new_key] = output[old_key]
            elif old_key == "*":
                result[new_key] = output
                
        return result if result else output
    
    def execute_chain(self, chain_id: str, input_data: Any = None,
                     breakpoints: List[str] = None) -> ChainResult:
        """执行管道链"""
        chain = self.chains.get(chain_id)
        
        if not chain:
            return ChainResult(
                success=False,
                chain_id=chain_id,
                results=[],
                final_output=None,
                total_duration=0,
                errors=[f"管道链不存在: {chain_id}"]
            )
        
        # 验证管道链
        valid, validation_errors = self.validate_chain(chain_id, input_data)
        if not valid:
            return ChainResult(
                success=False,
                chain_id=chain_id,
                results=[],
                final_output=None,
                total_duration=0,
                errors=validation_errors,
                error_trace="; ".join(validation_errors)
            )
        
        start_time = time.time()
        results = []
        current_data = input_data
        errors = []
        breakpoints_hit = []
        active_breakpoints = set(breakpoints or chain.breakpoints)
        
        total_steps = len(chain.steps)
        
        for idx, step in enumerate(chain.steps):
            if not step.enabled:
                continue
            
            step_num = idx + 1
            print(f"\n[{step_num}/{total_steps}] 执行步骤: {step.name}")
            
            # 断点检查
            if step.step_id in active_breakpoints or step.breakpoint:
                breakpoints_hit.append(step.step_id)
                print(f"  ** 断点触发: {step.name} **")
                
                if self._step_by_step:
                    user_input = input("  按 [Enter] 继续, [s] 跳过, [q] 退出: ")
                    if user_input.lower() == 'q':
                        errors.append(f"用户在断点 {step.name} 退出")
                        return ChainResult(
                            success=False,
                            chain_id=chain_id,
                            results=results,
                            final_output=current_data,
                            total_duration=time.time() - start_time,
                            errors=errors,
                            breakpoints_hit=breakpoints_hit
                        )
                    elif user_input.lower() == 's':
                        print("  跳过此步骤")
                        continue
            
            # 执行步骤
            result = self.execute_step(step, current_data)
            results.append(result)
            
            if result.success:
                current_data = result.output
                print(f"  成功 ({result.duration:.2f}s)")
            else:
                error_msg = f"步骤 {step.name} 失败: {result.error}"
                errors.append(error_msg)
                print(f"  失败: {result.error}")
                
                if result.error_context:
                    print(f"  上下文: {json.dumps(result.error_context, ensure_ascii=False)[:200]}")
                
                if step.on_error == "stop":
                    break
                elif step.on_error == "fallback":
                    current_data = step.input_mapping.get("_fallback", {})
                elif step.on_error == "continue":
                    continue
                    
            # 步骤间延迟 (如果启用了step_by_step)
            if self._step_by_step and idx < total_steps - 1:
                input("  按 [Enter] 继续下一步...")
        
        total_duration = time.time() - start_time
        success = all(r.success for r in results)
        
        return ChainResult(
            success=success,
            chain_id=chain_id,
            results=results,
            final_output=current_data,
            total_duration=total_duration,
            errors=errors,
            breakpoints_hit=breakpoints_hit
        )
    
    def execute_chain_parallel(self, chain_id: str, input_data: Any = None) -> ChainResult:
        """并行执行管道链 (仅PARALLEL模式)"""
        chain = self.chains.get(chain_id)
        
        if not chain:
            return ChainResult(
                success=False,
                chain_id=chain_id,
                results=[],
                final_output=None,
                total_duration=0,
                errors=[f"管道链不存在: {chain_id}"]
            )
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures: Dict[Future, PipeStep] = {}
            
            for step in chain.steps:
                if step.enabled:
                    future = executor.submit(self.execute_step, step, input_data)
                    futures[future] = step
            
            results = []
            errors = []
            
            for future in futures:
                step = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    if not result.success:
                        errors.append(f"步骤 {step.name} 失败: {result.error}")
                except Exception as e:
                    errors.append(f"步骤 {step.name} 执行异常: {str(e)}")
                    results.append(PipeResult(
                        success=False,
                        step_id=step.step_id,
                        step_name=step.name,
                        error=str(e),
                        error_context={"exception_type": type(e).__name__}
                    ))
        
        # 合并输出
        final_output = [r.output for r in results if r.success]
        
        return ChainResult(
            success=len(errors) == 0,
            chain_id=chain_id,
            results=results,
            final_output=final_output,
            total_duration=time.time() - start_time,
            errors=errors
        )
    
    def export_pipeline(self, chain_id: str, file_path: str = None) -> str:
        """导出管道为JSON"""
        chain = self.chains.get(chain_id)
        if not chain:
            raise PipelineValidationError(f"管道链不存在: {chain_id}")
        
        export_data = {
            "chain_id": chain.chain_id,
            "name": chain.name,
            "mode": chain.mode.value,
            "steps": [asdict(s) for s in chain.steps],
            "input_schema": chain.input_schema,
            "output_schema": chain.output_schema,
            "description": chain.description,
            "breakpoints": chain.breakpoints,
            "exported_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        json_str = json.dumps(export_data, ensure_ascii=False, indent=2)
        
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json_str)
        
        return json_str
    
    def import_pipeline(self, json_str: str = None, file_path: str = None) -> PipeChain:
        """从JSON导入管道"""
        if file_path:
            with open(file_path, "r", encoding="utf-8") as f:
                json_str = f.read()
        
        if not json_str:
            raise PipelineValidationError("未提供导入数据")
        
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise PipelineValidationError(f"JSON解析失败: {e}")
        
        # 验证必需字段
        required = ["chain_id", "name", "mode", "steps"]
        for field in required:
            if field not in data:
                raise PipelineValidationError(f"缺少必需字段: {field}")
        
        # 重建管道
        try:
            data["mode"] = PipeMode(data["mode"])
            steps = [PipeStep(**s) for s in data["steps"]]
            data["steps"] = steps
            chain = PipeChain(**data)
            
            self.chains[chain.chain_id] = chain
            self._save_chains()
            
            return chain
        except Exception as e:
            raise PipelineValidationError(f"管道数据无效: {e}")
    
    def visualize_pipeline(self, chain_id: str) -> str:
        """生成ASCII管道流程图"""
        chain = self.chains.get(chain_id)
        if not chain:
            return f"管道链不存在: {chain_id}"
        
        lines = []
        lines.append(f"\n管道: {chain.name} ({chain.chain_id})")
        lines.append(f"模式: {chain.mode.value}")
        lines.append("=" * 50)
        
        mode = chain.mode
        
        if mode == PipeMode.LINEAR:
            for i, step in enumerate(chain.steps):
                prefix = "├─► " if i < len(chain.steps) - 1 else "└─► "
                bp_marker = " [BP]" if step.breakpoint or step.step_id in chain.breakpoints else ""
                status = "[disabled]" if not step.enabled else ""
                lines.append(f"{prefix}{step.name}{bp_marker}{status}")
                lines.append(f"   └─ command: {step.command[:60]}{'...' if len(step.command) > 60 else ''}")
                
        elif mode == PipeMode.PARALLEL:
            lines.append("┌─ PARALLEL ─┐")
            for step in chain.steps:
                bp_marker = " [BP]" if step.breakpoint or step.step_id in chain.breakpoints else ""
                lines.append(f"├─ {step.name}{bp_marker}")
                lines.append(f"│   └─ {step.command[:50]}...")
            lines.append("└─ MERGE ────┘")
            
        elif mode == PipeMode.BRANCH:
            lines.append("┌─ BRANCH ────────────┐")
            lines.append(f"├─ {chain.steps[0].name if chain.steps else 'N/A'}")
            lines.append("│   ┌─ SPLIT ──────┐")
            for step in chain.steps[1:]:
                bp_marker = " [BP]" if step.breakpoint or step.step_id in chain.breakpoints else ""
                lines.append(f"│   ├─ {step.name}{bp_marker}")
            lines.append("│   └─ MERGE ──────┘")
            
        else:
            for i, step in enumerate(chain.steps):
                bp_marker = " [BP]" if step.breakpoint or step.step_id in chain.breakpoints else ""
                lines.append(f"{i+1}. {step.name}{bp_marker}")
                lines.append(f"   └─ {step.command[:60]}")
        
        lines.append("=" * 50)
        return "\n".join(lines)
    
    def parse_from_stdin(self) -> Optional[Any]:
        """从标准输入读取数据"""
        if sys.stdin.isatty():
            return None
            
        try:
            data = sys.stdin.read()
            if data.strip():
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data.strip()
        except Exception:
            pass
            
        return None
    
    def output_to_stdout(self, data: Any, format: DataFormat = DataFormat.JSON) -> None:
        """输出到标准输出"""
        if format == DataFormat.JSON:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        elif format == DataFormat.YAML:
            print(self._dict_to_yaml(data))
        else:
            print(str(data))
    
    def _dict_to_yaml(self, data: Any, indent: int = 0) -> str:
        """字典转YAML"""
        lines = []
        prefix = "  " * indent
        
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    lines.append(f"{prefix}{key}:")
                    lines.append(self._dict_to_yaml(value, indent + 1))
                else:
                    lines.append(f"{prefix}{key}: {value}")
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    lines.append(f"{prefix}-")
                    lines.append(self._dict_to_yaml(item, indent + 1))
                else:
                    lines.append(f"{prefix}- {item}")
        else:
            lines.append(f"{prefix}{data}")
            
        return "\n".join(lines)
    
    def list_chains(self) -> List[PipeChain]:
        """列出所有管道链"""
        return list(self.chains.values())
    
    def get_chain(self, chain_id: str) -> Optional[PipeChain]:
        """获取管道链"""
        return self.chains.get(chain_id)
    
    def delete_chain(self, chain_id: str) -> bool:
        """删除管道链"""
        if chain_id in self.chains:
            del self.chains[chain_id]
            self._save_chains()
            return True
        return False


# 别名以兼容旧代码
PipelineRunner = PipeRunner


class PipeCLI:
    """管道CLI接口"""
    
    def __init__(self, data_dir: str = "./data"):
        self.runner = PipeRunner(data_dir)
    
    def run(self, args: List[str]) -> int:
        """运行CLI命令"""
        if not args:
            return self._print_help()
            
        command = args[0]
        
        if command == "run":
            return self._run_chain(args[1:])
        elif command == "list":
            return self._list_chains()
        elif command == "create":
            return self._create_chain(args[1:])
        elif command == "add":
            return self._add_step(args[1:])
        elif command == "stdin":
            return self._run_from_stdin()
        elif command == "validate":
            return self._validate_chain(args[1:])
        elif command == "export":
            return self._export_chain(args[1:])
        elif command == "import":
            return self._import_chain(args[1:])
        elif command == "visualize":
            return self._visualize_chain(args[1:])
        elif command == "dry-run":
            return self._dry_run_chain(args[1:])
        elif command == "breakpoint":
            return self._breakpoint_cmd(args[1:])
        elif command == "help":
            return self._print_help()
        else:
            print(f"未知命令: {command}", file=sys.stderr)
            return 1
    
    def _run_chain(self, args: List[str]) -> int:
        """运行管道链"""
        if not args:
            print("用法: run <chain_id> [input_json]", file=sys.stderr)
            return 1
            
        chain_id = args[0]
        input_data = None
        
        if len(args) > 1:
            try:
                input_data = json.loads(args[1])
            except json.JSONDecodeError:
                print("无效的JSON输入", file=sys.stderr)
                return 1
        else:
            input_data = self.runner.parse_from_stdin()
        
        # 先验证
        valid, errors = self.runner.validate_chain(chain_id, input_data)
        if not valid:
            print(f"管道验证失败:", file=sys.stderr)
            for err in errors:
                print(f"  - {err}", file=sys.stderr)
            return 1
        
        result = self.runner.execute_chain(chain_id, input_data)
        
        self.runner.output_to_stdout({
            "success": result.success,
            "chain_id": result.chain_id,
            "duration": result.total_duration,
            "output": result.final_output,
            "errors": result.errors,
            "breakpoints_hit": result.breakpoints_hit
        })
        
        return 0 if result.success else 1
    
    def _dry_run_chain(self, args: List[str]) -> int:
        """干运行管道链"""
        if not args:
            print("用法: dry-run <chain_id> [input_json]", file=sys.stderr)
            return 1
        
        chain_id = args[0]
        self.runner.dry_run = True
        
        input_data = None
        if len(args) > 1:
            try:
                input_data = json.loads(args[1])
            except json.JSONDecodeError:
                print("无效的JSON输入", file=sys.stderr)
                return 1
        
        print(f"[DRY-RUN] 管道: {chain_id}")
        result = self.runner.execute_chain(chain_id, input_data)
        self.runner.dry_run = False
        
        print(f"\n[DRY-RUN] 结果:")
        print(f"  验证通过: {result.success}")
        print(f"  步骤数: {len(result.results)}")
        for r in result.results:
            print(f"  - {r.step_name}: {'成功' if r.success else '失败'}")
        
        return 0
    
    def _validate_chain(self, args: List[str]) -> int:
        """验证管道链"""
        if not args:
            print("用法: validate <chain_id>", file=sys.stderr)
            return 1
        
        chain_id = args[0]
        result = self.runner.validate_chain_safe(chain_id)
        
        print(f"管道: {chain_id}")
        print(f"有效: {result['valid']}")
        print(f"步骤数: {result['step_count']}")
        if result['errors']:
            print("错误:")
            for err in result['errors']:
                print(f"  - {err}")
        
        return 0 if result['valid'] else 1
    
    def _export_chain(self, args: List[str]) -> int:
        """导出管道链"""
        if not args:
            print("用法: export <chain_id> [file_path]", file=sys.stderr)
            return 1
        
        chain_id = args[0]
        file_path = args[1] if len(args) > 1 else f"{chain_id}.json"
        
        try:
            json_str = self.runner.export_pipeline(chain_id, file_path)
            print(f"已导出到: {file_path}")
            return 0
        except PipelineValidationError as e:
            print(f"导出失败: {e}", file=sys.stderr)
            return 1
    
    def _import_chain(self, args: List[str]) -> int:
        """导入管道链"""
        if not args:
            print("用法: import <file_path>", file=sys.stderr)
            return 1
        
        file_path = args[0]
        
        try:
            chain = self.runner.import_pipeline(file_path=file_path)
            print(f"已导入: {chain.chain_id} - {chain.name}")
            return 0
        except PipelineValidationError as e:
            print(f"导入失败: {e}", file=sys.stderr)
            return 1
    
    def _visualize_chain(self, args: List[str]) -> int:
        """可视化管道链"""
        if not args:
            print("用法: visualize <chain_id>", file=sys.stderr)
            return 1
        
        chain_id = args[0]
        print(self.runner.visualize_pipeline(chain_id))
        return 0
    
    def _breakpoint_cmd(self, args: List[str]) -> int:
        """断点命令"""
        if len(args) < 2:
            print("用法: breakpoint <chain_id> <step_id> [on|off]", file=sys.stderr)
            return 1
        
        chain_id = args[0]
        step_id = args[1]
        action = args[2] if len(args) > 2 else "toggle"
        
        if action == "on":
            success = self.runner.set_breakpoint(chain_id, step_id)
        elif action == "off":
            success = self.runner.clear_breakpoint(chain_id, step_id)
        else:
            # toggle
            chain = self.runner.get_chain(chain_id)
            if chain:
                for step in chain.steps:
                    if step.step_id == step_id:
                        if step.breakpoint:
                            success = self.runner.clear_breakpoint(chain_id, step_id)
                        else:
                            success = self.runner.set_breakpoint(chain_id, step_id)
                        break
            else:
                success = False
        
        if success:
            print(f"断点已更新: {step_id}")
            return 0
        else:
            print(f"操作失败", file=sys.stderr)
            return 1
    
    def _list_chains(self) -> int:
        """列出管道链"""
        chains = self.runner.list_chains()
        
        if not chains:
            print("暂无管道链")
            return 0
            
        print(f"管道链 ({len(chains)}个):\n")
        for chain in chains:
            status = "active" if any(s.enabled for s in chain.steps) else "disabled"
            bp_count = len(chain.breakpoints)
            print(f"  {chain.chain_id}: {chain.name} [{status}]")
            print(f"    模式: {chain.mode.value}, 步骤: {len(chain.steps)}, 断点: {bp_count}")
            if chain.description:
                print(f"    {chain.description}")
            print()
            
        return 0
    
    def _create_chain(self, args: List[str]) -> int:
        """创建管道链"""
        if not args:
            print("用法: create <name> [--mode linear|branch|parallel]", file=sys.stderr)
            return 1
            
        name = args[0]
        mode = PipeMode.LINEAR
        
        if "--mode" in args:
            idx = args.index("--mode")
            if idx + 1 < len(args):
                try:
                    mode = PipeMode(args[idx + 1])
                except ValueError:
                    print(f"无效模式: {args[idx + 1]}", file=sys.stderr)
                    return 1
        
        chain = self.runner.create_chain(name, mode)
        print(f"已创建管道链: {chain.chain_id}")
        return 0
    
    def _add_step(self, args: List[str]) -> int:
        """添加步骤"""
        if len(args) < 3:
            print("用法: add <chain_id> <name> <command> [--breakpoint]", file=sys.stderr)
            return 1
            
        chain_id = args[0]
        name = args[1]
        command = args[2]
        breakpoint = "--breakpoint" in args
        
        step = self.runner.add_step(chain_id, name, command, breakpoint=breakpoint)
        if step:
            print(f"已添加步骤: {step.step_id}")
            return 0
        else:
            print(f"管道链不存在: {chain_id}", file=sys.stderr)
            return 1
    
    def _run_from_stdin(self) -> int:
        """从stdin运行管道"""
        data = self.runner.parse_from_stdin()
        if data is None:
            print("没有从stdin读取到数据", file=sys.stderr)
            return 1
            
        print(f"接收到数据: {type(data).__name__}")
        self.runner.output_to_stdout({"received": data})
        return 0
    
    def _print_help(self) -> int:
        """打印帮助"""
        help_text = """
RabAI AutoClick v23 - 管道CLI

用法:
  rabai-pipe run <chain_id> [input_json]    运行管道链
  rabai-pipe dry-run <chain_id> [input]     干运行 (不实际执行)
  rabai-pipe validate <chain_id>            验证管道
  rabai-pipe list                           列出所有管道链
  rabai-pipe create <name> [--mode mode]   创建管道链
  rabai-pipe add <chain_id> <name> <cmd>    添加步骤 [--breakpoint]
  rabai-pipe export <chain_id> [file]       导出管道为JSON
  rabai-pipe import <file>                  从JSON导入管道
  rabai-pipe visualize <chain_id>           可视化管道流程
  rabai-pipe breakpoint <cid> <sid> [on|off] 管理断点
  rabai-pipe stdin                          从stdin测试读取
  rabai-pipe help                           显示帮助

管道模式:
  linear    线性管道 (A | B | C)
  branch   分支管道 (A -> [B, C])
  parallel 并行管道

示例:
  echo '{"email": "test@example.com"}' | rabai-pipe run chain_1
  rabai-pipe dry-run chain_1 '{"action": "process"}'
  rabai-pipe visualize chain_1
  rabai-pipe export chain_1 my_pipeline.json
"""
        print(help_text)
        return 0


def create_pipeline_runner(data_dir: str = "./data", 
                         workflow_executor: Any = None,
                         dry_run: bool = False,
                         step_by_step: bool = False) -> PipeRunner:
    """创建管道运行器"""
    return PipeRunner(data_dir, workflow_executor, dry_run, step_by_step)


# 测试
if __name__ == "__main__":
    runner = create_pipeline_runner("./data")
    
    # 创建管道链
    chain = runner.create_chain("邮件处理流程", PipeMode.LINEAR, "自动处理邮件")
    print(f"创建管道链: {chain.chain_id}")
    
    # 添加步骤
    runner.add_step(
        chain.chain_id, 
        "获取邮件", 
        "rabai workflow get_emails --folder inbox",
        {"input": "data"},
        {"emails": "result"}
    )
    
    runner.add_step(
        chain.chain_id,
        "分类邮件",
        "python classify.py --input ${emails}",
        {"emails": "input"},
        {"categorized": "output"}
    )
    
    runner.add_step(
        chain.chain_id,
        "发送通知",
        "rabai notify send --message '处理完成'",
        None,
        None,
        breakpoint=True
    )
    
    # 执行管道 (干运行)
    runner.dry_run = True
    result = runner.execute_chain(chain.chain_id, {"data": "test@example.com"})
    runner.dry_run = False
    
    print(f"\n执行结果:")
    print(f"  成功: {result.success}")
    print(f"  耗时: {result.total_duration:.2f}秒")
    print(f"  输出: {result.final_output}")
    if result.errors:
        print(f"  错误: {result.errors}")
    
    # 可视化
    print(runner.visualize_pipeline(chain.chain_id))
    
    # 列出所有管道
    print("\n管道链列表:")
    for c in runner.list_chains():
        print(f"  - {c.name} ({c.mode.value})")
