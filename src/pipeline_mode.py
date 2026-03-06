"""
CLI 管道集成模式 v22
P0级功能 - 支持 Unix 管道风格的工作流集成
"""
import sys
import json
import os
import subprocess
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import shlex


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


@dataclass
class PipeResult:
    """管道执行结果"""
    success: bool
    step_id: str
    output: Any
    error: Optional[str] = None
    duration: float = 0.0


@dataclass
class ChainResult:
    """管道链执行结果"""
    success: bool
    chain_id: str
    results: List[PipeResult]
    final_output: Any
    total_duration: float
    errors: List[str] = field(default_factory=list)


class PipelineRunner:
    """管道运行器"""
    
    def __init__(self, data_dir: str = "./data", workflow_executor: Any = None):
        self.data_dir = data_dir
        self.workflow_executor = workflow_executor
        self.chains: Dict[str, PipeChain] = {}
        self._load_chains()
        
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
                "description": chain.description
            }
            data[chain_id] = d
            
        with open(f"{self.data_dir}/pipe_chains.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
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
                output_mapping: Dict[str, str] = None) -> Optional[PipeStep]:
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
            output_mapping=output_mapping or {}
        )
        chain.steps.append(step)
        self._save_chains()
        return step
    
    def execute_step(self, step: PipeStep, input_data: Any) -> PipeResult:
        """执行单个管道步骤"""
        import time
        start_time = time.time()
        
        try:
            # 解析命令
            command = self._parse_command(step.command, input_data)
            
            # 执行命令
            if command.startswith("rabai "):
                # RabAI 工作流
                result = self._execute_workflow(command, input_data)
            else:
                # 外部命令
                result = self._execute_command(command, step.timeout)
            
            # 处理输出映射
            output = self._map_output(result, step.output_mapping)
            
            return PipeResult(
                success=True,
                step_id=step.step_id,
                output=output,
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return PipeResult(
                success=False,
                step_id=step.step_id,
                output=None,
                error=str(e),
                duration=time.time() - start_time
            )
    
    def _parse_command(self, command: str, input_data: Any) -> str:
        """解析命令，替换变量"""
        # 如果输入是字典，进行变量替换
        if isinstance(input_data, dict):
            for key, value in input_data.items():
                placeholder = f"${{{key}}}"
                if placeholder in command:
                    command = command.replace(placeholder, str(value))
                # 简短形式
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
                raise Exception(result.stderr)
        except subprocess.TimeoutExpired:
            raise Exception(f"命令执行超时 ({timeout}秒)")
    
    def _execute_workflow(self, command: str, input_data: Any) -> str:
        """执行 RabAI 工作流"""
        # 解析工作流命令
        parts = command.split()
        if len(parts) < 2:
            raise Exception("无效的工作流命令")
            
        workflow_name = parts[1]
        
        # 如果有工作流执行器，调用它
        if self.workflow_executor:
            result = self.workflow_executor.run(workflow_name, input_data)
            return json.dumps(result)
        else:
            # 模拟执行
            return json.dumps({
                "workflow": workflow_name,
                "status": "executed",
                "input": input_data
            })
    
    def _map_output(self, output: Any, mapping: Dict[str, str]) -> Any:
        """映射输出"""
        if not mapping or not output:
            return output
            
        # 尝试解析JSON
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except json.JSONDecodeError:
                pass
        
        if not isinstance(output, dict):
            return output
            
        # 应用映射
        result = {}
        for new_key, old_key in mapping.items():
            if old_key in output:
                result[new_key] = output[old_key]
            elif old_key == "*":
                result[new_key] = output
                
        return result if result else output
    
    def execute_chain(self, chain_id: str, input_data: Any = None) -> ChainResult:
        """执行管道链"""
        import time
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
        results = []
        current_data = input_data
        errors = []
        
        for step in chain.steps:
            if not step.enabled:
                continue
                
            # 执行步骤
            result = self.execute_step(step, current_data)
            results.append(result)
            
            if result.success:
                # 准备下一步输入
                current_data = result.output
            else:
                # 处理错误
                errors.append(f"步骤 {step.name} 失败: {result.error}")
                
                if step.on_error == "stop":
                    break
                elif step.on_error == "fallback":
                    # 使用备用数据
                    current_data = step.input_mapping.get("_fallback", {})
                    
        total_duration = time.time() - start_time
        success = all(r.success for r in results)
        
        return ChainResult(
            success=success,
            chain_id=chain_id,
            results=results,
            final_output=current_data,
            total_duration=total_duration,
            errors=errors
        )
    
    def execute_parallel(self, steps: List[PipeStep], 
                        input_data: Any) -> List[PipeResult]:
        """并行执行步骤"""
        import concurrent.futures
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.execute_step, step, input_data) 
                      for step in steps]
            results = [f.result() for f in futures]
            
        return results
    
    def parse_from_stdin(self) -> Optional[Any]:
        """从标准输入读取数据"""
        if sys.stdin.isatty():
            return None
            
        try:
            data = sys.stdin.read()
            if data.strip():
                # 尝试解析为JSON
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
            # 简单YAML输出
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


class PipeCLI:
    """管道CLI接口"""
    
    def __init__(self, data_dir: str = "./data"):
        self.runner = PipelineRunner(data_dir)
    
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
            # 尝试从stdin读取
            input_data = self.runner.parse_from_stdin()
        
        result = self.runner.execute_chain(chain_id, input_data)
        
        # 输出结果
        self.runner.output_to_stdout({
            "success": result.success,
            "chain_id": result.chain_id,
            "duration": result.total_duration,
            "output": result.final_output,
            "errors": result.errors
        })
        
        return 0 if result.success else 1
    
    def _list_chains(self) -> int:
        """列出管道链"""
        chains = self.runner.list_chains()
        
        if not chains:
            print("暂无管道链")
            return 0
            
        print(f"管道链 ({len(chains)}个):\n")
        for chain in chains:
            status = "active" if any(s.enabled for s in chain.steps) else "disabled"
            print(f"  {chain.chain_id}: {chain.name} [{status}]")
            print(f"    模式: {chain.mode.value}, 步骤: {len(chain.steps)}")
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
            print("用法: add <chain_id> <name> <command>", file=sys.stderr)
            return 1
            
        chain_id = args[0]
        name = args[1]
        command = args[2]
        
        step = self.runner.add_step(chain_id, name, command)
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
RabAI AutoClick v22 - 管道CLI

用法:
  rabai-pipe run <chain_id> [input_json]   运行管道链
  rabai-pipe list                           列出所有管道链
  rabai-pipe create <name> [--mode mode]   创建管道链
  rabai-pipe add <chain_id> <name> <cmd>   添加步骤
  rabai-pipe stdin                         从stdin测试读取
  rabai-pipe help                          显示帮助

管道模式:
  linear    线性管道 (A | B | C)
  branch   分支管道 (A -> [B, C])
  parallel 并行管道

示例:
  echo '{"email": "test@example.com"}' | rabai-pipe run chain_1
  rabai-pipe run chain_1 '{"action": "process"}'
  cat data.json | rabai-pipe run chain_1
"""
        print(help_text)
        return 0


def create_pipeline_runner(data_dir: str = "./data", 
                         workflow_executor: Any = None) -> PipelineRunner:
    """创建管道运行器"""
    return PipelineRunner(data_dir, workflow_executor)


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
        None
    )
    
    # 执行管道
    result = runner.execute_chain(chain.chain_id, {"data": "test@example.com"})
    
    print(f"\n执行结果:")
    print(f"  成功: {result.success}")
    print(f"  耗时: {result.total_duration:.2f}秒")
    print(f"  输出: {result.final_output}")
    if result.errors:
        print(f"  错误: {result.errors}")
    
    # 列出所有管道
    print("\n管道链列表:")
    for c in runner.list_chains():
        print(f"  - {c.name} ({c.mode.value})")


from dataclasses import asdict
