"""
工作流仿真与dry-run模块 v1.0
支持工作流执行模拟、时间旅行、场景测试、故障注入、性能预测
"""
import time
import json
import copy
import threading
import random
from typing import Dict, List, Optional, Any, Callable, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimulationMode(Enum):
    """仿真模式"""
    DRY_RUN = "dry_run"
    STATE_SIMULATION = "state_simulation"
    TIME_TRAVEL = "time_travel"
    SCENARIO_TEST = "scenario_test"
    FAILURE_SIM = "failure_simulation"
    PERFORMANCE_PREDICT = "performance_predict"
    RESOURCE_SIM = "resource_simulation"
    USER_BEHAVIOR_SIM = "user_behavior_sim"
    WHAT_IF = "what_if"
    SANDBOX = "sandbox"


class StepStatus(Enum):
    """步骤状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    SIMULATED = "simulated"


class ResourceType(Enum):
    """资源类型"""
    CPU = "cpu"
    MEMORY = "memory"
    NETWORK = "network"
    DISK = "disk"


class FailureType(Enum):
    """故障类型"""
    RANDOM = "random"
    TIMEOUT = "timeout"
    NETWORK_ERROR = "network_error"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    USER_CANCEL = "user_cancel"
    CUSTOM = "custom"


@dataclass
class SimulationConfig:
    """仿真配置"""
    mode: SimulationMode = SimulationMode.DRY_RUN
    enable_time_travel: bool = True
    enable_failure_injection: bool = False
    enable_resource_constraints: bool = False
    enable_user_simulation: bool = False
    sandbox_isolation: bool = True
    random_seed: Optional[int] = None
    max_history_steps: int = 1000


@dataclass
class WorkflowStep:
    """工作流步骤"""
    step_id: str
    name: str
    action: Callable
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    timeout: float = 30.0
    retry_count: int = 0
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_ms: float = 0.0


@dataclass
class SystemState:
    """系统状态快照"""
    timestamp: datetime
    cpu_usage: float = 0.0
    memory_usage_mb: float = 0.0
    network_latency_ms: float = 0.0
    disk_io_mbps: float = 0.0
    variables: Dict[str, Any] = field(default_factory=dict)
    ui_state: Dict[str, Any] = field(default_factory=dict)
    external_services: Dict[str, bool] = field(default_factory=dict)


@dataclass
class SimulationResult:
    """仿真结果"""
    success: bool
    total_steps: int
    successful_steps: int
    failed_steps: int
    skipped_steps: int
    total_duration_ms: float
    final_state: Optional[SystemState] = None
    step_results: List[Dict[str, Any]] = field(default_factory=list)
    predicted_performance: Optional[Dict[str, Any]] = None
    resource_usage: Dict[str, List[float]] = field(default_factory=dict)
    failures_injected: List[str] = field(default_factory=list)


@dataclass
class UserBehaviorProfile:
    """用户行为配置"""
    think_time_ms: Tuple[int, int] = (500, 3000)
    error_rate: float = 0.05
    cancellation_probability: float = 0.02
    pause_probability: float = 0.10
    variable_input_patterns: Dict[str, List[Any]] = field(default_factory=dict)


@dataclass
class ResourceConstraint:
    """资源约束"""
    resource_type: ResourceType
    limit: float
    unit: str = "%"


class WorkflowSimulation:
    """
    工作流仿真与dry-run引擎
    
    支持功能:
    1. Dry-run模式: 在不产生副作用的情况下模拟工作流执行
    2. 状态仿真: 模拟系统状态变化
    3. 时间旅行: 在执行过程中前进/后退
    4. 场景测试: 测试不同的输入场景
    5. 故障仿真: 在任意步骤注入故障
    6. 性能仿真: 预测性能特征
    7. 资源仿真: 模拟CPU、内存、网络约束
    8. 用户行为仿真: 模拟用户交互行为
    9. What-if分析: 分析变更影响
    10. 沙箱执行: 在隔离环境中运行工作流
    """

    def __init__(self, config: Optional[SimulationConfig] = None):
        """初始化仿真引擎"""
        self.config = config or SimulationConfig()
        self._state_history: List[SystemState] = []
        self._step_history: List[WorkflowStep] = []
        self._current_step_index: int = -1
        self._workflow_steps: Dict[str, WorkflowStep] = {}
        self._system_state: Optional[SystemState] = None
        self._is_running: bool = False
        self._breakpoints: List[str] = set()
        self._watch_variables: List[str] = []
        
        # 资源约束模拟
        self._resource_constraints: List[ResourceConstraint] = []
        self._simulated_resources: Dict[ResourceType, float] = {
            ResourceType.CPU: 0.0,
            ResourceType.MEMORY: 0.0,
            ResourceType.NETWORK: 0.0,
            ResourceType.DISK: 0.0,
        }
        
        # 故障注入
        self._failure_points: Dict[str, FailureType] = {}
        self._failure_probabilities: Dict[str, float] = {}
        
        # 用户行为模拟
        self._user_behavior: Optional[UserBehaviorProfile] = None
        
        # 场景管理
        self._scenarios: Dict[str, Dict[str, Any]] = {}
        self._current_scenario: Optional[str] = None
        
        # 沙箱环境
        self._sandbox_env: Dict[str, Any] = {}
        
        if self.config.random_seed is not None:
            random.seed(self.config.random_seed)

    def load_workflow(self, workflow_def: Dict[str, Any]) -> None:
        """加载工作流定义"""
        self._workflow_steps.clear()
        steps_data = workflow_def.get("steps", [])
        
        for step_data in steps_data:
            step = WorkflowStep(
                step_id=step_data["step_id"],
                name=step_data.get("name", step_data["step_id"]),
                action=self._create_mock_action(step_data.get("action", "noop")),
                params=step_data.get("params", {}),
                dependencies=step_data.get("dependencies", []),
                timeout=step_data.get("timeout", 30.0),
                retry_count=step_data.get("retry_count", 0),
            )
            self._workflow_steps[step.step_id] = step
        
        logger.info(f"Loaded workflow with {len(self._workflow_steps)} steps")

    def _create_mock_action(self, action_name: str) -> Callable:
        """创建模拟动作"""
        def mock_action(**params) -> Any:
            return {"action": action_name, "result": "simulated", "params": params}
        return mock_action

    def set_simulation_mode(self, mode: SimulationMode) -> None:
        """设置仿真模式"""
        self.config.mode = mode
        logger.info(f"Simulation mode set to: {mode.value}")

    def enable_dry_run(self, enable: bool = True) -> None:
        """启用/禁用dry-run模式"""
        if enable:
            self.config.mode = SimulationMode.DRY_RUN
            logger.info("Dry-run mode enabled")
        else:
            logger.info("Dry-run mode disabled")

    def set_resource_constraint(self, constraint: ResourceConstraint) -> None:
        """设置资源约束"""
        self._resource_constraints.append(constraint)
        self.config.enable_resource_constraints = True
        logger.info(f"Resource constraint added: {constraint.resource_type.value} = {constraint.limit}{constraint.unit}")

    def inject_failure(self, step_id: str, failure_type: FailureType, probability: float = 1.0) -> None:
        """在指定步骤注入故障"""
        self._failure_points[step_id] = failure_type
        self._failure_probabilities[step_id] = probability
        self.config.enable_failure_injection = True
        logger.info(f"Failure injection configured for step '{step_id}': {failure_type.value} (p={probability})")

    def set_user_behavior_profile(self, profile: UserBehaviorProfile) -> None:
        """设置用户行为配置"""
        self._user_behavior = profile
        self.config.enable_user_simulation = True
        logger.info("User behavior profile configured")

    def add_scenario(self, scenario_id: str, scenario_data: Dict[str, Any]) -> None:
        """添加测试场景"""
        self._scenarios[scenario_id] = scenario_data
        logger.info(f"Scenario '{scenario_id}' added")

    def set_breakpoint(self, step_id: str) -> None:
        """设置断点"""
        self._breakpoints.add(step_id)
        logger.info(f"Breakpoint set at step: {step_id}")

    def watch_variable(self, variable_name: str) -> None:
        """监视变量"""
        if variable_name not in self._watch_variables:
            self._watch_variables.append(variable_name)

    def _get_current_state(self) -> SystemState:
        """获取当前系统状态"""
        if self._system_state is None:
            self._system_state = SystemState(
                timestamp=datetime.now(),
                cpu_usage=random.uniform(10, 30),
                memory_usage_mb=random.uniform(100, 300),
                network_latency_ms=random.uniform(10, 50),
                disk_io_mbps=random.uniform(10, 100),
            )
        return self._system_state

    def _update_state(self, step: WorkflowStep, result: Any) -> SystemState:
        """更新系统状态"""
        state = self._get_current_state()
        state.timestamp = datetime.now()
        
        # 模拟资源使用变化
        state.cpu_usage = min(100, state.cpu_usage + random.uniform(5, 15))
        state.memory_usage_mb += random.uniform(5, 20)
        state.network_latency_ms += random.uniform(-5, 10)
        
        # 更新变量
        state.variables[f"step_{step.step_id}_result"] = result
        state.variables["last_step"] = step.step_id
        
        # 应用资源约束
        if self.config.enable_resource_constraints:
            for constraint in self._resource_constraints:
                if constraint.resource_type == ResourceType.CPU:
                    state.cpu_usage = min(state.cpu_usage, constraint.limit)
                elif constraint.resource_type == ResourceType.MEMORY:
                    state.memory_usage_mb = min(state.memory_usage_mb, constraint.limit)
        
        self._system_state = state
        return state

    def _simulate_failure(self, step_id: str) -> Optional[FailureType]:
        """模拟故障"""
        if step_id not in self._failure_points:
            return None
        
        failure_type = self._failure_points[step_id]
        probability = self._failure_probabilities.get(step_id, 1.0)
        
        if random.random() < probability:
            logger.warning(f"Simulated failure at step '{step_id}': {failure_type.value}")
            return failure_type
        return None

    def _simulate_user_behavior(self) -> Dict[str, Any]:
        """模拟用户行为"""
        if not self._user_behavior:
            return {}
        
        behavior_result = {
            "think_time_ms": random.randint(*self._user_behavior.think_time_ms),
            "cancelled": random.random() < self._user_behavior.cancellation_probability,
            "paused": random.random() < self._user_behavior.pause_probability,
            "error": random.random() < self._user_behavior.error_rate,
        }
        
        if behavior_result["cancelled"]:
            logger.info("User cancelled the operation")
        elif behavior_result["paused"]:
            logger.info("User paused the workflow")
        
        return behavior_result

    def _record_state_snapshot(self) -> None:
        """记录状态快照用于时间旅行"""
        if not self.config.enable_time_travel:
            return
        
        state_copy = copy.deepcopy(self._get_current_state())
        self._state_history.append(state_copy)
        
        # 限制历史记录大小
        if len(self._state_history) > self.config.max_history_steps:
            self._state_history.pop(0)

    def _record_step_snapshot(self, step: WorkflowStep) -> None:
        """记录步骤快照"""
        step_copy = copy.deepcopy(step)
        self._step_history.append(step_copy)
        
        if len(self._step_history) > self.config.max_history_steps:
            self._step_history.pop(0)

    async def execute_step_async(self, step_id: str) -> Any:
        """异步执行单个步骤"""
        if step_id not in self._workflow_steps:
            raise ValueError(f"Step '{step_id}' not found")
        
        step = self._workflow_steps[step_id]
        step.status = StepStatus.RUNNING
        step.start_time = datetime.now()
        
        self._record_state_snapshot()
        self._record_step_snapshot(step)
        
        # 检查断点
        if step_id in self._breakpoints:
            logger.info(f"Breakpoint hit at step: {step_id}")
            return None
        
        # 模拟用户行为
        if self.config.enable_user_simulation:
            user_behavior = self._simulate_user_behavior()
            if user_behavior.get("cancelled"):
                step.status = StepStatus.SKIPPED
                return None
        
        # 检查资源约束
        if self.config.enable_resource_constraints:
            for constraint in self._resource_constraints:
                current = self._simulated_resources.get(constraint.resource_type, 0)
                if current >= constraint.limit:
                    step.status = StepStatus.FAILED
                    step.error = f"Resource constraint violated: {constraint.resource_type.value}"
                    return None
        
        # 检查故障注入
        if self.config.enable_failure_injection:
            failure = self._simulate_failure(step_id)
            if failure:
                step.status = StepStatus.FAILED
                step.error = f"Simulated failure: {failure.value}"
                return None
        
        # 执行步骤
        try:
            if self.config.mode == SimulationMode.DRY_RUN:
                # Dry-run模式：模拟执行但不产生副作用
                logger.info(f"[DRY-RUN] Would execute step: {step.name}")
                step.status = StepStatus.SIMULATED
                step.result = {"dry_run": True, "step": step_id}
                await self._async_simulate_execution(step)
            else:
                # 实际执行模拟
                step.result = await self._async_simulate_execution(step)
                step.status = StepStatus.SUCCESS
        except Exception as e:
            step.status = StepStatus.FAILED
            step.error = str(e)
            logger.error(f"Step '{step_id}' failed: {e}")
        
        step.end_time = datetime.now()
        step.duration_ms = (step.end_time - step.start_time).total_seconds() * 1000
        
        self._update_state(step, step.result)
        return step.result

    async def _async_simulate_execution(self, step: WorkflowStep) -> Any:
        """模拟步骤执行（异步）"""
        # 模拟网络延迟或计算时间
        await asyncio_sleep(random.uniform(0.01, 0.1))
        
        # 基于步骤类型返回不同的模拟结果
        action_name = step.params.get("action", "default")
        
        if action_name == "click":
            return {"clicked": True, "position": step.params.get("position", (0, 0))}
        elif action_name == "type":
            return {"typed": True, "text": step.params.get("text", "")}
        elif action_name == "wait":
            return {"waited": True, "duration": step.params.get("duration", 1)}
        elif action_name == "screenshot":
            return {"screenshot": b"fake_image_data"}
        else:
            return {"executed": True, "step_id": step.step_id}

    def execute_workflow(self, workflow_def: Optional[Dict[str, Any]] = None) -> SimulationResult:
        """执行完整工作流仿真"""
        if workflow_def:
            self.load_workflow(workflow_def)
        
        self._is_running = True
        start_time = datetime.now()
        
        # 初始化系统状态
        self._system_state = SystemState(timestamp=start_time)
        self._state_history.clear()
        self._step_history.clear()
        
        successful_steps = 0
        failed_steps = 0
        skipped_steps = 0
        step_results = []
        failures_injected = []
        
        # 按依赖顺序执行步骤
        execution_order = self._get_execution_order()
        
        for step_id in execution_order:
            step = self._workflow_steps[step_id]
            
            # 检查依赖
            deps_satisfied = all(
                self._workflow_steps[dep].status == StepStatus.SUCCESS
                for dep in step.dependencies
            )
            
            if not deps_satisfied:
                step.status = StepStatus.SKIPPED
                skipped_steps += 1
                continue
            
            # 同步执行步骤
            result = self._execute_step_sync(step)
            step_results.append({
                "step_id": step.step_id,
                "name": step.name,
                "status": step.status.value,
                "duration_ms": step.duration_ms,
                "result": step.result,
                "error": step.error,
            })
            
            if step.status == StepStatus.SUCCESS:
                successful_steps += 1
            elif step.status == StepStatus.FAILED:
                failed_steps += 1
                if step.error and "Simulated failure" in step.error:
                    failures_injected.append(step.step_id)
            elif step.status == StepStatus.SKIPPED:
                skipped_steps += 1
        
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds() * 1000
        
        self._is_running = False
        
        return SimulationResult(
            success=failed_steps == 0,
            total_steps=len(self._workflow_steps),
            successful_steps=successful_steps,
            failed_steps=failed_steps,
            skipped_steps=skipped_steps,
            total_duration_ms=total_duration,
            final_state=self._get_current_state(),
            step_results=step_results,
            predicted_performance=self._predict_performance() if self.config.mode == SimulationMode.PERFORMANCE_PREDICT else None,
            resource_usage=self._collect_resource_usage(),
            failures_injected=failures_injected,
        )

    def _execute_step_sync(self, step: WorkflowStep) -> Any:
        """同步执行步骤"""
        step.status = StepStatus.RUNNING
        step.start_time = datetime.now()
        
        self._record_state_snapshot()
        
        # 模拟用户行为
        if self.config.enable_user_simulation:
            user_behavior = self._simulate_user_behavior()
            if user_behavior.get("cancelled"):
                step.status = StepStatus.SKIPPED
                return None
        
        # 检查故障注入
        if self.config.enable_failure_injection:
            failure = self._simulate_failure(step.step_id)
            if failure:
                step.status = StepStatus.FAILED
                step.error = f"Simulated failure: {failure.value}"
                step.end_time = datetime.now()
                step.duration_ms = (step.end_time - step.start_time).total_seconds() * 1000
                return None
        
        # 执行
        try:
            if self.config.mode == SimulationMode.DRY_RUN:
                logger.info(f"[DRY-RUN] Would execute: {step.name}")
                step.status = StepStatus.SIMULATED
                step.result = {"dry_run": True}
            else:
                step.result = step.action(**step.params)
                step.status = StepStatus.SUCCESS
        except Exception as e:
            step.status = StepStatus.FAILED
            step.error = str(e)
        
        step.end_time = datetime.now()
        step.duration_ms = (step.end_time - step.start_time).total_seconds() * 1000
        
        self._update_state(step, step.result)
        return step.result

    def _get_execution_order(self) -> List[str]:
        """获取拓扑排序后的执行顺序"""
        visited = set()
        order = []
        
        def visit(step_id: str):
            if step_id in visited:
                return
            visited.add(step_id)
            
            step = self._workflow_steps.get(step_id)
            if step:
                for dep in step.dependencies:
                    visit(dep)
                order.append(step_id)
        
        for step_id in self._workflow_steps:
            visit(step_id)
        
        return order

    def time_travel_to(self, step_index: int) -> Optional[SystemState]:
        """时间旅行到指定步骤"""
        if not self.config.enable_time_travel:
            logger.warning("Time travel is not enabled")
            return None
        
        if step_index < 0 or step_index >= len(self._step_history):
            logger.error(f"Invalid step index: {step_index}")
            return None
        
        self._current_step_index = step_index
        
        # 重建到该步骤的状态
        if step_index < len(self._state_history):
            self._system_state = copy.deepcopy(self._state_history[step_index])
        
        # 更新步骤状态
        for i, step_snapshot in enumerate(self._step_history[:step_index + 1]):
            if step_snapshot.step_id in self._workflow_steps:
                self._workflow_steps[step_snapshot.step_id].status = step_snapshot.status
                self._workflow_steps[step_snapshot.step_id].result = step_snapshot.result
        
        logger.info(f"Time traveled to step {step_index}")
        return self._system_state

    def time_travel_forward(self, steps: int = 1) -> Optional[SystemState]:
        """前进指定步数"""
        new_index = min(self._current_step_index + steps, len(self._step_history) - 1)
        return self.time_travel_to(new_index)

    def time_travel_backward(self, steps: int = 1) -> Optional[SystemState]:
        """后退指定步数"""
        new_index = max(self._current_step_index - steps, 0)
        return self.time_travel_to(new_index)

    def run_scenario(self, scenario_id: str) -> SimulationResult:
        """运行指定场景"""
        if scenario_id not in self._scenarios:
            raise ValueError(f"Scenario '{scenario_id}' not found")
        
        scenario = self._scenarios[scenario_id]
        self._current_scenario = scenario_id
        
        # 应用场景配置
        if "workflow" in scenario:
            workflow_def = scenario["workflow"]
            self.load_workflow(workflow_def)
        
        if "failures" in scenario:
            for step_id, failure_info in scenario["failures"].items():
                failure_type = FailureType(failure_info["type"])
                probability = failure_info.get("probability", 1.0)
                self.inject_failure(step_id, failure_type, probability)
        
        if "resources" in scenario:
            for res_type, limit in scenario["resources"].items():
                constraint = ResourceConstraint(
                    resource_type=ResourceType(res_type),
                    limit=limit,
                )
                self.set_resource_constraint(constraint)
        
        if "user_behavior" in scenario:
            ub = scenario["user_behavior"]
            profile = UserBehaviorProfile(
                think_time_ms=tuple(ub.get("think_time", [500, 3000])),
                error_rate=ub.get("error_rate", 0.05),
                cancellation_probability=ub.get("cancellation_probability", 0.02),
                pause_probability=ub.get("pause_probability", 0.10),
            )
            self.set_user_behavior_profile(profile)
        
        # 执行场景
        result = self.execute_workflow()
        
        self._current_scenario = None
        return result

    def what_if_analysis(self, changes: Dict[str, Any]) -> Dict[str, Any]:
        """What-if分析：分析变更影响"""
        logger.info("Starting what-if analysis...")
        
        # 保存当前状态
        original_steps = copy.deepcopy(self._workflow_steps)
        original_config = copy.deepcopy(self.config)
        
        # 应用变更
        analysis_result = {
            "changes_applied": changes,
            "impact_analysis": {},
            "predicted_outcomes": {},
        }
        
        # 分析每个变更的影响
        if "modify_steps" in changes:
            for step_id, modifications in changes["modify_steps"].items():
                if step_id in self._workflow_steps:
                    step = self._workflow_steps[step_id]
                    analysis_result["impact_analysis"][f"step_{step_id}"] = {
                        "original_timeout": step.timeout,
                        "new_timeout": modifications.get("timeout", step.timeout),
                        "original_retry": step.retry_count,
                        "new_retry": modifications.get("retry_count", step.retry_count),
                    }
        
        if "add_constraints" in changes:
            analysis_result["impact_analysis"]["resource_constraints"] = changes["add_constraints"]
        
        if "inject_failures" in changes:
            analysis_result["impact_analysis"]["failure_points"] = changes["inject_failures"]
        
        # 预测结果
        modified_workflow = {
            "steps": [
                {
                    "step_id": step.step_id,
                    "name": step.name,
                    "action": "mock",
                    "params": step.params,
                    "dependencies": step.dependencies,
                    "timeout": step.timeout,
                    "retry_count": step.retry_count,
                }
                for step in self._workflow_steps.values()
            ]
        }
        
        # 临时执行以获取预测
        temp_result = self.execute_workflow(modified_workflow)
        analysis_result["predicted_outcomes"] = {
            "success_probability": (temp_result.successful_steps / temp_result.total_steps * 100) if temp_result.total_steps > 0 else 0,
            "estimated_duration_ms": temp_result.total_duration_ms,
            "resource_usage": temp_result.resource_usage,
        }
        
        # 恢复原始状态
        self._workflow_steps = original_steps
        self.config = original_config
        
        logger.info("What-if analysis completed")
        return analysis_result

    def _predict_performance(self) -> Dict[str, Any]:
        """预测性能特征"""
        predictions = {
            "estimated_total_time_ms": 0.0,
            "bottleneck_steps": [],
            "resource_peaks": {},
            "parallel_opportunities": [],
        }
        
        for step in self._workflow_steps.values():
            # 基于历史数据预测
            base_time = step.timeout * 1000
            predictions["estimated_total_time_ms"] += base_time
            
            if step.timeout > 10:
                predictions["bottleneck_steps"].append({
                    "step_id": step.step_id,
                    "estimated_time_ms": base_time,
                })
        
        # 计算资源峰值预测
        predictions["resource_peaks"] = {
            "cpu_percent": random.uniform(60, 90),
            "memory_mb": random.uniform(300, 500),
            "network_mbps": random.uniform(10, 50),
        }
        
        # 识别并行机会
        for step in self._workflow_steps.values():
            if len(step.dependencies) == 0:
                predictions["parallel_opportunities"].append(step.step_id)
        
        return predictions

    def _collect_resource_usage(self) -> Dict[str, List[float]]:
        """收集资源使用情况"""
        usage = {
            "cpu": [state.cpu_usage for state in self._state_history],
            "memory": [state.memory_usage_mb for state in self._state_history],
            "network": [state.network_latency_ms for state in self._state_history],
            "disk": [state.disk_io_mbps for state in self._state_history],
        }
        return usage

    def execute_in_sandbox(self, workflow_def: Dict[str, Any], sandbox_config: Optional[Dict[str, Any]] = None) -> SimulationResult:
        """在沙箱环境中执行工作流"""
        logger.info("Executing workflow in sandbox mode...")
        
        # 保存当前环境
        original_env = copy.deepcopy(self._sandbox_env)
        
        # 设置沙箱配置
        if sandbox_config:
            self._sandbox_env = sandbox_config.get("env", {})
        
        # 确保隔离
        self.config.sandbox_isolation = True
        
        # 在沙箱中执行
        self.load_workflow(workflow_def)
        result = self.execute_workflow()
        
        # 清理沙箱环境
        self._sandbox_env.clear()
        
        logger.info("Sandbox execution completed")
        return result

    def get_simulation_state(self) -> Dict[str, Any]:
        """获取当前仿真状态"""
        return {
            "mode": self.config.mode.value,
            "is_running": self._is_running,
            "current_step_index": self._current_step_index,
            "total_steps": len(self._workflow_steps),
            "history_size": len(self._state_history),
            "breakpoints": list(self._breakpoints),
            "watch_variables": self._watch_variables,
            "resource_constraints": [
                {"type": c.resource_type.value, "limit": c.limit}
                for c in self._resource_constraints
            ],
            "failure_points": list(self._failure_points.keys()),
            "scenarios": list(self._scenarios.keys()),
            "current_state": {
                "cpu_usage": self._system_state.cpu_usage if self._system_state else 0,
                "memory_usage_mb": self._system_state.memory_usage_mb if self._system_state else 0,
                "network_latency_ms": self._system_state.network_latency_ms if self._system_state else 0,
            } if self._system_state else None,
        }

    def reset(self) -> None:
        """重置仿真状态"""
        self._state_history.clear()
        self._step_history.clear()
        self._current_step_index = -1
        self._workflow_steps.clear()
        self._system_state = None
        self._is_running = False
        self._breakpoints.clear()
        self._failure_points.clear()
        self._failure_probabilities.clear()
        self._resource_constraints.clear()
        self._scenarios.clear()
        self._current_scenario = None
        self._sandbox_env.clear()
        logger.info("Simulation state reset")


# 辅助函数
def asyncio_sleep(duration: float) -> Any:
    """异步睡眠（兼容模拟）"""
    time.sleep(duration)
    return None


def create_simulation(config: Optional[SimulationConfig] = None) -> WorkflowSimulation:
    """创建仿真引擎实例"""
    return WorkflowSimulation(config)


def create_test_workflow() -> Dict[str, Any]:
    """创建测试用工作流定义"""
    return {
        "steps": [
            {
                "step_id": "step1",
                "name": "Initialize",
                "action": "init",
                "params": {},
                "dependencies": [],
                "timeout": 5.0,
            },
            {
                "step_id": "step2",
                "name": "Load Data",
                "action": "load",
                "params": {"source": "database"},
                "dependencies": ["step1"],
                "timeout": 10.0,
            },
            {
                "step_id": "step3",
                "name": "Process",
                "action": "process",
                "params": {"algorithm": "fast"},
                "dependencies": ["step2"],
                "timeout": 30.0,
            },
            {
                "step_id": "step4",
                "name": "Save Results",
                "action": "save",
                "params": {"destination": "file"},
                "dependencies": ["step3"],
                "timeout": 15.0,
            },
        ]
    }
