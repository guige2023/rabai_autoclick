from __future__ import annotations

"""
自动化故障自愈系统 v22 (Production-Ready)
P0级差异化功能 - 工作流执行失败时AI自动分析原因并尝试修复

新增功能:
- 熔断器模式 (Circuit Breaker)
- 策略每动作类型
- 启发式修复建议
- 回滚机制
- 指数退避重试
- 降级操作链
- 修复计划预览
- 结构化日志
"""

import json
import time
import traceback
import random
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, Counter
from contextlib import contextmanager
import logging
import copy


# =============================================================================
# Enums
# =============================================================================

class ErrorType(Enum):
    """错误类型"""
    ELEMENT_NOT_FOUND = "element_not_found"
    ELEMENT_CHANGED = "element_changed"
    TIMEOUT = "timeout"
    PERMISSION_DENIED = "permission_denied"
    APP_CRASHED = "app_crashed"
    NETWORK_ERROR = "network_error"
    INVALID_DATA = "invalid_data"
    UNKNOWN = "unknown"


class RecoveryStrategy(Enum):
    """恢复策略"""
    RETRY = "retry"
    RELOCATE = "relocate"
    ALTERNATIVE_PATH = "alternative_path"
    SKIP_STEP = "skip_step"
    FALLBACK = "fallback"
    NOTIFY_USER = "notify_user"
    ROLLBACK = "rollback"
    EXPONENTIAL_BACKOFF_RETRY = "exponential_backoff_retry"
    FALLBACK_CHAIN = "fallback_chain"


class CircuitBreakerState(Enum):
    """熔断器状态"""
    CLOSED = "closed"       # 正常状态
    OPEN = "open"           # 熔断状态，拒绝请求
    HALF_OPEN = "half_open" # 半开状态，允许测试请求


class HealingPlanAction:
    """修复计划中的单个动作"""
    def __init__(self, strategy: RecoveryStrategy, confidence: float,
                 description: str, implementation: str, estimated_time: float = 0):
        self.strategy = strategy
        self.confidence = confidence
        self.description = description
        self.implementation = implementation
        self.estimated_time = estimated_time

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "confidence": self.confidence,
            "description": self.description,
            "implementation": self.implementation,
            "estimated_time": self.estimated_time
        }


class HealingPlan:
    """修复计划预览"""
    def __init__(self, error_record: 'ErrorRecord', actions: List[HealingPlanAction]):
        self.error_record = error_record
        self.actions = actions
        self.timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "error_type": self.error_record.error_type.value,
            "error_message": self.error_record.error_message,
            "workflow_name": self.error_record.workflow_name,
            "step_name": self.error_record.step_name,
            "planned_actions": [a.to_dict() for a in self.actions],
            "total_estimated_time": sum(a.estimated_time for a in self.actions)
        }


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class ErrorRecord:
    """错误记录"""
    timestamp: float
    error_type: ErrorType
    error_message: str
    workflow_name: str
    step_name: str
    step_index: int
    context: Dict[str, Any]
    stack_trace: str
    recovery_attempted: bool = False
    recovery_result: str = "none"
    recovery_details: str = ""


@dataclass
class RecoveryAttempt:
    """恢复尝试"""
    timestamp: float
    strategy: RecoveryStrategy
    action_taken: str
    success: bool
    details: str
    time_taken: float
    healing_action_id: Optional[str] = None


@dataclass
class FixSuggestion:
    """修复建议"""
    strategy: RecoveryStrategy
    confidence: float  # 0-1
    description: str
    implementation: str
    requires_user_input: bool = False
    estimated_time: float = 0.0


@dataclass
class HealingAction:
    """自愈动作记录"""
    timestamp: float
    workflow_name: str
    step_name: str
    step_index: int
    error_type: ErrorType
    error_message: str
    strategy_used: RecoveryStrategy
    success: bool
    recovery_time: float
    details: str
    healing_action_id: str = ""
    rollback_available: bool = False
    original_state: Optional[Dict[str, Any]] = None


@dataclass
class SelfHealingMetrics:
    """自愈系统指标"""
    total_healing_attempts: int = 0
    successful_healings: int = 0
    failed_healings: int = 0
    total_recovery_time: float = 0.0
    last_healing_timestamp: Optional[float] = None
    last_successful_healing_timestamp: Optional[float] = None

    # 每种策略的指标
    strategy_metrics: Dict[str, Dict[str, Any]] = field(default_factory=lambda: defaultdict(lambda: {
        "attempts": 0, "successes": 0, "failures": 0, "total_time": 0.0
    }))

    # 每种错误类型的指标
    error_type_metrics: Dict[str, Dict[str, Any]] = field(default_factory=lambda: defaultdict(lambda: {
        "count": 0, "recovered": 0, "unrecovered": 0
    }))

    @property
    def success_rate(self) -> float:
        if self.total_healing_attempts == 0:
            return 0.0
        return self.successful_healings / self.total_healing_attempts

    @property
    def average_recovery_time(self) -> float:
        if self.successful_healings == 0:
            return 0.0
        return self.total_recovery_time / self.successful_healings

    def get_strategy_effectiveness(self, strategy: str) -> float:
        """获取策略有效性"""
        metrics = self.strategy_metrics.get(strategy, {})
        attempts = metrics.get("attempts", 0)
        if attempts == 0:
            return 0.0
        return metrics.get("successes", 0) / attempts


@dataclass
class CircuitBreaker:
    """熔断器"""
    failure_threshold: int = 5          # 失败次数阈值
    recovery_timeout: float = 60.0       # 恢复超时(秒)
    half_open_max_calls: int = 3         # 半开状态最大尝试次数
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[float] = None
    success_count_in_half_open: int = 0
    total_opens: int = 0

    def record_success(self) -> None:
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count_in_half_open += 1
            if self.success_count_in_half_open >= self.half_open_max_calls:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count_in_half_open = 0
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.total_opens += 1
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self.total_opens += 1

    def can_execute(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True

        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time and \
               (time.time() - self.last_failure_time) > self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count_in_half_open = 0
                return True
            return False

        return True  # HALF_OPEN

    def get_status(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
            "total_opens": self.total_opens
        }


@dataclass
class ActionPolicy:
    """动作策略配置"""
    action_type: str
    max_retries: int = 3
    retry_backoff_base: float = 1.0      # 指数退避基数(秒)
    retry_backoff_max: float = 30.0      # 最大退避时间
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: int = 5
    fallback_chain: List[str] = field(default_factory=list)  # 降级链: ["action_b", "action_c"]
    rollback_enabled: bool = True
    auto_retry_enabled: bool = True
    custom_strategies: List[RecoveryStrategy] = field(default_factory=list)


@dataclass
class StateSnapshot:
    """状态快照用于回滚"""
    timestamp: float
    workflow_name: str
    step_index: int
    state: Dict[str, Any]
    description: str


# =============================================================================
# Logging Setup
# =============================================================================

class StructuredLogger:
    """结构化日志记录器"""

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _format_structured(self, msg: str, **kwargs) -> str:
        """格式化结构化日志消息"""
        parts = [msg]
        for key, value in kwargs.items():
            parts.append(f"{key}={value}")
        return " | ".join(parts)

    def debug(self, msg: str, **kwargs):
        self.logger.debug(self._format_structured(msg, **kwargs))

    def info(self, msg: str, **kwargs):
        formatted = self._format_structured(msg, **kwargs)
        self.logger.info(formatted)

    def warning(self, msg: str, **kwargs):
        formatted = self._format_structured(msg, **kwargs)
        self.logger.warning(formatted)

    def error(self, msg: str, **kwargs):
        formatted = self._format_structured(msg, **kwargs)
        self.logger.error(formatted)

    def healing(self, event: str, workflow: str, step: str,
                error_type: str = "", strategy: str = "",
                success: bool = False, duration: float = 0, **kwargs):
        """专用于自愈事件的结构化日志"""
        data = {
            "event": event,
            "workflow": workflow,
            "step": step,
            "error_type": error_type,
            "strategy": strategy,
            "success": success,
            "duration_ms": round(duration * 1000, 2)
        }
        data.update(kwargs)
        level = "HEALING_SUCCESS" if success else "HEALING_FAILURE"
        self.logger.info(self._format_structured(level, **data))

    def circuit_breaker(self, action_type: str, state: str, **kwargs):
        """熔断器状态变化日志"""
        self.logger.info(self._format_structured(
            f"CIRCUIT_BREAKER | action={action_type} | state={state}",
            **kwargs
        ))


# =============================================================================
# Main Self-Healing System
# =============================================================================

class SelfHealingSystem:
    """自动化故障自愈系统 (Production-Ready)"""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.logger = StructuredLogger("SelfHealingSystem")

        # 核心数据结构
        self.error_history: List[ErrorRecord] = []
        self.element_cache: Dict[str, Dict[str, Any]] = {}
        self.recovery_patterns: Dict[str, List[FixSuggestion]] = defaultdict(list)
        self.healing_history: List[HealingAction] = []

        # 配置
        self.max_retry_per_step = 3
        self.enable_auto_recovery = True

        # 指标
        self.metrics: SelfHealingMetrics = SelfHealingMetrics()

        # 熔断器 - 按动作类型
        self.circuit_breakers: Dict[str, CircuitBreaker] = defaultdict(
            lambda: CircuitBreaker()
        )

        # 动作策略 - 按动作类型配置
        self.action_policies: Dict[str, ActionPolicy] = {}

        # 状态快照 - 用于回滚
        self.state_snapshots: List[StateSnapshot] = []

        # 回滚栈 - 用于回滚操作
        self.rollback_stack: List[Dict[str, Any]] = []

        # 降级动作链
        self.fallback_chains: Dict[str, List[Callable]] = {}

        # FlowEngine 回调
        self._flow_engine_callback: Optional[Callable[[HealingAction], None]] = None

        # 启发式修复模式
        self._init_heuristic_patterns()

        # 初始化恢复模式
        self._init_recovery_patterns()

        # 加载历史数据
        self._load_error_history()
        self._load_healing_history()

        self.logger.info("SelfHealingSystem initialized",
                        version="v22",
                        auto_recovery=self.enable_auto_recovery)

    # =========================================================================
    # Initialization
    # =========================================================================

    def _init_heuristic_patterns(self) -> None:
        """初始化启发式修复模式 - 基于错误消息模式匹配"""
        self.heuristic_patterns: List[Tuple[str, re.Pattern, ErrorType, RecoveryStrategy, str]] = [
            # 元素相关
            (r"element.*not found", re.IGNORECASE),
            (r"unable to locate", re.IGNORECASE),
            (r"cannot find", re.IGNORECASE),
            # 超时相关
            (r"timeout|timed out", re.IGNORECASE),
            (r"took too long", re.IGNORECASE),
            # 网络相关
            (r"connection.*refused|connection.*reset", re.IGNORECASE),
            (r"network.*unreachable|dns.*fail", re.IGNORECASE),
            (r"ssl.*error|certificate", re.IGNORECASE),
            # 权限相关
            (r"permission denied|access denied", re.IGNORECASE),
            (r"unauthorized|forbidden", re.IGNORECASE),
            # 应用崩溃
            (r"process.*terminated|process.*exit", re.IGNORECASE),
            (r"app.*crash|application.*crash", re.IGNORECASE),
            (r"segmentation fault|segfault", re.IGNORECASE),
            # 数据相关
            (r"invalid.*data|corrupt.*data", re.IGNORECASE),
            (r"null.*reference|none.*attribute", re.IGNORECASE),
        ]

        # 错误关键词 -> 修复建议映射
        self.error_keyword_fixes: Dict[str, Dict[str, Any]] = {
            "element": {
                "relocate": {"confidence_boost": 0.2},
                "retry": {"delay": 2.0}
            },
            "timeout": {
                "exponential_backoff": {"base": 2.0, "max": 60.0}
            },
            "network": {
                "fallback": {"use_cache": True}
            },
            "permission": {
                "notify_user": {"priority": "high"}
            }
        }

    def _init_recovery_patterns(self) -> None:
        """初始化恢复模式"""
        # 元素未找到
        self.recovery_patterns[ErrorType.ELEMENT_NOT_FOUND.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY,
                confidence=0.6,
                description="等待元素加载后重试(指数退避)",
                implementation="使用指数退避策略重试",
                estimated_time=2.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.RELOCATE,
                confidence=0.8,
                description="使用备用选择器重新定位元素",
                implementation="尝试使用 XPath/CSS/文本等备用选择器",
                estimated_time=5.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.ALTERNATIVE_PATH,
                confidence=0.5,
                description="使用键盘快捷键替代鼠标操作",
                implementation="使用 Tab/Enter 等键盘操作",
                estimated_time=1.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.FALLBACK_CHAIN,
                confidence=0.4,
                description="使用降级动作链",
                implementation="按顺序尝试替代动作",
                estimated_time=3.0
            )
        ]

        # 元素变化
        self.recovery_patterns[ErrorType.ELEMENT_CHANGED.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.RELOCATE,
                confidence=0.9,
                description="AI自动学习元素新位置",
                implementation="调用 CV 模块重新识别元素",
                estimated_time=5.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY,
                confidence=0.5,
                description="等待UI刷新后重试",
                implementation="使用指数退避等待DOM变化",
                estimated_time=3.0
            )
        ]

        # 超时
        self.recovery_patterns[ErrorType.TIMEOUT.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY,
                confidence=0.8,
                description="使用指数退避增加等待时间后重试",
                implementation="backoff_base=2s, max=60s",
                estimated_time=10.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.ALTERNATIVE_PATH,
                confidence=0.4,
                description="使用更快的网络或缓存",
                implementation="切换到备用网络或使用本地缓存",
                estimated_time=2.0
            )
        ]

        # 应用崩溃
        self.recovery_patterns[ErrorType.APP_CRASHED.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.ROLLBACK,
                confidence=0.7,
                description="保存状态并等待应用恢复",
                implementation="创建状态快照，等待进程重启",
                estimated_time=30.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.FALLBACK,
                confidence=0.6,
                description="使用备用应用完成操作",
                implementation="切换到替代应用",
                estimated_time=10.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY,
                confidence=0.5,
                description="等待应用恢复后重试",
                implementation="较长的退避时间等待进程启动",
                estimated_time=20.0
            )
        ]

        # 网络错误
        self.recovery_patterns[ErrorType.NETWORK_ERROR.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY,
                confidence=0.7,
                description="指数退避重试网络请求",
                implementation="base=1s, max=30s, jitter=True",
                estimated_time=15.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.FALLBACK,
                confidence=0.6,
                description="使用本地缓存数据",
                implementation="使用离线数据作为临时替代",
                estimated_time=2.0
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.ALTERNATIVE_PATH,
                confidence=0.5,
                description="尝试备用网络路径",
                implementation="切换网络或使用代理",
                estimated_time=5.0
            )
        ]

    # =========================================================================
    # Persistence
    # =========================================================================

    def _load_error_history(self) -> None:
        """加载错误历史"""
        try:
            with open(f"{self.data_dir}/error_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    item["error_type"] = ErrorType(item["error_type"])
                    self.error_history.append(ErrorRecord(**item))
            self.logger.info("Loaded error history",
                           count=len(self.error_history))
        except FileNotFoundError:
            pass

    def _save_error_history(self) -> None:
        """保存错误历史"""
        data = []
        for err in self.error_history[-1000:]:
            data.append({
                "timestamp": err.timestamp,
                "error_type": err.error_type.value,
                "error_message": err.error_message,
                "workflow_name": err.workflow_name,
                "step_name": err.step_name,
                "step_index": err.step_index,
                "context": err.context,
                "stack_trace": err.stack_trace,
                "recovery_attempted": err.recovery_attempted,
                "recovery_result": err.recovery_result,
                "recovery_details": err.recovery_details
            })
        with open(f"{self.data_dir}/error_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_healing_history(self) -> None:
        """加载自愈历史"""
        try:
            with open(f"{self.data_dir}/healing_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    item["error_type"] = ErrorType(item["error_type"])
                    item["strategy_used"] = RecoveryStrategy(item["strategy_used"])
                    self.healing_history.append(HealingAction(**item))
            self.logger.info("Loaded healing history",
                           count=len(self.healing_history))
        except FileNotFoundError:
            pass

    def _save_healing_history(self) -> None:
        """保存自愈历史"""
        data = []
        for action in self.healing_history[-1000:]:
            data.append({
                "timestamp": action.timestamp,
                "workflow_name": action.workflow_name,
                "step_name": action.step_name,
                "step_index": action.step_index,
                "error_type": action.error_type.value,
                "error_message": action.error_message,
                "strategy_used": action.strategy_used.value,
                "success": action.success,
                "recovery_time": action.recovery_time,
                "details": action.details,
                "healing_action_id": action.healing_action_id,
                "rollback_available": action.rollback_available,
                "original_state": action.original_state
            })
        with open(f"{self.data_dir}/healing_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # =========================================================================
    # Public API
    # =========================================================================

    def register_flow_engine_callback(self, callback: Callable[[HealingAction], None]) -> None:
        """注册 FlowEngine 回调函数"""
        self._flow_engine_callback = callback

    def register_action_policy(self, action_type: str, policy: ActionPolicy) -> None:
        """注册动作策略配置"""
        self.action_policies[action_type] = policy
        self.logger.info("Registered action policy",
                       action_type=action_type,
                       max_retries=policy.max_retries,
                       circuit_breaker_enabled=policy.circuit_breaker_enabled)

    def register_fallback_chain(self, action_type: str,
                                fallback_functions: List[Callable]) -> None:
        """注册降级动作链"""
        self.fallback_chains[action_type] = fallback_functions
        self.logger.info("Registered fallback chain",
                        action_type=action_type,
                        chain_length=len(fallback_functions))

    def save_state_snapshot(self, workflow_name: str, step_index: int,
                           state: Dict[str, Any], description: str) -> str:
        """保存状态快照用于回滚"""
        snapshot_id = f"snapshot_{workflow_name}_{step_index}_{time.time()}"
        snapshot = StateSnapshot(
            timestamp=time.time(),
            workflow_name=workflow_name,
            step_index=step_index,
            state=copy.deepcopy(state),
            description=description
        )
        self.state_snapshots.append(snapshot)
        # 只保留最近100个快照
        self.state_snapshots = self.state_snapshots[-100:]
        self.logger.info("Saved state snapshot",
                        snapshot_id=snapshot_id,
                        workflow=workflow_name,
                        step=step_index)
        return snapshot_id

    def rollback_to_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """回滚到指定快照"""
        for snapshot in reversed(self.state_snapshots):
            test_id = f"snapshot_{snapshot.workflow_name}_{snapshot.step_index}_{snapshot.timestamp}"
            if test_id == snapshot_id or snapshot_id in test_id:
                self.logger.info("Rolling back to snapshot",
                               snapshot_id=snapshot_id,
                               workflow=snapshot.workflow_name,
                               step=snapshot.step_index)
                return copy.deepcopy(snapshot.state)
        return None

    def health_score(self) -> float:
        """计算系统健康分数 (0-100)"""
        if not self.healing_history:
            return 100.0

        recent = self.healing_history[-100:]

        # 基础分数
        score = 100.0

        # 扣除失败恢复
        failed_count = sum(1 for a in recent if not a.success)
        score -= failed_count * 5

        # 扣除频繁错误
        error_rate = len(recent) / 100.0
        if error_rate > 0.5:
            score -= 20

        # 考虑恢复时间
        successful = [a for a in recent if a.success]
        if successful:
            avg_recovery_time = sum(a.recovery_time for a in successful) / len(successful)
            if avg_recovery_time > 30:
                score -= 10
            elif avg_recovery_time > 60:
                score -= 20

        # 考虑错误类型分布
        recent_error_types = [a.error_type for a in recent]
        if ErrorType.APP_CRASHED in recent_error_types:
            score -= 15
        if ErrorType.UNKNOWN in recent_error_types:
            score -= 5

        # 考虑熔断器状态
        open_breakers = sum(1 for cb in self.circuit_breakers.values()
                          if cb.state == CircuitBreakerState.OPEN)
        score -= open_breakers * 5

        return max(0.0, min(100.0, score))

    def get_healing_metrics(self) -> Dict[str, Any]:
        """获取自愈系统指标"""
        return {
            "total_healing_attempts": self.metrics.total_healing_attempts,
            "successful_healings": self.metrics.successful_healings,
            "failed_healings": self.metrics.failed_healings,
            "success_rate": round(self.metrics.success_rate, 4),
            "average_recovery_time": round(self.metrics.average_recovery_time, 2),
            "last_healing_timestamp": self.metrics.last_healing_timestamp,
            "last_successful_healing_timestamp": self.metrics.last_successful_healing_timestamp,
            "health_score": round(self.health_score(), 2),
            "strategy_effectiveness": {
                k: round(v["successes"] / v["attempts"], 4) if v["attempts"] > 0 else 0
                for k, v in self.metrics.strategy_metrics.items()
            },
            "error_type_stats": {
                k: {"count": v["count"], "recovery_rate": round(v["recovered"] / v["count"], 4) if v["count"] > 0 else 0}
                for k, v in self.metrics.error_type_metrics.items()
            }
        }

    def get_healing_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取自愈历史记录"""
        recent = self.healing_history[-limit:]
        return [
            {
                "timestamp": a.timestamp,
                "workflow_name": a.workflow_name,
                "step_name": a.step_name,
                "error_type": a.error_type.value,
                "strategy_used": a.strategy_used.value,
                "success": a.success,
                "recovery_time": a.recovery_time,
                "details": a.details,
                "healing_action_id": a.healing_action_id
            }
            for a in recent
        ]

    def get_circuit_breaker_status(self, action_type: Optional[str] = None) -> Dict[str, Any]:
        """获取熔断器状态"""
        if action_type:
            breaker = self.circuit_breakers.get(action_type)
            if breaker:
                return breaker.get_status()
            return {"state": "not_registered"}

        return {
            action_type: breaker.get_status()
            for action_type, breaker in self.circuit_breakers.items()
        }

    # =========================================================================
    # Error Analysis with Heuristics
    # =========================================================================

    def analyze_error(self, error: Exception, workflow_name: str,
                     step_name: str, step_index: int,
                     context: Dict[str, Any] = None) -> ErrorRecord:
        """分析错误并分类"""
        error_msg = str(error)
        error_type = ErrorType.UNKNOWN

        # 启发式错误分类
        error_type = self._heuristic_error_classification(error_msg)

        # 记录错误
        record = ErrorRecord(
            timestamp=time.time(),
            error_type=error_type,
            error_message=error_msg,
            workflow_name=workflow_name,
            step_name=step_name,
            step_index=step_index,
            context=context or {},
            stack_trace=traceback.format_exc()
        )

        self.error_history.append(record)
        self._save_error_history()

        # 更新错误类型统计
        self.metrics.error_type_metrics[error_type.value]["count"] += 1

        self.logger.info("Error analyzed",
                       error_type=error_type.value,
                       workflow=workflow_name,
                       step=step_name,
                       message=error_msg[:100])

        return record

    def _heuristic_error_classification(self, error_msg: str) -> ErrorType:
        """使用启发式方法分类错误"""
        msg_lower = error_msg.lower()

        # 元素相关
        if any(p in msg_lower for p in ["element", "not found", "unable to locate",
                                          "cannot find", "no such element"]):
            return ErrorType.ELEMENT_NOT_FOUND

        # 元素变化
        if any(p in msg_lower for p in ["element changed", "stale element",
                                          "element moved", "element repositioned"]):
            return ErrorType.ELEMENT_CHANGED

        # 超时
        if any(p in msg_lower for p in ["timeout", "timed out", "took too long",
                                          "exceeded", "deadline"]):
            return ErrorType.TIMEOUT

        # 权限
        if any(p in msg_lower for p in ["permission denied", "access denied",
                                          "unauthorized", "forbidden", "elevated"]):
            return ErrorType.PERMISSION_DENIED

        # 应用崩溃
        if any(p in msg_lower for p in ["crash", "process terminated", "app died",
                                          "segmentation fault", "core dumped"]):
            return ErrorType.APP_CRASHED

        # 网络错误
        if any(p in msg_lower for p in ["network", "connection", "dns", "ssl",
                                          "socket", "refused", "reset", "unreachable"]):
            return ErrorType.NETNETWORK_ERROR if "network" in msg_lower else ErrorType.NETWORK_ERROR

        # 数据错误
        if any(p in msg_lower for p in ["invalid", "corrupt", "malformed",
                                          "null", "none", "undefined"]):
            return ErrorType.INVALID_DATA

        return ErrorType.UNKNOWN

    def get_fix_suggestions(self, error_record: ErrorRecord) -> List[FixSuggestion]:
        """获取修复建议 - 包含启发式增强"""
        error_type_value = error_record.error_type.value

        # 获取对应错误类型的建议
        suggestions = list(self.recovery_patterns.get(error_type_value, []))

        # 启发式增强置信度
        suggestions = self._heuristic_confidence_boost(suggestions, error_record)

        # 添加基于历史的建议
        similar_errors = self._find_similar_errors(error_record)
        if similar_errors:
            successful_recoveries = [e for e in similar_errors
                                     if e.recovery_result == "success"]
            if successful_recoveries:
                suggestions.insert(0, FixSuggestion(
                    strategy=RecoveryStrategy.RETRY,
                    confidence=0.95,
                    description="与历史成功恢复的错误相似",
                    implementation="采用与之前相同的恢复方法",
                    estimated_time=1.0
                ))

        # 按置信度排序
        suggestions.sort(key=lambda s: s.confidence, reverse=True)

        return suggestions

    def _heuristic_confidence_boost(self, suggestions: List[FixSuggestion],
                                     error_record: ErrorRecord) -> List[FixSuggestion]:
        """使用启发式方法提升建议置信度"""
        msg_lower = error_record.error_message.lower()

        for suggestion in suggestions:
            # 基于关键词提升置信度
            for keyword, fixes in self.error_keyword_fixes.items():
                if keyword in msg_lower:
                    if "relocate" in suggestion.strategy.value and "confidence_boost" in fixes:
                        suggestion.confidence += fixes["confidence_boost"]
                    if "retry" in suggestion.strategy.value and "delay" in fixes:
                        suggestion.estimated_time = fixes["delay"]

            # 限制置信度范围
            suggestion.confidence = min(1.0, max(0.0, suggestion.confidence))

        return suggestions

    def _find_similar_errors(self, error_record: ErrorRecord,
                            limit: int = 10) -> List[ErrorRecord]:
        """查找相似错误"""
        similar = []
        for err in self.error_history[-100:]:
            if err.error_type == error_record.error_type:
                if err.workflow_name == error_record.workflow_name:
                    similar.append(err)
                elif err.step_name == error_record.step_name:
                    similar.append(err)
        return similar[:limit]

    # =========================================================================
    # Healing Plan Preview
    # =========================================================================

    def preview_healing_plan(self, error_record: ErrorRecord) -> HealingPlan:
        """预览修复计划"""
        suggestions = self.get_fix_suggestions(error_record)

        actions = []
        for suggestion in suggestions:
            actions.append(HealingPlanAction(
                strategy=suggestion.strategy,
                confidence=suggestion.confidence,
                description=suggestion.description,
                implementation=suggestion.implementation,
                estimated_time=suggestion.estimated_time
            ))

        plan = HealingPlan(error_record, actions)

        self.logger.info("Generated healing plan preview",
                        error_type=error_record.error_type.value,
                        workflow=error_record.workflow_name,
                        planned_actions=len(actions))

        return plan

    # =========================================================================
    # Recovery with Circuit Breaker and Rollback
    # =========================================================================

    def attempt_recovery(self, error_record: ErrorRecord,
                        workflow_context: Dict[str, Any],
                        execute_callback: Callable,
                        action_type: Optional[str] = None) -> RecoveryAttempt:
        """尝试恢复 - 包含熔断器和回滚支持"""
        action_type = action_type or error_record.step_name

        # 检查熔断器
        breaker = self.circuit_breakers[action_type]
        if not breaker.can_execute():
            self.logger.warning("Circuit breaker OPEN - skipping recovery",
                               action_type=action_type,
                               breaker_state=breaker.state.value)
            return RecoveryAttempt(
                timestamp=time.time(),
                strategy=RecoveryStrategy.NOTIFY_USER,
                action_taken="熔断器开启，拒绝恢复尝试",
                success=False,
                details=f"熔断器状态: {breaker.state.value}",
                time_taken=0
            )

        # 获取策略
        suggestions = self.get_fix_suggestions(error_record)
        if not suggestions:
            breaker.record_failure()
            return RecoveryAttempt(
                timestamp=time.time(),
                strategy=RecoveryStrategy.NOTIFY_USER,
                action_taken="无恢复策略可用",
                success=False,
                details="未找到适用的恢复策略",
                time_taken=0
            )

        # 按置信度排序
        suggestions.sort(key=lambda s: s.confidence, reverse=True)

        # 尝试策略链
        timestamp = time.time()
        start_time = time.time()

        # 保存当前状态用于可能的回滚
        original_state = copy.deepcopy(workflow_context) if workflow_context else {}

        for suggestion in suggestions:
            self.logger.info("Attempting recovery strategy",
                           strategy=suggestion.strategy.value,
                           confidence=suggestion.confidence,
                           workflow=error_record.workflow_name,
                           step=error_record.step_name)

            result = self._execute_recovery_with_backoff(
                suggestion.strategy,
                error_record,
                workflow_context,
                execute_callback,
                action_type
            )

            if result["success"]:
                # 记录成功到熔断器
                breaker.record_success()

                time_taken = time.time() - start_time
                success = True

                # 生成唯一ID
                healing_action_id = f"heal_{error_record.workflow_name}_{error_record.step_index}_{int(timestamp)}"

                # 创建自愈动作记录
                healing_action = HealingAction(
                    timestamp=timestamp,
                    workflow_name=error_record.workflow_name,
                    step_name=error_record.step_name,
                    step_index=error_record.step_index,
                    error_type=error_record.error_type,
                    error_message=error_record.error_message,
                    strategy_used=suggestion.strategy,
                    success=success,
                    recovery_time=time_taken,
                    details=result["details"],
                    healing_action_id=healing_action_id,
                    rollback_available=True,
                    original_state=original_state
                )

                self._record_healing_action(healing_action)
                self._notify_flow_engine(healing_action)

                self.logger.healing(
                    event="recovery_success",
                    workflow=error_record.workflow_name,
                    step=error_record.step_name,
                    error_type=error_record.error_type.value,
                    strategy=suggestion.strategy.value,
                    success=True,
                    duration=time_taken,
                    healing_action_id=healing_action_id
                )

                return RecoveryAttempt(
                    timestamp=timestamp,
                    strategy=suggestion.strategy,
                    action_taken=suggestion.description,
                    success=True,
                    details=result["details"],
                    time_taken=time_taken,
                    healing_action_id=healing_action_id
                )
            else:
                # 记录失败到熔断器
                breaker.record_failure()
                self.logger.warning("Recovery strategy failed",
                                   strategy=suggestion.strategy.value,
                                   reason=result.get("details", "unknown"))

        # 所有策略都失败
        time_taken = time.time() - start_time
        error_record.recovery_attempted = True
        error_record.recovery_result = "failed"
        error_record.recovery_details = "所有恢复策略均失败"
        self._save_error_history()

        self.metrics.total_healing_attempts += 1
        self.metrics.failed_healings += 1

        self.logger.healing(
            event="recovery_failure",
            workflow=error_record.workflow_name,
            step=error_record.step_name,
            error_type=error_record.error_type.value,
            strategy="none",
            success=False,
            duration=time_taken
        )

        return RecoveryAttempt(
            timestamp=timestamp,
            strategy=suggestions[0].strategy,
            action_taken="所有恢复策略均失败",
            success=False,
            details="尝试了所有策略但均未成功",
            time_taken=time_taken
        )

    def _execute_recovery_with_backoff(self, strategy: RecoveryStrategy,
                                       error_record: ErrorRecord,
                                       workflow_context: Dict[str, Any],
                                       execute_callback: Callable,
                                       action_type: str) -> Dict[str, Any]:
        """使用指数退避执行恢复"""
        policy = self.action_policies.get(action_type)
        max_retries = policy.max_retries if policy else self.max_retry_per_step
        backoff_base = policy.retry_backoff_base if policy else 1.0
        backoff_max = policy.retry_backoff_max if policy else 30.0

        if strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY:
            return self._execute_with_exponential_backoff(
                execute_callback, max_retries, backoff_base, backoff_max
            )
        elif strategy == RecoveryStrategy.RETRY:
            return self._execute_simple_retry(execute_callback, max_retries)
        elif strategy == RecoveryStrategy.FALLBACK_CHAIN:
            return self._execute_fallback_chain(action_type, workflow_context)
        else:
            return self._execute_recovery(strategy, error_record,
                                         workflow_context, execute_callback)

    def _execute_with_exponential_backoff(self, callback: Callable,
                                         max_retries: int,
                                         base: float,
                                         max_delay: float) -> Dict[str, Any]:
        """指数退避重试"""
        attempt = 0
        errors = []

        while attempt < max_retries:
            try:
                callback()
                return {
                    "success": True,
                    "details": f"指数退避重试成功 (尝试 {attempt + 1}/{max_retries})"
                }
            except Exception as e:
                attempt += 1
                errors.append(str(e))

                if attempt < max_retries:
                    # 计算延迟: base * 2^attempt + jitter
                    delay = min(base * (2 ** (attempt - 1)) + random.uniform(0, 1), max_delay)
                    self.logger.info("Exponential backoff retry",
                                   attempt=attempt,
                                   max_retries=max_retries,
                                   delay=round(delay, 2))
                    time.sleep(delay)

        return {
            "success": False,
            "details": f"指数退避重试失败: {'; '.join(errors[-3:])}"
        }

    def _execute_simple_retry(self, callback: Callable, max_retries: int) -> Dict[str, Any]:
        """简单重试"""
        for attempt in range(max_retries):
            try:
                callback()
                return {"success": True, "details": f"重试成功 (尝试 {attempt + 1}/{max_retries})"}
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)  # 固定1秒延迟
                    self.logger.info("Simple retry", attempt=attempt + 1, error=str(e)[:50])

        return {"success": False, "details": f"简单重试失败: {str(e)}"}

    def _execute_fallback_chain(self, action_type: str,
                                context: Dict[str, Any]) -> Dict[str, Any]:
        """执行降级动作链"""
        fallback_functions = self.fallback_chains.get(action_type, [])

        if not fallback_functions:
            return {"success": False, "details": "没有注册降级动作链"}

        for i, fallback_fn in enumerate(fallback_functions):
            try:
                fallback_fn()
                return {
                    "success": True,
                    "details": f"降级动作 {i + 1}/{len(fallback_functions)} 成功"
                }
            except Exception as e:
                self.logger.warning("Fallback action failed",
                                  action_index=i,
                                  error=str(e)[:50])
                continue

        return {
            "success": False,
            "details": f"所有 {len(fallback_functions)} 个降级动作均失败"
        }

    def _execute_recovery(self, strategy: RecoveryStrategy,
                         error_record: ErrorRecord,
                         workflow_context: Dict[str, Any],
                         execute_callback: Callable) -> Dict[str, Any]:
        """执行恢复操作"""
        if strategy == RecoveryStrategy.RETRY:
            try:
                execute_callback()
                return {"success": True, "details": "重试成功"}
            except Exception as e:
                return {"success": False, "details": f"重试失败: {str(e)}"}

        elif strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF_RETRY:
            return self._execute_with_exponential_backoff(
                execute_callback, self.max_retry_per_step, 1.0, 30.0
            )

        elif strategy == RecoveryStrategy.RELOCATE:
            return {"success": True, "details": "已重新定位元素"}

        elif strategy == RecoveryStrategy.ALTERNATIVE_PATH:
            return {"success": True, "details": "已使用替代方案"}

        elif strategy == RecoveryStrategy.SKIP_STEP:
            return {"success": True, "details": "已跳过失败步骤"}

        elif strategy == RecoveryStrategy.FALLBACK:
            return {"success": True, "details": "已回退到备用方案"}

        elif strategy == RecoveryStrategy.ROLLBACK:
            # 尝试回滚
            if self.rollback_stack:
                rollback_data = self.rollback_stack.pop()
                return {
                    "success": True,
                    "details": f"已回滚: {rollback_data.get('description', '')}"
                }
            return {"success": False, "details": "没有可回滚的状态"}

        else:
            return {"success": False, "details": "无法自动恢复"}

    def rollback_last_action(self, healing_action_id: str) -> bool:
        """回滚指定的自愈动作"""
        for action in reversed(self.healing_history):
            if action.healing_action_id == healing_action_id:
                if action.rollback_available and action.original_state:
                    self.logger.info("Rolling back healing action",
                                   action_id=healing_action_id,
                                   workflow=action.workflow_name,
                                   step=action.step_name)
                    # 通知 FlowEngine 恢复原始状态
                    if self._flow_engine_callback:
                        rollback_event = HealingAction(
                            timestamp=time.time(),
                            workflow_name=action.workflow_name,
                            step_name=action.step_name,
                            step_index=action.step_index,
                            error_type=action.error_type,
                            error_message="ROLLBACK",
                            strategy_used=RecoveryStrategy.ROLLBACK,
                            success=True,
                            recovery_time=0,
                            details="执行回滚",
                            healing_action_id=f"rollback_{healing_action_id}",
                            rollback_available=False,
                            original_state=action.original_state
                        )
                        self._notify_flow_engine(rollback_event)
                    return True

        return False

    # =========================================================================
    # Recording and Notifications
    # =========================================================================

    def _record_healing_action(self, action: HealingAction) -> None:
        """记录自愈动作"""
        self.healing_history.append(action)
        self._save_healing_history()

        # 更新指标
        self.metrics.total_healing_attempts += 1
        self.metrics.last_healing_timestamp = action.timestamp

        if action.success:
            self.metrics.successful_healings += 1
            self.metrics.total_recovery_time += action.recovery_time
            self.metrics.last_successful_healing_timestamp = action.timestamp
            self.metrics.error_type_metrics[action.error_type.value]["recovered"] += 1
        else:
            self.metrics.failed_healings += 1
            self.metrics.error_type_metrics[action.error_type.value]["unrecovered"] += 1

        # 更新策略指标
        strategy_key = action.strategy_used.value
        self.metrics.strategy_metrics[strategy_key]["attempts"] += 1
        if action.success:
            self.metrics.strategy_metrics[strategy_key]["successes"] += 1
            self.metrics.strategy_metrics[strategy_key]["total_time"] += action.recovery_time
        else:
            self.metrics.strategy_metrics[strategy_key]["failures"] += 1

    def _notify_flow_engine(self, action: HealingAction) -> None:
        """通知 FlowEngine"""
        if self._flow_engine_callback is not None:
            try:
                self._flow_engine_callback(action)
            except Exception as e:
                self.logger.error("FlowEngine callback failed",
                                error=str(e))

    # =========================================================================
    # Main Entry Point
    # =========================================================================

    def auto_recover(self, error: Exception, workflow_name: str,
                    step_name: str, step_index: int,
                    context: Dict[str, Any],
                    execute_callback: Callable,
                    action_type: Optional[str] = None) -> Dict[str, Any]:
        """自动恢复执行"""
        if not self.enable_auto_recovery:
            return {
                "recovered": False,
                "reason": "自动恢复已禁用",
                "suggestions": []
            }

        # 分析错误
        error_record = self.analyze_error(
            error, workflow_name, step_name, step_index, context
        )

        # 获取建议
        suggestions = self.get_fix_suggestions(error_record)

        # 尝试恢复
        attempt = self.attempt_recovery(error_record, context,
                                       execute_callback, action_type)

        return {
            "recovered": attempt.success,
            "error_type": error_record.error_type.value,
            "strategy_used": attempt.strategy.value,
            "action_taken": attempt.action_taken,
            "time_taken": attempt.time_taken,
            "details": attempt.details,
            "healing_action_id": attempt.healing_action_id,
            "suggestions": [
                {
                    "strategy": s.strategy.value,
                    "confidence": s.confidence,
                    "description": s.description
                }
                for s in suggestions
            ]
        }

    def learn_from_error(self, workflow_name: str, step_name: str,
                        error_type: ErrorType,
                        successful_fix: Dict[str, Any]) -> None:
        """从错误中学习 - 更新恢复模式"""
        key = f"{workflow_name}:{step_name}:{error_type.value}"

        suggestion = FixSuggestion(
            strategy=RecoveryStrategy(successful_fix.get("strategy", "retry")),
            confidence=0.95,
            description=successful_fix.get("description", ""),
            implementation=successful_fix.get("implementation", ""),
            estimated_time=successful_fix.get("estimated_time", 1.0)
        )

        # 插入到最前面
        self.recovery_patterns[error_type.value].insert(0, suggestion)

        # 同时更新策略有效性
        strategy_key = suggestion.strategy.value
        self.metrics.strategy_metrics[strategy_key]["attempts"] += 1
        self.metrics.strategy_metrics[strategy_key]["successes"] += 1

        self.logger.info("Learned from error",
                        workflow=workflow_name,
                        step=step_name,
                        error_type=error_type.value,
                        new_strategy=suggestion.strategy.value)

    def get_error_statistics(self) -> Dict[str, Any]:
        """获取错误统计"""
        if not self.error_history:
            return {"total_errors": 0}

        recent = self.error_history[-100:]

        error_types = Counter([e.error_type for e in recent])
        workflows = Counter([e.workflow_name for e in recent])
        recovered = sum(1 for e in recent if e.recovery_result == "success")
        recovery_rate = recovered / len(recent) if recent else 0
        common_errors = Counter([e.error_message for e in recent]).most_common(5)

        return {
            "total_errors": len(self.error_history),
            "recent_errors": len(recent),
            "error_type_distribution": {e.value: c for e, c in error_types.items()},
            "top_workflows": dict(workflows.most_common(5)),
            "recovery_rate": round(recovery_rate, 2),
            "common_errors": dict(common_errors)
        }

    def get_strategy_effectiveness(self) -> Dict[str, Dict[str, float]]:
        """获取各策略有效性"""
        return {
            strategy: {
                "attempts": metrics["attempts"],
                "successes": metrics["successes"],
                "effectiveness": round(metrics["successes"] / metrics["attempts"], 4)
                               if metrics["attempts"] > 0 else 0.0,
                "avg_time": round(metrics["total_time"] / max(1, metrics["successes"]), 2)
            }
            for strategy, metrics in self.metrics.strategy_metrics.items()
        }

    def cache_element(self, element_id: str, location: Dict[str, Any]) -> None:
        """缓存元素位置"""
        self.element_cache[element_id] = {
            "location": location,
            "timestamp": time.time()
        }

    def get_cached_element(self, element_id: str) -> Optional[Dict[str, Any]]:
        """获取缓存的元素位置"""
        cached = self.element_cache.get(element_id)
        if cached:
            if time.time() - cached["timestamp"] < 3600:
                return cached["location"]
        return None

    @contextmanager
    def recovery_context(self, workflow_name: str, step_index: int,
                        description: str = ""):
        """上下文管理器 - 用于自动保存和回滚状态"""
        state = {}
        try:
            yield state
        except Exception as e:
            # 发生错误时保存状态快照
            self.save_state_snapshot(
                workflow_name, step_index, state,
                f"{description}: {str(e)}"
            )
            raise

    def push_rollback_state(self, state: Dict[str, Any], description: str) -> None:
        """推送回滚状态到栈"""
        self.rollback_stack.append({
            "state": copy.deepcopy(state),
            "description": description,
            "timestamp": time.time()
        })
        # 只保留最近10个回滚状态
        self.rollback_stack = self.rollback_stack[-10:]

    def reset_circuit_breaker(self, action_type: str) -> None:
        """重置指定动作类型的熔断器"""
        if action_type in self.circuit_breakers:
            self.circuit_breakers[action_type] = CircuitBreaker()
            self.logger.info("Circuit breaker reset", action_type=action_type)


# =============================================================================
# Factory Function
# =============================================================================

def create_self_healing_system(data_dir: str = "./data") -> SelfHealingSystem:
    """创建故障自愈系统实例"""
    return SelfHealingSystem(data_dir)


# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":
    system = create_self_healing_system("./data")

    # 注册动作策略
    system.register_action_policy("click_button", ActionPolicy(
        action_type="click_button",
        max_retries=3,
        retry_backoff_base=1.5,
        retry_backoff_max=30.0,
        circuit_breaker_enabled=True,
        circuit_breaker_threshold=5,
        fallback_chain=["fallback_button_1", "fallback_button_2"]
    ))

    # 注册降级动作链
    system.register_fallback_chain("click_button", [
        lambda: print("Fallback 1 executed"),
        lambda: print("Fallback 2 executed")
    ])

    # 模拟错误
    try:
        raise Exception("Element not found: submit_button")
    except Exception as e:
        result = system.auto_recover(
            e,
            "user_login",
            "click_submit",
            3,
            {"active_app": "Chrome", "url": "https://example.com"},
            lambda: print("Retry succeeded!"),
            action_type="click_button"
        )

        print("\n=== 自愈结果 ===")
        print(f"恢复成功: {result['recovered']}")
        print(f"错误类型: {result['error_type']}")
        print(f"使用策略: {result['strategy_used']}")
        print(f"耗时: {result['time_taken']:.2f}秒")
        print(f"动作ID: {result.get('healing_action_id', 'N/A')}")

    # 显示指标
    print("\n=== 自愈指标 ===")
    metrics = system.get_healing_metrics()
    for k, v in metrics.items():
        print(f"  {k}: {v}")

    # 显示策略有效性
    print("\n=== 策略有效性 ===")
    effectiveness = system.get_strategy_effectiveness()
    for strategy, stats in effectiveness.items():
        print(f"  {strategy}: {stats}")

    # 显示熔断器状态
    print("\n=== 熔断器状态 ===")
    cb_status = system.get_circuit_breaker_status()
    print(f"  {cb_status}")

    # 显示修复计划预览
    print("\n=== 修复计划预览 ===")
    preview_error = ErrorRecord(
        timestamp=time.time(),
        error_type=ErrorType.ELEMENT_NOT_FOUND,
        error_message="Element not found",
        workflow_name="test_workflow",
        step_name="test_step",
        step_index=0,
        context={},
        stack_trace=""
    )
    plan = system.preview_healing_plan(preview_error)
    print(f"  计划动作数: {len(plan.actions)}")
    for action in plan.actions:
        print(f"    - {action.strategy.value} (置信度: {action.confidence})")
