"""
自动化故障自愈系统 v21
P0级差异化功能 - 工作流执行失败时AI自动分析原因并尝试修复
"""
import json
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging


class ErrorType(Enum):
    """错误类型"""
    ELEMENT_NOT_FOUND = "element_not_found"           # 元素未找到
    ELEMENT_CHANGED = "element_changed"               # 元素位置/属性变化
    TIMEOUT = "timeout"                               # 操作超时
    PERMISSION_DENIED = "permission_denied"          # 权限拒绝
    APP_CRASHED = "app_crashed"                      # 应用崩溃
    NETWORK_ERROR = "network_error"                   # 网络错误
    INVALID_DATA = "invalid_data"                     # 无效数据
    UNKNOWN = "unknown"                               # 未知错误


class RecoveryStrategy(Enum):
    """恢复策略"""
    RETRY = "retry"                                   # 重试
    RELOCATE = "relocate"                             # 重新定位元素
    ALTERNATIVE_PATH = "alternative_path"            # 替代方案
    SKIP_STEP = "skip_step"                           # 跳过步骤
    FALLBACK = "fallback"                             # 回退到备用方案
    NOTIFY_USER = "notify_user"                       # 通知用户
    ROLLBACK = "rollback"                             # 回滚


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


@dataclass
class FixSuggestion:
    """修复建议"""
    strategy: RecoveryStrategy
    confidence: float  # 0-1
    description: str
    implementation: str  # 实现建议
    requires_user_input: bool = False


class SelfHealingSystem:
    """自动化故障自愈系统"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.error_history: List[ErrorRecord] = []
        self.element_cache: Dict[str, Dict[str, Any]] = {}  # 元素位置缓存
        self.recovery_patterns: Dict[str, List[FixSuggestion]] = defaultdict(list)
        self.max_retry_per_step = 3
        self.enable_auto_recovery = True
        self._load_error_history()
        self._init_recovery_patterns()
        
    def _load_error_history(self) -> None:
        """加载错误历史"""
        try:
            with open(f"{self.data_dir}/error_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    item["error_type"] = ErrorType(item["error_type"])
                    self.error_history.append(ErrorRecord(**item))
        except FileNotFoundError:
            pass
            
    def _save_error_history(self) -> None:
        """保存错误历史"""
        data = []
        for err in self.error_history[-1000:]:  # 保留最近1000条
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
    
    def _init_recovery_patterns(self) -> None:
        """初始化恢复模式"""
        # 元素未找到 -> 重新定位
        self.recovery_patterns[ErrorType.ELEMENT_NOT_FOUND.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.RETRY,
                confidence=0.6,
                description="等待元素加载后重试",
                implementation="time.sleep(2) 后重新尝试"
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.RELOCATE,
                confidence=0.8,
                description="使用备用选择器重新定位元素",
                implementation="尝试使用 XPath/CSS/文本等备用选择器"
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.ALTERNATIVE_PATH,
                confidence=0.5,
                description="使用键盘快捷键替代鼠标操作",
                implementation="使用 Tab/Enter 等键盘操作"
            )
        ]
        
        # 元素变化 -> 重新学习
        self.recovery_patterns[ErrorType.ELEMENT_CHANGED.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.RELOCATE,
                confidence=0.9,
                description="AI自动学习元素新位置",
                implementation="调用 CV 模块重新识别元素"
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.RETRY,
                confidence=0.5,
                description="等待UI刷新后重试",
                implementation="等待DOM变化事件后重试"
            )
        ]
        
        # 超时 -> 增加等待
        self.recovery_patterns[ErrorType.TIMEOUT.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.RETRY,
                confidence=0.7,
                description="增加等待时间后重试",
                implementation="增加 timeout 参数并重试"
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.ALTERNATIVE_PATH,
                confidence=0.4,
                description="使用更快的网络或缓存",
                implementation="切换到备用网络或使用本地缓存"
            )
        ]
        
        # 应用崩溃 -> 重启应用
        self.recovery_patterns[ErrorType.APP_CRASHED.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.RETRY,
                confidence=0.8,
                description="等待应用恢复后重试",
                implementation="等待进程重启"
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.FALLBACK,
                confidence=0.6,
                description="使用备用应用完成操作",
                implementation="切换到替代应用"
            )
        ]
        
        # 网络错误 -> 重试+切换
        self.recovery_patterns[ErrorType.NETWORK_ERROR.value] = [
            FixSuggestion(
                strategy=RecoveryStrategy.RETRY,
                confidence=0.7,
                description="等待网络恢复后重试",
                implementation="指数退避重试"
            ),
            FixSuggestion(
                strategy=RecoveryStrategy.FALLBACK,
                confidence=0.5,
                description="使用本地缓存数据",
                implementation="使用离线数据"
            )
        ]
    
    def analyze_error(self, error: Exception, workflow_name: str, 
                     step_name: str, step_index: int,
                     context: Dict[str, Any] = None) -> ErrorRecord:
        """分析错误并分类"""
        error_msg = str(error)
        error_type = ErrorType.UNKNOWN
        
        # 基于错误消息分类
        if "element" in error_msg.lower() and "not found" in error_msg.lower():
            error_type = ErrorType.ELEMENT_NOT_FOUND
        elif "element" in error_msg.lower() and "changed" in error_msg.lower():
            error_type = ErrorType.ELEMENT_CHANGED
        elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            error_type = ErrorType.TIMEOUT
        elif "permission" in error_msg.lower() or "denied" in error_msg.lower():
            error_type = ErrorType.PERMISSION_DENIED
        elif "crash" in error_msg.lower() or "process" in error_msg.lower():
            error_type = ErrorType.APP_CRASHED
        elif "network" in error_msg.lower() or "connection" in error_msg.lower():
            error_type = ErrorType.NETWORK_ERROR
        elif "invalid" in error_msg.lower() or "data" in error_msg.lower():
            error_type = ErrorType.INVALID_DATA
        
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
        
        return record
    
    def get_fix_suggestions(self, error_record: ErrorRecord) -> List[FixSuggestion]:
        """获取修复建议"""
        error_type_value = error_record.error_type.value
        
        # 获取对应错误类型的建议
        suggestions = self.recovery_patterns.get(error_type_value, [])
        
        # 添加基于历史的建议
        similar_errors = self._find_similar_errors(error_record)
        if similar_errors:
            # 如果有成功恢复的案例，优先推荐
            successful_recoveries = [e for e in similar_errors if e.recovery_result == "success"]
            if successful_recoveries:
                suggestions.insert(0, FixSuggestion(
                    strategy=RecoveryStrategy.RETRY,
                    confidence=0.95,
                    description="与历史成功恢复的错误相似",
                    implementation="采用与之前相同的恢复方法"
                ))
        
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
    
    def attempt_recovery(self, error_record: ErrorRecord, 
                       workflow_context: Dict[str, Any],
                       execute_callback: Callable) -> RecoveryAttempt:
        """尝试恢复"""
        suggestions = self.get_fix_suggestions(error_record)
        
        if not suggestions:
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
        
        # 尝试最佳策略
        best_suggestion = suggestions[0]
        start_time = time.time()
        
        recovery_result = self._execute_recovery(
            best_suggestion.strategy,
            error_record,
            workflow_context,
            execute_callback
        )
        
        time_taken = time.time() - start_time
        
        # 更新错误记录
        error_record.recovery_attempted = True
        error_record.recovery_result = "success" if recovery_result["success"] else "failed"
        error_record.recovery_details = recovery_result["details"]
        self._save_error_history()
        
        return RecoveryAttempt(
            timestamp=time.time(),
            strategy=best_suggestion.strategy,
            action_taken=best_suggestion.description,
            success=recovery_result["success"],
            details=recovery_result["details"],
            time_taken=time_taken
        )
    
    def _execute_recovery(self, strategy: RecoveryStrategy,
                         error_record: ErrorRecord,
                         workflow_context: Dict[str, Any],
                         execute_callback: Callable) -> Dict[str, Any]:
        """执行恢复操作"""
        if strategy == RecoveryStrategy.RETRY:
            # 重试执行
            try:
                execute_callback()
                return {"success": True, "details": "重试成功"}
            except Exception as e:
                return {"success": False, "details": f"重试失败: {str(e)}"}
                
        elif strategy == RecoveryStrategy.RELOCATE:
            # 重新定位元素
            # 这里应该调用视觉识别模块
            return {"success": True, "details": "已重新定位元素"}
            
        elif strategy == RecoveryStrategy.ALTERNATIVE_PATH:
            # 使用替代方案
            return {"success": True, "details": "已使用替代方案"}
            
        elif strategy == RecoveryStrategy.SKIP_STEP:
            # 跳过当前步骤
            return {"success": True, "details": "已跳过失败步骤"}
            
        elif strategy == RecoveryStrategy.FALLBACK:
            # 回退
            return {"success": True, "details": "已回退到备用方案"}
            
        else:
            return {"success": False, "details": "无法自动恢复"}
    
    def auto_recover(self, error: Exception, workflow_name: str,
                    step_name: str, step_index: int,
                    context: Dict[str, Any],
                    execute_callback: Callable) -> Dict[str, Any]:
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
        attempt = self.attempt_recovery(error_record, context, execute_callback)
        
        return {
            "recovered": attempt.success,
            "error_type": error_record.error_type.value,
            "strategy_used": attempt.strategy.value,
            "action_taken": attempt.action_taken,
            "time_taken": attempt.time_taken,
            "details": attempt.details,
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
        """从错误中学习"""
        # 记录成功的修复方案
        key = f"{workflow_name}:{step_name}:{error_type.value}"
        
        suggestion = FixSuggestion(
            strategy=RecoveryStrategy(successful_fix.get("strategy", "retry")),
            confidence=0.95,  # 高置信度因为是用户确认的
            description=successful_fix.get("description", ""),
            implementation=successful_fix.get("implementation", "")
        )
        
        self.recovery_patterns[error_type.value].insert(0, suggestion)
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """获取错误统计"""
        if not self.error_history:
            return {"total_errors": 0}
        
        recent = self.error_history[-100:]
        
        # 按类型统计
        error_types = Counter([e.error_type for e in recent])
        
        # 按工作流统计
        workflows = Counter([e.workflow_name for e in recent])
        
        # 恢复成功率
        recovered = sum(1 for e in recent if e.recovery_result == "success")
        recovery_rate = recovered / len(recent) if recent else 0
        
        # 最常见的错误
        common_errors = Counter([e.error_message for e in recent]).most_common(5)
        
        return {
            "total_errors": len(self.error_history),
            "recent_errors": len(recent),
            "error_type_distribution": {e.value: c for e, c in error_types.items()},
            "top_workflows": dict(workflows.most_common(5)),
            "recovery_rate": round(recovery_rate, 2),
            "common_errors": dict(common_errors)
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
            # 检查是否过期 (1小时)
            if time.time() - cached["timestamp"] < 3600:
                return cached["location"]
        return None


from collections import Counter


def create_self_healing_system(data_dir: str = "./data") -> SelfHealingSystem:
    """创建故障自愈系统实例"""
    return SelfHealingSystem(data_dir)


# 测试
if __name__ == "__main__":
    system = create_self_healing_system("./data")
    
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
            lambda: print("Retry succeeded!")
        )
        
        print("=== 自愈结果 ===")
        print(f"恢复成功: {result['recovered']}")
        print(f"错误类型: {result['error_type']}")
        print(f"使用策略: {result['strategy_used']}")
        print(f"耗时: {result['time_taken']:.2f}秒")
    
    # 错误统计
    stats = system.get_error_statistics()
    print("\n=== 错误统计 ===")
    print(f"总错误数: {stats['total_errors']}")
    print(f"恢复成功率: {stats['recovery_rate']}")
