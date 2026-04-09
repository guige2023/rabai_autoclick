"""
增强版智能工作流健康诊断 v22
用户体验优化 - 增强的诊断功能，包括趋势预测、根因分析、自动修复建议
"""
import json
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import statistics
import heapq


class HealthLevel(Enum):
    """健康等级"""
    EXCELLENT = "excellent"   # 优秀 90-100
    GOOD = "good"              # 良好 75-89
    FAIR = "fair"             # 一般 50-74
    POOR = "poor"             # 较差 25-49
    CRITICAL = "critical"      # 严重 0-24


class IssueSeverity(Enum):
    """问题严重程度"""
    CRITICAL = "critical"      # 严重
    HIGH = "high"             # 高
    MEDIUM = "medium"         # 中
    LOW = "low"               # 低
    INFO = "info"             # 信息


class RootCause(Enum):
    """根本原因"""
    NETWORK = "network"               # 网络问题
    PERMISSION = "permission"         # 权限问题
    TIMEOUT = "timeout"               # 超时
    RESOURCE = "resource"             # 资源不足
    CONFIG = "config"                 # 配置错误
    CODE = "code"                     # 代码错误
    DEPENDENCY = "dependency"         # 依赖问题
    ENVIRONMENT = "environment"       # 环境问题
    USER_INPUT = "user_input"         # 用户输入问题
    PERFORMANCE = "performance"       # 性能问题
    UNKNOWN = "unknown"               # 未知


@dataclass
class StepMetrics:
    """步骤指标"""
    step_name: str
    step_index: int
    avg_duration: float = 0.0
    min_duration: float = 0.0
    max_duration: float = 0.0
    std_duration: float = 0.0        # 标准差
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    error_messages: List[str] = field(default_factory=list)
    p50_duration: float = 0.0        # 中位数
    p95_duration: float = 0.0        # 95分位


@dataclass
class HealthIssue:
    """健康问题"""
    issue_id: str
    issue_type: str
    severity: IssueSeverity
    root_cause: RootCause
    title: str
    description: str
    location: str
    suggestion: str
    auto_fixable: bool = False
    fix_command: Optional[str] = None
    impact: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthTrend:
    """健康趋势"""
    period: str                   # 趋势周期
    success_rate_change: float    # 成功率变化
    duration_change: float        # 耗时变化
    trend_direction: str          # improving, declining, stable
    confidence: float             # 置信度


@dataclass
class AnomalyDetection:
    """异常检测"""
    detected_at: float
    anomaly_type: str            # spike, drop, drift
    metric: str                  # success_rate, duration
    expected_value: float
    actual_value: float
    deviation: float


@dataclass
class HealthReport:
    """增强健康报告"""
    workflow_id: str
    workflow_name: str
    overall_health: HealthLevel
    health_score: float          # 0-100
    
    # 基础统计
    execution_count: int
    success_rate: float
    avg_duration: float
    median_duration: float
    
    # 趋势分析
    trends: List[HealthTrend]
    anomalies: List[AnomalyDetection]
    
    # 问题列表
    issues: List[HealthIssue]
    step_metrics: List[StepMetrics]
    
    # 根因分析
    root_causes: Dict[str, int]     # 原因 -> 次数
    
    # 建议
    recommendations: List[Dict]     # 包含优先级、预期效果
    
    # 预测
    predicted_next_failure: Optional[str]
    predicted_duration: Optional[float]
    
    # 时间信息
    generated_at: float
    first_execution: Optional[float]
    last_execution: Optional[float]
    
    # 扩展
    comparison_to_average: Optional[float]  # 与平均水平的对比


class WorkflowDiagnosticsV2:
    """增强版智能工作流诊断室"""
    
    def __init__(self, data_dir: str = "./data", flow_engine_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.flow_engine_callback = flow_engine_callback
        self.execution_history: Dict[str, List[Dict]] = defaultdict(list)
        self.workflow_definitions: Dict[str, Dict] = {}
        # Health score trending
        self.health_score_history: Dict[str, List[Dict]] = defaultdict(list)
        self._ensure_data_dir()
        self._load_data()
        self._load_health_score_history()
        
    def _load_data(self) -> None:
        """加载数据"""
        try:
            with open(f"{self.data_dir}/execution_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for wf_id, executions in data.items():
                    self.execution_history[wf_id] = executions
        except FileNotFoundError:
            pass
            
        try:
            with open(f"{self.data_dir}/workflow_registry.json", "r", encoding="utf-8") as f:
                self.workflow_definitions = json.load(f)
        except FileNotFoundError:
            pass
    
    def _save_history(self) -> None:
        """保存历史"""
        data = {}
        for wf_id, executions in self.execution_history.items():
            data[wf_id] = executions[-100:]
        
        with open(f"{self.data_dir}/execution_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _ensure_data_dir(self) -> None:
        """确保数据目录存在"""
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _load_health_score_history(self) -> None:
        """加载健康分数历史"""
        try:
            with open(f"{self.data_dir}/health_score_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for wf_id, scores in data.items():
                    self.health_score_history[wf_id] = scores
        except FileNotFoundError:
            pass
    
    def _save_health_score_history(self) -> None:
        """保存健康分数历史"""
        data = {}
        for wf_id, scores in self.health_score_history.items():
            data[wf_id] = scores[-100:]  # 保留最近100条
        with open(f"{self.data_dir}/health_score_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def get_health_score_trend(self, workflow_id: str, period_hours: int = 168) -> Dict[str, Any]:
        """获取健康分数趋势
        
        Args:
            workflow_id: 工作流ID
            period_hours: 统计周期（默认168小时=7天）
            
        Returns:
            趋势信息字典
        """
        scores = self.health_score_history.get(workflow_id, [])
        if not scores:
            return {"status": "no_data", "trend": "unknown"}
        
        now = time.time()
        cutoff = now - (period_hours * 3600)
        recent_scores = [s for s in scores if s.get("timestamp", 0) > cutoff]
        
        if len(recent_scores) < 2:
            return {"status": "insufficient_data", "trend": "unknown"}
        
        # 计算趋势
        first_half = recent_scores[:len(recent_scores)//2]
        second_half = recent_scores[len(recent_scores)//2:]
        
        first_avg = sum(s.get("score", 0) for s in first_half) / len(first_half)
        second_avg = sum(s.get("score", 0) for s in second_half) / len(second_half)
        
        change = second_avg - first_avg
        
        if change > 5:
            trend = "improving"
        elif change < -5:
            trend = "declining"
        else:
            trend = "stable"
        
        return {
            "status": "ok",
            "trend": trend,
            "change": change,
            "current_score": recent_scores[-1].get("score", 0),
            "avg_score": (first_avg + second_avg) / 2,
            "data_points": len(recent_scores),
            "period_hours": period_hours
        }
    
    def perform_root_cause_analysis(self, workflow_id: str) -> Dict[str, Any]:
        """执行根因分析
        
        Args:
            workflow_id: 工作流ID
            
        Returns:
            根因分析结果
        """
        executions = self.execution_history.get(workflow_id, [])
        if not executions:
            return {"status": "no_data", "causes": []}
        
        # 收集失败信息
        failures = [e for e in executions if not e.get("success")]
        if not failures:
            return {"status": "healthy", "causes": []}
        
        # 按根因分组
        cause_counts = defaultdict(int)
        cause_examples = defaultdict(list)
        
        for failure in failures:
            error = failure.get("error", "Unknown error")
            cause = self._infer_root_cause(error)
            cause_counts[cause.value] += 1
            if len(cause_examples[cause.value]) < 3:
                cause_examples[cause.value].append(error[:100])
        
        # 计算百分比
        total_failures = len(failures)
        causes = []
        for cause_value, count in sorted(cause_counts.items(), key=lambda x: -x[1]):
            percentage = (count / total_failures) * 100
            causes.append({
                "cause": cause_value,
                "count": count,
                "percentage": round(percentage, 1),
                "examples": cause_examples[cause_value],
                "recommendation": self._get_suggestion_for_error(
                    RootCause(cause_value), ""
                )
            })
        
        # 时序分析
        time_patterns = self._analyze_failure_time_pattern(failures)
        
        return {
            "status": "analyzed",
            "total_failures": total_failures,
            "causes": causes,
            "time_patterns": time_patterns
        }
    
    def _analyze_failure_time_pattern(self, failures: List[Dict]) -> Dict:
        """分析失败时间模式"""
        if not failures:
            return {"pattern": "none"}
        
        hours = defaultdict(int)
        weekdays = defaultdict(int)
        
        for f in failures:
            ts = f.get("timestamp", 0)
            if ts:
                dt = datetime.fromtimestamp(ts)
                hours[dt.hour] += 1
                weekdays[dt.weekday()] += 1
        
        # 找出高发时段
        peak_hour = max(hours.items(), key=lambda x: x[1]) if hours else (0, 0)
        peak_weekday = max(weekdays.items(), key=lambda x: x[1]) if weekdays else (0, 0)
        
        weekdays_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        
        return {
            "peak_hour": peak_hour[0],
            "peak_hour_count": peak_hour[1],
            "peak_weekday": weekdays_names[peak_weekday[0]] if peak_weekday[0] < 7 else "未知",
            "peak_weekday_count": peak_weekday[1]
        }
    
    def set_flow_engine_callback(self, callback: Callable) -> None:
        """设置 FlowEngine 回调函数
        
        Args:
            callback: 回调函数，签名为 (event_type: str, data: Dict) -> None
        """
        self.flow_engine_callback = callback
    
    def _notify_flow_engine(self, event_type: str, data: Dict) -> None:
        """通知 FlowEngine 事件"""
        if self.flow_engine_callback:
            try:
                self.flow_engine_callback(event_type, data)
            except Exception:
                pass
    
    def record_execution(self, workflow_id: str, workflow_name: str,
                        step_results: List[Dict[str, Any]],
                        duration: float, success: bool,
                        error: str = None,
                        context: Dict = None) -> None:
        """记录执行结果"""
        record = {
            "timestamp": time.time(),
            "workflow_id": workflow_id,
            "workflow_name": workflow_name,
            "step_results": step_results,
            "duration": duration,
            "success": success,
            "error": error,
            "context": context or {}
        }
        
        self.execution_history[workflow_id].append(record)
        
        # 保留最近1000条
        if len(self.execution_history[workflow_id]) > 1000:
            self.execution_history[workflow_id] = \
                self.execution_history[workflow_id][-1000:]
        
        self._save_history()
        
        # 记录健康分数到历史
        report = self.diagnose(workflow_id)
        self.health_score_history[workflow_id].append({
            "timestamp": time.time(),
            "score": report.health_score,
            "success": success
        })
        self._save_health_score_history()
        
        # 通知 FlowEngine
        self._notify_flow_engine("execution_recorded", {
            "workflow_id": workflow_id,
            "success": success
        })
    
    def diagnose(self, workflow_id: str) -> HealthReport:
        """诊断工作流 - 增强版"""
        executions = self.execution_history.get(workflow_id, [])
        
        if not executions:
            return self._empty_report(workflow_id)
        
        # 基础统计
        execution_count = len(executions)
        success_count = sum(1 for e in executions if e.get("success"))
        success_rate = success_count / execution_count if execution_count else 0
        durations = [e["duration"] for e in executions]
        avg_duration = statistics.mean(durations) if durations else 0
        median_duration = statistics.median(durations) if durations else 0
        
        # 趋势分析
        trends = self._analyze_trends(executions)
        
        # 异常检测
        anomalies = self._detect_anomalies(executions)
        
        # 问题分析
        issues = self._analyze_issues(executions)
        
        # 根因分析
        root_causes = self._analyze_root_causes(executions)
        
        # 步骤指标
        step_metrics = self._calculate_step_metrics(executions)
        
        # 生成建议
        recommendations = self._generate_recommendations(
            issues, trends, root_causes, step_metrics
        )
        
        # 预测
        predicted_failure = self._predict_next_failure(executions)
        predicted_duration = self._predict_duration(executions)
        
        # 计算健康分数
        health_score = self._calculate_health_score_v2(
            success_rate, len(issues), avg_duration, trends, anomalies, issues
        )
        
        # 确定健康等级
        overall_health = self._get_health_level(health_score)
        
        # 与平均水平对比
        comparison = self._compare_to_average(workflow_id, health_score)
        
        report = HealthReport(
            workflow_id=workflow_id,
            workflow_name=executions[0].get("workflow_name", workflow_id),
            overall_health=overall_health,
            health_score=health_score,
            execution_count=execution_count,
            success_rate=success_rate,
            avg_duration=avg_duration,
            median_duration=median_duration,
            trends=trends,
            anomalies=anomalies,
            issues=issues,
            step_metrics=step_metrics,
            root_causes=root_causes,
            recommendations=recommendations,
            predicted_next_failure=predicted_failure,
            predicted_duration=predicted_duration,
            generated_at=time.time(),
            first_execution=executions[0].get("timestamp"),
            last_execution=executions[-1].get("timestamp"),
            comparison_to_average=comparison
        )
        
        # 通知 FlowEngine
        self._notify_flow_engine("diagnosis_complete", {
            "workflow_id": workflow_id,
            "health_score": health_score,
            "health_level": overall_health.value
        })
        
        return report
    
    def _empty_report(self, workflow_id: str) -> HealthReport:
        """空报告"""
        return HealthReport(
            workflow_id=workflow_id,
            workflow_name="Unknown",
            overall_health=HealthLevel.FAIR,
            health_score=50,
            execution_count=0,
            success_rate=0,
            avg_duration=0,
            median_duration=0,
            trends=[],
            anomalies=[],
            issues=[],
            step_metrics=[],
            root_causes={},
            recommendations=[{"priority": "high", "suggestion": "暂无执行数据，请先运行工作流收集数据"}],
            predicted_next_failure=None,
            predicted_duration=None,
            generated_at=time.time(),
            first_execution=None,
            last_execution=None,
            comparison_to_average=None
        )
    
    def _analyze_trends(self, executions: List[Dict]) -> List[HealthTrend]:
        """分析趋势"""
        if len(executions) < 5:
            return []
        
        trends = []
        
        # 按时间排序
        sorted_executions = sorted(executions, key=lambda x: x["timestamp"])
        
        # 多周期分析
        periods = [
            ("24h", 24 * 3600),
            ("7d", 7 * 24 * 3600),
            ("30d", 30 * 24 * 3600)
        ]
        
        now = time.time()
        
        for period_name, period_seconds in periods:
            # 筛选时间段内的执行
            period_executions = [
                e for e in sorted_executions 
                if now - e["timestamp"] <= period_seconds
            ]
            
            if len(period_executions) < 4:
                continue
            
            # 分割为前后两半
            mid = len(period_executions) // 2
            first_half = period_executions[:mid]
            second_half = period_executions[mid:]
            
            # 成功率趋势
            first_success = sum(1 for e in first_half if e.get("success")) / len(first_half)
            second_success = sum(1 for e in second_half if e.get("success")) / len(second_half)
            success_change = second_success - first_success
            
            # 耗时趋势
            first_duration = statistics.mean([e["duration"] for e in first_half])
            second_duration = statistics.mean([e["duration"] for e in second_half])
            duration_change = second_duration - first_duration
            
            # 趋势方向
            if abs(success_change) < 0.05:
                success_direction = "stable"
            elif success_change > 0:
                success_direction = "improving"
            else:
                success_direction = "declining"
            
            if abs(duration_change) < 1:
                duration_direction = "stable"
            elif duration_change < 0:
                duration_direction = "improving"
            else:
                duration_direction = "declining"
            
            # 置信度
            confidence = min(1.0, len(period_executions) / 20)
            
            trends.append(HealthTrend(
                period=period_name,
                success_rate_change=success_change,
                duration_change=duration_change,
                trend_direction=success_direction,
                confidence=confidence
            ))
        
        return trends
    
    def _detect_anomalies(self, executions: List[Dict]) -> List[AnomalyDetection]:
        """异常检测"""
        if len(executions) < 10:
            return []
        
        anomalies = []
        
        # 检测成功率突变
        success_values = [1 if e.get("success") else 0 for e in executions]
        recent_success = success_values[-5:]
        older_success = success_values[:-5]
        
        if older_success:
            older_rate = sum(older_success) / len(older_success)
            recent_rate = sum(recent_success) / len(recent_success)
            
            if recent_rate < older_rate - 0.3:
                anomalies.append(AnomalyDetection(
                    detected_at=time.time(),
                    anomaly_type="drop",
                    metric="success_rate",
                    expected_value=older_rate,
                    actual_value=recent_rate,
                    deviation=older_rate - recent_rate
                ))
        
        # 检测耗时异常
        durations = [e["duration"] for e in executions]
        if len(durations) >= 10:
            mean = statistics.mean(durations)
            std = statistics.stdev(durations) if len(durations) > 1 else 0
            
            recent_durations = [e["duration"] for e in executions[-3:]]
            recent_mean = statistics.mean(recent_durations)
            
            if std > 0 and (recent_mean - mean) / std > 2:
                anomalies.append(AnomalyDetection(
                    detected_at=time.time(),
                    anomaly_type="spike",
                    metric="duration",
                    expected_value=mean,
                    actual_value=recent_mean,
                    deviation=recent_mean - mean
                ))
        
        return anomalies
    
    def _analyze_issues(self, executions: List[Dict]) -> List[HealthIssue]:
        """分析问题"""
        issues = []
        
        # 收集错误
        errors = defaultdict(list)
        for e in executions:
            if not e.get("success") and e.get("error"):
                errors[e["error"]].append(e["timestamp"])
        
        # 分析失败模式
        for error_msg, timestamps in errors.items():
            count = len(timestamps)
            if count < 2:
                continue
            
            # 确定严重程度
            severity = IssueSeverity.LOW
            if count > 5:
                severity = IssueSeverity.CRITICAL
            elif count > 3:
                severity = IssueSeverity.HIGH
            elif count > 1:
                severity = IssueSeverity.MEDIUM
            
            # 推断根本原因
            root_cause = self._infer_root_cause(error_msg)
            
            issues.append(HealthIssue(
                issue_id=f"issue_{len(issues) + 1}",
                issue_type="recurring_error",
                severity=severity,
                root_cause=root_cause,
                title=f"重复错误: {error_msg[:50]}...",
                description=f"该错误已出现 {count} 次",
                location="工作流执行",
                suggestion=self._get_suggestion_for_error(root_cause, error_msg),
                auto_fixable=root_cause in [RootCause.CONFIG, RootCause.TIMEOUT],
                impact={"failure_count": count, "last_occurrence": max(timestamps)}
            ))
        
        # 检测慢步骤
        step_durations = defaultdict(list)
        for e in executions:
            for i, step in enumerate(e.get("step_results", [])):
                step_name = step.get("name", f"Step {i+1}")
                if "duration" in step:
                    step_durations[step_name].append(step["duration"])
        
        for step_name, durations in step_durations.items():
            if len(durations) >= 3:
                avg = statistics.mean(durations)
                max_d = max(durations)
                
                if avg > 10:
                    severity = IssueSeverity.MEDIUM
                    if avg > 30:
                        severity = IssueSeverity.HIGH
                        
                    issues.append(HealthIssue(
                        issue_id=f"issue_slow_{len(issues) + 1}",
                        issue_type="slow_step",
                        severity=severity,
                        root_cause=RootCause.PERFORMANCE,
                        title=f"慢步骤: {step_name}",
                        description=f"平均耗时 {avg:.1f}秒，最长 {max_d:.1f}秒",
                        location=step_name,
                        suggestion="考虑优化步骤或增加并行执行",
                        auto_fixable=False
                    ))
        
        # 按严重程度排序
        severity_order = {
            IssueSeverity.CRITICAL: 0,
            IssueSeverity.HIGH: 1,
            IssueSeverity.MEDIUM: 2,
            IssueSeverity.LOW: 3,
            IssueSeverity.INFO: 4
        }
        issues.sort(key=lambda i: severity_order[i.severity])
        
        return issues
    
    def _infer_root_cause(self, error_msg: str) -> RootCause:
        """推断根本原因"""
        error_lower = error_msg.lower()
        
        if any(kw in error_lower for kw in ["timeout", "超时", "timed out"]):
            return RootCause.TIMEOUT
        elif any(kw in error_lower for kw in ["network", "网络", "connection", "连接"]):
            return RootCause.NETWORK
        elif any(kw in error_lower for kw in ["permission", "权限", "denied", "拒绝"]):
            return RootCause.PERMISSION
        elif any(kw in error_lower for kw in ["not found", "不存在", "404"]):
            return RootCause.CONFIG
        elif any(kw in error_lower for kw in ["memory", "内存", "cpu", "资源"]):
            return RootCause.RESOURCE
        elif any(kw in error_lower for kw in ["import", "module", "dependency"]):
            return RootCause.DEPENDENCY
        elif any(kw in error_lower for kw in ["config", "配置", "setting"]):
            return RootCause.CONFIG
        else:
            return RootCause.UNKNOWN
    
    def _get_suggestion_for_error(self, root_cause: RootCause, 
                                  error_msg: str) -> str:
        """获取错误建议"""
        suggestions = {
            RootCause.TIMEOUT: "增加超时时间或优化网络连接",
            RootCause.NETWORK: "检查网络稳定性，考虑添加重试机制",
            RootCause.PERMISSION: "检查权限设置，确保有足够的访问权限",
            RootCause.CONFIG: "检查配置文件，确保路径和参数正确",
            RootCause.RESOURCE: "优化资源使用，考虑增加系统资源",
            RootCause.DEPENDENCY: "检查依赖包版本，确保兼容性",
            RootCause.ENVIRONMENT: "检查运行环境，确保环境配置正确",
            RootCause.USER_INPUT: "检查用户输入，确保输入数据有效",
            RootCause.UNKNOWN: "查看详细错误信息，联系技术支持"
        }
        return suggestions.get(root_cause, suggestions[RootCause.UNKNOWN])
    
    def _analyze_root_causes(self, executions: List[Dict]) -> Dict[str, int]:
        """分析根本原因"""
        root_causes = defaultdict(int)
        
        for e in executions:
            if not e.get("success") and e.get("error"):
                cause = self._infer_root_cause(e["error"])
                root_causes[cause.value] += 1
        
        return dict(root_causes)
    
    def _calculate_step_metrics(self, executions: List[Dict]) -> List[StepMetrics]:
        """计算步骤指标"""
        step_data = defaultdict(lambda: {"durations": [], "success": 0, "failure": 0, "errors": []})
        
        for e in executions:
            for i, step in enumerate(e.get("step_results", [])):
                step_name = step.get("name", f"Step {i+1}")
                
                if "duration" in step:
                    step_data[step_name]["durations"].append(step["duration"])
                
                if step.get("success", e.get("success", True)):
                    step_data[step_name]["success"] += 1
                else:
                    step_data[step_name]["failure"] += 1
                    if step.get("error"):
                        step_data[step_name]["errors"].append(step["error"])
        
        metrics = []
        for name, data in step_data.items():
            durations = data["durations"]
            count = len(durations)
            
            if count == 0:
                continue
                
            p50 = statistics.median(durations) if durations else 0
            sorted_durations = sorted(durations)
            p95_idx = int(count * 0.95)
            p95 = sorted_durations[p95_idx] if sorted_durations else 0
            
            metrics.append(StepMetrics(
                step_name=name,
                step_index=len(metrics),
                avg_duration=statistics.mean(durations) if durations else 0,
                min_duration=min(durations) if durations else 0,
                max_duration=max(durations) if durations else 0,
                std_duration=statistics.stdev(durations) if count > 1 else 0,
                execution_count=data["success"] + data["failure"],
                success_count=data["success"],
                failure_count=data["failure"],
                error_messages=list(set(data["errors"]))[:3],
                p50_duration=p50,
                p95_duration=p95
            ))
        
        return sorted(metrics, key=lambda m: m.step_index)
    
    def _generate_recommendations(self, issues: List[HealthIssue],
                                  trends: List[HealthTrend],
                                  root_causes: Dict[str, int],
                                  step_metrics: List[StepMetrics]) -> List[Dict]:
        """生成建议"""
        recommendations = []
        
        # 基于问题的建议
        for issue in issues[:5]:
            priority = "high" if issue.severity in [IssueSeverity.CRITICAL, IssueSeverity.HIGH] else "medium"
            recommendations.append({
                "priority": priority,
                "issue": issue.title,
                "suggestion": issue.suggestion,
                "auto_fixable": issue.auto_fixable
            })
        
        # 基于趋势的建议
        for trend in trends:
            if trend.trend_direction == "declining" and trend.confidence > 0.5:
                recommendations.append({
                    "priority": "high",
                    "suggestion": f"检测到{trend.period}内成功率下降趋势，建议检查最近是否有变化",
                    "trend": f"成功率变化: {trend.success_rate_change:.1%}"
                })
        
        # 基于根因的建议
        if root_causes:
            top_cause = max(root_causes.items(), key=lambda x: x[1])
            if top_cause[1] >= 3:
                recommendations.append({
                    "priority": "medium",
                    "suggestion": f"主要问题根因: {top_cause[0]}，建议重点排查"
                })
        
        # 基于慢步骤的建议
        slow_steps = [m for m in step_metrics if m.avg_duration > 10]
        if slow_steps:
            recommendations.append({
                "priority": "low",
                "suggestion": f"发现 {len(slow_steps)} 个慢步骤，建议优化"
            })
        
        return recommendations
    
    def _predict_next_failure(self, executions: List[Dict]) -> Optional[str]:
        """预测下一次失败"""
        if len(executions) < 10:
            return None
        
        # 简单预测：基于连续失败模式
        recent = executions[-5:]
        failures = [e for e in recent if not e.get("success")]
        
        if len(failures) >= 2:
            # 连续失败，可能再次失败
            if failures[-1].get("error"):
                return failures[-1]["error"][:100]
        
        # 基于时间模式预测
        now = datetime.now()
        hour = now.hour
        
        # 如果在历史失败高发时段
        failure_hours = []
        for e in executions:
            if not e.get("success"):
                hour = datetime.fromtimestamp(e["timestamp"]).hour
                failure_hours.append(hour)
        
        if failure_hours and hour in failure_hours:
            return f"当前小时({hour}时)历史失败率较高"
        
        return None
    
    def _predict_duration(self, executions: List[Dict]) -> Optional[float]:
        """预测执行时长"""
        if len(executions) < 3:
            return None
        
        recent = executions[-5:]
        durations = [e["duration"] for e in recent]
        
        # 加权平均，最近的权重更高
        weights = [1, 2, 3, 4, 5]
        weighted_sum = sum(d * w for d, w in zip(durations, weights[:len(durations)]))
        weight_sum = sum(weights[:len(durations)])
        
        return weighted_sum / weight_sum
    
    def _calculate_health_score_v2(self, success_rate: float, issue_count: int,
                                    avg_duration: float, trends: List[HealthTrend],
                                    anomalies: List[AnomalyDetection],
                                    issues: List[HealthIssue] = None) -> float:
        """计算健康分数 v2"""
        # 基础分数
        score = success_rate * 50
        
        # 问题扣分
        issues = issues or []
        critical_issues = sum(1 for i in issues if i.severity in [IssueSeverity.CRITICAL, IssueSeverity.HIGH])
        score -= critical_issues * 10
        score -= max(0, (issue_count - critical_issues) * 2)
        
        # 趋势加分/扣分
        for trend in trends:
            if trend.trend_direction == "improving":
                score += 10 * trend.confidence
            elif trend.trend_direction == "declining":
                score -= 10 * trend.confidence
        
        # 异常扣分
        score -= len(anomalies) * 5
        
        # 速度加分
        if avg_duration < 5:
            score += 20
        elif avg_duration < 10:
            score += 15
        elif avg_duration < 30:
            score += 10
        elif avg_duration < 60:
            score += 5
        
        return min(100, max(0, score))
    
    def _get_health_level(self, score: float) -> HealthLevel:
        """获取健康等级"""
        if score >= 90:
            return HealthLevel.EXCELLENT
        elif score >= 75:
            return HealthLevel.GOOD
        elif score >= 50:
            return HealthLevel.FAIR
        elif score >= 25:
            return HealthLevel.POOR
        else:
            return HealthLevel.CRITICAL
    
    def _compare_to_average(self, workflow_id: str, health_score: float = None) -> Optional[float]:
        """与平均水平对比"""
        # 获取所有工作流（排除当前）
        all_workflow_ids = list(self.execution_history.keys())
        if len(all_workflow_ids) <= 1 or health_score is None:
            return None
        
        other_ids = [wid for wid in all_workflow_ids if wid != workflow_id]
        if not other_ids:
            return None
        
        # 计算其他工作流的平均分数
        scores = []
        for wid in other_ids:
            # 避免递归，只计算基础指标
            executions = self.execution_history.get(wid, [])
            if executions:
                success = sum(1 for e in executions if e.get("success"))
                score = (success / len(executions)) * 50 + 30  # 简化计算
                scores.append(score)
        
        if not scores:
            return None
            
        avg_score = sum(scores) / len(scores)
        return health_score - avg_score
    
    def generate_report_text(self, report: HealthReport) -> str:
        """生成报告文本"""
        lines = []
        
        lines.append("=" * 60)
        lines.append(f"📊 工作流健康诊断报告 (v22 增强版)")
        lines.append("=" * 60)
        
        # 基本信息
        lines.append(f"\n工作流: {report.workflow_name}")
        lines.append(f"执行次数: {report.execution_count}")
        lines.append(f"成功率: {report.success_rate*100:.1f}%")
        lines.append(f"平均耗时: {report.avg_duration:.1f}秒")
        lines.append(f"中位耗时: {report.median_duration:.1f}秒")
        
        # 健康状态
        emoji = {
            HealthLevel.EXCELLENT: "🟢",
            HealthLevel.GOOD: "🟡",
            HealthLevel.FAIR: "🟠",
            HealthLevel.POOR: "🔴",
            HealthLevel.CRITICAL: "⛔"
        }
        lines.append(f"\n🩺 健康等级: {emoji.get(report.overall_health)} {report.overall_health.value}")
        lines.append(f"   健康分数: {report.health_score:.1f}/100")
        
        if report.comparison_to_average is not None:
            comparison = "↑" if report.comparison_to_average > 0 else "↓"
            lines.append(f"   与平均对比: {comparison}{abs(report.comparison_to_average):.1f}分")
        
        # 趋势
        if report.trends:
            lines.append(f"\n📈 趋势分析:")
            for trend in report.trends:
                icon = "↗️" if trend.trend_direction == "improving" else "↘️" if trend.trend_direction == "declining" else "➡️"
                lines.append(f"   {trend.period}: {icon} 成功率 {trend.success_rate_change:+.1%}")
        
        # 异常
        if report.anomalies:
            lines.append(f"\n⚠️ 异常检测:")
            for anomaly in report.anomalies:
                lines.append(f"   - {anomaly.anomaly_type}: {anomaly.metric} 偏离 {anomaly.deviation:.2f}")
        
        # 问题
        if report.issues:
            lines.append(f"\n❌ 发现问题 ({len(report.issues)}个):")
            for issue in report.issues[:5]:
                icon = "🔴" if issue.severity == IssueSeverity.CRITICAL else "🟠" if issue.severity == IssueSeverity.HIGH else "🟡"
                lines.append(f"   {icon} [{issue.severity.value}] {issue.title}")
                lines.append(f"      💡 {issue.suggestion}")
        
        # 根因
        if report.root_causes:
            lines.append(f"\n🔍 根因分析:")
            for cause, count in sorted(report.root_causes.items(), key=lambda x: -x[1]):
                lines.append(f"   - {cause}: {count}次")
        
        # 步骤性能
        if report.step_metrics:
            lines.append(f"\n📊 步骤性能 TOP5:")
            for m in report.step_metrics[:5]:
                success_rate = m.success_count / m.execution_count if m.execution_count else 0
                lines.append(f"   • {m.step_name}:")
                lines.append(f"     执行 {m.execution_count}次, 成功率 {success_rate:.0%}")
                lines.append(f"     耗时: {m.avg_duration:.1f}秒 (P50: {m.p50_duration:.1f}s, P95: {m.p95_duration:.1f}s)")
        
        # 建议
        if report.recommendations:
            lines.append(f"\n💡 优化建议:")
            for rec in report.recommendations[:5]:
                priority_icon = "🔴" if rec.get("priority") == "high" else "🟡"
                lines.append(f"   {priority_icon} {rec['suggestion']}")
        
        # 预测
        if report.predicted_next_failure:
            lines.append(f"\n🔮 故障预测:")
            lines.append(f"   可能的失败原因: {report.predicted_next_failure}")
        
        if report.predicted_duration:
            lines.append(f"   预测下次耗时: {report.predicted_duration:.1f}秒")
        
        lines.append(f"\n{'=' * 60}")
        lines.append(f"生成时间: {datetime.fromtimestamp(report.generated_at).strftime('%Y-%m-%d %H:%M:%S')}")
        
        return "\n".join(lines)
    
    def get_all_workflows_health(self) -> List[HealthReport]:
        """获取所有工作流健康状态"""
        reports = []
        for workflow_id in self.execution_history.keys():
            report = self.diagnose(workflow_id)
            reports.append(report)
        return sorted(reports, key=lambda r: r.health_score)
    
    def get_health_summary(self) -> Dict[str, Any]:
        """获取健康概览"""
        reports = self.get_all_workflows_health()
        
        if not reports:
            return {"total_workflows": 0}
        
        distribution = defaultdict(int)
        for r in reports:
            distribution[r.overall_health.value] += 1
        
        return {
            "total_workflows": len(reports),
            "avg_health_score": sum(r.health_score for r in reports) / len(reports),
            "health_distribution": dict(distribution),
            "avg_success_rate": sum(r.success_rate for r in reports) / len(reports),
            "avg_duration": sum(r.avg_duration for r in reports) / len(reports),
            "needs_attention": [r.workflow_name for r in reports if r.health_score < 50]
        }
    
    @property
    def health_level(self) -> str:
        """获取所有工作流的总体健康等级（枚举值）
        
        Returns:
            HealthLevel enum value string
        """
        reports = self.get_all_workflows_health()
        if not reports:
            return HealthLevel.FAIR.value
        
        avg_score = sum(r.health_score for r in reports) / len(reports)
        return self._get_health_level(avg_score).value


# 兼容旧版本
WorkflowDiagnostics = WorkflowDiagnosticsV2


def create_diagnostics(data_dir: str = "./data") -> WorkflowDiagnosticsV2:
    """创建诊断系统实例"""
    return WorkflowDiagnosticsV2(data_dir)


# 测试
if __name__ == "__main__":
    diag = create_diagnostics("./data")
    
    # 模拟执行记录
    for i in range(20):
        success = i < 16
        error = None if success else "Connection timeout after 30s"
        
        diag.record_execution(
            "wf_test",
            "测试工作流",
            [
                {"name": "打开应用", "duration": 2.5, "success": True},
                {"name": "点击按钮", "duration": 1.2 + (i % 3), "success": success},
                {"name": "保存结果", "duration": 3.0, "success": True}
            ],
            6.7 + (i % 5),
            success,
            error
        )
    
    # 诊断
    report = diag.diagnose("wf_test")
    
    # 输出报告
    print(diag.generate_report_text(report))
    
    # 健康概览
    summary = diag.get_health_summary()
    print(f"\n=== 健康概览 ===")
    print(f"总工作流数: {summary['total_workflows']}")
    print(f"平均健康分: {summary['avg_health_score']:.1f}")
    print(f"健康分布: {summary['health_distribution']}")
