"""
预测性自动化引擎 v21
P0级差异化功能 - 基于用户历史行为预测下一个可能需要的动作
"""
import json
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from collections import defaultdict, Counter
import re


@dataclass
class UserAction:
    """用户动作记录"""
    timestamp: float
    action_type: str  # click, type, hotkey, app_launch, workflow_trigger
    target: str  # 目标元素/应用/工作流名称
    context: Dict[str, Any]  # 上下文信息
    result: str = "success"  # success, failed, cancelled
    duration: float = 0.0  # 耗时(秒)


@dataclass
class Prediction:
    """预测结果"""
    predicted_action: str
    confidence: float  # 0-1 置信度
    reasoning: str
    suggested_workflow: Optional[str] = None
    alternatives: List[str] = field(default_factory=list)
    context_match: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ActionPattern:
    """动作模式"""
    sequence: List[str]
    frequency: int
    avg_interval: float  # 平均间隔(秒)
    time_of_day: str
    day_of_week: str
    trigger_context: Dict[str, Any]


class PredictiveAutomationEngine:
    """预测性自动化引擎"""
    
    def __init__(self, data_dir: str = "./data", flow_engine_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.action_history: List[UserAction] = []
        self.patterns: List[ActionPattern] = []
        self.time_patterns: Dict[str, List[str]] = defaultdict(list)
        self.app_patterns: Dict[str, List[str]] = defaultdict(list)
        self.weekly_patterns: Dict[str, List[str]] = defaultdict(list)
        self.prediction_cache: Dict[str, Prediction] = {}
        self.last_prediction_time: float = 0
        # User correction learning
        self.user_corrections: Dict[str, int] = defaultdict(int)  # action -> correction count
        self.correction_history: List[Dict] = []
        # FlowEngine callback integration
        self.flow_engine_callback = flow_engine_callback
        self._ensure_data_dir()
        self._load_history()
        self._load_corrections()
        
    def _load_history(self) -> None:
        """加载历史数据"""
        try:
            with open(f"{self.data_dir}/action_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    self.action_history.append(UserAction(**item))
        except FileNotFoundError:
            pass
            
    def _save_history(self) -> None:
        """保存历史数据"""
        data = []
        for action in self.action_history[-10000:]:  # 保留最近10000条
            data.append({
                "timestamp": action.timestamp,
                "action_type": action.action_type,
                "target": action.target,
                "context": action.context,
                "result": action.result,
                "duration": action.duration
            })
        with open(f"{self.data_dir}/action_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _ensure_data_dir(self) -> None:
        """确保数据目录存在"""
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _load_corrections(self) -> None:
        """加载用户纠正数据"""
        try:
            with open(f"{self.data_dir}/user_corrections.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                self.user_corrections = defaultdict(int, data.get("corrections", {}))
                self.correction_history = data.get("history", [])
        except FileNotFoundError:
            pass
    
    def _save_corrections(self) -> None:
        """保存用户纠正数据"""
        data = {
            "corrections": dict(self.user_corrections),
            "history": self.correction_history[-1000:]
        }
        with open(f"{self.data_dir}/user_corrections.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def record_user_correction(self, predicted_action: str, user_action: str, 
                               context: Dict[str, Any] = None) -> None:
        """记录用户纠正，学习用户偏好
        
        Args:
            predicted_action: 系统预测的动作
            user_action: 用户实际执行的动作
            context: 上下文信息
        """
        if predicted_action != user_action:
            self.user_corrections[user_action] += 1
            self.correction_history.append({
                "timestamp": time.time(),
                "predicted": predicted_action,
                "actual": user_action,
                "context": context or {}
            })
            # 如果某个动作被多次纠正，降低其预测置信度
            if len(self.correction_history) % 10 == 0:
                self._save_corrections()
    
    def get_action_confidence(self, action: str) -> float:
        """获取动作的置信度（基于用户纠正历史）
        
        Returns:
            0.0-1.0 的置信度分数
        """
        total = sum(self.user_corrections.values())
        if total == 0:
            return 0.5  # 默认置信度
        
        # 被纠正次数越多的动作，置信度越低
        correction_count = self.user_corrections.get(action, 0)
        confidence = max(0.1, 1.0 - (correction_count / max(total, 1)) * 0.8)
        return confidence
    
    def export_learned_patterns(self, filepath: str = None) -> str:
        """导出学习到的模式
        
        Args:
            filepath: 导出文件路径，默认为 data_dir/patterns_export.json
            
        Returns:
            导出文件路径
        """
        if filepath is None:
            filepath = os.path.join(self.data_dir, "patterns_export.json")
        
        export_data = {
            "version": "1.0",
            "export_time": time.time(),
            "time_patterns": dict(self.time_patterns),
            "app_patterns": dict(self.app_patterns),
            "weekly_patterns": dict(self.weekly_patterns),
            "user_corrections": dict(self.user_corrections),
            "action_history_count": len(self.action_history)
        }
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        return filepath
    
    def import_learned_patterns(self, filepath: str) -> bool:
        """导入学习到的模式
        
        Args:
            filepath: 导入文件路径
            
        Returns:
            是否导入成功
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            self.time_patterns = defaultdict(list, data.get("time_patterns", {}))
            self.app_patterns = defaultdict(list, data.get("app_patterns", {}))
            self.weekly_patterns = defaultdict(list, data.get("weekly_patterns", {}))
            self.user_corrections = defaultdict(int, data.get("user_corrections", {}))
            
            self._save_corrections()
            return True
        except Exception as e:
            print(f"Import failed: {e}")
            return False
    
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
    
    def record_action(self, action_type: str, target: str, 
                     context: Dict[str, Any] = None, 
                     result: str = "success", 
                     duration: float = 0.0) -> None:
        """记录用户动作"""
        action = UserAction(
            timestamp=time.time(),
            action_type=action_type,
            target=target,
            context=context or {},
            result=result,
            duration=duration
        )
        self.action_history.append(action)
        
        # 每100条保存一次
        if len(self.action_history) % 100 == 0:
            self._save_history()
            
        # 更新模式
        self._update_patterns(action)
        
        # 通知 FlowEngine
        self._notify_flow_engine("action_recorded", {
            "action_type": action_type,
            "target": target,
            "result": result
        })
    
    def _update_patterns(self, action: UserAction) -> None:
        """更新动作模式"""
        dt = datetime.fromtimestamp(action.timestamp)
        time_key = self._get_time_key(dt)
        day_key = self._get_day_key(dt)
        
        # 更新时间段模式
        self.time_patterns[time_key].append(action.target)
        
        # 更新星期模式
        self.weekly_patterns[day_key].append(action.target)
        
        # 更新应用模式 (基于上下文中的活动应用)
        if "active_app" in action.context:
            self.app_patterns[action.context["active_app"]].append(action.target)
    
    def _get_time_key(self, dt: datetime) -> str:
        """获取时间键"""
        hour = dt.hour
        if 6 <= hour < 9:
            return "morning_early"
        elif 9 <= hour < 12:
            return "morning"
        elif 12 <= hour < 14:
            return "noon"
        elif 14 <= hour < 18:
            return "afternoon"
        elif 18 <= hour < 21:
            return "evening"
        elif 21 <= hour < 24:
            return "night_late"
        else:
            return "night"
    
    def _get_day_key(self, dt: datetime) -> str:
        """获取星期键"""
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        return days[dt.weekday()]
    
    def predict_next_action(self, current_context: Dict[str, Any] = None) -> Optional[Prediction]:
        """预测下一个动作"""
        # 检查缓存 (5分钟内有效)
        if time.time() - self.last_prediction_time < 300 and self.prediction_cache:
            return list(self.prediction_cache.values())[0] if self.prediction_cache else None
            
        context = current_context or {}
        now = datetime.now()
        time_key = self._get_time_key(now)
        day_key = self._get_day_key(now)
        
        predictions = []
        
        # 1. 基于时间模式预测
        if time_key in self.time_patterns:
            recent = self.time_patterns[time_key][-10:]
            if recent:
                most_common = Counter(recent).most_common(1)[0]
                predictions.append(Prediction(
                    predicted_action=most_common[0],
                    confidence=0.6,
                    reasoning=f"基于{time_key}时间段的常见动作",
                    context_match={"pattern": "time_based"}
                ))
        
        # 2. 基于星期模式预测
        if day_key in self.weekly_patterns:
            recent = self.weekly_patterns[day_key][-10:]
            if recent:
                most_common = Counter(recent).most_common(1)[0]
                # 检查是否与时间模式结果一致
                if predictions and predictions[0].predicted_action == most_common[0]:
                    predictions[0].confidence += 0.15
                    predictions[0].reasoning += f"，与{day_key}模式吻合"
                else:
                    predictions.append(Prediction(
                        predicted_action=most_common[0],
                        confidence=0.5,
                        reasoning=f"基于{day_key}的常见动作",
                        context_match={"pattern": "day_based"}
                    ))
        
        # 3. 基于应用上下文预测
        active_app = context.get("active_app")
        if active_app and active_app in self.app_patterns:
            recent = self.app_patterns[active_app][-5:]
            if recent:
                most_common = Counter(recent).most_common(1)[0]
                for p in predictions:
                    if p.predicted_action == most_common[0]:
                        p.confidence += 0.2
                        p.reasoning += f"，在使用{active_app}时常见"
                        break
                else:
                    predictions.append(Prediction(
                        predicted_action=most_common[0],
                        confidence=0.7,
                        reasoning=f"在使用{active_app}后的常见动作",
                        context_match={"pattern": "app_based", "app": active_app}
                    ))
        
        # 4. 序列模式预测 (连续动作)
        if len(self.action_history) >= 3:
            recent_sequence = [a.target for a in self.action_history[-5:]]
            for i in range(len(recent_sequence) - 2):
                pattern = recent_sequence[i:i+3]
                # 查找历史中相同模式后的下一个动作
                matches = self._find_sequence_next(pattern, recent_sequence[-1])
                if matches:
                    predictions.append(Prediction(
                        predicted_action=matches[0],
                        confidence=0.75,
                        reasoning="基于连续动作序列模式",
                        context_match={"pattern": "sequence", "matched": pattern}
                    ))
        
        # 5. 周期性预测 (如周一早上固定动作)
        if day_key == "monday" and time_key == "morning":
            monday_morning_actions = [a for a in self.action_history 
                                      if datetime.fromtimestamp(a.timestamp).weekday() == 0
                                      and 9 <= datetime.fromtimestamp(a.timestamp).hour < 12]
            if monday_morning_actions:
                targets = [a.target for a in monday_morning_actions[-20:]]
                most_common = Counter(targets).most_common(1)
                if most_common:
                    predictions.append(Prediction(
                        predicted_action=most_common[0][0],
                        confidence=0.65,
                        reasoning="周一早上常见工作流程",
                        context_match={"pattern": "periodic", "type": "monday_morning"}
                    ))
        
        # 排序并返回最佳预测
        if predictions:
            predictions.sort(key=lambda p: p.confidence, reverse=True)
            best = predictions[0]
            
            # 应用用户纠正学习的置信度调整
            action_confidence = self.get_action_confidence(best.predicted_action)
            best.confidence = best.confidence * 0.7 + action_confidence * 0.3
            best.alternatives = [p.predicted_action for p in predictions[1:3]]
            
            # 缓存结果
            self.prediction_cache = {best.predicted_action: best}
            self.last_prediction_time = time.time()
            
            # 通知 FlowEngine
            self._notify_flow_engine("prediction_made", {
                "predicted_action": best.predicted_action,
                "confidence": best.confidence
            })
            
            return best
            
        return None
    
    def _find_sequence_next(self, pattern: List[str], last_action: str) -> List[str]:
        """查找序列模式后的下一个动作"""
        if len(self.action_history) < 10:
            return []
            
        # 简化实现：查找历史中相同模式
        history_targets = [a.target for a in self.action_history[-1000:]]
        
        for i in range(len(history_targets) - len(pattern) - 1):
            if history_targets[i:i+len(pattern)] == pattern:
                next_action = history_targets[i+len(pattern)]
                return [next_action]
                
        return []
    
    def suggest_workflow_creation(self) -> Optional[str]:
        """建议创建自动化工作流"""
        if len(self.action_history) < 20:
            return None
            
        # 分析最近的动作序列
        recent_targets = [a.target for a in self.action_history[-50:]]
        target_counts = Counter(recent_targets)
        
        # 找出频繁重复的动作
        repeated = [t for t, count in target_counts.items() if count >= 5]
        
        if repeated:
            # 找到最频繁的
            most_frequent = target_counts.most_common(1)[0]
            return f"检测到您经常执行「{most_frequent[0]}」，建议创建自动化工作流"
            
        return None
    
    def get_time_based_suggestions(self) -> List[Dict[str, Any]]:
        """获取基于时间的建议"""
        now = datetime.now()
        time_key = self._get_time_key(now)
        day_key = self._get_day_key(now)
        
        suggestions = []
        
        # 周一早上建议
        if day_key == "monday" and time_key in ["morning", "morning_early"]:
            suggestions.append({
                "type": "workflow",
                "title": "周一早上工作准备",
                "description": "一键打开工作文档、邮件、通讯工具",
                "workflow": "monday_morning_routine"
            })
        
        # 下午建议
        if time_key == "afternoon":
            suggestions.append({
                "type": "workflow",
                "title": "下午茶时间",
                "description": "自动整理上午工作、准备下午任务",
                "workflow": "afternoon_tea_routine"
            })
        
        # 下班前建议
        if time_key == "evening":
            suggestions.append({
                "type": "workflow",
                "title": "下班Shutdown",
                "description": "自动保存文件、清理桌面、锁屏",
                "workflow": "shutdown_routine"
            })
        
        return suggestions
    
    def analyze_user_behavior(self) -> Dict[str, Any]:
        """分析用户行为"""
        if not self.action_history:
            return {"status": "no_data"}
        
        recent = self.action_history[-100:]
        
        # 动作类型分布
        action_types = Counter([a.action_type for a in recent])
        
        # 最常用的目标
        targets = Counter([a.target for a in recent])
        
        # 平均耗时
        avg_duration = sum(a.duration for a in recent) / len(recent) if recent else 0
        
        # 成功率
        success_count = sum(1 for a in recent if a.result == "success")
        success_rate = success_count / len(recent) if recent else 0
        
        return {
            "total_actions": len(self.action_history),
            "recent_actions": len(recent),
            "action_type_distribution": dict(action_types),
            "top_targets": dict(targets.most_common(10)),
            "avg_duration": round(avg_duration, 2),
            "success_rate": round(success_rate, 2),
            "time_patterns": {k: len(v) for k, v in self.time_patterns.items()},
            "app_patterns": {k: len(v) for k, v in self.app_patterns.items()}
        }
    
    def enable_predictive_execution(self, workflow_name: str) -> bool:
        """启用预测性执行"""
        prediction = self.predict_next_action()
        if prediction and prediction.confidence > 0.7:
            # 可以自动触发工作流
            return True
        return False
    
    def get_prediction_for_workflow(self, workflow_name: str) -> Optional[Prediction]:
        """获取特定工作流的预测信息"""
        return self.prediction_cache.get(workflow_name)


def create_predictive_engine(data_dir: str = "./data") -> PredictiveAutomationEngine:
    """创建预测性自动化引擎实例"""
    return PredictiveAutomationEngine(data_dir)


# 测试
if __name__ == "__main__":
    engine = create_predictive_engine("./data")
    
    # 模拟记录一些动作
    engine.record_action("app_launch", "Chrome", {"active_app": None})
    engine.record_action("click", "Email button", {"active_app": "Chrome"})
    engine.record_action("workflow_trigger", "Read emails", {"active_app": "Mail"})
    
    # 预测下一个动作
    prediction = engine.predict_next_action({"active_app": "Mail"})
    if prediction:
        print("=== 预测结果 ===")
        print(f"预测动作: {prediction.predicted_action}")
        print(f"置信度: {prediction.confidence}")
        print(f"推理: {prediction.reasoning}")
        print(f"备选: {prediction.alternatives}")
    
    # 行为分析
    analysis = engine.analyze_user_behavior()
    print("\n=== 行为分析 ===")
    print(f"总动作数: {analysis.get('total_actions', 0)}")
    print(f"成功率: {analysis.get('success_rate', 0)}")
