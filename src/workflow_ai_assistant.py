"""
工作流AI助手 v22
P0级差异化功能 - 基于自然语言的工作流创建、调试和优化

功能:
- 自然语言工作流创建
- 工作流调试和修复建议
- 工作流代码审查
- 自动生成文档
- 步骤建议
- 错误解释
- 优化建议
- 模板推荐
- 动作推荐
- 从失败中学习
"""

import json
import time
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging

# ============== Enums ==============

class AIProvider(Enum):
    """支持的AI提供商"""
    OPENAI = "openai"
    LOCAL = "local"
    CUSTOM = "custom"


class TaskType(Enum):
    """任务类型"""
    CREATE_WORKFLOW = "create_workflow"
    DEBUG_WORKFLOW = "debug_workflow"
    REVIEW_WORKFLOW = "review_workflow"
    DOCUMENT_WORKFLOW = "document_workflow"
    SUGGEST_STEPS = "suggest_steps"
    EXPLAIN_ERROR = "explain_error"
    OPTIMIZE_WORKFLOW = "optimize_workflow"
    SUGGEST_TEMPLATE = "suggest_template"
    RECOMMEND_ACTION = "recommend_action"
    LEARN_FROM_FAILURE = "learn_from_failure"


# ============== Data Classes ==============

@dataclass
class WorkflowStep:
    """工作流步骤"""
    id: str
    action: str
    target: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None


@dataclass
class Workflow:
    """工作流"""
    name: str
    description: str
    steps: List[WorkflowStep]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DebugResult:
    """调试结果"""
    error_type: str
    error_message: str
    root_cause: str
    suggested_fixes: List[str]
    confidence: float
    healing_plan: Optional[Dict[str, Any]] = None


@dataclass
class ReviewResult:
    """审查结果"""
    issues: List[Dict[str, Any]]
    suggestions: List[str]
    score: float
    overall_feedback: str


@dataclass
class FailureRecord:
    """失败记录"""
    timestamp: float
    workflow_name: str
    error_type: str
    error_message: str
    context: Dict[str, Any]
    solution: Optional[str] = None


# ============== AI Backend Interfaces ==============

class AIBackend:
    """AI后端基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        raise NotImplementedError


class OpenAIBackend(AIBackend):
    """OpenAI API后端"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = config.get("api_key", os.environ.get("OPENAI_API_KEY", ""))
        self.model = config.get("model", "gpt-4")
        self._client = None
    
    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=self.api_key)
            except ImportError:
                raise ImportError("openai package not installed. Run: pip install openai")
        return self._client
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        client = self._get_client()
        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.7,
            max_tokens=2000
        )
        return response.choices[0].message.content


class LocalLLMBackend(AIBackend):
    """本地LLM后端 (如 Ollama)"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url", "http://localhost:11434")
        self.model = config.get("model", "llama2")
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        try:
            import requests
        except ImportError:
            raise ImportError("requests package not installed. Run: pip install requests")
        
        url = f"{self.base_url}/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt,
            "system": system_prompt or "",
            "stream": False
        }
        
        try:
            response = requests.post(url, json=payload, timeout=120)
            response.raise_for_status()
            return response.json().get("response", "")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Local LLM connection failed: {e}")


class CustomBackend(AIBackend):
    """自定义后端"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.endpoint = config.get("endpoint", "")
        self.headers = config.get("headers", {})
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        try:
            import requests
        except ImportError:
            raise ImportError("requests package not installed. Run: pip install requests")
        
        payload = {
            "prompt": prompt,
            "system": system_prompt or ""
        }
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                headers=self.headers,
                timeout=120
            )
            response.raise_for_status()
            return response.json().get("response", "")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Custom AI endpoint failed: {e}")


# ============== AI Assistant ==============

class AIAssistant:
    """
    工作流AI助手
    
    提供10种核心功能:
    1. 自然语言工作流创建
    2. 工作流调试
    3. 代码审查
    4. 自动文档生成
    5. 步骤建议
    6. 错误解释
    7. 优化建议
    8. 模板推荐
    9. 动作推荐
    10. 从失败中学习
    """
    
    def __init__(self, provider: AIProvider = AIProvider.OPENAI, config: Optional[Dict[str, Any]] = None):
        """
        初始化AI助手
        
        Args:
            provider: AI提供商
            config: 提供商配置
        """
        self.provider = provider
        self.config = config or {}
        self.backend = self._create_backend(provider, config)
        self.logger = logging.getLogger(__name__)
        self._failure_history: List[FailureRecord] = []
        self._workflow_templates: Dict[str, Dict[str, Any]] = {}
        self._load_failure_history()
        self._load_templates()
    
    def _create_backend(self, provider: AIProvider, config: Dict[str, Any]) -> AIBackend:
        """创建AI后端"""
        if provider == AIProvider.OPENAI:
            return OpenAIBackend(config)
        elif provider == AIProvider.LOCAL:
            return LocalLLMBackend(config)
        elif provider == AIProvider.CUSTOM:
            return CustomBackend(config)
        else:
            raise ValueError(f"Unknown provider: {provider}")
    
    def _load_failure_history(self):
        """加载失败历史"""
        history_file = os.path.join(os.path.dirname(__file__), "data", "ai_failure_history.json")
        try:
            if os.path.exists(history_file):
                with open(history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for item in data:
                        self._failure_history.append(FailureRecord(**item))
        except Exception as e:
            self.logger.warning(f"Failed to load failure history: {e}")
    
    def _save_failure_history(self):
        """保存失败历史"""
        history_file = os.path.join(os.path.dirname(__file__), "data", "ai_failure_history.json")
        try:
            os.makedirs(os.path.dirname(history_file), exist_ok=True)
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump([r.__dict__ for r in self._failure_history[-100:]], f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning(f"Failed to save failure history: {e}")
    
    def _load_templates(self):
        """加载工作流模板"""
        template_file = os.path.join(os.path.dirname(__file__), "data", "ai_workflow_templates.json")
        try:
            if os.path.exists(template_file):
                with open(template_file, 'r', encoding='utf-8') as f:
                    self._workflow_templates = json.load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load templates: {e}")
    
    def _save_templates(self):
        """保存工作流模板"""
        template_file = os.path.join(os.path.dirname(__file__), "data", "ai_workflow_templates.json")
        try:
            os.makedirs(os.path.dirname(template_file), exist_ok=True)
            with open(template_file, 'w', encoding='utf-8') as f:
                json.dump(self._workflow_templates, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning(f"Failed to save templates: {e}")
    
    def _generate(self, prompt: str, task_type: TaskType) -> str:
        """生成AI响应"""
        system_prompt = self._get_system_prompt(task_type)
        try:
            return self.backend.generate(prompt, system_prompt)
        except Exception as e:
            self.logger.error(f"AI generation failed: {e}")
            raise
    
    def _get_system_prompt(self, task_type: TaskType) -> str:
        """获取任务类型的系统提示"""
        base_prompt = """你是一个专业的工作流自动化助手。你帮助用户创建、调试和优化自动化工作流。
工作流由一系列步骤组成，每步包含: action(动作)、target(目标)、params(参数)。

常见动作类型:
- click: 点击元素
- double_click: 双击
- right_click: 右键点击
- type: 输入文本
- hotkey: 快捷键
- wait: 等待
- screenshot: 截图
- image_match: 图像匹配
- ocr: 文字识别
- app_launch: 启动应用
- app_close: 关闭应用

返回格式应为清晰的文本描述或JSON结构。"""
        
        task_prompts = {
            TaskType.CREATE_WORKFLOW: """你是一个工作流设计专家。用户会描述他们想要自动化的任务，你需要将其转化为JSON格式的工作流定义。

输出格式:
{
    "name": "工作流名称",
    "description": "简短描述",
    "steps": [
        {
            "id": "step_1",
            "action": "动作类型",
            "target": "目标元素/应用",
            "params": {},
            "description": "步骤描述"
        }
    ],
    "metadata": {}
}""",
            
            TaskType.DEBUG_WORKFLOW: """你是一个自动化调试专家。分析工作流执行失败的原因，提供详细的诊断和修复建议。

分析维度:
1. 错误类型识别 (元素未找到、超时、权限等)
2. 根本原因分析
3. 修复方案建议
4. 预防措施""",
            
            TaskType.REVIEW_WORKFLOW: """你是一个代码审查专家。审查工作流JSON定义，找出潜在问题和改进空间。

审查维度:
1. 完整性检查
2. 最佳实践遵循
3. 潜在错误
4. 性能考虑
5. 可维护性""",
            
            TaskType.DOCUMENT_WORKFLOW: """你是一个技术文档专家。为工作流生成清晰、详细的文档。

文档应包含:
1. 概述
2. 步骤详解
3. 参数说明
4. 注意事项
5. 使用示例""",
            
            TaskType.SUGGEST_STEPS: """你是一个工作流规划专家。基于给定的部分工作流或任务描述，推荐下一步应该添加的步骤。

分析上下文，考虑:
1. 常见的下一步动作
2. 错误处理步骤
3. 验证步骤""",
            
            TaskType.EXPLAIN_ERROR: """你是一个错误解释专家。用简单易懂的语言解释自动化错误。

解释内容:
1. 发生了什么
2. 为什么发生
3. 如何修复""",
            
            TaskType.OPTIMIZE_WORKFLOW: """你是一个性能优化专家。分析工作流并提供优化建议。

优化方向:
1. 减少步骤
2. 并行化机会
3. 减少等待时间
4. 改进图像匹配
5. 错误处理改进""",
            
            TaskType.SUGGEST_TEMPLATE: """你是一个模板库专家。根据用户描述的自动化任务，推荐最合适的工作流模板。

考虑因素:
1. 任务类型 (桌面自动化、Web自动化等)
2. 复杂度
3. 常见模式""",
            
            TaskType.RECOMMEND_ACTION: """你是一个动作推荐专家。为特定任务推荐最佳动作序列。

考虑:
1. 任务目标
2. 环境上下文
3. 效率
4. 可靠性""",
            
            TaskType.LEARN_FROM_FAILURE: """你是一个学习系统。从过去的失败中提取模式和建议，防止未来再次发生。"""
        }
        
        return base_prompt + "\n\n" + task_prompts.get(task_type, "")
    
    # ============== Public API ==============
    
    def create_workflow_from_natural_language(self, description: str) -> Dict[str, Any]:
        """
        1. 自然语言工作流创建
        
        Args:
            description: 用户用自然语言描述的工作流任务
            
        Returns:
            工作流JSON定义
        """
        prompt = f"""请将以下任务描述转换为工作流JSON定义:

任务: {description}

请仅返回JSON，不要包含其他解释。"""
        
        response = self._generate(prompt, TaskType.CREATE_WORKFLOW)
        
        try:
            json_match = re.search(r'\{[\s\S]*\}', response)
            if json_match:
                workflow = json.loads(json_match.group())
                return workflow
        except json.JSONDecodeError:
            pass
        
        return {"raw_response": response, "parse_error": True}
    
    def debug_workflow(self, workflow: Dict[str, Any], error: Dict[str, Any]) -> DebugResult:
        """
        2. 工作流调试
        
        Args:
            workflow: 工作流定义
            error: 错误信息
            
        Returns:
            DebugResult包含诊断和修复建议
        """
        failure_context = self._get_relevant_failures(error.get("type", ""))
        
        prompt = f"""分析以下工作流的执行失败:

工作流:
{json.dumps(workflow, indent=2, ensure_ascii=False)}

错误信息:
{json.dumps(error, indent=2, ensure_ascii=False)}

{failure_context}

请提供:
1. 错误类型
2. 根本原因
3. 建议的修复方案 (至少3个)
4. 置信度 (0-1)
5. 如果适用，提供修复计划"""
        
        response = self._generate(prompt, TaskType.DEBUG_WORKFLOW)
        
        return self._parse_debug_result(response, error)
    
    def _get_relevant_failures(self, error_type: str) -> str:
        """获取相关的历史失败记录"""
        relevant = [f for f in self._failure_history if f.error_type == error_type]
        if not relevant:
            return ""
        
        samples = relevant[-5:]
        context = "\n\n相关历史失败 (供参考):\n"
        for f in samples:
            context += f"- [{f.timestamp}] {f.error_message}"
            if f.solution:
                context += f" -> 解决方案: {f.solution}"
            context += "\n"
        return context
    
    def _parse_debug_result(self, response: str, error: Dict[str, Any]) -> DebugResult:
        """解析调试结果"""
        suggested_fixes = []
        root_cause = ""
        confidence = 0.7
        
        lines = response.split('\n')
        for line in lines:
            line = line.strip()
            if '根本原因' in line or 'root cause' in line.lower():
                root_cause = line.split(':', 1)[-1].strip()
            elif any(x in line.lower() for x in ['建议', 'fix', '修复', '方案']):
                if ':' in line:
                    suggested_fixes.append(line.split(':', 1)[-1].strip())
            elif '置信度' in line or 'confidence' in line.lower():
                try:
                    num = re.search(r'0\.\d+|1\.0', line)
                    if num:
                        confidence = float(num.group())
                except:
                    pass
        
        return DebugResult(
            error_type=error.get("type", "unknown"),
            error_message=error.get("message", ""),
            root_cause=root_cause or "分析中",
            suggested_fixes=suggested_fixes or ["检查目标元素", "增加等待时间", "验证应用状态"],
            confidence=confidence
        )
    
    def review_workflow(self, workflow: Dict[str, Any]) -> ReviewResult:
        """
        3. 代码审查
        
        Args:
            workflow: 工作流定义
            
        Returns:
            ReviewResult包含问题、建议和评分
        """
        prompt = f"""请审查以下工作流:

{json.dumps(workflow, indent=2, ensure_ascii=False)}

审查维度:
1. 结构完整性
2. 最佳实践
3. 潜在错误
4. 性能
5. 可维护性

请给出:
- 问题列表
- 改进建议
- 评分 (0-100)
- 总体反馈"""
        
        response = self._generate(prompt, TaskType.REVIEW_WORKFLOW)
        return self._parse_review_result(response)
    
    def _parse_review_result(self, response: str) -> ReviewResult:
        """解析审查结果"""
        issues = []
        suggestions = []
        score = 80.0
        feedback = response
        
        lines = response.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('- 问题') or 'issue' in line.lower():
                current_list = issues
            elif line.startswith('- 建议') or 'suggestion' in line.lower():
                current_list = suggestions
            elif ':' in line and len(line) < 200:
                if any(x in line.lower() for x in ['问题', 'issue', '问题:']):
                    issues.append({"description": line.split(':', 1)[-1].strip()})
                elif any(x in line.lower() for x in ['建议', 'suggest']):
                    suggestions.append(line.split(':', 1)[-1].strip())
            elif '评分' in line or 'score' in line.lower():
                try:
                    num = re.search(r'\d+', line)
                    if num:
                        score = float(num.group())
                except:
                    pass
        
        return ReviewResult(
            issues=issues,
            suggestions=suggestions,
            score=score,
            overall_feedback=feedback
        )
    
    def generate_documentation(self, workflow: Dict[str, Any]) -> str:
        """
        4. 自动文档生成
        
        Args:
            workflow: 工作流定义
            
        Returns:
            人类可读的工作流文档
        """
        prompt = f"""为以下工作流生成详细文档:

{json.dumps(workflow, indent=2, ensure_ascii=False)}

文档格式:
# 工作流名称

## 概述
[简短描述]

## 步骤详解
### 步骤1: [名称]
- 动作: [类型]
- 目标: [目标]
- 参数: [参数说明]

## 使用说明
[如何使用]

## 注意事项
[重要提醒]"""
        
        return self._generate(prompt, TaskType.DOCUMENT_WORKFLOW)
    
    def suggest_next_steps(self, workflow: Dict[str, Any], current_step: int) -> List[Dict[str, Any]]:
        """
        5. 步骤建议
        
        Args:
            workflow: 当前工作流定义
            current_step: 当前步骤索引
            
        Returns:
            建议的下一步骤列表
        """
        prompt = f"""基于以下工作流，假设刚完成步骤{current_step}，推荐接下来的步骤:

工作流:
{json.dumps(workflow, indent=2, ensure_ascii=False)}

当前完成步骤: {current_step}

请推荐3-5个合理的下一步骤，格式:
[
    {{"action": "动作", "target": "目标", "reasoning": "为什么推荐"}},
    ...
]"""
        
        response = self._generate(prompt, TaskType.SUGGEST_STEPS)
        
        try:
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                return json.loads(json_match.group())
        except (json.JSONDecodeError, TypeError):
            pass
        
        return []
    
    def explain_error(self, error: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
        """
        6. 错误解释
        
        Args:
            error: 错误信息
            context: 额外上下文
            
        Returns:
            简单易懂的错误解释
        """
        ctx_str = f"\n\n上下文:\n{json.dumps(context, indent=2, ensure_ascii=False)}" if context else ""
        
        prompt = f"""用简单易懂的语言解释以下错误:

{json.dumps(error, indent=2, ensure_ascii=False)}{ctx_str}

解释格式:
## 发生了什么
[简单描述]

## 为什么发生
[原因分析]

## 如何解决
[具体步骤]"""
        
        return self._generate(prompt, TaskType.EXPLAIN_ERROR)
    
    def suggest_optimizations(self, workflow: Dict[str, Any]) -> List[str]:
        """
        7. 优化建议
        
        Args:
            workflow: 工作流定义
            
        Returns:
            优化建议列表
        """
        failure_context = ""
        if self._failure_history:
            recent = self._failure_history[-10:]
            failure_context = "\n历史失败记录:\n" + "\n".join(
                f"- {f.error_type}: {f.error_message}" for f in recent
            )
        
        prompt = f"""分析以下工作流的优化机会:

{json.dumps(workflow, indent=2, ensure_ascii=False)}{failure_context}

请提供具体、可行的优化建议，格式:
[
    {{"area": "优化领域", "suggestion": "建议内容", "expected_improvement": "预期改进"}},
    ...
]"""
        
        response = self._generate(prompt, TaskType.OPTIMIZE_WORKFLOW)
        
        try:
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                return json.loads(json_match.group())
        except (json.JSONDecodeError, TypeError):
            pass
        
        return []
    
    def suggest_template(self, task_description: str) -> List[Dict[str, Any]]:
        """
        8. 模板建议
        
        Args:
            task_description: 用户描述的任务
            
        Returns:
            推荐的模板列表
        """
        prompt = f"""根据以下任务描述，推荐最合适的工作流模板:

任务: {task_description}

可用模板:
{json.dumps(self._workflow_templates, indent=2, ensure_ascii=False) if self._workflow_templates else "无可用模板，请基于任务创建新的工作流"}

请推荐最合适的模板并解释原因。"""
        
        response = self._generate(prompt, TaskType.SUGGEST_TEMPLATE)
        
        try:
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                return json.loads(json_match.group())
        except (json.JSONDecodeError, TypeError):
            pass
        
        return []
    
    def recommend_action(self, task: str, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        9. 动作推荐
        
        Args:
            task: 用户想要完成的任务
            context: 当前上下文
            
        Returns:
            推荐的行动序列
        """
        ctx_str = f"\n当前上下文:\n{json.dumps(context, indent=2, ensure_ascii=False)}" if context else ""
        
        prompt = f"""为以下任务推荐最佳动作序列:

任务: {task}{ctx_str}

请提供最优的动作序列，考虑效率和可靠性。"""
        
        response = self._generate(prompt, TaskType.RECOMMEND_ACTION)
        
        try:
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                return json.loads(json_match.group())
        except (json.JSONDecodeError, TypeError):
            pass
        
        return []
    
    def learn_from_failure(self, workflow_name: str, error: Dict[str, Any], 
                          attempted_fix: Optional[str] = None, 
                          success: bool = False) -> Dict[str, Any]:
        """
        10. 从失败中学习
        
        Args:
            workflow_name: 工作流名称
            error: 错误信息
            attempted_fix: 尝试的修复方案
            success: 修复是否成功
            
        Returns:
            学习总结
        """
        record = FailureRecord(
            timestamp=time.time(),
            workflow_name=workflow_name,
            error_type=error.get("type", "unknown"),
            error_message=error.get("message", ""),
            context=error.get("context", {}),
            solution=attempted_fix if success else None
        )
        
        self._failure_history.append(record)
        self._save_failure_history()
        
        prompt = f"""基于以下失败案例，总结经验教训:

工作流: {workflow_name}
错误: {error.get('type', 'unknown')}
错误消息: {error.get('message', '')}
修复尝试: {attempted_fix or '无'}
成功: {success}

请总结:
1. 失败的根本原因
2. 如果成功，关键是做了什么
3. 预防类似失败的建议"""
        
        response = self._generate(prompt, TaskType.LEARN_FROM_FAILURE)
        
        return {
            "summary": response,
            "failure_recorded": True,
            "total_failures_learned": len(self._failure_history)
        }
    
    def add_template(self, name: str, workflow: Dict[str, Any], 
                   description: str = "", tags: Optional[List[str]] = None):
        """
        添加新模板到模板库
        
        Args:
            name: 模板名称
            workflow: 工作流定义
            description: 描述
            tags: 标签
        """
        self._workflow_templates[name] = {
            "workflow": workflow,
            "description": description,
            "tags": tags or [],
            "added_at": datetime.now().isoformat()
        }
        self._save_templates()
    
    def get_failure_patterns(self) -> Dict[str, Any]:
        """
        获取失败模式分析
        
        Returns:
            失败模式统计
        """
        patterns = defaultdict(list)
        for failure in self._failure_history:
            patterns[failure.error_type].append({
                "workflow": failure.workflow_name,
                "message": failure.error_message,
                "timestamp": failure.timestamp
            })
        
        return {
            "total_failures": len(self._failure_history),
            "by_type": {k: len(v) for k, v in patterns.items()},
            "recent_failures": [
                {"type": f.error_type, "message": f.error_message, "timestamp": f.timestamp}
                for f in self._failure_history[-10:]
            ]
        }
    
    def set_backend(self, provider: AIProvider, config: Dict[str, Any]):
        """
        切换AI后端
        
        Args:
            provider: 提供商
            config: 配置
        """
        self.provider = provider
        self.config = config
        self.backend = self._create_backend(provider, config)


# ============== Factory Function ==============

def create_ai_assistant(provider: str = "openai", **kwargs) -> AIAssistant:
    """
    创建AI助手的便捷工厂函数
    
    Args:
        provider: 提供商名称 ("openai", "local", "custom")
        **kwargs: 传递给后端的配置
        
    Returns:
        AIAssistant实例
    """
    provider_map = {
        "openai": AIProvider.OPENAI,
        "local": AIProvider.LOCAL,
        "custom": AIProvider.CUSTOM
    }
    
    if provider.lower() not in provider_map:
        raise ValueError(f"Unknown provider: {provider}. Available: {list(provider_map.keys())}")
    
    return AIAssistant(provider=provider_map[provider.lower()], config=kwargs)
