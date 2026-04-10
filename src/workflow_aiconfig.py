"""
AI配置管理 v1.0
AI配置管理系统 - 模型注册、路由、提示词管理、备援、成本追踪

功能:
- 模型注册与管理
- 模型路由
- 提示词存储与版本管理
- 模型自动备援
- 成本追踪
- 延迟追踪
- 模型对比
- 提示词模板
- Few-shot示例管理
- 模型响应缓存
"""

import json
import time
import hashlib
import copy
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging

# ============== Enums ==============

class ModelProvider(Enum):
    """AI模型提供商"""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    LOCAL = "local"
    CUSTOM = "custom"


class ModelCapability(Enum):
    """模型能力"""
    TEXT_GENERATION = "text_generation"
    CODE_GENERATION = "code_generation"
    SUMMARIZATION = "summarization"
    ANALYSIS = "analysis"
    CREATIVE = "creative"
    REASONING = "reasoning"


# ============== Data Classes ==============

@dataclass
class ModelInfo:
    """模型信息"""
    model_id: str
    name: str
    provider: ModelProvider
    capabilities: List[ModelCapability]
    cost_per_token: float
    latency_ms_avg: float
    max_tokens: int
    version: str = "1.0"
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True


@dataclass
class PromptTemplate:
    """提示词模板"""
    template_id: str
    name: str
    description: str
    template: str
    variables: List[str]
    version: int = 1
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    usage_count: int = 0


@dataclass
class PromptVersion:
    """提示词版本"""
    version_id: str
    content: str
    version: int
    created_at: float
    created_by: str
    changelog: str = ""


@dataclass
class FewShotExample:
    """Few-shot示例"""
    example_id: str
    input: str
    output: str
    category: str
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CachedResponse:
    """缓存的模型响应"""
    cache_key: str
    model_id: str
    prompt_hash: str
    response: Any
    created_at: float
    ttl_seconds: int
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)


@dataclass
class ModelUsageRecord:
    """模型使用记录"""
    timestamp: float
    model_id: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    success: bool
    error: Optional[str] = None


@dataclass
class ModelComparisonResult:
    """模型对比结果"""
    prompt: str
    results: Dict[str, Dict[str, Any]]
    winner: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============== AIConfigManager ==============

class AIConfigManager:
    """
    AI配置管理器
    提供模型注册、路由、提示词管理、成本追踪等功能
    """

    def __init__(self, cache_dir: Optional[str] = None):
        """初始化AI配置管理器"""
        self.logger = logging.getLogger(__name__)
        
        # 模型注册表
        self._models: Dict[str, ModelInfo] = {}
        self._model_routes: Dict[str, List[str]] = defaultdict(list)
        
        # 提示词管理
        self._prompts: Dict[str, List[PromptVersion]] = {}
        self._prompt_templates: Dict[str, PromptTemplate] = {}
        
        # Few-shot示例
        self._few_shot_examples: Dict[str, List[FewShotExample]] = defaultdict(list)
        
        # 响应缓存
        self._response_cache: Dict[str, CachedResponse] = {}
        self._cache_ttl_default: int = 3600  # 1小时
        
        # 追踪数据
        self._usage_records: List[ModelUsageRecord] = []
        self._cost_tracker: Dict[str, float] = defaultdict(float)
        self._latency_tracker: Dict[str, List[float]] = defaultdict(list)
        
        # 缓存目录
        self._cache_dir = cache_dir
        
        # 注册默认模型
        self._register_default_models()
    
    # ============== 1. 模型注册 ==============
    
    def _register_default_models(self):
        """注册默认模型"""
        default_models = [
            ModelInfo(
                model_id="gpt-4",
                name="GPT-4",
                provider=ModelProvider.OPENAI,
                capabilities=[ModelCapability.TEXT_GENERATION, ModelCapability.CODE_GENERATION, 
                             ModelCapability.REASONING],
                cost_per_token=0.00003,
                latency_ms_avg=1000,
                max_tokens=8192
            ),
            ModelInfo(
                model_id="gpt-3.5-turbo",
                name="GPT-3.5 Turbo",
                provider=ModelProvider.OPENAI,
                capabilities=[ModelCapability.TEXT_GENERATION, ModelCapability.CODE_GENERATION],
                cost_per_token=0.000002,
                latency_ms_avg=500,
                max_tokens=4096
            ),
            ModelInfo(
                model_id="claude-3",
                name="Claude 3",
                provider=ModelProvider.ANTHROPIC,
                capabilities=[ModelCapability.TEXT_GENERATION, ModelCapability.ANALYSIS,
                             ModelCapability.REASONING],
                cost_per_token=0.000015,
                latency_ms_avg=1200,
                max_tokens=100000
            ),
        ]
        
        for model in default_models:
            self.register_model(model)
    
    def register_model(self, model: ModelInfo) -> bool:
        """
        注册新模型
        
        Args:
            model: 模型信息
            
        Returns:
            是否注册成功
        """
        if model.model_id in self._models:
            self.logger.warning(f"Model {model.model_id} already registered, updating")
        
        self._models[model.model_id] = model
        self.logger.info(f"Registered model: {model.model_id} ({model.name})")
        return True
    
    def unregister_model(self, model_id: str) -> bool:
        """取消注册模型"""
        if model_id in self._models:
            del self._models[model_id]
            self.logger.info(f"Unregistered model: {model_id}")
            return True
        return False
    
    def get_model(self, model_id: str) -> Optional[ModelInfo]:
        """获取模型信息"""
        return self._models.get(model_id)
    
    def list_models(self, provider: Optional[ModelProvider] = None,
                   capability: Optional[ModelCapability] = None) -> List[ModelInfo]:
        """列出模型"""
        models = list(self._models.values())
        
        if provider:
            models = [m for m in models if m.provider == provider]
        
        if capability:
            models = [m for m in models if capability in m.capabilities]
        
        return models
    
    def update_model(self, model_id: str, **kwargs) -> bool:
        """更新模型信息"""
        if model_id not in self._models:
            return False
        
        model = self._models[model_id]
        for key, value in kwargs.items():
            if hasattr(model, key):
                setattr(model, key, value)
        
        self.logger.info(f"Updated model {model_id}")
        return True
    
    # ============== 2. 模型路由 ==============
    
    def register_route(self, route_name: str, model_ids: List[str]) -> bool:
        """
        注册路由规则
        
        Args:
            route_name: 路由名称
            model_ids: 模型ID列表，按优先级排序
        """
        # 验证所有模型已注册
        for model_id in model_ids:
            if model_id not in self._models:
                self.logger.warning(f"Model {model_id} not registered, skipping")
                continue
            self._model_routes[route_name].append(model_id)
        
        self.logger.info(f"Registered route: {route_name} -> {model_ids}")
        return True
    
    def route_request(self, route_name: str, 
                      capability: Optional[ModelCapability] = None,
                      preferred_model: Optional[str] = None) -> Optional[str]:
        """
        路由请求到合适的模型
        
        Args:
            route_name: 路由名称
            capability: 所需能力
            preferred_model: 首选模型
            
        Returns:
            选中的模型ID
        """
        if route_name not in self._model_routes:
            # 如果没有指定路由，返回最合适的模型
            if preferred_model and preferred_model in self._models:
                return preferred_model
            return self._select_best_available_model(capability)
        
        model_ids = self._model_routes[route_name]
        
        # 如果有首选模型且在路由中，优先使用
        if preferred_model and preferred_model in model_ids:
            return preferred_model
        
        # 按顺序选择可用的模型
        for model_id in model_ids:
            model = self._models.get(model_id)
            if model and model.is_active:
                if capability is None or capability in model.capabilities:
                    return model_id
        
        return None
    
    def _select_best_available_model(self, 
                                     capability: Optional[ModelCapability] = None) -> Optional[str]:
        """选择最佳可用模型"""
        candidates = [m for m in self._models.values() if m.is_active]
        
        if capability:
            candidates = [m for m in candidates if capability in m.capabilities]
        
        if not candidates:
            return None
        
        # 选择平均延迟最低的模型
        return min(candidates, key=lambda m: m.latency_ms_avg).model_id
    
    # ============== 3. 提示词管理 ==============
    
    def save_prompt(self, prompt_id: str, content: str, 
                   created_by: str = "system", changelog: str = "") -> PromptVersion:
        """
        保存提示词版本
        
        Args:
            prompt_id: 提示词ID
            content: 提示词内容
            created_by: 创建者
            changelog: 变更日志
            
        Returns:
            版本信息
        """
        if prompt_id not in self._prompts:
            self._prompts[prompt_id] = []
        
        version_num = len(self._prompts[prompt_id]) + 1
        version_id = f"{prompt_id}_v{version_num}"
        
        version = PromptVersion(
            version_id=version_id,
            content=content,
            version=version_num,
            created_at=time.time(),
            created_by=created_by,
            changelog=changelog
        )
        
        self._prompts[prompt_id].append(version)
        self.logger.info(f"Saved prompt {version_id}")
        
        return version
    
    def get_prompt(self, prompt_id: str, version: Optional[int] = None) -> Optional[str]:
        """
        获取提示词
        
        Args:
            prompt_id: 提示词ID
            version: 指定版本，None获取最新
            
        Returns:
            提示词内容
        """
        if prompt_id not in self._prompts:
            return None
        
        versions = self._prompts[prompt_id]
        if not versions:
            return None
        
        if version is None:
            return versions[-1].content
        
        for v in versions:
            if v.version == version:
                return v.content
        
        return None
    
    def list_prompt_versions(self, prompt_id: str) -> List[PromptVersion]:
        """列出提示词所有版本"""
        return self._prompts.get(prompt_id, [])
    
    def delete_prompt(self, prompt_id: str, version: Optional[int] = None) -> bool:
        """
        删除提示词
        
        Args:
            prompt_id: 提示词ID
            version: 指定版本，None删除所有版本
        """
        if prompt_id not in self._prompts:
            return False
        
        if version is None:
            del self._prompts[prompt_id]
        else:
            self._prompts[prompt_id] = [
                v for v in self._prompts[prompt_id] if v.version != version
            ]
        
        return True
    
    # ============== 4. 模型备援 ==============
    
    def call_with_fallback(self, 
                          route_name: str,
                          prompt: str,
                          capability: Optional[ModelCapability] = None,
                          max_retries: int = 3,
                          **kwargs) -> Dict[str, Any]:
        """
        使用备援机制调用模型
        
        Args:
            route_name: 路由名称
            prompt: 提示词
            capability: 所需能力
            max_retries: 最大重试次数
            **kwargs: 传递给模型的额外参数
            
        Returns:
            包含response和model_id的字典
        """
        # 获取路由的模型列表
        model_ids = self._model_routes.get(route_name, [])
        if not model_ids:
            model_ids = [self._select_best_available_model(capability)]
        
        last_error = None
        
        for attempt in range(max_retries):
            for model_id in model_ids:
                try:
                    result = self._call_model(model_id, prompt, **kwargs)
                    
                    # 记录成功
                    self._record_usage(model_id, result.get('input_tokens', 0),
                                      result.get('output_tokens', 0),
                                      result.get('latency_ms', 0), True)
                    
                    result['model_id_used'] = model_id
                    result['attempt'] = attempt + 1
                    return result
                    
                except Exception as e:
                    last_error = str(e)
                    self.logger.warning(f"Model {model_id} failed: {e}")
                    continue
        
        return {
            'success': False,
            'error': f"All models failed. Last error: {last_error}",
            'model_id_used': None,
            'attempt': max_retries
        }
    
    def _call_model(self, model_id: str, prompt: str, **kwargs) -> Dict[str, Any]:
        """
        调用模型（模拟实现）
        
        实际应用中，这里会调用真实的AI API
        """
        model = self._models.get(model_id)
        if not model:
            raise ValueError(f"Model {model_id} not found")
        
        # 模拟API调用
        start_time = time.time()
        time.sleep(0.01)  # 模拟延迟
        
        # 检查缓存
        cache_key = self._generate_cache_key(model_id, prompt)
        cached = self.get_cached_response(cache_key)
        if cached:
            return {
                'response': cached.response,
                'cached': True,
                'latency_ms': 0
            }
        
        # 模拟响应
        input_tokens = len(prompt) // 4
        output_tokens = 100
        latency_ms = (time.time() - start_time) * 1000
        
        response = f"[{model.name}] Response for: {prompt[:50]}..."
        
        result = {
            'response': response,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'latency_ms': latency_ms,
            'cached': False
        }
        
        # 缓存响应
        self._cache_response(cache_key, model_id, prompt, response)
        
        return result
    
    # ============== 5. 成本追踪 ==============
    
    def _record_usage(self, model_id: str, input_tokens: int, 
                     output_tokens: int, latency_ms: float, success: bool,
                     error: Optional[str] = None):
        """记录模型使用情况"""
        model = self._models.get(model_id)
        if not model:
            return
        
        # 计算成本
        cost = (input_tokens + output_tokens) * model.cost_per_token
        
        # 更新追踪器
        self._cost_tracker[model_id] += cost
        
        # 记录使用
        record = ModelUsageRecord(
            timestamp=time.time(),
            model_id=model_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency_ms,
            success=success,
            error=error
        )
        self._usage_records.append(record)
    
    def get_cost_summary(self, model_id: Optional[str] = None,
                        start_time: Optional[float] = None,
                        end_time: Optional[float] = None) -> Dict[str, Any]:
        """
        获取成本摘要
        
        Args:
            model_id: 模型ID，None获取所有
            start_time: 开始时间戳
            end_time: 结束时间戳
        """
        records = self._usage_records
        
        if start_time:
            records = [r for r in records if r.timestamp >= start_time]
        if end_time:
            records = [r for r in records if r.timestamp <= end_time]
        
        if model_id:
            records = [r for r in records if r.model_id == model_id]
        
        total_cost = sum(
            (r.input_tokens + r.output_tokens) * self._models[r.model_id].cost_per_token
            for r in records if r.model_id in self._models
        )
        
        total_input_tokens = sum(r.input_tokens for r in records)
        total_output_tokens = sum(r.output_tokens for r in records)
        success_count = sum(1 for r in records if r.success)
        
        return {
            'total_cost': total_cost,
            'total_input_tokens': total_input_tokens,
            'total_output_tokens': total_output_tokens,
            'total_requests': len(records),
            'success_count': success_count,
            'success_rate': success_count / len(records) if records else 0,
            'records': records
        }
    
    def get_cost_by_model(self) -> Dict[str, float]:
        """获取各模型成本"""
        return dict(self._cost_tracker)
    
    # ============== 6. 延迟追踪 ==============
    
    def record_latency(self, model_id: str, latency_ms: float):
        """记录延迟"""
        self._latency_tracker[model_id].append(latency_ms)
        
        # 保持最近1000条记录
        if len(self._latency_tracker[model_id]) > 1000:
            self._latency_tracker[model_id] = self._latency_tracker[model_id][-1000:]
    
    def get_latency_stats(self, model_id: str) -> Dict[str, float]:
        """
        获取延迟统计
        
        Returns:
            包含avg, min, max, p50, p95, p99的字典
        """
        if model_id not in self._latency_tracker:
            return {}
        
        latencies = sorted(self._latency_tracker[model_id])
        if not latencies:
            return {}
        
        n = len(latencies)
        return {
            'avg': sum(latencies) / n,
            'min': latencies[0],
            'max': latencies[-1],
            'p50': latencies[int(n * 0.5)],
            'p95': latencies[int(n * 0.95)],
            'p99': latencies[int(n * 0.99)],
            'count': n
        }
    
    # ============== 7. 模型对比 ==============
    
    def compare_models(self, model_ids: List[str], 
                      prompts: List[str],
                      **kwargs) -> ModelComparisonResult:
        """
        对比多个模型的输出
        
        Args:
            model_ids: 要对比的模型ID列表
            prompts: 测试提示词列表
            **kwargs: 额外参数
            
        Returns:
            对比结果
        """
        results = {}
        
        for model_id in model_ids:
            if model_id not in self._models:
                self.logger.warning(f"Model {model_id} not found, skipping")
                continue
            
            model_results = []
            for prompt in prompts:
                try:
                    result = self._call_model(model_id, prompt, **kwargs)
                    model_results.append({
                        'prompt': prompt,
                        'response': result.get('response'),
                        'latency_ms': result.get('latency_ms'),
                        'tokens': result.get('output_tokens', 0)
                    })
                except Exception as e:
                    model_results.append({
                        'prompt': prompt,
                        'error': str(e)
                    })
            
            results[model_id] = {
                'model_name': self._models[model_id].name,
                'outputs': model_results,
                'avg_latency': sum(r.get('latency_ms', 0) for r in model_results) / len(model_results) if model_results else 0
            }
        
        # 简单的评判逻辑：选择平均延迟最低的
        winner = min(results.keys(), 
                    key=lambda m: results[m]['avg_latency']) if results else None
        
        return ModelComparisonResult(
            prompt=prompts[0] if prompts else "",
            results=results,
            winner=winner
        )
    
    # ============== 8. 提示词模板 ==============
    
    def create_prompt_template(self, template_id: str, name: str,
                              template: str, variables: List[str],
                              description: str = "") -> PromptTemplate:
        """
        创建提示词模板
        
        Args:
            template_id: 模板ID
            name: 模板名称
            template: 模板内容，使用{variable}作为占位符
            variables: 变量列表
            description: 描述
        """
        tmpl = PromptTemplate(
            template_id=template_id,
            name=name,
            description=description,
            template=template,
            variables=variables
        )
        
        self._prompt_templates[template_id] = tmpl
        self.logger.info(f"Created prompt template: {template_id}")
        
        return tmpl
    
    def render_template(self, template_id: str, 
                        context: Dict[str, Any]) -> Optional[str]:
        """
        渲染提示词模板
        
        Args:
            template_id: 模板ID
            context: 变量上下文
        """
        if template_id not in self._prompt_templates:
            return None
        
        tmpl = self._prompt_templates[template_id]
        tmpl.usage_count += 1
        
        rendered = tmpl.template
        for var in tmpl.variables:
            placeholder = f"{{{var}}}"
            value = context.get(var, f"{{{var}}}")
            rendered = rendered.replace(placeholder, str(value))
        
        return rendered
    
    def get_template(self, template_id: str) -> Optional[PromptTemplate]:
        """获取模板"""
        return self._prompt_templates.get(template_id)
    
    def list_templates(self) -> List[PromptTemplate]:
        """列出所有模板"""
        return list(self._prompt_templates.values())
    
    def delete_template(self, template_id: str) -> bool:
        """删除模板"""
        if template_id in self._prompt_templates:
            del self._prompt_templates[template_id]
            return True
        return False
    
    # ============== 9. Few-shot示例管理 ==============
    
    def add_few_shot_example(self, example: FewShotExample) -> bool:
        """
        添加few-shot示例
        
        Args:
            example: 示例对象
        """
        self._few_shot_examples[example.category].append(example)
        self.logger.info(f"Added few-shot example: {example.example_id}")
        return True
    
    def get_few_shot_examples(self, category: str,
                             tags: Optional[List[str]] = None,
                             limit: int = 10) -> List[FewShotExample]:
        """
        获取few-shot示例
        
        Args:
            category: 类别
            tags: 标签过滤
            limit: 返回数量限制
        """
        examples = self._few_shot_examples.get(category, [])
        
        if tags:
            examples = [e for e in examples if any(t in e.tags for t in tags)]
        
        return examples[:limit]
    
    def build_few_shot_prompt(self, category: str, 
                             main_prompt: str,
                             num_examples: int = 3,
                             tags: Optional[List[str]] = None) -> str:
        """
        构建few-shot提示词
        
        Args:
            category: 类别
            main_prompt: 主提示词
            num_examples: 示例数量
            tags: 标签过滤
        """
        examples = self.get_few_shot_examples(category, tags, num_examples)
        
        if not examples:
            return main_prompt
        
        parts = []
        for ex in examples:
            parts.append(f"Input: {ex.input}")
            parts.append(f"Output: {ex.output}\n")
        
        parts.append(f"Input: {main_prompt}")
        parts.append("Output:")
        
        return "\n".join(parts)
    
    def delete_few_shot_example(self, example_id: str, category: str) -> bool:
        """删除few-shot示例"""
        if category in self._few_shot_examples:
            original_len = len(self._few_shot_examples[category])
            self._few_shot_examples[category] = [
                e for e in self._few_shot_examples[category]
                if e.example_id != example_id
            ]
            return len(self._few_shot_examples[category]) < original_len
        return False
    
    # ============== 10. 模型响应缓存 ==============
    
    def _generate_cache_key(self, model_id: str, prompt: str) -> str:
        """生成缓存键"""
        content = f"{model_id}:{prompt}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _cache_response(self, cache_key: str, model_id: str,
                       prompt: str, response: Any,
                       ttl_seconds: Optional[int] = None):
        """缓存响应"""
        ttl = ttl_seconds or self._cache_ttl_default
        
        cached = CachedResponse(
            cache_key=cache_key,
            model_id=model_id,
            prompt_hash=self._generate_cache_key(model_id, prompt),
            response=response,
            created_at=time.time(),
            ttl_seconds=ttl
        )
        
        self._response_cache[cache_key] = cached
    
    def get_cached_response(self, cache_key: str) -> Optional[CachedResponse]:
        """
        获取缓存的响应
        
        Args:
            cache_key: 缓存键
            
        Returns:
            缓存的响应，如果不存在或已过期返回None
        """
        if cache_key not in self._response_cache:
            return None
        
        cached = self._response_cache[cache_key]
        
        # 检查TTL
        if time.time() - cached.created_at > cached.ttl_seconds:
            del self._response_cache[cache_key]
            return None
        
        # 更新访问统计
        cached.access_count += 1
        cached.last_accessed = time.time()
        
        return cached
    
    def invalidate_cache(self, model_id: Optional[str] = None,
                        older_than: Optional[float] = None):
        """
        使缓存失效
        
        Args:
            model_id: 模型ID，None清除所有
            older_than: 清除早于此时间戳的缓存
        """
        if model_id is None and older_than is None:
            self._response_cache.clear()
            return
        
        keys_to_delete = []
        for key, cached in self._response_cache.items():
            if model_id and cached.model_id != model_id:
                continue
            if older_than and cached.created_at > older_than:
                continue
            keys_to_delete.append(key)
        
        for key in keys_to_delete:
            del self._response_cache[key]
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        total_entries = len(self._response_cache)
        total_accesses = sum(c.access_count for c in self._response_cache.values())
        
        return {
            'total_entries': total_entries,
            'total_accesses': total_accesses,
            'hit_rate': total_accesses / total_entries if total_entries > 0 else 0
        }
    
    # ============== 持久化 ==============
    
    def save_config(self, filepath: str) -> bool:
        """
        保存配置到文件
        
        Args:
            filepath: 文件路径
        """
        try:
            config = {
                'models': {k: {
                    'model_id': v.model_id,
                    'name': v.name,
                    'provider': v.provider.value,
                    'capabilities': [c.value for c in v.capabilities],
                    'cost_per_token': v.cost_per_token,
                    'latency_ms_avg': v.latency_ms_avg,
                    'max_tokens': v.max_tokens,
                    'version': v.version,
                    'metadata': v.metadata,
                    'is_active': v.is_active
                } for k, v in self._models.items()},
                'routes': dict(self._model_routes),
                'templates': {k: {
                    'template_id': v.template_id,
                    'name': v.name,
                    'description': v.description,
                    'template': v.template,
                    'variables': v.variables,
                    'version': v.version,
                    'created_at': v.created_at,
                    'updated_at': v.updated_at,
                    'usage_count': v.usage_count
                } for k, v in self._prompt_templates.items()},
                'few_shot_examples': {k: [{
                    'example_id': e.example_id,
                    'input': e.input,
                    'output': e.output,
                    'category': e.category,
                    'tags': e.tags,
                    'metadata': e.metadata
                } for e in v] for k, v in self._few_shot_examples.items()}
            }
            
            with open(filepath, 'w') as f:
                json.dump(config, f, indent=2)
            
            self.logger.info(f"Saved config to {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save config: {e}")
            return False
    
    def load_config(self, filepath: str) -> bool:
        """
        从文件加载配置
        
        Args:
            filepath: 文件路径
        """
        try:
            with open(filepath, 'r') as f:
                config = json.load(f)
            
            # 加载模型
            for k, v in config.get('models', {}).items():
                model = ModelInfo(
                    model_id=v['model_id'],
                    name=v['name'],
                    provider=ModelProvider(v['provider']),
                    capabilities=[ModelCapability(c) for c in v['capabilities']],
                    cost_per_token=v['cost_per_token'],
                    latency_ms_avg=v['latency_ms_avg'],
                    max_tokens=v['max_tokens'],
                    version=v.get('version', '1.0'),
                    metadata=v.get('metadata', {}),
                    is_active=v.get('is_active', True)
                )
                self._models[model.model_id] = model
            
            # 加载路由
            self._model_routes = defaultdict(list, config.get('routes', {}))
            
            # 加载模板
            for k, v in config.get('templates', {}).items():
                tmpl = PromptTemplate(
                    template_id=v['template_id'],
                    name=v['name'],
                    description=v.get('description', ''),
                    template=v['template'],
                    variables=v['variables'],
                    version=v.get('version', 1),
                    created_at=v.get('created_at', time.time()),
                    updated_at=v.get('updated_at', time.time()),
                    usage_count=v.get('usage_count', 0)
                )
                self._prompt_templates[tmpl.template_id] = tmpl
            
            # 加载few-shot示例
            for k, examples in config.get('few_shot_examples', {}).items():
                self._few_shot_examples[k] = [
                    FewShotExample(
                        example_id=e['example_id'],
                        input=e['input'],
                        output=e['output'],
                        category=e['category'],
                        tags=e.get('tags', []),
                        metadata=e.get('metadata', {})
                    ) for e in examples
                ]
            
            self.logger.info(f"Loaded config from {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            return False
    
    # ============== 工具方法 ==============
    
    def get_all_stats(self) -> Dict[str, Any]:
        """获取所有统计信息"""
        return {
            'models': {
                'total': len(self._models),
                'active': sum(1 for m in self._models.values() if m.is_active),
                'by_provider': self._get_model_stats_by_provider()
            },
            'cost': self.get_cost_by_model(),
            'cache': self.get_cache_stats(),
            'templates': len(self._prompt_templates),
            'few_shot_examples': sum(len(v) for v in self._few_shot_examples.values()),
            'usage_records': len(self._usage_records)
        }
    
    def _get_model_stats_by_provider(self) -> Dict[str, int]:
        """按提供商统计模型"""
        stats = defaultdict(int)
        for model in self._models.values():
            stats[model.provider.value] += 1
        return dict(stats)
