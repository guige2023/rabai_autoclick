"""Intent detection and classification action module for RabAI AutoClick.

Provides intent recognition capabilities:
- IntentClassifierAction: Classify user intent from text commands
- IntentRouterAction: Route actions based on detected intent
- IntentExtractorAction: Extract entities and parameters from intent
- PatternIntentMatcher: Match intents against registered patterns
"""

from typing import Any, Dict, List, Optional, Tuple
import re
import logging

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class IntentClassifierAction(BaseAction):
    """Classify user intent from text input using pattern matching."""
    
    action_type = "intent_classifier"
    display_name = "意图分类"
    description = "从文本中识别用户意图"
    
    # Predefined intent patterns
    DEFAULT_INTENTS = {
        "click": [r"点击", r"click", r"按下", r"tap", r"select"],
        "type": [r"输入", r"type", r"typing", r"填写", r"enter text"],
        "scroll": [r"滚动", r"scroll", r"swipe", r"滑"],
        "open": [r"打开", r"open", r"启动", r"launch", r"start"],
        "close": [r"关闭", r"close", r"quit", r"exit"],
        "wait": [r"等待", r"wait", r"延时", r"delay", r"暂停"],
        "screenshot": [r"截图", r"screenshot", r"屏幕截图", r"capture"],
        "search": [r"搜索", r"search", r"查找", r"find"],
        "submit": [r"提交", r"submit", r"确认", r"confirm", r"ok"],
        "cancel": [r"取消", r"cancel", r"abort", r"back"],
        "navigate": [r"导航", r"navigate", r"go to", r"跳转"],
        "repeat": [r"重复", r"repeat", r"again", r"再来"],
        "stop": [r"停止", r"stop", r"halt", r"end"],
    }
    
    def __init__(self) -> None:
        super().__init__()
        self._intent_patterns: Dict[str, List[re.Pattern]] = {}
        self._custom_intents: Dict[str, Dict[str, Any]] = {}
        self._initialize_patterns()
    
    def _initialize_patterns(self) -> None:
        """Compile all default intent patterns."""
        for intent_name, patterns in self.DEFAULT_INTENTS.items():
            self._intent_patterns[intent_name] = [
                re.compile(p, re.IGNORECASE) for p in patterns
            ]
    
    def register_intent(
        self,
        intent_name: str,
        patterns: List[str],
        params_schema: Optional[Dict[str, Any]] = None,
        examples: Optional[List[str]] = None
    ) -> None:
        """Register a custom intent with patterns and schema.
        
        Args:
            intent_name: Unique name for the intent.
            patterns: List of regex patterns to match.
            params_schema: JSON schema for intent parameters.
            examples: Example utterances for this intent.
        """
        self._intent_patterns[intent_name] = [
            re.compile(p, re.IGNORECASE) for p in patterns
        ]
        self._custom_intents[intent_name] = {
            "patterns": patterns,
            "params_schema": params_schema or {},
            "examples": examples or []
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Classify intent from input text.
        
        Args:
            params: {
                "text": Input text to classify (str, required),
                "threshold": Minimum confidence threshold (float, default 0.5),
                "top_k": Return top k matches (int, default 3),
                "include_confidence": Include confidence scores (bool, default True)
            }
        """
        try:
            text = params.get("text", "")
            threshold = params.get("threshold", 0.5)
            top_k = params.get("top_k", 3)
            include_confidence = params.get("include_confidence", True)
            
            if not text:
                return ActionResult(success=False, message="text parameter is required")
            
            results = self._classify(text, threshold, top_k, include_confidence)
            
            if not results:
                return ActionResult(
                    success=True,
                    message="No intent matched",
                    data={"intent": None, "confidence": 0.0, "alternatives": []}
                )
            
            top_intent = results[0]
            return ActionResult(
                success=True,
                message=f"Classified intent: {top_intent['intent']}",
                data={
                    "intent": top_intent["intent"],
                    "confidence": top_intent["confidence"],
                    "alternatives": results[1:] if include_confidence else []
                }
            )
        
        except Exception as e:
            logger.error(f"Intent classification failed: {e}")
            return ActionResult(success=False, message=f"Classification error: {str(e)}")
    
    def _classify(
        self,
        text: str,
        threshold: float,
        top_k: int,
        include_confidence: bool
    ) -> List[Dict[str, Any]]:
        """Perform intent classification.
        
        Args:
            text: Input text.
            threshold: Minimum confidence.
            top_k: Number of results.
            include_confidence: Whether to include scores.
        """
        matches: List[Tuple[str, float]] = []
        
        for intent_name, patterns in self._intent_patterns.items():
            for pattern in patterns:
                if pattern.search(text):
                    # Calculate confidence based on pattern match length
                    match = pattern.search(text)
                    if match:
                        match_ratio = len(match.group()) / len(text)
                        confidence = min(1.0, match_ratio + 0.3)
                        matches.append((intent_name, confidence))
                    break
        
        # Sort by confidence descending
        matches.sort(key=lambda x: x[1], reverse=True)
        
        results = []
        for intent, conf in matches[:top_k]:
            if include_confidence or conf >= threshold:
                results.append({"intent": intent, "confidence": round(conf, 3)})
        
        return results
    
    def get_supported_intents(self) -> List[str]:
        """Get list of all registered intent names."""
        return list(self._intent_patterns.keys())


class IntentRouterAction(BaseAction):
    """Route to appropriate action handler based on classified intent."""
    
    action_type = "intent_router"
    display_name = "意图路由"
    description = "根据意图路由到对应处理器"
    
    def __init__(self) -> None:
        super().__init__()
        self._handlers: Dict[str, Dict[str, Any]] = {}
        self._fallback_handler: Optional[Dict[str, Any]] = None
        self._classifier = IntentClassifierAction()
    
    def register_handler(
        self,
        intent: str,
        handler_fn: Any,
        param_mapping: Optional[Dict[str, str]] = None,
        priority: int = 0
    ) -> None:
        """Register a handler for a specific intent.
        
        Args:
            intent: Intent name to handle.
            handler_fn: Function to call for this intent.
            param_mapping: Map intent params to handler params.
            priority: Handler priority (higher = tried first).
        """
        self._handlers[intent] = {
            "handler": handler_fn,
            "param_mapping": param_mapping or {},
            "priority": priority
        }
    
    def set_fallback(self, handler_fn: Any, param_mapping: Optional[Dict[str, str]] = None) -> None:
        """Set fallback handler for unmatched intents."""
        self._fallback_handler = {"handler": handler_fn, "param_mapping": param_mapping or {}}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Route intent to appropriate handler.
        
        Args:
            params: {
                "text": Input text (str, required),
                "context": Additional context data (dict),
                "handlers": Override handlers (dict)
            }
        """
        try:
            text = params.get("text", "")
            context_data = params.get("context", {})
            handlers = params.get("handlers", {})
            
            if not text:
                return ActionResult(success=False, message="text parameter is required")
            
            # Classify intent
            classifier_params = {
                "text": text,
                "threshold": params.get("threshold", 0.3),
                "top_k": 1
            }
            classify_result = self._classifier.execute(context, classifier_params)
            
            if not classify_result.success or not classify_result.data.get("intent"):
                return self._execute_fallback(context, params, "No intent detected")
            
            intent = classify_result.data["intent"]
            confidence = classify_result.data.get("confidence", 0.0)
            
            # Find handler
            handler_info = handlers.get(intent) or self._handlers.get(intent)
            
            if not handler_info:
                return self._execute_fallback(context, params, f"No handler for intent: {intent}")
            
            # Map parameters
            handler_fn = handler_info.get("handler")
            param_mapping = handler_info.get("param_mapping", {})
            
            mapped_params = self._map_params(context_data, param_mapping)
            
            # Execute handler
            try:
                if callable(handler_fn):
                    result = handler_fn(context, mapped_params)
                    if isinstance(result, ActionResult):
                        return result
                    return ActionResult(success=True, message="Handler executed", data=result)
                else:
                    return ActionResult(success=False, message="Handler not callable")
            
            except Exception as e:
                logger.error(f"Handler execution failed: {e}")
                return ActionResult(success=False, message=f"Handler error: {str(e)}")
        
        except Exception as e:
            return ActionResult(success=False, message=f"Router error: {str(e)}")
    
    def _map_params(
        self,
        source: Dict[str, Any],
        mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """Map source params to handler params."""
        mapped = {}
        for target_key, source_key in mapping.items():
            if source_key in source:
                mapped[target_key] = source[source_key]
        return mapped
    
    def _execute_fallback(
        self,
        context: Any,
        params: Dict[str, Any],
        reason: str
    ) -> ActionResult:
        """Execute fallback handler."""
        if self._fallback_handler:
            try:
                handler_fn = self._fallback_handler["handler"]
                mapped_params = self._map_params(
                    params.get("context", {}),
                    self._fallback_handler.get("param_mapping", {})
                )
                if callable(handler_fn):
                    return handler_fn(context, mapped_params)
            except Exception as e:
                return ActionResult(success=False, message=f"Fallback error: {str(e)}")
        
        return ActionResult(success=False, message=reason)


class IntentExtractorAction(BaseAction):
    """Extract entities and parameters from classified intent."""
    
    action_type = "intent_extractor"
    display_name = "意图实体提取"
    description = "从意图中提取实体和参数"
    
    ENTITY_PATTERNS = {
        "number": [r"\d+(?:\.\d+)?", r"one|two|three|four|five|six|seven|eight|nine|ten"],
        "coordinate": [r"\(\s*\d+\s*,\s*\d+\s*\)", r"x\s*=\s*\d+\s*,\s*y\s*=\s*\d+"],
        "color": [r"#?[0-9A-Fa-f]{6}", r"red|green|blue|white|black|yellow"],
        "url": [r"https?://[^\s]+", r"www\.[^\s]+"],
        "file_path": [r"/[^\s]+\.[a-zA-Z]+", r"[A-Z]:\\[^\s]+", r"\./[^\s]+"],
        "time": [r"\d+\s*(?:秒|分钟|小时|min|sec|hour)s?", r"\d+:\d+"],
    }
    
    def __init__(self) -> None:
        super().__init__()
        self._compiled_patterns: Dict[str, List[re.Pattern]] = {}
        self._initialize_patterns()
    
    def _initialize_patterns(self) -> None:
        """Compile all entity patterns."""
        for entity_type, patterns in self.ENTITY_PATTERNS.items():
            self._compiled_patterns[entity_type] = [
                re.compile(p, re.IGNORECASE) for p in patterns
            ]
    
    def register_entity_pattern(
        self,
        entity_type: str,
        patterns: List[str]
    ) -> None:
        """Register custom entity patterns."""
        self._compiled_patterns[entity_type] = [
            re.compile(p, re.IGNORECASE) for p in patterns
        ]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Extract entities from text.
        
        Args:
            params: {
                "text": Input text (str, required),
                "entity_types": Types to extract (list, default all),
                "include_context": Include surrounding text (bool, default False)
            }
        """
        try:
            text = params.get("text", "")
            entity_types = params.get("entity_types", list(self._compiled_patterns.keys()))
            include_context = params.get("include_context", False)
            
            if not text:
                return ActionResult(success=False, message="text parameter is required")
            
            extracted: Dict[str, List[Dict[str, Any]]] = {}
            
            for entity_type in entity_types:
                if entity_type not in self._compiled_patterns:
                    continue
                
                entities = []
                patterns = self._compiled_patterns[entity_type]
                
                for pattern in patterns:
                    for match in pattern.finditer(text):
                        entity_data = {
                            "value": match.group(),
                            "start": match.start(),
                            "end": match.end()
                        }
                        if include_context:
                            entity_data["context"] = text[
                                max(0, match.start() - 20):min(len(text), match.end() + 20)
                            ]
                        entities.append(entity_data)
                
                if entities:
                    extracted[entity_type] = entities
            
            return ActionResult(
                success=True,
                message=f"Extracted {sum(len(v) for v in extracted.values())} entities",
                data={
                    "entities": extracted,
                    "total_count": sum(len(v) for v in extracted.values())
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Extraction error: {str(e)}")


class PatternIntentMatcher(BaseAction):
    """Match intents against registered pattern library."""
    
    action_type = "pattern_intent_matcher"
    display_name = "模式意图匹配"
    description = "使用模式库匹配意图"
    
    def __init__(self) -> None:
        super().__init__()
        self._pattern_library: Dict[str, Dict[str, Any]] = {}
        self._match_history: List[Dict[str, Any]] = []
    
    def register_pattern(
        self,
        pattern_id: str,
        intent: str,
        patterns: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register a pattern with associated intent."""
        self._pattern_library[pattern_id] = {
            "intent": intent,
            "patterns": [re.compile(p, re.IGNORECASE) for p in patterns],
            "raw_patterns": patterns,
            "metadata": metadata or {},
            "match_count": 0
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Match text against registered patterns.
        
        Args:
            params: {
                "text": Input text (str, required),
                "pattern_ids": Limit to specific patterns (list, optional),
                "best_match": Only return best match (bool, default True)
            }
        """
        try:
            text = params.get("text", "")
            pattern_ids = params.get("pattern_ids")
            best_match_only = params.get("best_match", True)
            
            if not text:
                return ActionResult(success=False, message="text parameter is required")
            
            candidates = (
                {k: v for k, v in self._pattern_library.items() if k in pattern_ids}
                if pattern_ids
                else self._pattern_library
            )
            
            matches: List[Dict[str, Any]] = []
            
            for pattern_id, pattern_data in candidates.items():
                for compiled in pattern_data["patterns"]:
                    match = compiled.search(text)
                    if match:
                        match_data = {
                            "pattern_id": pattern_id,
                            "intent": pattern_data["intent"],
                            "matched_text": match.group(),
                            "match_start": match.start(),
                            "match_end": match.end(),
                            "confidence": min(1.0, len(match.group()) / len(text) + 0.2),
                            "metadata": pattern_data["metadata"]
                        }
                        matches.append(match_data)
                        pattern_data["match_count"] += 1
                        
                        self._match_history.append(match_data.copy())
                        break
            
            if not matches:
                return ActionResult(
                    success=True,
                    message="No pattern matched",
                    data={"matches": [], "best_match": None}
                )
            
            matches.sort(key=lambda x: x["confidence"], reverse=True)
            
            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={
                    "matches": matches if not best_match_only else matches[:1],
                    "best_match": matches[0] if matches else None,
                    "total_matches": len(matches)
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Pattern matching error: {str(e)}")
