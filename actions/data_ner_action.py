"""Data NER Action Module.

Provides Named Entity Recognition with custom entity types,
relationship extraction, and entity linking.
"""

import time
import re
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EntityCategory(Enum):
    """Entity categories."""
    PERSON = "person"
    ORGANIZATION = "organization"
    LOCATION = "location"
    PRODUCT = "product"
    EVENT = "event"
    DATE = "date"
    TIME = "time"
    MONEY = "money"
    PERCENT = "percent"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    CUSTOM = "custom"


@dataclass
class NEREntity:
    """Named entity representation."""
    text: str
    category: EntityCategory
    subtype: Optional[str]
    start_pos: int
    end_pos: int
    confidence: float
    metadata: Dict[str, Any]


@dataclass
class EntityRelation:
    """Entity relation representation."""
    subject: NEREntity
    relation_type: str
    object: NEREntity
    confidence: float


class DataNerAction(BaseAction):
    """Named Entity Recognition Action.

    Extracts named entities from text with relationship
    extraction and entity linking capabilities.
    """
    action_type = "data_ner"
    display_name = "命名实体识别"
    description = "命名实体识别与关系抽取"

    _entity_cache: Dict[str, List[NEREntity]] = {}
    _custom_patterns: Dict[str, re.Pattern] = {}
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute NER operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'recognize', 'recognize_batch', 'extract_relations',
                               'link_entities', 'add_pattern', 'get_statistics'
                - text: str - text to analyze
                - texts: list (optional) - batch texts
                - categories: list (optional) - entity categories to extract
                - custom_type: str (optional) - custom entity type name

        Returns:
            ActionResult with NER results.
        """
        start_time = time.time()
        operation = params.get('operation', 'recognize')

        try:
            with self._lock:
                if operation == 'recognize':
                    return self._recognize(params, start_time)
                elif operation == 'recognize_batch':
                    return self._recognize_batch(params, start_time)
                elif operation == 'extract_relations':
                    return self._extract_relations(params, start_time)
                elif operation == 'link_entities':
                    return self._link_entities(params, start_time)
                elif operation == 'add_pattern':
                    return self._add_pattern(params, start_time)
                elif operation == 'get_statistics':
                    return self._get_statistics(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"NER error: {str(e)}",
                duration=time.time() - start_time
            )

    def _recognize(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Recognize named entities in text."""
        text = params.get('text', '')
        categories = params.get('categories', None)

        if not text:
            return ActionResult(success=False, message="No text provided", duration=time.time() - start_time)

        entities = self._extract_entities(text, categories)

        cache_key = text[:100]
        self._entity_cache[cache_key] = entities

        by_category: Dict[str, List[str]] = {}
        for e in entities:
            cat = e.category.value
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(e.text)

        return ActionResult(
            success=True,
            message=f"Found {len(entities)} entities",
            data={
                'entities': [
                    {'text': e.text, 'category': e.category.value, 'subtype': e.subtype,
                     'start': e.start_pos, 'end': e.end_pos, 'confidence': e.confidence}
                    for e in entities
                ],
                'count': len(entities),
                'by_category': by_category,
            },
            duration=time.time() - start_time
        )

    def _recognize_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Recognize entities in multiple texts."""
        texts = params.get('texts', [])
        categories = params.get('categories', None)

        results = []
        total_entities = 0

        for i, text in enumerate(texts):
            entities = self._extract_entities(text, categories)
            total_entities += len(entities)
            results.append({
                'index': i,
                'entity_count': len(entities),
                'entities': [{'text': e.text, 'category': e.category.value, 'confidence': e.confidence} for e in entities]
            })

        return ActionResult(
            success=True,
            message=f"Batch NER: {len(texts)} texts, {total_entities} entities",
            data={'results': results, 'total_entities': total_entities, 'document_count': len(texts)},
            duration=time.time() - start_time
        )

    def _extract_entities(self, text: str, categories: Optional[List[str]]) -> List[NEREntity]:
        """Extract entities from text."""
        entities: List[NEREntity] = []
        categories_set = set(categories) if categories else None

        patterns = {
            EntityCategory.PERSON: (r'\b([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b', 'PERSON'),
            EntityCategory.ORGANIZATION: (r'\b([A-Z][a-z]*(?:\s+[A-Z][a-z]*)*\s+(?:Inc|Corp|Ltd|LLC|Company|Technologies|Group|Partners))\b', 'ORG'),
            EntityCategory.LOCATION: (r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:City|State|Country|County|Street|Avenue|Road))\b', 'LOC'),
            EntityCategory.EMAIL: (r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', None),
            EntityCategory.URL: (r'\b(https?://[^\s]+)\b', None),
            EntityCategory.PHONE: (r'\b(\+?[\d\s\-().]{10,})\b', None),
            EntityCategory.DATE: (r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\w+ \d{1,2},? \d{4})\b', None),
            EntityCategory.MONEY: (r'\b(\$[\d,]+(?:\.\d{2})?)\b', None),
        }

        for entity_cat, (pattern, subtype) in patterns.items():
            if categories_set and entity_cat.value not in categories_set:
                continue
            for match in re.finditer(pattern, text):
                if entity_cat == EntityCategory.PERSON and any(org in match.group() for org in ['Inc', 'Corp', 'Ltd', 'Company']):
                    continue
                entities.append(NEREntity(
                    text=match.group(),
                    category=entity_cat,
                    subtype=subtype,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    confidence=0.95 if entity_cat in {EntityCategory.EMAIL, EntityCategory.URL, EntityCategory.DATE, EntityCategory.MONEY} else 0.85,
                    metadata={}
                ))

        for custom_name, pattern in self._custom_patterns.items():
            if categories_set and 'custom' not in categories_set:
                continue
            for match in pattern.finditer(text):
                entities.append(NEREntity(
                    text=match.group(),
                    category=EntityCategory.CUSTOM,
                    subtype=custom_name,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    confidence=0.90,
                    metadata={'custom_type': custom_name}
                ))

        entities.sort(key=lambda e: e.start_pos)
        return entities

    def _extract_relations(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Extract relations between entities."""
        text = params.get('text', '')

        entities = self._extract_entities(text, None)
        relations: List[EntityRelation] = []

        person_entities = [e for e in entities if e.category == EntityCategory.PERSON]
        org_entities = [e for e in entities if e.category == EntityCategory.ORGANIZATION]
        location_entities = [e for e in entities if e.category == EntityCategory.LOCATION]

        for person in person_entities:
            for org in org_entities:
                if abs(person.start_pos - org.end_pos) < 50:
                    relations.append(EntityRelation(person, 'works_at', org, 0.80))

        for person in person_entities:
            for loc in location_entities:
                if abs(person.start_pos - loc.end_pos) < 50:
                    relations.append(EntityRelation(person, 'located_at', loc, 0.75))

        return ActionResult(
            success=True,
            message=f"Extracted {len(relations)} relations",
            data={
                'relations': [
                    {'subject': r.subject.text, 'relation': r.relation_type, 'object': r.object.text, 'confidence': r.confidence}
                    for r in relations
                ],
                'count': len(relations),
            },
            duration=time.time() - start_time
        )

    def _link_entities(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Link entities to external knowledge."""
        text = params.get('text', '')
        entities = self._extract_entities(text, None)

        linked = []
        for entity in entities:
            kb_id = f"KB:{hash(entity.text) % 100000:05d}"
            linked.append({
                'text': entity.text,
                'category': entity.category.value,
                'kb_id': kb_id,
                'description': f"Entity: {entity.text}",
            })

        return ActionResult(
            success=True,
            message=f"Linked {len(linked)} entities",
            data={'linked_entities': linked, 'count': len(linked)},
            duration=time.time() - start_time
        )

    def _add_pattern(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a custom entity pattern."""
        pattern_name = params.get('pattern_name', 'custom')
        pattern_str = params.get('pattern', r'')

        if not pattern_str:
            return ActionResult(success=False, message="No pattern provided", duration=time.time() - start_time)

        try:
            compiled = re.compile(pattern_str)
            self._custom_patterns[pattern_name] = compiled
            return ActionResult(
                success=True,
                message=f"Added pattern '{pattern_name}'",
                data={'pattern_name': pattern_name, 'pattern': pattern_str},
                duration=time.time() - start_time
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {e}", duration=time.time() - start_time)

    def _get_statistics(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get NER statistics."""
        total_entities = sum(len(ents) for ents in self._entity_cache.values())
        cache_entries = len(self._entity_cache)

        return ActionResult(
            success=True,
            message=f"NER statistics: {total_entities} entities in {cache_entries} texts",
            data={'total_entities': total_entities, 'cache_entries': cache_entries, 'custom_patterns': list(self._custom_patterns.keys())},
            duration=time.time() - start_time
        )
