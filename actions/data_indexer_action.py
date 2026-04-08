"""Data indexer action module for RabAI AutoClick.

Provides data indexing with multiple index types,
full-text search, and index management.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from collections import defaultdict
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataIndexerAction(BaseAction):
    """Index data for fast retrieval and search.
    
    Supports primary, secondary, composite, and full-text indexes.
    Provides search and range query capabilities.
    """
    action_type = "data_indexer"
    display_name = "数据索引"
    description = "数据索引构建和查询"
    
    def __init__(self):
        super().__init__()
        self._indexes: Dict[str, Dict[str, Any]] = {}
        self._data: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute indexing operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (create_index, insert, search,
                   range_query, drop_index, list_indexes), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'create_index')
        
        if action == 'create_index':
            return self._create_index(params)
        elif action == 'insert':
            return self._insert(params)
        elif action == 'insert_batch':
            return self._insert_batch(params)
        elif action == 'search':
            return self._search(params)
        elif action == 'range_query':
            return self._range_query(params)
        elif action == 'drop_index':
            return self._drop_index(params)
        elif action == 'list_indexes':
            return self._list_indexes(params)
        elif action == 'get':
            return self._get(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_index(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a new index."""
        index_name = params.get('index_name')
        if not index_name:
            return ActionResult(success=False, message="index_name is required")
        
        index_type = params.get('index_type', 'hash')
        fields = params.get('fields', [])
        unique = params.get('unique', False)
        
        with self._lock:
            self._indexes[index_name] = {
                'name': index_name,
                'type': index_type,
                'fields': fields,
                'unique': unique,
                'index_data': defaultdict(list) if index_type != 'btree' else {},
                'created_at': __import__('time').time()
            }
        
        return ActionResult(
            success=True,
            message=f"Created index '{index_name}'",
            data={
                'index_name': index_name,
                'type': index_type,
                'fields': fields
            }
        )
    
    def _insert(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Insert a record and update indexes."""
        doc_id = params.get('doc_id')
        if not doc_id:
            return ActionResult(success=False, message="doc_id is required")
        
        document = params.get('document', {})
        if not document:
            return ActionResult(success=False, message="document is required")
        
        with self._lock:
            self._data[doc_id] = document.copy()
            
            for index_name, index_config in self._indexes.items():
                self._update_index(doc_id, document, index_config)
        
        return ActionResult(
            success=True,
            message=f"Inserted document {doc_id}",
            data={'doc_id': doc_id}
        )
    
    def _insert_batch(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Insert multiple records."""
        documents = params.get('documents', [])
        if not documents:
            return ActionResult(success=False, message="No documents provided")
        
        doc_id_field = params.get('doc_id_field', 'id')
        
        inserted = 0
        failed = []
        
        with self._lock:
            for idx, doc in enumerate(documents):
                doc_id = doc.get(doc_id_field, f"doc_{idx}")
                
                if doc_id in self._data:
                    failed.append({'doc_id': doc_id, 'error': 'already exists'})
                    continue
                
                self._data[doc_id] = doc.copy()
                
                for index_name, index_config in self._indexes.items():
                    self._update_index(doc_id, doc, index_config)
                
                inserted += 1
        
        return ActionResult(
            success=len(failed) == 0,
            message=f"Inserted {inserted} documents",
            data={
                'inserted': inserted,
                'failed': len(failed),
                'failures': failed
            }
        )
    
    def _update_index(
        self,
        doc_id: str,
        document: Dict[str, Any],
        index_config: Dict[str, Any]
    ) -> None:
        """Update an index with a document."""
        index_type = index_config['type']
        fields = index_config['fields']
        index_data = index_config['index_data']
        
        if not fields:
            return
        
        key_values = []
        for field in fields:
            value = document.get(field)
            if value is not None:
                key_values.append(str(value))
        
        key = '|'.join(key_values)
        
        if index_type == 'hash':
            index_data[key].append(doc_id)
        elif index_type == 'btree':
            if key not in index_data:
                index_data[key] = []
            index_data[key].append(doc_id)
        elif index_type == 'fulltext':
            text = ' '.join(str(document.get(f, '')) for f in fields)
            words = text.lower().split()
            for word in words:
                index_data[word].append(doc_id)
    
    def _search(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Search for documents."""
        index_name = params.get('index_name')
        query = params.get('query')
        
        if not index_name:
            return ActionResult(success=False, message="index_name is required")
        
        with self._lock:
            if index_name not in self._indexes:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_name}' not found"
                )
            
            index_config = self._indexes[index_name]
            index_data = index_config['index_data']
            index_type = index_config['type']
            
            if index_type == 'fulltext':
                words = query.lower().split()
                result_sets = []
                for word in words:
                    if word in index_data:
                        result_sets.append(set(index_data[word]))
                
                if result_sets:
                    doc_ids = list(result_sets[0].intersection(*result_sets[1:]))
                else:
                    doc_ids = []
            else:
                if query in index_data:
                    doc_ids = index_data[query]
                else:
                    doc_ids = []
            
            results = [self._data.get(doc_id) for doc_id in doc_ids if doc_id in self._data]
        
        return ActionResult(
            success=True,
            message=f"Found {len(results)} results",
            data={
                'results': results,
                'count': len(results)
            }
        )
    
    def _range_query(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a range query on a B-tree index."""
        index_name = params.get('index_name')
        start_key = params.get('start_key')
        end_key = params.get('end_key')
        
        if not index_name:
            return ActionResult(success=False, message="index_name is required")
        
        with self._lock:
            if index_name not in self._indexes:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_name}' not found"
                )
            
            index_config = self._indexes[index_name]
            
            if index_config['type'] != 'btree':
                return ActionResult(
                    success=False,
                    message=f"Range query only supported on btree indexes"
                )
            
            index_data = index_config['index_data']
            
            doc_ids = []
            for key in sorted(index_data.keys()):
                if start_key and key < start_key:
                    continue
                if end_key and key > end_key:
                    break
                doc_ids.extend(index_data[key])
            
            results = [self._data.get(doc_id) for doc_id in doc_ids if doc_id in self._data]
        
        return ActionResult(
            success=True,
            message=f"Found {len(results)} results in range",
            data={
                'results': results,
                'count': len(results)
            }
        )
    
    def _drop_index(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Drop an index."""
        index_name = params.get('index_name')
        
        if not index_name:
            return ActionResult(success=False, message="index_name is required")
        
        with self._lock:
            if index_name not in self._indexes:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_name}' not found"
                )
            
            del self._indexes[index_name]
        
        return ActionResult(
            success=True,
            message=f"Dropped index '{index_name}'"
        )
    
    def _list_indexes(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List all indexes."""
        with self._lock:
            indexes = []
            
            for name, config in self._indexes.items():
                indexes.append({
                    'name': name,
                    'type': config['type'],
                    'fields': config['fields'],
                    'unique': config['unique'],
                    'size': len(config['index_data'])
                })
        
        return ActionResult(
            success=True,
            message=f"Found {len(indexes)} indexes",
            data={'indexes': indexes, 'count': len(indexes)}
        )
    
    def _get(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get a document by ID."""
        doc_id = params.get('doc_id')
        
        if not doc_id:
            return ActionResult(success=False, message="doc_id is required")
        
        with self._lock:
            if doc_id not in self._data:
                return ActionResult(
                    success=False,
                    message=f"Document '{doc_id}' not found"
                )
            
            return ActionResult(
                success=True,
                message=f"Retrieved document {doc_id}",
                data={'document': self._data[doc_id]}
            )
