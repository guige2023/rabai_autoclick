"""Data Indexer Action Module.

Provides indexing capabilities for fast data lookup.
"""

import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataIndexerAction(BaseAction):
    """Create and manage indexes on data.
    
    Supports primary, secondary, and composite indexes.
    """
    action_type = "data_indexer"
    display_name: "数据索引"
    description: "创建和管理数据索引"
    
    def __init__(self):
        super().__init__()
        self._indexes: Dict[str, Dict] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute indexing operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, index_name, data, fields.
        
        Returns:
            ActionResult with indexing result.
        """
        action = params.get('action', 'create')
        index_name = params.get('index_name', '')
        
        if action == 'create':
            return self._create_index(index_name, params)
        elif action == 'rebuild':
            return self._rebuild_index(index_name, params)
        elif action == 'lookup':
            return self._lookup(index_name, params)
        elif action == 'range':
            return self._range_lookup(index_name, params)
        elif action == 'drop':
            return self._drop_index(index_name)
        elif action == 'list':
            return self._list_indexes()
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _create_index(self, index_name: str, params: Dict) -> ActionResult:
        """Create an index."""
        data = params.get('data', [])
        fields = params.get('fields', [])
        unique = params.get('unique', False)
        
        if not fields:
            return ActionResult(
                success=False,
                data=None,
                error="Index fields required"
            )
        
        # Build index
        index = {}
        for i, record in enumerate(data):
            if isinstance(record, dict):
                key = tuple(record.get(f) for f in fields)
                
                if unique:
                    if key in index:
                        return ActionResult(
                            success=False,
                            data=None,
                            error=f"Duplicate key in unique index: {key}"
                        )
                    index[key] = i
                else:
                    if key not in index:
                        index[key] = []
                    index[key].append(i)
        
        self._indexes[index_name] = {
            'name': index_name,
            'fields': fields,
            'unique': unique,
            'index': index,
            'created_at': time.time()
        }
        
        return ActionResult(
            success=True,
            data={
                'index_name': index_name,
                'fields': fields,
                'unique': unique,
                'entries': len(index)
            },
            error=None
        )
    
    def _rebuild_index(self, index_name: str, params: Dict) -> ActionResult:
        """Rebuild an existing index."""
        if index_name not in self._indexes:
            return ActionResult(
                success=False,
                data=None,
                error=f"Index {index_name} not found"
            )
        
        data = params.get('data', [])
        index_info = self._indexes[index_name]
        fields = index_info['fields']
        unique = index_info['unique']
        
        # Rebuild
        index = {}
        for i, record in enumerate(data):
            if isinstance(record, dict):
                key = tuple(record.get(f) for f in fields)
                
                if unique:
                    index[key] = i
                else:
                    if key not in index:
                        index[key] = []
                    index[key].append(i)
        
        self._indexes[index_name]['index'] = index
        self._indexes[index_name]['rebuilt_at'] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'index_name': index_name,
                'entries': len(index)
            },
            error=None
        )
    
    def _lookup(self, index_name: str, params: Dict) -> ActionResult:
        """Lookup by key."""
        if index_name not in self._indexes:
            return ActionResult(
                success=False,
                data=None,
                error=f"Index {index_name} not found"
            )
        
        key_values = params.get('key', [])
        if not isinstance(key_values, tuple):
            key_values = tuple(key_values) if key_values else ()
        
        index_info = self._indexes[index_name]
        index = index_info['index']
        
        if key_values in index:
            positions = index[key_values]
            return ActionResult(
                success=True,
                data={
                    'found': True,
                    'positions': positions,
                    'count': len(positions) if isinstance(positions, list) else 1
                },
                error=None
            )
        else:
            return ActionResult(
                success=True,
                data={'found': False},
                error=None
            )
    
    def _range_lookup(self, index_name: str, params: Dict) -> ActionResult:
        """Range query on index."""
        if index_name not in self._indexes:
            return ActionResult(
                success=False,
                data=None,
                error=f"Index {index_name} not found"
            )
        
        min_key = params.get('min_key', None)
        max_key = params.get('max_key', None)
        
        index_info = self._indexes[index_name]
        index = index_info['index']
        
        results = []
        for key, positions in index.items():
            if min_key is not None and key < tuple(min_key):
                continue
            if max_key is not None and key > tuple(max_key):
                continue
            results.append({'key': key, 'positions': positions})
        
        return ActionResult(
            success=True,
            data={
                'results': results,
                'count': len(results)
            },
            error=None
        )
    
    def _drop_index(self, index_name: str) -> ActionResult:
        """Drop an index."""
        if index_name in self._indexes:
            del self._indexes[index_name]
        
        return ActionResult(
            success=True,
            data={'index_name': index_name, 'dropped': True},
            error=None
        )
    
    def _list_indexes(self) -> ActionResult:
        """List all indexes."""
        indexes = [
            {
                'name': name,
                'fields': info['fields'],
                'unique': info['unique'],
                'entries': len(info['index'])
            }
            for name, info in self._indexes.items()
        ]
        
        return ActionResult(
            success=True,
            data={
                'indexes': indexes,
                'count': len(indexes)
            },
            error=None
        )


class DataLookupAction(BaseAction):
    """Fast data lookup using indexes.
    
    Provides efficient key-based data retrieval.
    """
    action_type = "data_lookup"
    display_name: "数据查找"
    description: "使用索引进行快速数据查找"
    
    def __init__(self):
        super().__init__()
        self._data: List[Dict] = []
        self._primary_index: Dict[Any, int] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lookup operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, key, load_data.
        
        Returns:
            ActionResult with lookup result.
        """
        action = params.get('action', 'get')
        
        if action == 'load':
            return self._load_data(params)
        elif action == 'get':
            return self._get(params)
        elif action == 'get_many':
            return self._get_many(params)
        elif action == 'search':
            return self._search(params)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _load_data(self, params: Dict) -> ActionResult:
        """Load data for lookup."""
        data = params.get('data', [])
        key_field = params.get('key_field', 'id')
        
        self._data = data
        self._primary_index = {}
        
        for i, record in enumerate(data):
            if isinstance(record, dict):
                key = record.get(key_field)
                self._primary_index[key] = i
        
        return ActionResult(
            success=True,
            data={
                'loaded': len(data),
                'key_field': key_field
            },
            error=None
        )
    
    def _get(self, params: Dict) -> ActionResult:
        """Get single record by key."""
        key = params.get('key')
        
        if key in self._primary_index:
            idx = self._primary_index[key]
            return ActionResult(
                success=True,
                data={
                    'found': True,
                    'record': self._data[idx]
                },
                error=None
            )
        else:
            return ActionResult(
                success=True,
                data={'found': False},
                error=None
            )
    
    def _get_many(self, params: Dict) -> ActionResult:
        """Get multiple records by keys."""
        keys = params.get('keys', [])
        
        results = []
        for key in keys:
            if key in self._primary_index:
                results.append(self._data[self._primary_index[key]])
        
        return ActionResult(
            success=True,
            data={
                'found': results,
                'count': len(results),
                'requested': len(keys)
            },
            error=None
        )
    
    def _search(self, params: Dict) -> ActionResult:
        """Search records by field values."""
        field = params.get('field', '')
        value = params.get('value', None)
        
        results = []
        for record in self._data:
            if isinstance(record, dict) and record.get(field) == value:
                results.append(record)
        
        return ActionResult(
            success=True,
            data={
                'results': results,
                'count': len(results)
            },
            error=None
        )


def register_actions():
    """Register all Data Indexer actions."""
    return [
        DataIndexerAction,
        DataLookupAction,
    ]
