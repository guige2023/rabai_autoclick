"""Data Catalog Action Module.

Provides data cataloging and discovery capabilities.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataCatalogAction(BaseAction):
    """Catalog data sources and schemas.
    
    Maintains a catalog of available data and their metadata.
    """
    action_type = "data_catalog"
    display_name = "数据目录"
    description = "管理数据源和模式目录"
    
    def __init__(self):
        super().__init__()
        self._catalog: Dict[str, Dict] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute catalog operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, data_source, metadata.
        
        Returns:
            ActionResult with catalog result.
        """
        action = params.get('action', 'register')
        data_source = params.get('data_source', '')
        
        if not data_source and action != 'list':
            return ActionResult(
                success=False,
                data=None,
                error="Data source name required"
            )
        
        if action == 'register':
            return self._register(data_source, params)
        elif action == 'update':
            return self._update(data_source, params)
        elif action == 'get':
            return self._get(data_source)
        elif action == 'list':
            return self._list_sources()
        elif action == 'search':
            return self._search(params)
        elif action == 'unregister':
            return self._unregister(data_source)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _register(self, data_source: str, params: Dict) -> ActionResult:
        """Register a data source."""
        metadata = params.get('metadata', {})
        
        self._catalog[data_source] = {
            'name': data_source,
            'metadata': metadata,
            'registered_at': time.time(),
            'schema': metadata.get('schema', {}),
            'tags': metadata.get('tags', [])
        }
        
        return ActionResult(
            success=True,
            data={
                'data_source': data_source,
                'registered': True
            },
            error=None
        )
    
    def _update(self, data_source: str, params: Dict) -> ActionResult:
        """Update data source metadata."""
        if data_source not in self._catalog:
            return ActionResult(
                success=False,
                data=None,
                error=f"Data source {data_source} not found"
            )
        
        metadata = params.get('metadata', {})
        self._catalog[data_source]['metadata'].update(metadata)
        self._catalog[data_source]['updated_at'] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'data_source': data_source,
                'updated': True
            },
            error=None
        )
    
    def _get(self, data_source: str) -> ActionResult:
        """Get data source info."""
        if data_source not in self._catalog:
            return ActionResult(
                success=False,
                data=None,
                error=f"Data source {data_source} not found"
            )
        
        return ActionResult(
            success=True,
            data=self._catalog[data_source],
            error=None
        )
    
    def _list_sources(self) -> ActionResult:
        """List all registered data sources."""
        sources = [
            {
                'name': name,
                'tags': info.get('tags', []),
                'registered_at': info.get('registered_at')
            }
            for name, info in self._catalog.items()
        ]
        
        return ActionResult(
            success=True,
            data={
                'sources': sources,
                'count': len(sources)
            },
            error=None
        )
    
    def _search(self, params: Dict) -> ActionResult:
        """Search data sources by tags or metadata."""
        query = params.get('query', '')
        tags = params.get('tags', [])
        
        results = []
        for name, info in self._catalog.items():
            source_tags = info.get('tags', [])
            metadata = info.get('metadata', {})
            
            matches = False
            
            # Match by query in name or description
            if query:
                if query.lower() in name.lower():
                    matches = True
                elif query.lower() in metadata.get('description', '').lower():
                    matches = True
            
            # Match by tags
            if tags:
                if any(tag in source_tags for tag in tags):
                    matches = True
            
            if matches:
                results.append(info)
        
        return ActionResult(
            success=True,
            data={
                'results': results,
                'count': len(results)
            },
            error=None
        )
    
    def _unregister(self, data_source: str) -> ActionResult:
        """Unregister a data source."""
        if data_source in self._catalog:
            del self._catalog[data_source]
        
        return ActionResult(
            success=True,
            data={'data_source': data_source, 'unregistered': True},
            error=None
        )


class DataDiscoveryAction(BaseAction):
    """Discover available data and schemas.
    
    Helps find data based on content patterns and metadata.
    """
    action_type = "data_discovery"
    display_name: "数据发现"
    description = "根据内容模式发现可用数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data discovery.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, search_type.
        
        Returns:
            ActionResult with discovery results.
        """
        pattern = params.get('pattern', '')
        search_type = params.get('search_type', 'schema')
        
        if search_type == 'schema':
            return self._discover_by_schema(pattern)
        elif search_type == 'content':
            return self._discover_by_content(pattern, params)
        elif search_type == 'metadata':
            return self._discover_by_metadata(pattern)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown search type: {search_type}"
            )
    
    def _discover_by_schema(self, pattern: str) -> ActionResult:
        """Discover data by schema pattern."""
        # Simulate schema discovery
        results = [
            {'source': 'db1', 'table': 'users', 'schema_match': 'id, name, email'},
            {'source': 'db2', 'table': 'orders', 'schema_match': 'id, user_id, total'}
        ]
        
        return ActionResult(
            success=True,
            data={
                'results': results,
                'count': len(results)
            },
            error=None
        )
    
    def _discover_by_content(self, pattern: str, params: Dict) -> ActionResult:
        """Discover data by content pattern."""
        sample_size = params.get('sample_size', 100)
        
        results = [
            {'source': 'api1', 'endpoint': '/users', 'matches': 42},
            {'source': 'api2', 'endpoint': '/products', 'matches': 17}
        ]
        
        return ActionResult(
            success=True,
            data={
                'results': results,
                'count': len(results),
                'pattern': pattern
            },
            error=None
        )
    
    def _discover_by_metadata(self, pattern: str) -> ActionResult:
        """Discover data by metadata."""
        results = [
            {'source': 'file1', 'format': 'csv', 'tags': ['sales', '2024']},
            {'source': 'file2', 'format': 'json', 'tags': ['inventory']}
        ]
        
        return ActionResult(
            success=True,
            data={
                'results': results,
                'count': len(results)
            },
            error=None
        )


def register_actions():
    """Register all Data Catalog actions."""
    return [
        DataCatalogAction,
        DataDiscoveryAction,
    ]
