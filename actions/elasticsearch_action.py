"""Elasticsearch action module for RabAI AutoClick.

Provides Elasticsearch operations:
- ElasticsearchSearchAction: Search documents
- ElasticsearchIndexAction: Index document
- ElasticsearchDeleteAction: Delete document
- ElasticsearchCreateIndexAction: Create index
- ElasticsearchBulkAction: Bulk index documents
- ElasticsearchCountAction: Count documents
- ElasticsearchMappingAction: Get index mapping
- ElasticsearchClusterHealthAction: Cluster health
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


def get_es_client(host='localhost', port=9200, scheme='http', user=None, password=None):
    """Get Elasticsearch client."""
    try:
        from elasticsearch import Elasticsearch
        hosts = [f"{scheme}://{host}:{port}"]
        if user and password:
            return Elasticsearch(hosts, basic_auth=(user, password))
        return Elasticsearch(hosts)
    except ImportError:
        return None


class ElasticsearchSearchAction(BaseAction):
    """Search documents."""
    action_type = "elasticsearch_search"
    display_name = "ES搜索"
    description = "搜索Elasticsearch文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute search.

        Args:
            context: Execution context.
            params: Dict with index, query, host, port, output_var.

        Returns:
            ActionResult with search results.
        """
        index = params.get('index', '')
        query = params.get('query', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)
        output_var = params.get('output_var', 'es_results')

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_query = context.resolve_value(query) if query else {'match_all': {}}
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装: pip install elasticsearch"
                )

            response = client.search(index=resolved_index, body=resolved_query)
            hits = response.get('hits', {}).get('hits', [])
            total = response.get('hits', {}).get('total', {}).get('value', 0)

            results = [hit['_source'] for hit in hits]
            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"搜索完成: {total} 条匹配, 返回 {len(results)} 条",
                data={'total': total, 'results': results, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES搜索失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'query': {}, 'host': 'localhost', 'port': 9200, 'output_var': 'es_results'}


class ElasticsearchIndexAction(BaseAction):
    """Index document."""
    action_type = "elasticsearch_index"
    display_name = "ES索引文档"
    description = "将文档索引到Elasticsearch"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute index.

        Args:
            context: Execution context.
            params: Dict with index, doc_id, document, host, port.

        Returns:
            ActionResult with result.
        """
        index = params.get('index', '')
        doc_id = params.get('doc_id', '')
        document = params.get('document', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_doc = context.resolve_value(document)
            resolved_id = context.resolve_value(doc_id) if doc_id else None
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            kwargs = {'index': resolved_index, 'document': resolved_doc}
            if resolved_id:
                kwargs['id'] = resolved_id

            result = client.index(**kwargs)

            return ActionResult(
                success=True,
                message=f"文档已索引: {result.get('_id', resolved_id)}",
                data={'id': result.get('_id'), 'result': result.get('result')}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index', 'document']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'doc_id': '', 'host': 'localhost', 'port': 9200}


class ElasticsearchDeleteAction(BaseAction):
    """Delete document."""
    action_type = "elasticsearch_delete"
    display_name = "ES删除文档"
    description = "删除Elasticsearch文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with index, doc_id, host, port.

        Returns:
            ActionResult indicating success.
        """
        index = params.get('index', '')
        doc_id = params.get('doc_id', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(doc_id, str, 'doc_id')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_id = context.resolve_value(doc_id)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            result = client.delete(index=resolved_index, id=resolved_id)

            return ActionResult(
                success=True,
                message=f"文档已删除: {resolved_id}",
                data={'id': resolved_id, 'result': result.get('result')}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index', 'doc_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 9200}


class ElasticsearchCreateIndexAction(BaseAction):
    """Create index."""
    action_type = "elasticsearch_create_index"
    display_name = "ES创建索引"
    description = "创建Elasticsearch索引"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create index.

        Args:
            context: Execution context.
            params: Dict with index, mappings, settings, host, port.

        Returns:
            ActionResult indicating success.
        """
        index = params.get('index', '')
        mappings = params.get('mappings', {})
        settings = params.get('settings', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_mappings = context.resolve_value(mappings) if mappings else {}
            resolved_settings = context.resolve_value(settings) if settings else {}
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            kwargs = {'index': resolved_index}
            if resolved_mappings:
                kwargs['mappings'] = resolved_mappings
            if resolved_settings:
                kwargs['settings'] = resolved_settings

            result = client.indices.create(**kwargs)

            return ActionResult(
                success=True,
                message=f"索引已创建: {resolved_index}",
                data={'index': resolved_index, 'acknowledged': result.get('acknowledged')}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES创建索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mappings': {}, 'settings': {}, 'host': 'localhost', 'port': 9200}


class ElasticsearchBulkAction(BaseAction):
    """Bulk index documents."""
    action_type = "elasticsearch_bulk"
    display_name = "ES批量索引"
    description = "批量索引Elasticsearch文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bulk.

        Args:
            context: Execution context.
            params: Dict with index, documents, host, port, output_var.

        Returns:
            ActionResult with bulk result.
        """
        index = params.get('index', '')
        documents = params.get('documents', [])
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)
        output_var = params.get('output_var', 'bulk_result')

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_docs = context.resolve_value(documents)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            from elasticsearch.helpers import bulk

            actions = []
            for doc in resolved_docs:
                actions.append({
                    '_index': resolved_index,
                    '_source': doc
                })

            success, failed = bulk(client, actions)

            result = {'success': success, 'failed': len(failed) if failed else 0}
            context.set(output_var, result)

            return ActionResult(
                success=failed is None or len(failed) == 0,
                message=f"批量索引完成: {success} 成功, {len(failed) if failed else 0} 失败",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES批量索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index', 'documents']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 9200, 'output_var': 'bulk_result'}


class ElasticsearchCountAction(BaseAction):
    """Count documents."""
    action_type = "elasticsearch_count"
    display_name = "ES计数"
    description = "统计Elasticsearch文档数量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with index, query, host, port, output_var.

        Returns:
            ActionResult with count.
        """
        index = params.get('index', '')
        query = params.get('query', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)
        output_var = params.get('output_var', 'es_count')

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_query = context.resolve_value(query) if query else {}
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            kwargs = {'index': resolved_index}
            if resolved_query:
                kwargs['query'] = resolved_query

            result = client.count(**kwargs)
            count = result.get('count', 0)

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"文档数量: {count}",
                data={'count': count, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'query': {}, 'host': 'localhost', 'port': 9200, 'output_var': 'es_count'}


class ElasticsearchMappingAction(BaseAction):
    """Get index mapping."""
    action_type = "elasticsearch_mapping"
    display_name = "ES获取映射"
    description = "获取Elasticsearch索引映射"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mapping.

        Args:
            context: Execution context.
            params: Dict with index, host, port, output_var.

        Returns:
            ActionResult with mapping.
        """
        index = params.get('index', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)
        output_var = params.get('output_var', 'es_mapping')

        valid, msg = self.validate_type(index, str, 'index')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_index = context.resolve_value(index)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            mapping = client.indices.get_mapping(index=resolved_index)
            context.set(output_var, mapping)

            return ActionResult(
                success=True,
                message=f"获取映射: {resolved_index}",
                data={'mapping': mapping, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES获取映射失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 9200, 'output_var': 'es_mapping'}


class ElasticsearchClusterHealthAction(BaseAction):
    """Get cluster health."""
    action_type = "elasticsearch_cluster_health"
    display_name = "ES集群健康"
    description = "获取Elasticsearch集群健康状态"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cluster health.

        Args:
            context: Execution context.
            params: Dict with host, port, output_var.

        Returns:
            ActionResult with health status.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 9200)
        output_var = params.get('output_var', 'es_health')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_es_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="elasticsearch-py未安装"
                )

            health = client.cluster.health()
            context.set(output_var, health)

            status = health.get('status', 'unknown')

            return ActionResult(
                success=status != 'red',
                message=f"集群健康: {status}",
                data=health
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ES集群健康检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 9200, 'output_var': 'es_health'}
