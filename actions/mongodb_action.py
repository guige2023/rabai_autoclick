"""MongoDB action module for RabAI AutoClick.

Provides MongoDB operations:
- MongoInsertAction: Insert document
- MongoFindAction: Find documents
- MongoUpdateAction: Update documents
- MongoDeleteAction: Delete documents
- MongoCountAction: Count documents
- MongoListCollectionsAction: List collections
- MongoAggregateAction: Run aggregation pipeline
- MongoIndexAction: Create index
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


def get_mongo_client(host='localhost', port=27017, user=None, password=None, database=None):
    """Get MongoDB client."""
    try:
        from pymongo import MongoClient
        if user and password:
            uri = f"mongodb://{user}:{password}@{host}:{port}/"
        else:
            uri = f"mongodb://{host}:{port}/"
        client = MongoClient(uri)
        return client
    except ImportError:
        return None


class MongoInsertAction(BaseAction):
    """Insert document."""
    action_type = "mongo_insert"
    display_name = "MongoDB插入"
    description = "向MongoDB插入文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute insert.

        Args:
            context: Execution context.
            params: Dict with database, collection, document, host, port.

        Returns:
            ActionResult with inserted ID.
        """
        database_name = params.get('database', 'test')
        collection = params.get('collection', '')
        document = params.get('document', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')

        valid, msg = self.validate_type(collection, str, 'collection')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_coll = context.resolve_value(collection)
            resolved_doc = context.resolve_value(document)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装: pip install pymongo"
                )

            db = client[resolved_db]
            coll = db[resolved_coll]

            if isinstance(resolved_doc, list):
                result = coll.insert_many(resolved_doc)
                ids = [str(i) for i in result.inserted_ids]
            else:
                result = coll.insert_one(resolved_doc)
                ids = [str(result.inserted_id)]

            client.close()

            return ActionResult(
                success=True,
                message=f"文档已插入: {ids}",
                data={'ids': ids, 'count': len(ids)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB插入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection', 'document']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': 'test', 'host': 'localhost', 'port': 27017, 'user': '', 'password': ''}


class MongoFindAction(BaseAction):
    """Find documents."""
    action_type = "mongo_find"
    display_name = "MongoDB查询"
    description = "查询MongoDB文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find.

        Args:
            context: Execution context.
            params: Dict with database, collection, query, projection, limit, host, port, output_var.

        Returns:
            ActionResult with documents.
        """
        database_name = params.get('database', 'test')
        collection = params.get('collection', '')
        query = params.get('query', {})
        projection = params.get('projection', {})
        limit = params.get('limit', 100)
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'mongo_results')

        valid, msg = self.validate_type(collection, str, 'collection')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_coll = context.resolve_value(collection)
            resolved_query = context.resolve_value(query) if query else {}
            resolved_proj = context.resolve_value(projection) if projection else None
            resolved_limit = context.resolve_value(limit)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装"
                )

            db = client[resolved_db]
            coll = db[resolved_coll]

            cursor = coll.find(resolved_query, resolved_proj).limit(int(resolved_limit))
            docs = [doc for doc in cursor]

            # Convert ObjectId to string for JSON serialization
            for doc in docs:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])

            client.close()

            context.set(output_var, docs)

            return ActionResult(
                success=True,
                message=f"查询完成: {len(docs)} 条文档",
                data={'count': len(docs), 'documents': docs, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'database': 'test', 'query': {}, 'projection': {}, 'limit': 100,
            'host': 'localhost', 'port': 27017, 'user': '', 'password': '',
            'output_var': 'mongo_results'
        }


class MongoUpdateAction(BaseAction):
    """Update documents."""
    action_type = "mongo_update"
    display_name = "MongoDB更新"
    description = "更新MongoDB文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute update.

        Args:
            context: Execution context.
            params: Dict with database, collection, query, update, upsert, host, port.

        Returns:
            ActionResult with update result.
        """
        database_name = params.get('database', 'test')
        collection = params.get('collection', '')
        query = params.get('query', {})
        update = params.get('update', {})
        upsert = params.get('upsert', False)
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')

        valid, msg = self.validate_type(collection, str, 'collection')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_coll = context.resolve_value(collection)
            resolved_query = context.resolve_value(query)
            resolved_update = context.resolve_value(update)
            resolved_upsert = context.resolve_value(upsert)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装"
                )

            db = client[resolved_db]
            coll = db[resolved_coll]

            result = coll.update_many(
                resolved_query,
                resolved_update,
                upsert=resolved_upsert
            )

            client.close()

            return ActionResult(
                success=True,
                message=f"更新完成: {result.modified_count} 修改, {result.upserted_id} 新增",
                data={'modified': result.modified_count, 'upserted': str(result.upserted_id) if result.upserted_id else None, 'matched': result.matched_count}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB更新失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection', 'query', 'update']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': 'test', 'upsert': False, 'host': 'localhost', 'port': 27017, 'user': '', 'password': ''}


class MongoDeleteAction(BaseAction):
    """Delete documents."""
    action_type = "mongo_delete"
    display_name = "MongoDB删除"
    description = "删除MongoDB文档"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with database, collection, query, host, port.

        Returns:
            ActionResult with delete result.
        """
        database_name = params.get('database', 'test')
        collection = params.get('collection', '')
        query = params.get('query', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')

        valid, msg = self.validate_type(collection, str, 'collection')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_coll = context.resolve_value(collection)
            resolved_query = context.resolve_value(query)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装"
                )

            db = client[resolved_db]
            coll = db[resolved_coll]

            result = coll.delete_many(resolved_query)

            client.close()

            return ActionResult(
                success=True,
                message=f"删除完成: {result.deleted_count} 条文档",
                data={'deleted': result.deleted_count}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection', 'query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': 'test', 'host': 'localhost', 'port': 27017, 'user': '', 'password': ''}


class MongoCountAction(BaseAction):
    """Count documents."""
    action_type = "mongo_count"
    display_name = "MongoDB计数"
    description = "统计MongoDB文档数量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with database, collection, query, host, port, output_var.

        Returns:
            ActionResult with count.
        """
        database_name = params.get('database', 'test')
        collection = params.get('collection', '')
        query = params.get('query', {})
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'mongo_count')

        valid, msg = self.validate_type(collection, str, 'collection')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_coll = context.resolve_value(collection)
            resolved_query = context.resolve_value(query) if query else {}
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装"
                )

            db = client[resolved_db]
            coll = db[resolved_coll]

            count = coll.count_documents(resolved_query)

            client.close()

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"文档数量: {count}",
                data={'count': count, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': 'test', 'query': {}, 'host': 'localhost', 'port': 27017, 'user': '', 'password': '', 'output_var': 'mongo_count'}


class MongoListCollectionsAction(BaseAction):
    """List collections."""
    action_type = "mongo_list_collections"
    display_name = "MongoDB列出集合"
    description = "列出MongoDB数据库中的集合"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list collections.

        Args:
            context: Execution context.
            params: Dict with database, host, port, output_var.

        Returns:
            ActionResult with collection names.
        """
        database_name = params.get('database', 'test')
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'mongo_collections')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装"
                )

            db = client[resolved_db]
            collections = db.list_collection_names()

            client.close()

            context.set(output_var, collections)

            return ActionResult(
                success=True,
                message=f"集合: {len(collections)} 个",
                data={'count': len(collections), 'collections': collections, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB列出集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': 'test', 'host': 'localhost', 'port': 27017, 'user': '', 'password': '', 'output_var': 'mongo_collections'}


class MongoAggregateAction(BaseAction):
    """Run aggregation pipeline."""
    action_type = "mongo_aggregate"
    display_name = "MongoDB聚合"
    description = "运行MongoDB聚合管道"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute aggregate.

        Args:
            context: Execution context.
            params: Dict with database, collection, pipeline, host, port, output_var.

        Returns:
            ActionResult with aggregation results.
        """
        database_name = params.get('database', 'test')
        collection = params.get('collection', '')
        pipeline = params.get('pipeline', [])
        host = params.get('host', 'localhost')
        port = params.get('port', 27017)
        user = params.get('user', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'mongo_aggregate')

        valid, msg = self.validate_type(collection, str, 'collection')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_db = context.resolve_value(database_name)
            resolved_coll = context.resolve_value(collection)
            resolved_pipeline = context.resolve_value(pipeline)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user) if user else None
            resolved_pwd = context.resolve_value(password) if password else None

            client = get_mongo_client(resolved_host, int(resolved_port), resolved_user, resolved_pwd)
            if client is None:
                return ActionResult(
                    success=False,
                    message="pymongo未安装"
                )

            db = client[resolved_db]
            coll = db[resolved_coll]

            cursor = coll.aggregate(resolved_pipeline)
            results = [doc for doc in cursor]

            # Convert ObjectId to string
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])

            client.close()

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"聚合完成: {len(results)} 结果",
                data={'count': len(results), 'results': results, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MongoDB聚合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection', 'pipeline']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': 'test', 'host': 'localhost', 'port': 27017, 'user': '', 'password': '', 'output_var': 'mongo_aggregate'}
