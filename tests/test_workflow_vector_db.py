"""
WorkflowVectorDB 测试
向量数据库模块测试
"""
import unittest
import time
from unittest.mock import Mock, patch, MagicMock

import sys
import os
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_vector_db import (
    VectorDB,
    VectorDBBackend,
    DistanceMetric,
    IndexType,
    VectorEntry,
    SearchResult,
    IndexConfig,
    BatchConfig,
    HybridSearchConfig,
    create_vector_db,
    VectorDatabase,
    create_vector_database,
)


class TestVectorDBBackend(unittest.TestCase):
    """测试向量数据库后端枚举"""
    
    def test_backend_values(self):
        """测试后端枚举值"""
        self.assertEqual(VectorDBBackend.FAISS.value, "faiss")
        self.assertEqual(VectorDBBackend.PINECONE.value, "pinecone")
        self.assertEqual(VectorDBBackend.WEAVIATE.value, "weaviate")
        self.assertEqual(VectorDBBackend.CHROMA.value, "chroma")
        self.assertEqual(VectorDBBackend.QDRANT.value, "qdrant")


class TestDistanceMetric(unittest.TestCase):
    """测试距离度量枚举"""
    
    def test_metric_values(self):
        """测试度量枚举值"""
        self.assertEqual(DistanceMetric.EUCLIDEAN.value, "euclidean")
        self.assertEqual(DistanceMetric.COSINE.value, "cosine")
        self.assertEqual(DistanceMetric.DOT_PRODUCT.value, "dot_product")


class TestIndexType(unittest.TestCase):
    """测试索引类型枚举"""
    
    def test_index_type_values(self):
        """测试索引类型枚举值"""
        self.assertEqual(IndexType.FLAT.value, "flat")
        self.assertEqual(IndexType.IVF.value, "ivf")
        self.assertEqual(IndexType.HNSW.value, "hnsw")
        self.assertEqual(IndexType.PQ.value, "pq")
        self.assertEqual(IndexType.RHI.value, "rhi")


class TestVectorEntry(unittest.TestCase):
    """测试向量条目"""
    
    def test_create_vector_entry(self):
        """测试创建向量条目"""
        entry = VectorEntry(
            id="vec_1",
            vector=[0.1, 0.2, 0.3],
            metadata={"source": "test"},
            embedding=[0.1, 0.2, 0.3]
        )
        
        self.assertEqual(entry.id, "vec_1")
        self.assertEqual(len(entry.vector), 3)
        self.assertEqual(entry.metadata, {"source": "test"})
    
    def test_vector_entry_defaults(self):
        """测试向量条目默认值"""
        entry = VectorEntry(id="vec_2", vector=[0.1, 0.2])
        
        self.assertEqual(entry.metadata, {})
        self.assertIsNone(entry.embedding)


class TestSearchResult(unittest.TestCase):
    """测试搜索结果"""
    
    def test_create_search_result(self):
        """测试创建搜索结果"""
        result = SearchResult(
            id="result_1",
            score=0.95,
            vector=[0.1, 0.2],
            metadata={"type": "match"}
        )
        
        self.assertEqual(result.id, "result_1")
        self.assertEqual(result.score, 0.95)
    
    def test_search_result_defaults(self):
        """测试搜索结果默认值"""
        result = SearchResult(id="result_2", score=0.8)
        
        self.assertIsNone(result.vector)
        self.assertEqual(result.metadata, {})


class TestIndexConfig(unittest.TestCase):
    """测试索引配置"""
    
    def test_create_index_config(self):
        """测试创建索引配置"""
        config = IndexConfig(
            name="test_index",
            dimension=128,
            metric=DistanceMetric.COSINE,
            index_type=IndexType.HNSW,
            nlist=100,
            nprobe=10,
            m=16,
            ef_construction=200,
            ef_search=50,
            capacity=1000000
        )
        
        self.assertEqual(config.name, "test_index")
        self.assertEqual(config.dimension, 128)
        self.assertEqual(config.metric, DistanceMetric.COSINE)
        self.assertEqual(config.index_type, IndexType.HNSW)
    
    def test_index_config_defaults(self):
        """测试索引配置默认值"""
        config = IndexConfig(name="default", dimension=64)
        
        self.assertEqual(config.metric, DistanceMetric.COSINE)
        self.assertEqual(config.index_type, IndexType.FLAT)
        self.assertEqual(config.nlist, 100)
        self.assertEqual(config.nprobe, 10)


class TestBatchConfig(unittest.TestCase):
    """测试批处理配置"""
    
    def test_create_batch_config(self):
        """测试创建批处理配置"""
        config = BatchConfig(batch_size=500, max_workers=8, async_enabled=True)
        
        self.assertEqual(config.batch_size, 500)
        self.assertEqual(config.max_workers, 8)
        self.assertTrue(config.async_enabled)
    
    def test_batch_config_defaults(self):
        """测试批处理配置默认值"""
        config = BatchConfig()
        
        self.assertEqual(config.batch_size, 1000)
        self.assertEqual(config.max_workers, 4)
        self.assertTrue(config.async_enabled)


class TestHybridSearchConfig(unittest.TestCase):
    """测试混合搜索配置"""
    
    def test_create_hybrid_config(self):
        """测试创建混合搜索配置"""
        config = HybridSearchConfig(
            vector_weight=0.7,
            keyword_weight=0.3,
            rerank=True,
            top_k=10
        )
        
        self.assertEqual(config.vector_weight, 0.7)
        self.assertEqual(config.keyword_weight, 0.3)
        self.assertTrue(config.rerank)
        self.assertEqual(config.top_k, 10)
    
    def test_hybrid_config_defaults(self):
        """测试混合搜索配置默认值"""
        config = HybridSearchConfig()
        
        self.assertEqual(config.vector_weight, 0.7)
        self.assertEqual(config.keyword_weight, 0.3)
        self.assertTrue(config.rerank)
        self.assertEqual(config.top_k, 10)


class TestVectorDBCRUD(unittest.TestCase):
    """测试VectorDB CRUD操作"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_create_vector(self):
        """测试创建向量"""
        result = self.db.create(
            id="vec_1",
            vector=[0.1] * 128,
            metadata={"type": "test"}
        )
        
        self.assertTrue(result)
        self.assertEqual(self.db.count(), 1)
    
    def test_create_duplicate_vector(self):
        """测试创建重复向量"""
        self.db.create(id="vec_1", vector=[0.1] * 128)
        result = self.db.create(id="vec_1", vector=[0.2] * 128)
        
        self.assertFalse(result)
        self.assertEqual(self.db.count(), 1)
    
    def test_read_vector(self):
        """测试读取向量"""
        self.db.create(id="vec_1", vector=[0.1] * 128, metadata={"key": "value"})
        
        entry = self.db.read("vec_1")
        
        self.assertIsNotNone(entry)
        self.assertEqual(entry.id, "vec_1")
        self.assertEqual(entry.metadata, {"key": "value"})
    
    def test_read_nonexistent_vector(self):
        """测试读取不存在的向量"""
        entry = self.db.read("nonexistent")
        self.assertIsNone(entry)
    
    def test_update_vector(self):
        """测试更新向量"""
        self.db.create(id="vec_1", vector=[0.1] * 128, metadata={"key": "old"})
        
        result = self.db.update(
            id="vec_1",
            vector=[0.2] * 128,
            metadata={"key": "new"}
        )
        
        self.assertTrue(result)
        
        entry = self.db.read("vec_1")
        self.assertEqual(entry.vector[0], 0.2)
        self.assertEqual(entry.metadata["key"], "new")
    
    def test_update_nonexistent_vector(self):
        """测试更新不存在的向量"""
        result = self.db.update(
            id="nonexistent",
            vector=[0.1] * 128
        )
        
        self.assertFalse(result)
    
    def test_update_vector_partial(self):
        """测试部分更新向量"""
        self.db.create(id="vec_1", vector=[0.1] * 128, metadata={"key": "value"})
        
        result = self.db.update(id="vec_1", metadata={"new_key": "new_value"})
        
        self.assertTrue(result)
        entry = self.db.read("vec_1")
        self.assertEqual(entry.vector[0], 0.1)  # Unchanged
        self.assertEqual(entry.metadata["key"], "value")  # Original kept
        self.assertEqual(entry.metadata["new_key"], "new_value")  # New added
    
    def test_delete_vector(self):
        """测试删除向量"""
        self.db.create(id="vec_1", vector=[0.1] * 128)
        
        result = self.db.delete("vec_1")
        
        self.assertTrue(result)
        self.assertEqual(self.db.count(), 0)
        self.assertIsNone(self.db.read("vec_1"))
    
    def test_delete_nonexistent_vector(self):
        """测试删除不存在的向量"""
        result = self.db.delete("nonexistent")
        self.assertFalse(result)


class TestVectorDBBatch(unittest.TestCase):
    """测试VectorDB批量操作"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_batch_insert(self):
        """测试批量插入"""
        entries = [
            VectorEntry(id=f"vec_{i}", vector=[0.1 * i] * 128, metadata={"index": i})
            for i in range(10)
        ]
        
        result = self.db.batch_insert(entries)
        
        self.assertEqual(result["success"], 10)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(self.db.count(), 10)
    
    def test_batch_insert_with_auto_batch(self):
        """测试自动批处理插入"""
        self.db.batch_config = BatchConfig(batch_size=5)
        
        entries = [
            VectorEntry(id=f"vec_{i}", vector=[0.1 * i] * 128)
            for i in range(12)
        ]
        
        result = self.db.batch_insert(entries, auto_batch=True)
        
        self.assertEqual(result["success"], 12)
    
    def test_batch_delete(self):
        """测试批量删除"""
        # First insert some vectors
        entries = [
            VectorEntry(id=f"vec_{i}", vector=[0.1 * i] * 128)
            for i in range(10)
        ]
        self.db.batch_insert(entries)
        
        # Delete some
        result = self.db.batch_delete(["vec_0", "vec_1", "vec_2"])
        
        self.assertEqual(result["success"], 3)
        self.assertEqual(self.db.count(), 7)
    
    def test_batch_delete_with_auto_batch(self):
        """测试自动批处理删除"""
        entries = [
            VectorEntry(id=f"vec_{i}", vector=[0.1 * i] * 128)
            for i in range(12)
        ]
        self.db.batch_insert(entries)
        
        self.db.batch_config = BatchConfig(batch_size=5)
        
        ids_to_delete = [f"vec_{i}" for i in range(10)]
        result = self.db.batch_delete(ids_to_delete, auto_batch=True)
        
        self.assertEqual(result["success"], 10)


class TestVectorDBSearch(unittest.TestCase):
    """测试VectorDB搜索功能"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
        # Insert test vectors
        for i in range(10):
            vector = [0.01 * i] * 128  # Simple linear vectors
            self.db.create(id=f"vec_{i}", vector=vector, metadata={"index": i})
    
    def test_search_basic(self):
        """测试基本搜索"""
        query = [0.05] * 128
        
        results = self.db.search(query, top_k=3)
        
        self.assertLessEqual(len(results), 3)
        for result in results:
            self.assertIsInstance(result, SearchResult)
    
    def test_search_with_filter(self):
        """测试带过滤的搜索"""
        query = [0.05] * 128
        filter_dict = {"index": {"$gte": 5}}
        
        results = self.db.search(query, top_k=5, filter_metadata=filter_dict)
        
        for result in results:
            self.assertGreaterEqual(result.metadata.get("index", 0), 5)
    
    def test_search_with_various_filters(self):
        """测试各种过滤条件"""
        self.db.clear()
        self.db.create("v1", [0.1]*128, metadata={"value": 10, "tag": "a"})
        self.db.create("v2", [0.2]*128, metadata={"value": 20, "tag": "b"})
        self.db.create("v3", [0.3]*128, metadata={"value": 30, "tag": "a"})
        
        # Test $gt
        results = self.db.search([0.15]*128, top_k=10, filter_metadata={"value": {"$gt": 15}})
        self.assertTrue(all(r.metadata["value"] > 15 for r in results))
        
        # Test $lt
        results = self.db.search([0.25]*128, top_k=10, filter_metadata={"value": {"$lt": 25}})
        self.assertTrue(all(r.metadata["value"] < 25 for r in results))
        
        # Test $in
        results = self.db.search([0.2]*128, top_k=10, filter_metadata={"tag": {"$in": ["a", "b"]}})
        self.assertTrue(all(r.metadata["tag"] in ["a", "b"] for r in results))
    
    def test_search_return_vector(self):
        """测试返回向量的搜索"""
        query = [0.05] * 128
        
        results_with_vector = self.db.search(query, top_k=1, return_vector=True)
        results_without_vector = self.db.search(query, top_k=1, return_vector=False)
        
        self.assertIsNotNone(results_with_vector[0].vector)
        self.assertIsNone(results_without_vector[0].vector)


class TestVectorDBHybridSearch(unittest.TestCase):
    """测试VectorDB混合搜索"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
        self.db.create("doc1", [0.1]*128, metadata={"text": "hello world test"})
        self.db.create("doc2", [0.2]*128, metadata={"text": "foo bar"})
        self.db.create("doc3", [0.3]*128, metadata={"text": "hello world another"})
    
    def test_hybrid_search_basic(self):
        """测试基本混合搜索"""
        query_vector = [0.15] * 128
        query_text = "hello world"
        
        results = self.db.hybrid_search(query_vector, query_text, top_k=3)
        
        self.assertLessEqual(len(results), 3)
    
    def test_hybrid_search_with_config(self):
        """测试带配置的混合搜索"""
        query_vector = [0.15] * 128
        query_text = "hello"
        
        config = HybridSearchConfig(
            vector_weight=0.5,
            keyword_weight=0.5,
            rerank=True,
            top_k=5
        )
        
        results = self.db.hybrid_search(query_vector, query_text, top_k=5, config=config)
        
        self.assertLessEqual(len(results), 5)
    
    def test_keyword_search(self):
        """测试关键词搜索"""
        results = self.db._keyword_search("hello world", top_k=10)
        
        self.assertGreaterEqual(len(results), 2)
        texts = [r.metadata.get("text", "") for r in results]
        self.assertTrue(any("hello" in t and "world" in t for t in texts))


class TestVectorDBMetadata(unittest.TestCase):
    """测试VectorDB元数据操作"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
        self.db.create("v1", [0.1]*128, metadata={"type": "a", "value": 10})
        self.db.create("v2", [0.2]*128, metadata={"type": "b", "value": 20})
        self.db.create("v3", [0.3]*128, metadata={"type": "a", "value": 30})
    
    def test_filter_by_metadata(self):
        """测试按元数据过滤"""
        results = self.db.filter_by_metadata({"type": "a"})
        
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.startswith("v") for r in results))
    
    def test_filter_by_metadata_with_limit(self):
        """测试带限制的元数据过滤"""
        results = self.db.filter_by_metadata({"type": "a"}, limit=1)
        
        self.assertEqual(len(results), 1)
    
    def test_get_all_metadata(self):
        """测试获取所有元数据"""
        all_metadata = self.db.get_all_metadata()
        
        self.assertEqual(len(all_metadata), 3)
    
    def test_get_all_metadata_with_filter(self):
        """测试带过滤的获取元数据"""
        filtered = self.db.get_all_metadata({"type": "a"})
        
        self.assertEqual(len(filtered), 2)


class TestVectorDBIndex(unittest.TestCase):
    """测试VectorDB索引管理"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_create_index(self):
        """测试创建索引"""
        config = IndexConfig(
            name="test_index",
            dimension=128,
            metric=DistanceMetric.COSINE,
            index_type=IndexType.HNSW,
            m=16
        )
        
        result = self.db.create_index(config)
        
        self.assertTrue(result)
        self.assertIsNotNone(self.db.index_config)
    
    def test_rebuild_index(self):
        """测试重建索引"""
        # Add some vectors first
        for i in range(5):
            self.db.create(id=f"vec_{i}", vector=[0.1*i]*128)
        
        result = self.db.rebuild_index()
        
        self.assertTrue(result)
    
    def test_get_index_info(self):
        """测试获取索引信息"""
        config = IndexConfig(
            name="test_index",
            dimension=128,
            metric=DistanceMetric.COSINE,
            index_type=IndexType.FLAT
        )
        self.db.create_index(config)
        
        info = self.db.get_index_info()
        
        self.assertEqual(info["backend"], "faiss")
        self.assertEqual(info["total_vectors"], 0)
        self.assertIsNotNone(info["index_config"])


class TestVectorDBEmbedding(unittest.TestCase):
    """测试VectorDB Embedding管理"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_store_embedding(self):
        """测试存储embedding"""
        result = self.db.store_embedding(
            id="emb_1",
            embedding=[0.1] * 128,
            metadata={"model": "test"}
        )
        
        self.assertTrue(result)
    
    def test_get_embedding(self):
        """测试获取embedding"""
        self.db.store_embedding(id="emb_1", embedding=[0.1] * 128)
        
        embedding = self.db.get_embedding("emb_1")
        
        self.assertIsNotNone(embedding)
        self.assertEqual(embedding[0], 0.1)
    
    def test_get_nonexistent_embedding(self):
        """测试获取不存在的embedding"""
        embedding = self.db.get_embedding("nonexistent")
        self.assertIsNone(embedding)
    
    def test_update_embedding(self):
        """测试更新embedding"""
        self.db.store_embedding(id="emb_1", embedding=[0.1] * 128)
        
        result = self.db.update_embedding(
            id="emb_1",
            embedding=[0.2] * 128,
            update_metadata={"updated": True}
        )
        
        self.assertTrue(result)
        embedding = self.db.get_embedding("emb_1")
        self.assertEqual(embedding[0], 0.2)
    
    def test_delete_embedding(self):
        """测试删除embedding"""
        self.db.store_embedding(id="emb_1", embedding=[0.1] * 128)
        
        result = self.db.delete_embedding("emb_1")
        
        self.assertTrue(result)
        self.assertIsNone(self.db.get_embedding("emb_1"))
    
    def test_get_embeddings_batch(self):
        """测试批量获取embeddings"""
        self.db.store_embedding(id="emb_1", embedding=[0.1] * 128)
        self.db.store_embedding(id="emb_2", embedding=[0.2] * 128)
        
        results = self.db.get_embeddings_batch(["emb_1", "emb_2", "emb_3"])
        
        self.assertEqual(len(results), 2)
        self.assertIn("emb_1", results)
        self.assertIn("emb_2", results)


class TestVectorDBUtility(unittest.TestCase):
    """测试VectorDB实用方法"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
        for i in range(5):
            self.db.create(id=f"vec_{i}", vector=[0.1 * i] * 128)
    
    def test_count(self):
        """测试计数"""
        self.assertEqual(self.db.count(), 5)
        
        self.db.create(id="vec_5", vector=[0.5] * 128)
        self.assertEqual(self.db.count(), 6)
    
    def test_clear(self):
        """测试清空"""
        result = self.db.clear()
        
        self.assertTrue(result)
        self.assertEqual(self.db.count(), 0)
    
    def test_export_data(self):
        """测试导出数据"""
        self.db.create(id="vec_extra", vector=[0.9] * 128, metadata={"key": "value"})
        
        data = self.db.export_data()
        
        self.assertEqual(data["backend"], "faiss")
        self.assertIn("vectors", data)
        self.assertIn("index_config", data)
    
    def test_import_data(self):
        """测试导入数据"""
        export_data = self.db.export_data()
        self.db.clear()
        
        result = self.db.import_data(export_data)
        
        self.assertEqual(result["success"], 6)
        self.assertEqual(self.db.count(), 6)
    
    def test_get_stats(self):
        """测试获取统计信息"""
        stats = self.db.get_stats()
        
        self.assertEqual(stats["backend"], "faiss")
        self.assertEqual(stats["total_vectors"], 5)
        self.assertIn("index_info", stats)
        self.assertIn("batch_config", stats)


class TestVectorDBDistanceCalculation(unittest.TestCase):
    """测试距离计算"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_calculate_distance(self):
        """测试距离计算"""
        v1 = [1.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0]
        
        distance = self.db._calculate_distance(v1, v2)
        
        self.assertGreater(distance, 0)
    
    def test_calculate_distance_same_vector(self):
        """测试相同向量距离"""
        v = [1.0, 0.0, 0.0]
        
        distance = self.db._calculate_distance(v, v)
        
        # For cosine similarity, same vector = 1.0; for euclidean = 0.0
        self.assertGreaterEqual(distance, 0)


class TestVectorDBMemoryBackend(unittest.TestCase):
    """测试内存后端"""
    
    def setUp(self):
        """设置测试环境"""
        # Use a backend that doesn't require external dependencies
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_memory_search(self):
        """测试内存搜索"""
        # Add some vectors
        self.db.create("v1", [1.0, 0.0], metadata={"id": "v1"})
        self.db.create("v2", [0.0, 1.0], metadata={"id": "v2"})
        
        results = self.db._memory_search([1.0, 0.0], top_k=2, filter_metadata=None, return_vector=True)
        
        self.assertLessEqual(len(results), 2)


class TestCreateVectorDB(unittest.TestCase):
    """测试创建VectorDB工厂函数"""
    
    def test_create_vector_db_faiss(self):
        """测试创建FAISS后端"""
        db = create_vector_db(backend="faiss", dimension=128, metric="cosine")
        
        self.assertEqual(db.backend, VectorDBBackend.FAISS)
        self.assertIsNotNone(db.index_config)
    
    def test_create_vector_db_euclidean(self):
        """测试创建欧氏距离后端"""
        db = create_vector_db(backend="faiss", dimension=128, metric="euclidean")
        
        self.assertEqual(db.index_config.metric, DistanceMetric.EUCLIDEAN)
    
    def test_create_vector_db_dot_product(self):
        """测试创建点积后端"""
        db = create_vector_db(backend="faiss", dimension=128, metric="dot_product")
        
        self.assertEqual(db.index_config.metric, DistanceMetric.DOT_PRODUCT)
    
    def test_create_vector_db_custom_config(self):
        """测试创建带自定义配置的VectorDB"""
        db = create_vector_db(
            backend="faiss",
            dimension=256,
            metric="cosine",
            extra_param="value"
        )
        
        self.assertEqual(db.index_config.dimension, 256)


class TestBackwardCompatibility(unittest.TestCase):
    """测试向后兼容性"""
    
    def test_vector_database_alias(self):
        """测试VectorDatabase别名"""
        self.assertEqual(VectorDatabase, VectorDB)
    
    def test_create_vector_database_alias(self):
        """测试create_vector_database别名"""
        self.assertEqual(create_vector_database, create_vector_db)


class TestVectorDBThreadSafety(unittest.TestCase):
    """测试VectorDB线程安全"""
    
    def test_concurrent_create(self):
        """测试并发创建"""
        import threading
        
        db = VectorDB(backend=VectorDBBackend.FAISS)
        errors = []
        
        def create_vectors(start, count):
            try:
                for i in range(start, start + count):
                    db.create(id=f"vec_{i}", vector=[0.01 * i] * 128)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=create_vectors, args=(0, 10)),
            threading.Thread(target=create_vectors, args=(10, 10)),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(len(errors), 0)
        self.assertEqual(db.count(), 20)


class TestVectorDBMatchMetadata(unittest.TestCase):
    """测试元数据匹配"""
    
    def setUp(self):
        """设置测试环境"""
        self.db = VectorDB(backend=VectorDBBackend.FAISS)
    
    def test_match_exact(self):
        """测试精确匹配"""
        data = {"type": "a", "value": 10}
        self.assertTrue(self.db._match_metadata(data, {"type": "a"}))
        self.assertFalse(self.db._match_metadata(data, {"type": "b"}))
    
    def test_match_gt(self):
        """测试大于匹配"""
        data = {"value": 10}
        self.assertTrue(self.db._match_metadata(data, {"value": {"$gt": 5}}))
        self.assertFalse(self.db._match_metadata(data, {"value": {"$gt": 15}}))
    
    def test_match_gte(self):
        """测试大于等于匹配"""
        data = {"value": 10}
        self.assertTrue(self.db._match_metadata(data, {"value": {"$gte": 10}}))
        self.assertFalse(self.db._match_metadata(data, {"value": {"$gte": 15}}))
    
    def test_match_lt(self):
        """测试小于匹配"""
        data = {"value": 10}
        self.assertTrue(self.db._match_metadata(data, {"value": {"$lt": 15}}))
        self.assertFalse(self.db._match_metadata(data, {"value": {"$lt": 5}}))
    
    def test_match_lte(self):
        """测试小于等于匹配"""
        data = {"value": 10}
        self.assertTrue(self.db._match_metadata(data, {"value": {"$lte": 10}}))
        self.assertFalse(self.db._match_metadata(data, {"value": {"$lte": 5}}))
    
    def test_match_ne(self):
        """测试不等于匹配"""
        data = {"value": 10}
        self.assertTrue(self.db._match_metadata(data, {"value": {"$ne": 5}}))
        self.assertFalse(self.db._match_metadata(data, {"value": {"$ne": 10}}))
    
    def test_match_in(self):
        """测试IN匹配"""
        data = {"tag": "a"}
        self.assertTrue(self.db._match_metadata(data, {"tag": {"$in": ["a", "b"]}}))
        self.assertFalse(self.db._match_metadata(data, {"tag": {"$in": ["b", "c"]}}))
    
    def test_match_nin(self):
        """测试NOT IN匹配"""
        data = {"tag": "a"}
        self.assertTrue(self.db._match_metadata(data, {"tag": {"$nin": ["b", "c"]}}))
        self.assertFalse(self.db._match_metadata(data, {"tag": {"$nin": ["a", "b"]}}))
    
    def test_match_missing_key(self):
        """测试缺失键"""
        data = {"value": 10}
        self.assertFalse(self.db._match_metadata(data, {"missing": "value"}))


if __name__ == "__main__":
    unittest.main()
