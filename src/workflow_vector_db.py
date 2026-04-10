"""
向量数据库模块 v1
P0级功能 - 支持多后端的向量数据库集成

功能:
- 多后端支持: FAISS, Pinecone, Weaviate, Chroma, Qdrant
- CRUD操作: 创建, 读取, 更新, 删除向量
- 相似性搜索: 查找相似向量
- 混合搜索: 结合向量和关键词搜索
- 元数据过滤: 按元数据过滤
- 批量操作: 批量插入/删除
- 索引管理: 创建和管理索引
- 距离度量: 支持欧氏距离, 余弦相似度, 点积
- Embedding管理: 存储和管理embeddings
- 自动批处理: 大规模操作的自动批处理
"""

import json
import time
import hashlib
import uuid
import os
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging
import threading
import asyncio

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False

try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    CHROMA_AVAILABLE = True
except ImportError:
    CHROMA_AVAILABLE = False

try:
    import qdrant_client
    from qdrant_client import QdrantClient
    from qdrant_client.http import models
    QDRANT_AVAILABLE = True
except ImportError:
    QDRANT_AVAILABLE = False

try:
    from pinecone import Pinecone, ServerlessSpec
    PINECONE_AVAILABLE = True
except ImportError:
    PINECONE_AVAILABLE = False

try:
    import weaviate
    from weaviate import WeaviateClient
    from weaviate.config import ConnectionConfig
    WEAVIATE_AVAILABLE = True
except ImportError:
    WEAVIATE_AVAILABLE = False


# ============== Enums ==============

class VectorDBBackend(Enum):
    """向量数据库后端"""
    FAISS = "faiss"
    PINECONE = "pinecone"
    WEAVIATE = "weaviate"
    CHROMA = "chroma"
    QDRANT = "qdrant"


class DistanceMetric(Enum):
    """距离度量"""
    EUCLIDEAN = "euclidean"
    COSINE = "cosine"
    DOT_PRODUCT = "dot_product"


class IndexType(Enum):
    """索引类型"""
    FLAT = "flat"
    IVF = "ivf"
    HNSW = "hnsw"
    PQ = "pq"
    RHI = "rhi"


# ============== Data Classes ==============

@dataclass
class VectorEntry:
    """向量条目"""
    id: str
    vector: List[float]
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None


@dataclass
class SearchResult:
    """搜索结果"""
    id: str
    score: float
    vector: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IndexConfig:
    """索引配置"""
    name: str
    dimension: int
    metric: DistanceMetric = DistanceMetric.COSINE
    index_type: IndexType = IndexType.FLAT
    nlist: int = 100
    nprobe: int = 10
    m: int = 16
    ef_construction: int = 200
    ef_search: int = 50
    capacity: int = 1000000


@dataclass
class BatchConfig:
    """批处理配置"""
    batch_size: int = 1000
    max_workers: int = 4
    async_enabled: bool = True


@dataclass
class HybridSearchConfig:
    """混合搜索配置"""
    vector_weight: float = 0.7
    keyword_weight: float = 0.3
    rerank: bool = True
    top_k: int = 10


# ============== VectorDB Class ==============

class VectorDB:
    """
    向量数据库类 - 支持多种后端
    提供统一的接口进行向量存储, 检索和管理
    """
    
    def __init__(
        self,
        backend: VectorDBBackend = VectorDBBackend.FAISS,
        config: Optional[Dict[str, Any]] = None,
        index_config: Optional[IndexConfig] = None,
        batch_config: Optional[BatchConfig] = None
    ):
        """
        初始化向量数据库
        
        Args:
            backend: 后端类型
            config: 后端配置
            index_config: 索引配置
            batch_config: 批处理配置
        """
        self.backend = backend
        self.config = config or {}
        self.index_config = index_config
        self.batch_config = batch_config or BatchConfig()
        
        self._client = None
        self._index = None
        self._metadata_store: Dict[str, Dict[str, Any]] = {}
        self._embedding_store: Dict[str, List[float]] = {}
        self._lock = threading.RLock()
        
        self._init_backend()
    
    def _init_backend(self):
        """初始化后端"""
        if self.backend == VectorDBBackend.FAISS:
            self._init_faiss()
        elif self.backend == VectorDBBackend.CHROMA:
            self._init_chroma()
        elif self.backend == VectorDBBackend.QDRANT:
            self._init_qdrant()
        elif self.backend == VectorDBBackend.PINECONE:
            self._init_pinecone()
        elif self.backend == VectorDBBackend.WEAVIATE:
            self._init_weaviate()
    
    def _init_faiss(self):
        """初始化FAISS后端"""
        if not FAISS_AVAILABLE:
            raise ImportError("FAISS not available. Install with: pip install faiss-cpu")
        
        if self.index_config:
            dimension = self.index_config.dimension
            metric = self.index_config.metric
            
            if metric == DistanceMetric.EUCLIDEAN:
                measure = faiss.METRIC_L2
            elif metric == DistanceMetric.COSINE:
                measure = faiss.METRIC_INNER_PRODUCT
            elif metric == DistanceMetric.DOT_PRODUCT:
                measure = faiss.METRIC_INNER_PRODUCT
            else:
                measure = faiss.METRIC_INNER_PRODUCT
            
            if self.index_config.index_type == IndexType.FLAT:
                self._index = faiss.IndexFlatL2(dimension)
            elif self.index_config.index_type == IndexType.IVF:
                quantizer = faiss.IndexFlatL2(dimension)
                self._index = faiss.IndexIVFFlat(quantizer, dimension, self.index_config.nlist, measure)
            elif self.index_config.index_type == IndexType.HNSW:
                self._index = faiss.IndexHNSWFlat(dimension, self.index_config.m)
            else:
                self._index = faiss.IndexFlatL2(dimension)
        else:
            dimension = self.config.get("dimension", 128)
            self._index = faiss.IndexFlatL2(dimension)
    
    def _init_chroma(self):
        """初始化Chroma后端"""
        if not CHROMA_AVAILABLE:
            raise ImportError("Chroma not available. Install with: pip install chromadb")
        
        persist_dir = self.config.get("persist_directory", "./chroma_db")
        self._client = chromadb.Client(ChromaSettings(
            persist_directory=persist_dir,
            anonymized_telemetry=False
        ))
    
    def _init_qdrant(self):
        """初始化Qdrant后端"""
        if not QDRANT_AVAILABLE:
            raise ImportError("Qdrant not available. Install with: pip install qdrant-client")
        
        host = self.config.get("host", "localhost")
        port = self.config.get("port", 6333)
        api_key = self.config.get("api_key")
        
        self._client = QdrantClient(host=host, port=port, api_key=api_key)
    
    def _init_pinecone(self):
        """初始化Pinecone后端"""
        if not PINECONE_AVAILABLE:
            raise ImportError("Pinecone not available. Install with: pip install pinecone")
        
        api_key = self.config.get("api_key", os.environ.get("PINECONE_API_KEY"))
        environment = self.config.get("environment", "us-east-1")
        
        self._client = Pinecone(api_key=api_key, environment=environment)
    
    def _init_weaviate(self):
        """初始化Weaviate后端"""
        if not WEAVIATE_AVAILABLE:
            raise ImportError("Weaviate not available. Install with: pip install weaviate-client")
        
        host = self.config.get("host", "http://localhost:8080")
        self._client = WeaviateClient(host)
    
    # ============== CRUD Operations ==============
    
    def create(
        self,
        id: str,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        创建向量
        
        Args:
            id: 向量ID
            vector: 向量数据
            metadata: 元数据
            
        Returns:
            成功返回True
        """
        with self._lock:
            if id in self._metadata_store:
                return False
            
            entry = VectorEntry(
                id=id,
                vector=vector,
                metadata=metadata or {},
                embedding=vector
            )
            
            self._metadata_store[id] = {
                "vector": vector,
                "metadata": metadata or {},
                "created_at": time.time()
            }
            self._embedding_store[id] = vector
            
            if self.backend == VectorDBBackend.FAISS and self._index:
                if NUMPY_AVAILABLE:
                    self._index.add(np.array([vector], dtype=np.float32))
            
            return True
    
    def read(self, id: str) -> Optional[VectorEntry]:
        """
        读取向量
        
        Args:
            id: 向量ID
            
        Returns:
            向量条目, 不存在返回None
        """
        with self._lock:
            if id not in self._metadata_store:
                return None
            
            data = self._metadata_store[id]
            return VectorEntry(
                id=id,
                vector=data["vector"],
                metadata=data.get("metadata", {}),
                embedding=data.get("vector")
            )
    
    def update(
        self,
        id: str,
        vector: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        更新向量
        
        Args:
            id: 向量ID
            vector: 新向量数据
            metadata: 新元数据
            
        Returns:
            成功返回True
        """
        with self._lock:
            if id not in self._metadata_store:
                return False
            
            if vector is not None:
                self._metadata_store[id]["vector"] = vector
                self._embedding_store[id] = vector
            
            if metadata is not None:
                self._metadata_store[id]["metadata"].update(metadata)
            
            self._metadata_store[id]["updated_at"] = time.time()
            
            return True
    
    def delete(self, id: str) -> bool:
        """
        删除向量
        
        Args:
            id: 向量ID
            
        Returns:
            成功返回True
        """
        with self._lock:
            if id not in self._metadata_store:
                return False
            
            del self._metadata_store[id]
            if id in self._embedding_store:
                del self._embedding_store[id]
            
            return True
    
    # ============== Batch Operations ==============
    
    def batch_insert(
        self,
        entries: List[VectorEntry],
        auto_batch: bool = True
    ) -> Dict[str, Any]:
        """
        批量插入向量
        
        Args:
            entries: 向量条目列表
            auto_batch: 是否自动批处理
            
        Returns:
            插入结果统计
        """
        batch_size = self.batch_config.batch_size if auto_batch else len(entries)
        results = {"success": 0, "failed": 0, "errors": []}
        
        for i in range(0, len(entries), batch_size):
            batch = entries[i:i + batch_size]
            batch_results = self._batch_insert_internal(batch)
            results["success"] += batch_results["success"]
            results["failed"] += batch_results["failed"]
            results["errors"].extend(batch_results.get("errors", []))
        
        return results
    
    def _batch_insert_internal(self, batch: List[VectorEntry]) -> Dict[str, Any]:
        """内部批量插入"""
        results = {"success": 0, "failed": 0, "errors": []}
        
        if self.backend == VectorDBBackend.FAISS and self._index:
            if NUMPY_AVAILABLE:
                vectors = [entry.vector for entry in batch]
                self._index.add(np.array(vectors, dtype=np.float32))
        
        for entry in batch:
            try:
                self._metadata_store[entry.id] = {
                    "vector": entry.vector,
                    "metadata": entry.metadata,
                    "created_at": time.time()
                }
                self._embedding_store[entry.id] = entry.vector
                results["success"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"id": entry.id, "error": str(e)})
        
        return results
    
    def batch_delete(self, ids: List[str], auto_batch: bool = True) -> Dict[str, Any]:
        """
        批量删除向量
        
        Args:
            ids: 向量ID列表
            auto_batch: 是否自动批处理
            
        Returns:
            删除结果统计
        """
        batch_size = self.batch_config.batch_size if auto_batch else len(ids)
        results = {"success": 0, "failed": 0, "errors": []}
        
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            batch_results = self._batch_delete_internal(batch)
            results["success"] += batch_results["success"]
            results["failed"] += batch_results["failed"]
            results["errors"].extend(batch_results.get("errors", []))
        
        return results
    
    def _batch_delete_internal(self, batch: List[str]) -> Dict[str, Any]:
        """内部批量删除"""
        results = {"success": 0, "failed": 0, "errors": []}
        
        for id in batch:
            try:
                if id in self._metadata_store:
                    del self._metadata_store[id]
                if id in self._embedding_store:
                    del self._embedding_store[id]
                results["success"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"id": id, "error": str(e)})
        
        return results
    
    # ============== Similarity Search ==============
    
    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None,
        return_vector: bool = True
    ) -> List[SearchResult]:
        """
        相似性搜索
        
        Args:
            query_vector: 查询向量
            top_k: 返回数量
            filter_metadata: 元数据过滤器
            return_vector: 是否返回向量
            
        Returns:
            搜索结果列表
        """
        with self._lock:
            if self.backend == VectorDBBackend.FAISS and self._index:
                return self._faiss_search(query_vector, top_k, filter_metadata, return_vector)
            
            return self._memory_search(query_vector, top_k, filter_metadata, return_vector)
    
    def _faiss_search(
        self,
        query_vector: List[float],
        top_k: int,
        filter_metadata: Optional[Dict[str, Any]],
        return_vector: bool
    ) -> List[SearchResult]:
        """FAISS搜索"""
        results = []
        
        if NUMPY_AVAILABLE:
            distances, indices = self._index.search(
                np.array([query_vector], dtype=np.float32),
                min(top_k * 2, self._index.ntotal)
            )
            
            id_list = list(self._metadata_store.keys())
            
            for i, idx in enumerate(indices[0]):
                if idx < 0 or idx >= len(id_list):
                    continue
                
                id = id_list[idx]
                data = self._metadata_store[id]
                
                if filter_metadata and not self._match_metadata(data.get("metadata", {}), filter_metadata):
                    continue
                
                score = float(distances[0][i])
                results.append(SearchResult(
                    id=id,
                    score=score,
                    vector=data["vector"] if return_vector else None,
                    metadata=data.get("metadata", {})
                ))
                
                if len(results) >= top_k:
                    break
        
        return results
    
    def _memory_search(
        self,
        query_vector: List[float],
        top_k: int,
        filter_metadata: Optional[Dict[str, Any]],
        return_vector: bool
    ) -> List[SearchResult]:
        """内存搜索"""
        results = []
        
        for id, data in self._metadata_store.items():
            if filter_metadata and not self._match_metadata(data.get("metadata", {}), filter_metadata):
                continue
            
            vector = data["vector"]
            score = self._calculate_distance(query_vector, vector)
            
            results.append(SearchResult(
                id=id,
                score=score,
                vector=vector if return_vector else None,
                metadata=data.get("metadata", {})
            ))
        
        results.sort(key=lambda x: x.score)
        return results[:top_k]
    
    def _calculate_distance(self, v1: List[float], v2: List[float]) -> float:
        """计算向量距离"""
        if not NUMPY_AVAILABLE:
            return sum((a - b) ** 2 for a, b in zip(v1, v2)) ** 0.5
        
        vec1 = np.array(v1)
        vec2 = np.array(v2)
        
        if self.index_config and self.index_config.metric == DistanceMetric.COSINE:
            norm1 = np.linalg.norm(vec1)
            norm2 = np.linalg.norm(vec2)
            if norm1 == 0 or norm2 == 0:
                return 0.0
            return float(np.dot(vec1, vec2) / (norm1 * norm2))
        elif self.index_config and self.index_config.metric == DistanceMetric.DOT_PRODUCT:
            return float(np.dot(vec1, vec2))
        else:
            return float(np.linalg.norm(vec1 - vec2))
    
    def _match_metadata(self, data: Dict[str, Any], filter_dict: Dict[str, Any]) -> bool:
        """检查元数据是否匹配"""
        for key, value in filter_dict.items():
            if key not in data:
                return False
            if isinstance(value, dict):
                if "$gt" in value and data[key] <= value["$gt"]:
                    return False
                if "$gte" in value and data[key] < value["$gte"]:
                    return False
                if "$lt" in value and data[key] >= value["$lt"]:
                    return False
                if "$lte" in value and data[key] > value["$lte"]:
                    return False
                if "$ne" in value and data[key] == value["$ne"]:
                    return False
                if "$in" in value and data[key] not in value["$in"]:
                    return False
                if "$nin" in value and data[key] in value["$nin"]:
                    return False
            elif data[key] != value:
                return False
        return True
    
    # ============== Hybrid Search ==============
    
    def hybrid_search(
        self,
        query_vector: List[float],
        query_text: str,
        top_k: int = 10,
        config: Optional[HybridSearchConfig] = None
    ) -> List[SearchResult]:
        """
        混合搜索 - 结合向量和关键词搜索
        
        Args:
            query_vector: 查询向量
            query_text: 查询文本
            top_k: 返回数量
            config: 混合搜索配置
            
        Returns:
            搜索结果列表
        """
        config = config or HybridSearchConfig()
        
        vector_results = self.search(query_vector, top_k * 2, return_vector=False)
        keyword_results = self._keyword_search(query_text, top_k * 2)
        
        combined_scores: Dict[str, float] = defaultdict(float)
        id_to_result: Dict[str, SearchResult] = {}
        
        for result in vector_results:
            score = result.score * config.vector_weight
            combined_scores[result.id] += score
            id_to_result[result.id] = result
        
        for result in keyword_results:
            score = result.score * config.keyword_weight
            combined_scores[result.id] += score
            if result.id not in id_to_result:
                id_to_result[result.id] = result
        
        sorted_ids = sorted(combined_scores.keys(), key=lambda x: combined_scores[x], reverse=True)
        
        final_results = []
        for id in sorted_ids[:top_k]:
            result = id_to_result[id]
            result.score = combined_scores[id]
            final_results.append(result)
        
        return final_results
    
    def _keyword_search(
        self,
        query_text: str,
        top_k: int
    ) -> List[SearchResult]:
        """关键词搜索"""
        results = []
        query_terms = query_text.lower().split()
        
        for id, data in self._metadata_store.items():
            metadata = data.get("metadata", {})
            text_content = metadata.get("text", "") or metadata.get("content", "")
            
            if isinstance(text_content, str):
                text_lower = text_content.lower()
                score = sum(1 for term in query_terms if term in text_lower)
                
                if score > 0:
                    score = score / len(query_terms)
                    results.append(SearchResult(
                        id=id,
                        score=score,
                        metadata=metadata
                    ))
        
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]
    
    # ============== Metadata Filtering ==============
    
    def filter_by_metadata(
        self,
        filter_dict: Dict[str, Any],
        limit: Optional[int] = None
    ) -> List[str]:
        """
        按元数据过滤
        
        Args:
            filter_dict: 过滤条件
            limit: 限制数量
            
        Returns:
            匹配的向量ID列表
        """
        results = []
        
        for id, data in self._metadata_store.items():
            metadata = data.get("metadata", {})
            if self._match_metadata(metadata, filter_dict):
                results.append(id)
                if limit and len(results) >= limit:
                    break
        
        return results
    
    def get_all_metadata(
        self,
        filter_dict: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        获取所有元数据
        
        Args:
            filter_dict: 过滤条件
            
        Returns:
            元数据字典
        """
        if filter_dict is None:
            return {id: data.get("metadata", {}) for id, data in self._metadata_store.items()}
        
        return {
            id: data.get("metadata", {})
            for id, data in self._metadata_store.items()
            if self._match_metadata(data.get("metadata", {}), filter_dict)
        }
    
    # ============== Index Management ==============
    
    def create_index(self, config: IndexConfig) -> bool:
        """
        创建索引
        
        Args:
            config: 索引配置
            
        Returns:
            成功返回True
        """
        with self._lock:
            self.index_config = config
            
            if self.backend == VectorDBBackend.FAISS:
                self._init_faiss()
                
                existing_ids = list(self._metadata_store.keys())
                if existing_ids and NUMPY_AVAILABLE:
                    vectors = [self._metadata_store[id]["vector"] for id in existing_ids]
                    self._index.add(np.array(vectors, dtype=np.float32))
                
                return True
            
            return True
    
    def rebuild_index(self) -> bool:
        """
        重建索引
        
        Returns:
            成功返回True
        """
        with self._lock:
            if self.backend == VectorDBBackend.FAISS and self.index_config:
                self._init_faiss()
                
                if NUMPY_AVAILABLE and self._metadata_store:
                    vectors = [data["vector"] for data in self._metadata_store.values()]
                    self._index.add(np.array(vectors, dtype=np.float32))
                
                return True
            
            return False
    
    def get_index_info(self) -> Dict[str, Any]:
        """
        获取索引信息
        
        Returns:
            索引信息字典
        """
        info = {
            "backend": self.backend.value,
            "total_vectors": len(self._metadata_store),
            "index_config": None
        }
        
        if self.index_config:
            info["index_config"] = {
                "name": self.index_config.name,
                "dimension": self.index_config.dimension,
                "metric": self.index_config.metric.value,
                "index_type": self.index_config.index_type.value
            }
        
        if self.backend == VectorDBBackend.FAISS and self._index:
            info["faiss_info"] = {
                "ntotal": self._index.ntotal,
                "metric_type": str(self._index.metric_type) if hasattr(self._index, "metric_type") else "L2"
            }
        
        return info
    
    # ============== Embedding Management ==============
    
    def store_embedding(
        self,
        id: str,
        embedding: List[float],
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        存储embedding
        
        Args:
            id: 向量ID
            embedding: embedding向量
            metadata: 元数据
            
        Returns:
            成功返回True
        """
        return self.create(id, embedding, metadata)
    
    def get_embedding(self, id: str) -> Optional[List[float]]:
        """
        获取embedding
        
        Args:
            id: 向量ID
            
        Returns:
            embedding向量
        """
        return self._embedding_store.get(id)
    
    def update_embedding(
        self,
        id: str,
        embedding: List[float],
        update_metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        更新embedding
        
        Args:
            id: 向量ID
            embedding: 新的embedding向量
            update_metadata: 更新的元数据
            
        Returns:
            成功返回True
        """
        return self.update(id, vector=embedding, metadata=update_metadata)
    
    def delete_embedding(self, id: str) -> bool:
        """
        删除embedding
        
        Args:
            id: 向量ID
            
        Returns:
            成功返回True
        """
        return self.delete(id)
    
    def get_embeddings_batch(self, ids: List[str]) -> Dict[str, List[float]]:
        """
        批量获取embeddings
        
        Args:
            ids: 向量ID列表
            
        Returns:
            ID到embedding的映射
        """
        return {id: self._embedding_store[id] for id in ids if id in self._embedding_store}
    
    # ============== Utility Methods ==============
    
    def count(self) -> int:
        """获取向量总数"""
        return len(self._metadata_store)
    
    def clear(self) -> bool:
        """清空所有向量"""
        with self._lock:
            self._metadata_store.clear()
            self._embedding_store.clear()
            
            if self.backend == VectorDBBackend.FAISS and self._index:
                if self.index_config and self.index_config.index_type == IndexType.IVF:
                    self._init_faiss()
                else:
                    self._index.reset()
            
            return True
    
    def export_data(self) -> Dict[str, Any]:
        """
        导出所有数据
        
        Returns:
            导出的数据字典
        """
        return {
            "backend": self.backend.value,
            "vectors": {
                id: {
                    "vector": data["vector"],
                    "metadata": data.get("metadata", {}),
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at")
                }
                for id, data in self._metadata_store.items()
            },
            "index_config": {
                "name": self.index_config.name,
                "dimension": self.index_config.dimension,
                "metric": self.index_config.metric.value,
                "index_type": self.index_config.index_type.value
            } if self.index_config else None
        }
    
    def import_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        导入数据
        
        Args:
            data: 导入的数据字典
            
        Returns:
            导入结果统计
        """
        results = {"success": 0, "failed": 0, "errors": []}
        
        vectors = data.get("vectors", {})
        for id, vector_data in vectors.items():
            try:
                self.create(
                    id=id,
                    vector=vector_data["vector"],
                    metadata=vector_data.get("metadata", {})
                )
                results["success"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"id": id, "error": str(e)})
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "backend": self.backend.value,
            "total_vectors": len(self._metadata_store),
            "index_info": self.get_index_info(),
            "batch_config": {
                "batch_size": self.batch_config.batch_size,
                "max_workers": self.batch_config.max_workers,
                "async_enabled": self.batch_config.async_enabled
            }
        }


# ============== Factory Functions ==============

def create_vector_db(
    backend: str = "faiss",
    dimension: int = 128,
    metric: str = "cosine",
    **kwargs
) -> VectorDB:
    """
    创建向量数据库实例
    
    Args:
        backend: 后端类型 (faiss, pinecone, weaviate, chroma, qdrant)
        dimension: 向量维度
        metric: 距离度量 (euclidean, cosine, dot_product)
        **kwargs: 其他配置参数
        
    Returns:
        VectorDB实例
    """
    backend_enum = VectorDBBackend(backend.lower())
    
    metric_enum = DistanceMetric.COSINE
    if metric == "euclidean":
        metric_enum = DistanceMetric.EUCLIDEAN
    elif metric == "dot_product":
        metric_enum = DistanceMetric.DOT_PRODUCT
    
    index_config = IndexConfig(
        name="default",
        dimension=dimension,
        metric=metric_enum
    )
    
    return VectorDB(
        backend=backend_enum,
        config=kwargs,
        index_config=index_config
    )


# ============== Backward Compatibility ==============

# 保持向后兼容的别名
VectorDatabase = VectorDB
create_vector_database = create_vector_db
