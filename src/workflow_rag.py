"""
工作流RAG模块 v22
P0级差异化功能 - 基于检索增强生成的工作流知识管理

功能:
- 文档索引
- 向量存储
- 语义搜索
- 混合搜索
- 重排序
- 工作流推荐
- FAQ生成
- 答案合成
"""

import json
import time
import re
import os
import hashlib
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging

# ============== Enums ==============

class SearchMode(Enum):
    """搜索模式"""
    SEMANTIC = "semantic"
    KEYWORD = "keyword"
    HYBRID = "hybrid"


class ChunkStrategy(Enum):
    """分块策略"""
    FIXED_SIZE = "fixed_size"
    BY_SENTENCE = "by_sentence"
    BY_PARAGRAPH = "by_paragraph"
    RECURSIVE = "recursive"


class RerankModel(Enum):
    """重排序模型"""
    CROSS_ENCODER = "cross_encoder"
    BM25 = "bm25"
    DIVERSITY = "diversity"


# ============== Data Classes ==============

@dataclass
class Document:
    """文档"""
    id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    chunks: List['Chunk'] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class Chunk:
    """文档块"""
    id: str
    content: str
    document_id: str
    chunk_index: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[np.ndarray] = None


@dataclass
class SearchResult:
    """搜索结果"""
    chunk: Chunk
    score: float
    rank: int
    query: str


@dataclass
class WorkflowRecommendation:
    """工作流推荐"""
    workflow_id: str
    workflow_name: str
    similarity: float
    reason: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FAQ:
    """FAQ"""
    question: str
    answer: str
    source_chunk_id: str
    confidence: float


# ============== Embedding Generator ==============

class EmbeddingGenerator:
    """嵌入向量生成器"""
    
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2", 
                 dimension: int = 384):
        self.model_name = model_name
        self.dimension = dimension
        self.model = None
        self._load_model()
    
    def _load_model(self):
        """加载模型"""
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(self.model_name)
            logging.info(f"Loaded embedding model: {self.model_name}")
        except ImportError:
            logging.warning("sentence-transformers not available, using random embeddings")
            self.model = None
    
    def generate(self, text: str) -> np.ndarray:
        """生成单个文本的嵌入向量"""
        if self.model is None:
            return np.random.randn(self.dimension)
        
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding
    
    def generate_batch(self, texts: List[str]) -> List[np.ndarray]:
        """批量生成嵌入向量"""
        if self.model is None:
            return [np.random.randn(self.dimension) for _ in texts]
        
        embeddings = self.model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
        return [emb for emb in embeddings]


# ============== Vector Store ==============

class VectorStore:
    """向量存储"""
    
    def __init__(self, dimension: int = 384, storage_path: str = None):
        self.dimension = dimension
        self.storage_path = storage_path or "./data/vector_store"
        self.vectors: Dict[str, np.ndarray] = {}
        self.metadata: Dict[str, Dict[str, Any]] = {}
        self._ensure_storage_path()
    
    def _ensure_storage_path(self):
        """确保存储目录存在"""
        os.makedirs(self.storage_path, exist_ok=True)
    
    def add(self, chunk_id: str, embedding: np.ndarray, metadata: Dict[str, Any] = None):
        """添加向量"""
        self.vectors[chunk_id] = embedding
        self.metadata[chunk_id] = metadata or {}
    
    def search(self, query_embedding: np.ndarray, top_k: int = 10, 
               filter_func: Callable[[str], bool] = None) -> List[Tuple[str, float]]:
        """搜索最近邻"""
        results = []
        
        for chunk_id, vector in self.vectors.items():
            if filter_func and not filter_func(chunk_id):
                continue
            
            similarity = self._cosine_similarity(query_embedding, vector)
            results.append((chunk_id, similarity))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """计算余弦相似度"""
        dot_product = np.dot(a, b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        
        return float(dot_product / (norm_a * norm_b))
    
    def save(self):
        """保存向量存储"""
        save_path = os.path.join(self.storage_path, "vectors.npz")
        np.savez(save_path, 
                 vectors=json.dumps(list(self.vectors.items())),
                 metadata=json.dumps(self.metadata))
    
    def load(self):
        """加载向量存储"""
        save_path = os.path.join(self.storage_path, "vectors.npz")
        if not os.path.exists(save_path):
            return
        
        data = np.load(save_path, allow_pickle=True)
        self.vectors = dict(json.loads(data["vectors"]))
        self.metadata = dict(json.loads(data["metadata"]))


# ============== Keyword Index ==============

class KeywordIndex:
    """关键词索引"""
    
    def __init__(self):
        self.inverted_index: Dict[str, List[str]] = defaultdict(list)
        self.doc_term_freq: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    
    def add_document(self, doc_id: str, content: str):
        """添加文档到索引"""
        tokens = self._tokenize(content)
        
        for token in tokens:
            if doc_id not in self.inverted_index[token]:
                self.inverted_index[token].append(doc_id)
        
        for token in tokens:
            self.doc_term_freq[doc_id][token] += 1
    
    def _tokenize(self, text: str) -> List[str]:
        """分词"""
        text = text.lower()
        tokens = re.findall(r'\w+', text)
        return tokens
    
    def search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """搜索"""
        tokens = self._tokenize(query)
        doc_scores = defaultdict(float)
        
        for token in tokens:
            if token in self.inverted_index:
                for doc_id in self.inverted_index[token]:
                    doc_scores[doc_id] += 1
        
        results = [(doc_id, score) for doc_id, score in doc_scores.items()]
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]


# ============== Chunk Manager ==============

class ChunkManager:
    """文档分块管理器"""
    
    def __init__(self, strategy: ChunkStrategy = ChunkStrategy.RECURSIVE,
                 chunk_size: int = 512, overlap: int = 50):
        self.strategy = strategy
        self.chunk_size = chunk_size
        self.overlap = overlap
    
    def split(self, document: Document) -> List[Chunk]:
        """分割文档"""
        if self.strategy == ChunkStrategy.FIXED_SIZE:
            return self._split_fixed_size(document)
        elif self.strategy == ChunkStrategy.BY_SENTENCE:
            return self._split_by_sentence(document)
        elif self.strategy == ChunkStrategy.BY_PARAGRAPH:
            return self._split_by_paragraph(document)
        elif self.strategy == ChunkStrategy.RECURSIVE:
            return self._split_recursive(document)
        else:
            return self._split_fixed_size(document)
    
    def _split_fixed_size(self, document: Document) -> List[Chunk]:
        """固定大小分块"""
        content = document.content
        chunks = []
        
        for i in range(0, len(content), self.chunk_size - self.overlap):
            chunk_content = content[i:i + self.chunk_size]
            if chunk_content:
                chunk_id = self._generate_chunk_id(document.id, len(chunks))
                chunk = Chunk(
                    id=chunk_id,
                    content=chunk_content,
                    document_id=document.id,
                    chunk_index=len(chunks),
                    metadata={"start_char": i, "end_char": i + len(chunk_content)}
                )
                chunks.append(chunk)
        
        return chunks
    
    def _split_by_sentence(self, document: Document) -> List[Chunk]:
        """按句子分块"""
        content = document.content
        sentences = re.split(r'[.!?]+', content)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip() + "."
            if len(current_chunk) + len(sentence) <= self.chunk_size:
                current_chunk += " " + sentence
            else:
                if current_chunk:
                    chunk_id = self._generate_chunk_id(document.id, len(chunks))
                    chunks.append(Chunk(
                        id=chunk_id,
                        content=current_chunk.strip(),
                        document_id=document.id,
                        chunk_index=len(chunks)
                    ))
                current_chunk = sentence
        
        if current_chunk:
            chunk_id = self._generate_chunk_id(document.id, len(chunks))
            chunks.append(Chunk(
                id=chunk_id,
                content=current_chunk.strip(),
                document_id=document.id,
                chunk_index=len(chunks)
            ))
        
        return chunks
    
    def _split_by_paragraph(self, document: Document) -> List[Chunk]:
        """按段落分块"""
        content = document.content
        paragraphs = re.split(r'\n\n+', content)
        chunks = []
        current_chunk = ""
        
        for para in paragraphs:
            if len(current_chunk) + len(para) <= self.chunk_size:
                current_chunk += "\n\n" + para
            else:
                if current_chunk:
                    chunk_id = self._generate_chunk_id(document.id, len(chunks))
                    chunks.append(Chunk(
                        id=chunk_id,
                        content=current_chunk.strip(),
                        document_id=document.id,
                        chunk_index=len(chunks)
                    ))
                current_chunk = para
        
        if current_chunk:
            chunk_id = self._generate_chunk_id(document.id, len(chunks))
            chunks.append(Chunk(
                id=chunk_id,
                content=current_chunk.strip(),
                document_id=document.id,
                chunk_index=len(chunks)
            ))
        
        return chunks
    
    def _split_recursive(self, document: Document) -> List[Chunk]:
        """递归分块"""
        content = document.content
        chunks = []
        
        def split_text(text: str, min_size: int = 100) -> List[str]:
            if len(text) <= self.chunk_size:
                return [text]
            
            paragraphs = re.split(r'\n\n+', text)
            if len(paragraphs) > 1:
                result = []
                for para in paragraphs:
                    result.extend(split_text(para, min_size))
                return result
            
            sentences = re.split(r'(?<=[.!?])\s+', text)
            if len(sentences) > 1:
                result = []
                current = ""
                for sent in sentences:
                    if len(current) + len(sent) <= self.chunk_size:
                        current += " " + sent
                    else:
                        if current:
                            result.append(current.strip())
                        current = sent
                if current:
                    result.append(current.strip())
                return result
            
            mid = len(text) // 2
            return split_text(text[:mid]) + split_text(text[mid:])
        
        split_contents = split_text(content)
        for idx, chunk_content in enumerate(split_contents):
            if chunk_content:
                chunk_id = self._generate_chunk_id(document.id, idx)
                chunks.append(Chunk(
                    id=chunk_id,
                    content=chunk_content,
                    document_id=document.id,
                    chunk_index=idx,
                    metadata={"char_count": len(chunk_content)}
                ))
        
        return chunks
    
    def _generate_chunk_id(self, doc_id: str, chunk_index: int) -> str:
        """生成块ID"""
        raw = f"{doc_id}:{chunk_index}:{time.time()}"
        return hashlib.md5(raw.encode()).hexdigest()[:16]


# ============== Reranker ==============

class Reranker:
    """搜索结果重排序"""
    
    def __init__(self, model_type: RerankModel = RerankModel.BM25):
        self.model_type = model_type
        self.cross_encoder = None
    
    def rerank(self, query: str, results: List[SearchResult], 
               top_k: int = 5) -> List[SearchResult]:
        """重排序搜索结果"""
        if not results:
            return []
        
        if self.model_type == RerankModel.CROSS_ENCODER:
            return self._cross_encoder_rerank(query, results, top_k)
        elif self.model_type == RerankModel.BM25:
            return self._bm25_rerank(query, results, top_k)
        elif self.model_type == RerankModel.DIVERSITY:
            return self._diversity_rerank(results, top_k)
        else:
            return results[:top_k]
    
    def _cross_encoder_rerank(self, query: str, results: List[SearchResult],
                              top_k: int) -> List[SearchResult]:
        """Cross-Encoder重排序"""
        if self.cross_encoder is None:
            try:
                from sentence_transformers import CrossEncoder
                self.cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
            except ImportError:
                logging.warning("Cross-encoder not available, falling back to BM25")
                return self._bm25_rerank(query, results, top_k)
        
        pairs = [(query, r.chunk.content) for r in results]
        scores = self.cross_encoder.predict(pairs)
        
        for result, score in zip(results, scores):
            result.score = float(score)
        
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]
    
    def _bm25_rerank(self, query: str, results: List[SearchResult],
                     top_k: int) -> List[SearchResult]:
        """BM25重排序"""
        query_tokens = set(re.findall(r'\w+', query.lower()))
        
        for result in results:
            content_tokens = set(re.findall(r'\w+', result.chunk.content.lower()))
            overlap = len(query_tokens & content_tokens)
            result.score = result.score * 0.7 + (overlap / max(len(query_tokens), 1)) * 0.3
        
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]
    
    def _diversity_rerank(self, results: List[SearchResult], top_k: int) -> List[SearchResult]:
        """多样性重排序"""
        if len(results) <= top_k:
            return results
        
        selected = [results[0]]
        remaining = results[1:]
        
        while len(selected) < top_k and remaining:
            best_next = None
            best_min_sim = -1
            
            for candidate in remaining:
                min_sim_to_selected = min(
                    self._jaccard_similarity(selected[i].chunk.content, candidate.chunk.content)
                    for i in range(len(selected))
                )
                
                if min_sim_to_selected > best_min_sim:
                    best_min_sim = min_sim_to_selected
                    best_next = candidate
            
            if best_next:
                selected.append(best_next)
                remaining.remove(best_next)
            else:
                break
        
        for i, result in enumerate(selected):
            result.rank = i + 1
        
        return selected
    
    def _jaccard_similarity(self, text1: str, text2: str) -> float:
        """Jaccard相似度"""
        tokens1 = set(re.findall(r'\w+', text1.lower()))
        tokens2 = set(re.findall(r'\w+', text2.lower()))
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)
        
        return intersection / union if union > 0 else 0.0


# ============== FAQ Generator ==============

class FAQGenerator:
    """FAQ生成器"""
    
    def __init__(self, llm_provider: str = None):
        self.llm_provider = llm_provider
        self.question_patterns = [
            r'how to (.+)',
            r'what is (.+)',
            r'why does (.+)',
            r'when (.+)',
            r'can I (.+)',
            r'is it possible to (.+)',
            r'how do I (.+)',
            r'what (.+) for',
            r'how can (.+)',
        ]
    
    def generate(self, chunks: List[Chunk]) -> List[FAQ]:
        """从文档块生成FAQ"""
        faqs = []
        
        for chunk in chunks:
            questions = self._extract_questions(chunk.content)
            for question in questions:
                answer = self._generate_answer(question, chunk.content)
                if answer:
                    faqs.append(FAQ(
                        question=question,
                        answer=answer,
                        source_chunk_id=chunk.id,
                        confidence=0.8
                    ))
        
        return faqs
    
    def _extract_questions(self, content: str) -> List[str]:
        """从内容中提取问题"""
        questions = []
        
        for pattern in self.question_patterns:
            matches = re.finditer(pattern, content.lower())
            for match in matches:
                question = match.group(0).strip()
                question = question[0].upper() + question[1:]
                if not question.endswith('?'):
                    question += '?'
                questions.append(question)
        
        return questions[:5]
    
    def _generate_answer(self, question: str, context: str) -> str:
        """生成答案"""
        keywords = re.findall(r'\w+', question.lower())
        
        relevant_sentences = []
        sentences = re.split(r'[.!?]+', context)
        
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence:
                sentence_lower = sentence.lower()
                keyword_matches = sum(1 for kw in keywords if kw in sentence_lower)
                if keyword_matches >= 2:
                    relevant_sentences.append(sentence)
        
        if relevant_sentences:
            answer = ". ".join(relevant_sentences[:3])
            return answer + "."
        
        return None


# ============== Answer Synthesizer ==============

class AnswerSynthesizer:
    """答案合成器"""
    
    def __init__(self, max_context_length: int = 2000):
        self.max_context_length = max_context_length
    
    def synthesize(self, query: str, relevant_chunks: List[Chunk]) -> Dict[str, Any]:
        """合成答案"""
        if not relevant_chunks:
            return {
                "answer": "No relevant information found.",
                "sources": [],
                "confidence": 0.0
            }
        
        context = self._build_context(relevant_chunks)
        answer = self._generate_answer(query, context)
        
        return {
            "answer": answer,
            "sources": [chunk.id for chunk in relevant_chunks],
            "confidence": self._calculate_confidence(relevant_chunks),
            "context_used": len(context)
        }
    
    def _build_context(self, chunks: List[Chunk]) -> str:
        """构建上下文"""
        contexts = []
        total_length = 0
        
        for chunk in chunks:
            chunk_text = f"[Source: {chunk.id}]\n{chunk.content}\n"
            if total_length + len(chunk_text) <= self.max_context_length:
                contexts.append(chunk_text)
                total_length += len(chunk_text)
            else:
                break
        
        return "\n".join(contexts)
    
    def _generate_answer(self, query: str, context: str) -> str:
        """生成答案"""
        query_lower = query.lower()
        
        if any(kw in query_lower for kw in ['what', 'which', 'who']):
            answer_type = "definition"
        elif any(kw in query_lower for kw in ['how', 'to', 'do']):
            answer_type = "instruction"
        elif any(kw in query_lower for kw in ['why', 'because']):
            answer_type = "explanation"
        elif any(kw in query_lower for kw in ['when', 'time', 'date']):
            answer_type = "temporal"
        else:
            answer_type = "general"
        
        key_points = self._extract_key_points(context, query)
        
        if answer_type == "definition":
            return self._format_definition(key_points)
        elif answer_type == "instruction":
            return self._format_instruction(key_points)
        elif answer_type == "explanation":
            return self._format_explanation(key_points)
        else:
            return self._format_general(key_points)
    
    def _extract_key_points(self, context: str, query: str) -> List[str]:
        """提取关键点"""
        query_keywords = set(re.findall(r'\w+', query.lower()))
        sentences = re.split(r'[.!?]+', context)
        
        scored_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence:
                sentence_keywords = set(re.findall(r'\w+', sentence.lower()))
                overlap = len(query_keywords & sentence_keywords)
                scored_sentences.append((sentence, overlap))
        
        scored_sentences.sort(key=lambda x: x[1], reverse=True)
        
        return [s[0] for s in scored_sentences[:5] if s[1] > 0]
    
    def _calculate_confidence(self, chunks: List[Chunk]) -> float:
        """计算置信度"""
        if not chunks:
            return 0.0
        
        relevance_scores = [getattr(chunk, 'relevance_score', 1.0) for chunk in chunks]
        avg_relevance = sum(relevance_scores) / len(relevance_scores)
        
        coverage = min(len(chunks) / 3, 1.0)
        
        return (avg_relevance * 0.6 + coverage * 0.4)
    
    def _format_definition(self, key_points: List[str]) -> str:
        """格式化定义类型答案"""
        if not key_points:
            return "I found some related information but couldn't extract a clear definition."
        
        return f"{key_points[0]}"
    
    def _format_instruction(self, key_points: List[str]) -> str:
        """格式化指令类型答案"""
        if not key_points:
            return "I found some related information but couldn't extract clear instructions."
        
        steps = []
        for i, point in enumerate(key_points[:4], 1):
            steps.append(f"{i}. {point}")
        
        return "\n".join(steps)
    
    def _format_explanation(self, key_points: List[str]) -> str:
        """格式化解释类型答案"""
        if not key_points:
            return "I found some related information but couldn't extract a clear explanation."
        
        return f"Here's the explanation:\n{key_points[0]}"
    
    def _format_general(self, key_points: List[str]) -> str:
        """格式化一般类型答案"""
        if not key_points:
            return "No relevant information found."
        
        return f"Based on the documentation:\n{'. '.join(key_points[:3])}"


# ============== Workflow RAG (Main Class) ==============

class WorkflowRAG:
    """
    工作流RAG主类
    
    提供完整的工作流知识检索增强生成功能:
    - 文档索引和分块
    - 向量存储和关键词索引
    - 语义搜索和混合搜索
    - 结果重排序
    - 工作流推荐
    - FAQ生成
    - 答案合成
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化RAG系统"""
        config = config or {}
        
        self.embedding_dimension = config.get("embedding_dimension", 384)
        self.chunk_size = config.get("chunk_size", 512)
        self.chunk_overlap = config.get("chunk_overlap", 50)
        self.storage_path = config.get("storage_path", "./data/rag_storage")
        self.default_top_k = config.get("default_top_k", 10)
        self.rerank_top_k = config.get("rerank_top_k", 5)
        
        self.documents: Dict[str, Document] = {}
        self.embedding_generator = EmbeddingGenerator(
            model_name=config.get("embedding_model", "sentence-transformers/all-MiniLM-L6-v2"),
            dimension=self.embedding_dimension
        )
        self.vector_store = VectorStore(
            dimension=self.embedding_dimension,
            storage_path=os.path.join(self.storage_path, "vectors")
        )
        self.keyword_index = KeywordIndex()
        self.chunk_manager = ChunkManager(
            strategy=ChunkStrategy(config.get("chunk_strategy", "recursive")),
            chunk_size=self.chunk_size,
            overlap=self.chunk_overlap
        )
        self.reranker = Reranker(
            model_type=RerankModel(config.get("rerank_model", "bm25"))
        )
        self.faq_generator = FAQGenerator()
        self.answer_synthesizer = AnswerSynthesizer(
            max_context_length=config.get("max_context_length", 2000)
        )
        
        self.logger = logging.getLogger(__name__)
        self._load_state()
    
    # ============== Document Indexing ==============
    
    def index_document(self, doc_id: str, content: str, metadata: Dict[str, Any] = None) -> Document:
        """
        索引文档
        
        Args:
            doc_id: 文档ID
            content: 文档内容
            metadata: 文档元数据
        
        Returns:
            Document: 索引后的文档
        """
        metadata = metadata or {}
        metadata["indexed_at"] = datetime.now().isoformat()
        
        document = Document(
            id=doc_id,
            content=content,
            metadata=metadata
        )
        
        chunks = self.chunk_manager.split(document)
        document.chunks = chunks
        
        embeddings = self.embedding_generator.generate_batch([c.content for c in chunks])
        
        for chunk, embedding in zip(chunks, embeddings):
            chunk.embedding = embedding
            self.vector_store.add(chunk.id, embedding, {
                "document_id": doc_id,
                "content": chunk.content
            })
            self.keyword_index.add_document(chunk.id, chunk.content)
        
        self.documents[doc_id] = document
        self._save_state()
        
        self.logger.info(f"Indexed document {doc_id} with {len(chunks)} chunks")
        return document
    
    def index_workflow(self, workflow_id: str, workflow_data: Dict[str, Any]) -> Document:
        """
        索引工作流
        
        Args:
            workflow_id: 工作流ID
            workflow_data: 工作流数据
        
        Returns:
            Document: 索引后的文档
        """
        content_parts = []
        
        if "name" in workflow_data:
            content_parts.append(f"Workflow Name: {workflow_data['name']}")
        
        if "description" in workflow_data:
            content_parts.append(f"Description: {workflow_data['description']}")
        
        if "steps" in workflow_data:
            content_parts.append("Steps:")
            for i, step in enumerate(workflow_data["steps"], 1):
                step_text = f"{i}. {step.get('name', 'Unnamed step')}"
                if "description" in step:
                    step_text += f" - {step['description']}"
                content_parts.append(step_text)
        
        if "triggers" in workflow_data:
            content_parts.append(f"Triggers: {', '.join(workflow_data['triggers'])}")
        
        if "actions" in workflow_data:
            content_parts.append(f"Actions: {', '.join(workflow_data['actions'])}")
        
        if "examples" in workflow_data:
            content_parts.append("Examples:")
            content_parts.append(workflow_data["examples"])
        
        content = "\n".join(content_parts)
        
        metadata = {
            "workflow_id": workflow_id,
            "type": "workflow",
            "indexed_at": datetime.now().isoformat()
        }
        
        return self.index_document(f"workflow_{workflow_id}", content, metadata)
    
    # ============== Vector Storage ==============
    
    def save_vector_store(self):
        """保存向量存储"""
        self.vector_store.save()
        self._save_state()
    
    def load_vector_store(self):
        """加载向量存储"""
        self.vector_store.load()
        self._load_state()
    
    # ============== Semantic Search ==============
    
    def semantic_search(self, query: str, top_k: int = None, 
                        filter_func: Callable[[str], bool] = None) -> List[SearchResult]:
        """
        语义搜索
        
        Args:
            query: 查询文本
            top_k: 返回结果数量
            filter_func: 过滤函数
        
        Returns:
            List[SearchResult]: 搜索结果
        """
        top_k = top_k or self.default_top_k
        
        query_embedding = self.embedding_generator.generate(query)
        
        chunk_results = self.vector_store.search(
            query_embedding, 
            top_k=top_k * 3,
            filter_func=filter_func
        )
        
        results = []
        for chunk_id, score in chunk_results:
            chunk_data = self.vector_store.metadata.get(chunk_id, {})
            document_id = chunk_data.get("document_id", "")
            
            if document_id in self.documents:
                chunks = self.documents[document_id].chunks
                chunk = next((c for c in chunks if c.id == chunk_id), None)
                
                if chunk:
                    results.append(SearchResult(
                        chunk=chunk,
                        score=score,
                        rank=len(results) + 1,
                        query=query
                    ))
        
        return results
    
    # ============== Hybrid Search ==============
    
    def hybrid_search(self, query: str, top_k: int = None,
                      semantic_weight: float = 0.7,
                      keyword_weight: float = 0.3) -> List[SearchResult]:
        """
        混合搜索
        
        Args:
            query: 查询文本
            top_k: 返回结果数量
            semantic_weight: 语义搜索权重
            keyword_weight: 关键词搜索权重
        
        Returns:
            List[SearchResult]: 搜索结果
        """
        top_k = top_k or self.default_top_k
        
        semantic_results = self.semantic_search(query, top_k=top_k * 2)
        keyword_results = self.keyword_search(query, top_k=top_k * 2)
        
        all_chunks: Dict[str, SearchResult] = {}
        
        for result in semantic_results:
            result.score = result.score * semantic_weight
            all_chunks[result.chunk.id] = result
        
        for chunk_id, kw_score in keyword_results:
            if chunk_id in all_chunks:
                all_chunks[chunk_id].score += kw_score * keyword_weight
            else:
                chunk_data = self.vector_store.metadata.get(chunk_id, {})
                document_id = chunk_data.get("document_id", "")
                
                if document_id in self.documents:
                    chunks = self.documents[document_id].chunks
                    chunk = next((c for c in chunks if c.id == chunk_id), None)
                    
                    if chunk:
                        search_result = SearchResult(
                            chunk=chunk,
                            score=kw_score * keyword_weight,
                            rank=0,
                            query=query
                        )
                        all_chunks[chunk_id] = search_result
        
        results = list(all_chunks.values())
        results.sort(key=lambda x: x.score, reverse=True)
        
        for i, result in enumerate(results[:top_k]):
            result.rank = i + 1
        
        return results[:top_k]
    
    # ============== Keyword Search ==============
    
    def keyword_search(self, query: str, top_k: int = None) -> List[Tuple[str, float]]:
        """
        关键词搜索
        
        Args:
            query: 查询文本
            top_k: 返回结果数量
        
        Returns:
            List[Tuple[str, float]]: (chunk_id, score)
        """
        top_k = top_k or self.default_top_k
        return self.keyword_index.search(query, top_k)
    
    # ============== Reranking ==============
    
    def rerank_results(self, query: str, results: List[SearchResult],
                       top_k: int = None) -> List[SearchResult]:
        """
        重排序搜索结果
        
        Args:
            query: 查询文本
            results: 搜索结果
            top_k: 返回结果数量
        
        Returns:
            List[SearchResult]: 重排序后的结果
        """
        top_k = top_k or self.rerank_top_k
        return self.reranker.rerank(query, results, top_k)
    
    # ============== Workflow Recommendations ==============
    
    def recommend_workflows(self, query: str, top_k: int = 5,
                            workflow_filter: Callable[[str], bool] = None) -> List[WorkflowRecommendation]:
        """
        推荐相似工作流
        
        Args:
            query: 查询文本
            top_k: 返回结果数量
            workflow_filter: 工作流过滤函数
        
        Returns:
            List[WorkflowRecommendation]: 工作流推荐
        """
        def is_workflow(chunk_id: str) -> bool:
            chunk_data = self.vector_store.metadata.get(chunk_id, {})
            return chunk_data.get("type") == "workflow" or "workflow_" in chunk_data.get("document_id", "")
        
        search_filter = workflow_filter or is_workflow
        
        results = self.semantic_search(query, top_k=top_k * 2, filter_func=search_filter)
        results = self.rerank_results(query, results, top_k)
        
        recommendations = []
        seen_workflows = set()
        
        for result in results:
            doc_id = result.chunk.document_id
            workflow_id = doc_id.replace("workflow_", "")
            
            if workflow_id in seen_workflows:
                continue
            
            seen_workflows.add(workflow_id)
            
            doc = self.documents.get(doc_id)
            workflow_name = doc.metadata.get("name", workflow_id) if doc else workflow_id
            
            recommendations.append(WorkflowRecommendation(
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                similarity=result.score,
                reason=self._generate_recommendation_reason(result),
                metadata=doc.metadata if doc else {}
            ))
        
        return recommendations[:top_k]
    
    def _generate_recommendation_reason(self, result: SearchResult) -> str:
        """生成推荐原因"""
        content_preview = result.chunk.content[:100]
        return f"Related to '{result.query}': {content_preview}..."
    
    # ============== FAQ Generation ==============
    
    def generate_faqs(self, doc_id: str = None) -> List[FAQ]:
        """
        生成FAQ
        
        Args:
            doc_id: 文档ID，如果为None则对所有文档生成FAQ
        
        Returns:
            List[FAQ]: 生成的FAQ列表
        """
        if doc_id:
            documents = [self.documents.get(doc_id)] if doc_id in self.documents else []
        else:
            documents = list(self.documents.values())
        
        all_faqs = []
        for document in documents:
            if document:
                faqs = self.faq_generator.generate(document.chunks)
                all_faqs.extend(faqs)
        
        return all_faqs
    
    def get_faq_for_query(self, query: str) -> Optional[FAQ]:
        """
        获取与查询相关的FAQ
        
        Args:
            query: 查询文本
        
        Returns:
            Optional[FAQ]: 相关的FAQ
        """
        faqs = self.generate_faqs()
        
        if not faqs:
            return None
        
        query_lower = query.lower()
        query_keywords = set(re.findall(r'\w+', query_lower))
        
        best_faq = None
        best_score = 0
        
        for faq in faqs:
            faq_keywords = set(re.findall(r'\w+', faq.question.lower()))
            overlap = len(query_keywords & faq_keywords)
            
            if overlap > best_score:
                best_score = overlap
                best_faq = faq
        
        return best_faq if best_score >= 2 else None
    
    # ============== Answer Synthesis ==============
    
    def synthesize_answer(self, query: str, search_mode: SearchMode = SearchMode.HYBRID,
                          top_k: int = None) -> Dict[str, Any]:
        """
        合成答案
        
        Args:
            query: 查询文本
            search_mode: 搜索模式
            top_k: 返回结果数量
        
        Returns:
            Dict[str, Any]: 合成结果
        """
        top_k = top_k or self.default_top_k
        
        if search_mode == SearchMode.SEMANTIC:
            results = self.semantic_search(query, top_k=top_k)
        elif search_mode == SearchMode.KEYWORD:
            kw_results = self.keyword_search(query, top_k=top_k)
            results = []
            for chunk_id, score in kw_results:
                chunk_data = self.vector_store.metadata.get(chunk_id, {})
                document_id = chunk_data.get("document_id", "")
                if document_id in self.documents:
                    chunks = self.documents[document_id].chunks
                    chunk = next((c for c in chunks if c.id == chunk_id), None)
                    if chunk:
                        results.append(SearchResult(chunk=chunk, score=score, rank=len(results)+1, query=query))
        else:
            results = self.hybrid_search(query, top_k=top_k)
        
        results = self.rerank_results(query, results, top_k=top_k)
        
        relevant_chunks = [r.chunk for r in results]
        
        for chunk, result in zip(relevant_chunks, results):
            chunk.relevance_score = result.score
        
        answer_data = self.answer_synthesizer.synthesize(query, relevant_chunks)
        
        answer_data["search_results"] = [
            {
                "chunk_id": r.chunk.id,
                "content_preview": r.chunk.content[:200],
                "score": r.score,
                "rank": r.rank
            }
            for r in results
        ]
        
        return answer_data
    
    # ============== State Management ==============
    
    def _save_state(self):
        """保存状态"""
        state = {
            "documents": {
                doc_id: {
                    "id": doc.id,
                    "content": doc.content,
                    "metadata": doc.metadata,
                    "chunks": [
                        {
                            "id": c.id,
                            "content": c.content,
                            "document_id": c.document_id,
                            "chunk_index": c.chunk_index,
                            "metadata": c.metadata
                        }
                        for c in doc.chunks
                    ],
                    "created_at": doc.created_at.isoformat()
                }
                for doc_id, doc in self.documents.items()
            }
        }
        
        os.makedirs(self.storage_path, exist_ok=True)
        state_path = os.path.join(self.storage_path, "rag_state.json")
        
        with open(state_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    
    def _load_state(self):
        """加载状态"""
        state_path = os.path.join(self.storage_path, "rag_state.json")
        
        if not os.path.exists(state_path):
            return
        
        try:
            with open(state_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            for doc_id, doc_data in state.get("documents", {}).items():
                chunks = [
                    Chunk(
                        id=c["id"],
                        content=c["content"],
                        document_id=c["document_id"],
                        chunk_index=c["chunk_index"],
                        metadata=c.get("metadata", {})
                    )
                    for c in doc_data.get("chunks", [])
                ]
                
                document = Document(
                    id=doc_data["id"],
                    content=doc_data["content"],
                    metadata=doc_data.get("metadata", {}),
                    chunks=chunks,
                    created_at=datetime.fromisoformat(doc_data.get("created_at", datetime.now().isoformat()))
                )
                
                self.documents[doc_id] = document
                
        except Exception as e:
            self.logger.error(f"Failed to load state: {e}")
    
    # ============== Utility Methods ==============
    
    def get_document(self, doc_id: str) -> Optional[Document]:
        """获取文档"""
        return self.documents.get(doc_id)
    
    def list_documents(self) -> List[Dict[str, Any]]:
        """列出所有文档"""
        return [
            {
                "id": doc.id,
                "metadata": doc.metadata,
                "chunk_count": len(doc.chunks),
                "created_at": doc.created_at.isoformat()
            }
            for doc in self.documents.values()
        ]
    
    def delete_document(self, doc_id: str) -> bool:
        """删除文档"""
        if doc_id not in self.documents:
            return False
        
        document = self.documents[doc_id]
        
        for chunk in document.chunks:
            if chunk.id in self.vector_store.vectors:
                del self.vector_store.vectors[chunk.id]
            if chunk.id in self.vector_store.metadata:
                del self.vector_store.metadata[chunk.id]
        
        del self.documents[doc_id]
        self._save_state()
        
        return True
    
    def clear(self):
        """清空所有数据"""
        self.documents.clear()
        self.vector_store.vectors.clear()
        self.vector_store.metadata.clear()
        self.keyword_index.inverted_index.clear()
        self.keyword_index.doc_term_freq.clear()
        
        state_path = os.path.join(self.storage_path, "rag_state.json")
        if os.path.exists(state_path):
            os.remove(state_path)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        total_chunks = sum(len(doc.chunks) for doc in self.documents.values())
        
        return {
            "total_documents": len(self.documents),
            "total_chunks": total_chunks,
            "vector_dimension": self.embedding_dimension,
            "chunk_size": self.chunk_size,
            "storage_path": self.storage_path,
            "embedding_model": self.embedding_generator.model_name
        }
