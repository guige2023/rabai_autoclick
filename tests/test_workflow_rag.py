"""
Tests for Workflow RAG Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_rag import (
    Document,
    Chunk,
    SearchResult,
    WorkflowRecommendation,
    FAQ,
    SearchMode,
    ChunkStrategy,
    RerankModel,
    EmbeddingGenerator,
    VectorStore,
    KeywordIndex,
    ChunkManager,
)


class TestDocument(unittest.TestCase):
    """Test Document dataclass"""

    def test_create_document(self):
        """Test creating a document"""
        doc = Document(
            id="doc_001",
            content="Test workflow content",
            metadata={"source": "test"}
        )
        self.assertEqual(doc.id, "doc_001")
        self.assertEqual(doc.content, "Test workflow content")
        self.assertEqual(doc.metadata["source"], "test")
        self.assertIsInstance(doc.created_at, datetime)

    def test_document_with_chunks(self):
        """Test document with chunks"""
        doc = Document(
            id="doc_002",
            content="Test content"
        )
        chunk = Chunk(
            id="chunk_001",
            content="Test chunk",
            document_id="doc_002",
            chunk_index=0
        )
        doc.chunks.append(chunk)
        self.assertEqual(len(doc.chunks), 1)


class TestChunk(unittest.TestCase):
    """Test Chunk dataclass"""

    def test_create_chunk(self):
        """Test creating a chunk"""
        chunk = Chunk(
            id="chunk_001",
            content="Test chunk content",
            document_id="doc_001",
            chunk_index=0,
            metadata={"page": 1}
        )
        self.assertEqual(chunk.id, "chunk_001")
        self.assertEqual(chunk.document_id, "doc_001")
        self.assertEqual(chunk.chunk_index, 0)


class TestSearchResult(unittest.TestCase):
    """Test SearchResult dataclass"""

    def test_create_search_result(self):
        """Test creating a search result"""
        chunk = Chunk(
            id="chunk_001",
            content="Test content",
            document_id="doc_001",
            chunk_index=0
        )
        result = SearchResult(
            chunk=chunk,
            score=0.95,
            rank=1,
            query="test query"
        )
        self.assertEqual(result.score, 0.95)
        self.assertEqual(result.rank, 1)
        self.assertEqual(result.query, "test query")


class TestWorkflowRecommendation(unittest.TestCase):
    """Test WorkflowRecommendation dataclass"""

    def test_create_recommendation(self):
        """Test creating a workflow recommendation"""
        rec = WorkflowRecommendation(
            workflow_id="wf_001",
            workflow_name="Test Workflow",
            similarity=0.85,
            reason="Matches your query",
            metadata={"category": "automation"}
        )
        self.assertEqual(rec.workflow_id, "wf_001")
        self.assertEqual(rec.similarity, 0.85)


class TestFAQ(unittest.TestCase):
    """Test FAQ dataclass"""

    def test_create_faq(self):
        """Test creating a FAQ"""
        faq = FAQ(
            question="What is this?",
            answer="This is a test",
            source_chunk_id="chunk_001",
            confidence=0.9
        )
        self.assertEqual(faq.question, "What is this?")
        self.assertEqual(faq.confidence, 0.9)


class TestSearchMode(unittest.TestCase):
    """Test SearchMode enum"""

    def test_search_modes(self):
        """Test all search modes exist"""
        self.assertEqual(SearchMode.SEMANTIC.value, "semantic")
        self.assertEqual(SearchMode.KEYWORD.value, "keyword")
        self.assertEqual(SearchMode.HYBRID.value, "hybrid")


class TestChunkStrategy(unittest.TestCase):
    """Test ChunkStrategy enum"""

    def test_chunk_strategies(self):
        """Test all chunk strategies exist"""
        self.assertEqual(ChunkStrategy.FIXED_SIZE.value, "fixed_size")
        self.assertEqual(ChunkStrategy.BY_SENTENCE.value, "by_sentence")
        self.assertEqual(ChunkStrategy.BY_PARAGRAPH.value, "by_paragraph")
        self.assertEqual(ChunkStrategy.RECURSIVE.value, "recursive")


class TestRerankModel(unittest.TestCase):
    """Test RerankModel enum"""

    def test_rerank_models(self):
        """Test all rerank models exist"""
        self.assertEqual(RerankModel.CROSS_ENCODER.value, "cross_encoder")
        self.assertEqual(RerankModel.BM25.value, "bm25")
        self.assertEqual(RerankModel.DIVERSITY.value, "diversity")


class TestEmbeddingGenerator(unittest.TestCase):
    """Test EmbeddingGenerator class"""

    def setUp(self):
        """Set up test fixtures"""
        self.generator = EmbeddingGenerator(dimension=384)

    def test_init(self):
        """Test generator initialization"""
        self.assertEqual(self.generator.dimension, 384)

    def test_generate_returns_array(self):
        """Test generate returns numpy array"""
        embedding = self.generator.generate("test text")
        self.assertEqual(len(embedding), 384)

    def test_generate_batch(self):
        """Test batch generation"""
        texts = ["text1", "text2", "text3"]
        embeddings = self.generator.generate_batch(texts)
        self.assertEqual(len(embeddings), 3)
        for emb in embeddings:
            self.assertEqual(len(emb), 384)


class TestVectorStore(unittest.TestCase):
    """Test VectorStore class"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.store = VectorStore(dimension=384, storage_path=self.temp_dir)

    def tearDown(self):
        """Clean up temp directory"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init(self):
        """Test vector store initialization"""
        self.assertEqual(self.store.dimension, 384)
        self.assertEqual(self.store.storage_path, self.temp_dir)

    def test_add_vector(self):
        """Test adding a vector"""
        import numpy as np
        embedding = np.random.randn(384)
        self.store.add("chunk_001", embedding, {"doc_id": "doc_001"})
        self.assertIn("chunk_001", self.store.vectors)

    def test_search_vectors(self):
        """Test searching vectors"""
        import numpy as np
        embedding = np.random.randn(384)
        self.store.add("chunk_001", embedding, {"doc_id": "doc_001"})
        results = self.store.search(embedding, top_k=5)
        self.assertIsInstance(results, list)

    def test_cosine_similarity(self):
        """Test cosine similarity calculation"""
        import numpy as np
        a = np.array([1.0, 0.0, 0.0])
        b = np.array([1.0, 0.0, 0.0])
        similarity = self.store._cosine_similarity(a, b)
        self.assertAlmostEqual(similarity, 1.0, places=5)

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_and_load(self, mock_makedirs, mock_file):
        """Test saving and loading vector store"""
        import numpy as np
        embedding = np.random.randn(384)
        self.store.add("chunk_001", embedding, {"doc_id": "doc_001"})
        # Mock the save/load operations
        self.store.save()
        self.store.load()


class TestKeywordIndex(unittest.TestCase):
    """Test KeywordIndex class"""

    def setUp(self):
        """Set up test fixtures"""
        self.index = KeywordIndex()

    def test_init(self):
        """Test keyword index initialization"""
        self.assertIsInstance(self.index.inverted_index, dict)

    def test_add_document(self):
        """Test adding document to index"""
        self.index.add_document("doc_001", "Hello world test")
        self.assertIn("hello", self.index.inverted_index)
        self.assertIn("world", self.index.inverted_index)
        self.assertIn("test", self.index.inverted_index)

    def test_tokenize(self):
        """Test tokenization"""
        tokens = self.index._tokenize("Hello World! Test 123")
        self.assertIn("hello", tokens)
        self.assertIn("world", tokens)
        self.assertIn("test", tokens)

    def test_search(self):
        """Test searching"""
        self.index.add_document("doc_001", "Hello world test")
        self.index.add_document("doc_002", "Hello universe test")
        results = self.index.search("hello test", top_k=10)
        self.assertEqual(len(results), 2)


class TestChunkManager(unittest.TestCase):
    """Test ChunkManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = ChunkManager(
            strategy=ChunkStrategy.FIXED_SIZE,
            chunk_size=100,
            overlap=20
        )
        self.document = Document(
            id="doc_001",
            content="A" * 500,
            metadata={"source": "test"}
        )

    def test_init(self):
        """Test chunk manager initialization"""
        self.assertEqual(self.manager.strategy, ChunkStrategy.FIXED_SIZE)
        self.assertEqual(self.manager.chunk_size, 100)
        self.assertEqual(self.manager.overlap, 20)

    def test_split_fixed_size(self):
        """Test fixed size splitting"""
        chunks = self.manager.split(self.document)
        self.assertGreater(len(chunks), 0)
        for chunk in chunks:
            self.assertEqual(chunk.document_id, "doc_001")

    def test_split_by_sentence(self):
        """Test sentence-based splitting"""
        manager = ChunkManager(strategy=ChunkStrategy.BY_SENTENCE, chunk_size=100)
        doc = Document(
            id="doc_002",
            content="First sentence. Second sentence. Third sentence.",
            metadata={}
        )
        chunks = manager.split(doc)
        self.assertGreater(len(chunks), 0)

    def test_split_by_paragraph(self):
        """Test paragraph-based splitting"""
        manager = ChunkManager(strategy=ChunkStrategy.BY_PARAGRAPH, chunk_size=200)
        doc = Document(
            id="doc_003",
            content="Paragraph one.\n\nParagraph two.\n\nParagraph three.",
            metadata={}
        )
        chunks = manager.split(doc)
        self.assertGreater(len(chunks), 0)

    def test_generate_chunk_id(self):
        """Test chunk ID generation"""
        chunk_id = self.manager._generate_chunk_id("doc_001", 0)
        self.assertIn("doc_001", chunk_id)
        self.assertIn("0", chunk_id)


class TestEnumsIntegration(unittest.TestCase):
    """Integration tests for enums"""

    def test_search_mode_values(self):
        """Test SearchMode enum values"""
        modes = [SearchMode.SEMANTIC, SearchMode.KEYWORD, SearchMode.HYBRID]
        values = [m.value for m in modes]
        self.assertIn("semantic", values)
        self.assertIn("keyword", values)
        self.assertIn("hybrid", values)


if __name__ == '__main__':
    unittest.main()
