"""
Text similarity and NLP utilities - distance metrics, tokenization, stemming, keyword extraction.
"""
from typing import Any, Dict, List, Optional, Set, Tuple
import re
import math
import logging
from collections import Counter

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _tokenize(text: str, lowercase: bool = True) -> List[str]:
    tokens = re.findall(r"\b\w+\b", text.lower() if lowercase else text)
    return tokens


def _stem_word(word: str) -> str:
    """Simple Porter-like suffix stripping."""
    suffixes = ["ational", "tional", "enci", "anci", "izer", "ising", "izing",
                "ational", "fulness", "ousness", "alism", "ation", "ator",
                "alism", "iveness", "fulness", "ousness", "aliti", "iviti",
                "biliti", "ies", "ment", "ness", "ance", "ence", "able", "ible",
                "ant", "ent", "ism", "ate", "iti", "ous", "ive", "ize", "ies",
                "ing", "ful", "est", "ion", "ous", "ive", "ble", "ter", "per", "led"]
    word = word.lower()
    for suffix in sorted(suffixes, key=len, reverse=True):
        if word.endswith(suffix) and len(word) > len(suffix) + 2:
            return word[: -len(suffix)]
    return word


def _jaccard(set1: Set[str], set2: Set[str]) -> float:
    if not set1 and not set2:
        return 1.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def _cosine_similarity(vec1: Dict[str, int], vec2: Dict[str, int]) -> float:
    all_keys = set(vec1.keys()) | set(vec2.keys())
    dot_product = sum(vec1.get(k, 0) * vec2.get(k, 0) for k in all_keys)
    mag1 = math.sqrt(sum(v ** 2 for v in vec1.values()))
    mag2 = math.sqrt(sum(v ** 2 for v in vec2.values()))
    if mag1 == 0 or mag2 == 0:
        return 0.0
    return dot_product / (mag1 * mag2)


def _levenshtein(s: str, t: str) -> int:
    m, n = len(s), len(t)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if s[i - 1] == t[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)
    return dp[m][n]


def _ngrams(tokens: List[str], n: int) -> List[Tuple[str, ...]]:
    return [tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]


def _tfidf_score(tokens: List[str], document_freq: Dict[str, int], num_docs: int) -> Dict[str, float]:
    tf = Counter(tokens)
    max_tf = max(tf.values()) if tf else 1
    scores = {}
    for word, count in tf.items():
        tf_score = 0.5 + 0.5 * (count / max_tf)
        idf = math.log((num_docs + 1) / (document_freq.get(word, 0) + 1)) + 1
        scores[word] = tf_score * idf
    return scores


class TextSimilarityAction(BaseAction):
    """Text similarity and NLP operations.

    Provides tokenization, stemming, Jaccard/Cosine/Levenshtein similarity, n-grams, TF-IDF.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "tokenize")
        text = params.get("text", "")
        text1 = params.get("text1", "")
        text2 = params.get("text2", "")

        try:
            if operation == "tokenize":
                lowercase = params.get("lowercase", True)
                tokens = _tokenize(text, lowercase)
                return {"success": True, "tokens": tokens, "count": len(tokens)}

            elif operation == "stem":
                words = text.split()
                stemmed = [_stem_word(w) for w in words]
                return {"success": True, "stemmed": stemmed}

            elif operation == "jaccard":
                tokens1 = set(_tokenize(text1))
                tokens2 = set(_tokenize(text2))
                similarity = _jaccard(tokens1, tokens2)
                return {"success": True, "similarity": similarity, "tokens1": len(tokens1), "tokens2": len(tokens2)}

            elif operation == "cosine":
                tokens1 = Counter(_tokenize(text1))
                tokens2 = Counter(_tokenize(text2))
                similarity = _cosine_similarity(dict(tokens1), dict(tokens2))
                return {"success": True, "similarity": similarity}

            elif operation == "levenshtein":
                distance = _levenshtein(text1, text2)
                max_len = max(len(text1), len(text2))
                normalized = 1 - distance / max_len if max_len > 0 else 1.0
                return {"success": True, "distance": distance, "normalized_similarity": normalized}

            elif operation == "ngrams":
                n = int(params.get("n", 2))
                tokens = _tokenize(text)
                ngrams_list = _ngrams(tokens, n)
                ngram_strs = [" ".join(ng) for ng in ngrams_list]
                return {"success": True, "ngrams": ngram_strs, "count": len(ngram_strs), "n": n}

            elif operation == "tfidf":
                documents = params.get("documents", [text1, text2])
                num_docs = len(documents)
                all_tokens = [_tokenize(d) for d in documents]
                df = Counter()
                for tokens in all_tokens:
                    df.update(set(tokens))
                results = []
                for i, tokens in enumerate(all_tokens):
                    scores = _tfidf_score(tokens, dict(df), num_docs)
                    top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
                    results.append({"doc": i, "top_keywords": top, "scores": scores})
                return {"success": True, "results": results}

            elif operation == "word_count":
                tokens = _tokenize(text)
                counter = Counter(tokens)
                return {"success": True, "words": counter, "unique": len(counter), "total": len(tokens)}

            elif operation == "extract_keywords":
                tokens = _tokenize(text)
                counter = Counter(tokens)
                top_n = int(params.get("top_n", 10))
                stop_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did", "will", "would", "could", "should", "may", "might", "can"}
                filtered = {k: v for k, v in counter.items() if k not in stop_words and len(k) > 2}
                top_keywords = sorted(filtered.items(), key=lambda x: x[1], reverse=True)[:top_n]
                return {"success": True, "keywords": top_keywords, "count": len(top_keywords)}

            elif operation == "sentences":
                sentences = re.split(r"[.!?]+", text)
                sentences = [s.strip() for s in sentences if s.strip()]
                return {"success": True, "sentences": sentences, "count": len(sentences)}

            elif operation == "bigram":
                tokens = _tokenize(text)
                bigrams = _ngrams(tokens, 2)
                bigram_strs = [" ".join(bg) for bg in bigrams]
                return {"success": True, "bigrams": bigram_strs, "count": len(bigram_strs)}

            elif operation == "similarity":
                tokens1 = set(_tokenize(text1))
                tokens2 = set(_tokenize(text2))
                jaccard_sim = _jaccard(tokens1, tokens2)
                lev_dist = _levenshtein(text1, text2)
                return {
                    "success": True,
                    "jaccard": jaccard_sim,
                    "levenshtein_distance": lev_dist,
                    "tokens1": len(tokens1),
                    "tokens2": len(tokens2),
                }

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"TextSimilarityAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for text similarity operations."""
    return TextSimilarityAction().execute(context, params)
