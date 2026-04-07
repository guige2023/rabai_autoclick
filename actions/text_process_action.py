"""Text processing action module for RabAI AutoClick.

Provides text processing operations:
- TextNormalizeAction: Normalize text
- TextSearchAction: Search text patterns
- TextReplaceAction: Find and replace text
- TextSplitAction: Split text
- TextExtractAction: Extract text patterns
- TextSummarizeAction: Summarize text
"""

import re
from typing import Any, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextNormalizeAction(BaseAction):
    """Normalize text."""
    action_type = "text_normalize"
    display_name = "文本规范化"
    description = "规范化文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            operations = params.get("operations", ["trim"])

            if not text:
                return ActionResult(success=False, message="text is required")

            result = text

            for op in operations:
                if op == "trim":
                    result = result.strip()
                elif op == "lower":
                    result = result.lower()
                elif op == "upper":
                    result = result.upper()
                elif op == "title":
                    result = result.title()
                elif op == "remove_extra_spaces":
                    result = re.sub(r"\s+", " ", result)
                elif op == "remove_punctuation":
                    result = re.sub(r"[^\w\s]", "", result)
                elif op == "remove_digits":
                    result = re.sub(r"\d", "", result)
                elif op == "normalize_unicode":
                    import unicodedata
                    result = unicodedata.normalize("NFKC", result)
                elif op == "remove_newlines":
                    result = re.sub(r"[\r\n]+", " ", result)
                elif op == "remove_tabs":
                    result = re.sub(r"\t+", " ", result)
                elif op == "remove_special":
                    result = re.sub(r"[^a-zA-Z0-9\s]", "", result)
                elif op == "normalize_whitespace":
                    result = " ".join(result.split())

            return ActionResult(
                success=True,
                message=f"Normalized text from {len(text)} to {len(result)} chars",
                data={"text": result, "original_length": len(text), "normalized_length": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Normalize error: {str(e)}")


class TextSearchAction(BaseAction):
    """Search text patterns."""
    action_type = "text_search"
    display_name = "文本搜索"
    description = "搜索文本模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            regex = params.get("regex", False)
            case_sensitive = params.get("case_sensitive", False)
            all_matches = params.get("all_matches", True)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            flags = 0 if case_sensitive else re.IGNORECASE

            if regex:
                try:
                    compiled = re.compile(pattern, flags)
                except re.error as e:
                    return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
            else:
                if all_matches:
                    compiled = re.compile(re.escape(pattern), flags)
                else:
                    compiled = re.compile(re.escape(pattern), flags)

            matches = []
            if all_matches:
                for match in compiled.finditer(text):
                    matches.append({
                        "match": match.group(),
                        "start": match.start(),
                        "end": match.end(),
                        "groups": match.groups()
                    })
            else:
                match = compiled.search(text)
                if match:
                    matches.append({
                        "match": match.group(),
                        "start": match.start(),
                        "end": match.end(),
                        "groups": match.groups()
                    })

            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={"matches": matches, "count": len(matches), "pattern": pattern}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Search error: {str(e)}")


class TextReplaceAction(BaseAction):
    """Find and replace text."""
    action_type = "text_replace"
    display_name = "文本替换"
    description = "查找和替换文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            replacement = params.get("replacement", "")
            regex = params.get("regex", False)
            case_sensitive = params.get("case_sensitive", False)
            count = params.get("count", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            flags = 0 if case_sensitive else re.IGNORECASE

            if regex:
                try:
                    compiled = re.compile(pattern, flags)
                except re.error as e:
                    return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
                result = compiled.sub(replacement, text, count=count or 0)
            else:
                if count > 0:
                    result = text
                    replaced = 0
                    start = 0
                    while replaced < count:
                        idx = result.lower().find(pattern.lower(), start) if not case_sensitive else result.find(pattern, start)
                        if idx == -1:
                            break
                        result = result[:idx] + replacement + result[idx + len(pattern):]
                        start = idx + len(replacement)
                        replaced += 1
                else:
                    if case_sensitive:
                        result = text.replace(pattern, replacement)
                    else:
                        result = re.sub(re.escape(pattern), replacement, text, flags=flags)

            return ActionResult(
                success=True,
                message=f"Replaced text",
                data={"text": result, "original_length": len(text), "result_length": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Replace error: {str(e)}")


class TextSplitAction(BaseAction):
    """Split text."""
    action_type = "text_split"
    display_name = "文本分割"
    description = "分割文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            delimiter = params.get("delimiter", None)
            pattern = params.get("pattern", None)
            max_split = params.get("max_split", 0)
            lines = params.get("lines", False)
            chars = params.get("chars", 0)
            words = params.get("words", 0)

            if not text:
                return ActionResult(success=False, message="text is required")

            if lines:
                parts = text.split("\n")
                if max_split > 0:
                    parts = parts[:max_split] + ["\n".join(parts[max_split:])] if len(parts) > max_split else parts
                return ActionResult(
                    success=True,
                    message=f"Split into {len(parts)} lines",
                    data={"parts": parts, "count": len(parts)}
                )

            elif chars > 0:
                parts = [text[i:i+chars] for i in range(0, len(text), chars)]
                if max_split > 0:
                    parts = parts[:max_split]
                return ActionResult(
                    success=True,
                    message=f"Split into {len(parts)} chunks",
                    data={"parts": parts, "count": len(parts)}
                )

            elif words > 0:
                word_list = text.split()
                parts = [" ".join(word_list[i:i+words]) for i in range(0, len(word_list), words)]
                if max_split > 0:
                    parts = parts[:max_split]
                return ActionResult(
                    success=True,
                    message=f"Split into {len(parts)} word groups",
                    data={"parts": parts, "count": len(parts)}
                )

            elif pattern:
                parts = re.split(pattern, text, maxsplit=max_split)
                return ActionResult(
                    success=True,
                    message=f"Split into {len(parts)} parts",
                    data={"parts": parts, "count": len(parts)}
                )

            elif delimiter is not None:
                parts = text.split(delimiter)
                if max_split > 0:
                    parts = parts[:max_split] + [delimiter.join(parts[max_split:])] if len(parts) > max_split else parts
                return ActionResult(
                    success=True,
                    message=f"Split into {len(parts)} parts",
                    data={"parts": parts, "count": len(parts)}
                )

            else:
                return ActionResult(success=False, message="No split method specified")

        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class TextExtractAction(BaseAction):
    """Extract text patterns."""
    action_type = "text_extract"
    display_name: "文本提取"
    description = "提取文本模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            regex = params.get("regex", True)
            group = params.get("group", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            if regex:
                try:
                    compiled = re.compile(pattern)
                except re.error as e:
                    return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
            else:
                compiled = re.compile(re.escape(pattern))

            matches = []
            for match in compiled.finditer(text):
                if group == 0:
                    matches.append(match.group())
                elif group <= len(match.groups()):
                    matches.append(match.groups()[group - 1])
                else:
                    matches.append(match.group())

            return ActionResult(
                success=True,
                message=f"Extracted {len(matches)} matches",
                data={"extracted": matches, "count": len(matches)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Extract error: {str(e)}")


class TextSummarizeAction(BaseAction):
    """Summarize text."""
    action_type = "text_summarize"
    display_name = "文本摘要"
    description = "生成文本摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            method = params.get("method", "sentence")
            max_sentences = params.get("max_sentences", 3)
            max_words = params.get("max_words", 100)

            if not text:
                return ActionResult(success=False, message="text is required")

            if method == "sentence":
                sentences = re.split(r"[.!?]+", text)
                sentences = [s.strip() for s in sentences if s.strip()]

                if len(sentences) <= max_sentences:
                    summary = " ".join(sentences)
                else:
                    words_per_sentence = len(" ".join(sentences).split()) / len(sentences)
                    target_words = max_sentences * words_per_sentence

                    scored = []
                    for sent in sentences:
                        words = sent.split()
                        word_count = len(words)
                        score = word_count / (target_words / max_sentences)
                        scored.append((score, sent))

                    scored.sort(key=lambda x: x[0], reverse=True)
                    top_sentences = [s for _, s in scored[:max_sentences]]
                    top_sentences.sort(key=lambda s: text.find(s))
                    summary = " ".join(top_sentences)

            elif method == "word_count":
                words = text.split()
                if len(words) <= max_words:
                    summary = text
                else:
                    word_freq = {}
                    for word in words:
                        word_lower = word.lower().strip(".,!?;:'\"")
                        if len(word_lower) > 3:
                            word_freq[word_lower] = word_freq.get(word_lower, 0) + 1

                    important_words = sorted(word_freq.keys(), key=lambda w: word_freq[w], reverse=True)[:20]
                    summary_words = []
                    for word in words[:max_words]:
                        if word.lower().strip(".,!?;:'\"") in important_words:
                            summary_words.append(word)
                        if len(summary_words) >= max_words:
                            break
                    summary = " ".join(summary_words[:max_words])

            elif method == "first_n":
                lines = text.split("\n")
                summary = "\n".join(lines[:max_sentences])

            else:
                summary = text[:max_words] + "..." if len(text) > max_words else text

            return ActionResult(
                success=True,
                message=f"Summarized to {len(summary)} chars",
                data={"summary": summary, "original_length": len(text), "summary_length": len(summary)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Summarize error: {str(e)}")
