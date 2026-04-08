"""Splitter action module for RabAI AutoClick.

Provides data splitting and partitioning actions for
lists, strings, and structured data.
"""

import sys
import os
import json
import re
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSplitter:
    """Split and partition data structures."""
    
    @staticmethod
    def split_list(data: List[Any], size: int, allow_partial: bool = True) -> List[List[Any]]:
        """Split list into chunks.
        
        Args:
            data: List to split.
            size: Chunk size.
            allow_partial: Allow last chunk smaller than size.
        
        Returns:
            List of chunks.
        """
        if size <= 0:
            return [data]
        
        chunks = []
        for i in range(0, len(data), size):
            chunk = data[i:i + size]
            if chunk or allow_partial:
                chunks.append(chunk)
        
        return chunks
    
    @staticmethod
    def split_dict(data: Dict[str, Any], keys: List[str]) -> List[Dict[str, Any]]:
        """Split dictionary into multiple dicts by keys.
        
        Args:
            data: Dict to split.
            keys: List of keys for each output dict.
        
        Returns:
            List of dictionaries.
        """
        if not isinstance(keys, list) or not isinstance(keys[0], list):
            keys = [keys]
        
        result = []
        for key_group in keys:
            result.append({k: data.get(k) for k in key_group if k in data})
        
        return result
    
    @staticmethod
    def split_by_delimiter(text: str, delimiter: str, max_split: int = -1) -> List[str]:
        """Split text by delimiter.
        
        Args:
            text: Text to split.
            delimiter: Delimiter string.
            max_split: Maximum splits (-1 for all).
        
        Returns:
            List of text parts.
        """
        if max_split > 0:
            parts = text.split(delimiter, max_split)
        else:
            parts = text.split(delimiter)
        
        return parts
    
    @staticmethod
    def split_by_length(text: str, length: int) -> List[str]:
        """Split text into fixed-length chunks.
        
        Args:
            text: Text to split.
            length: Chunk length.
        
        Returns:
            List of text chunks.
        """
        return [text[i:i + length] for i in range(0, len(text), length)]
    
    @staticmethod
    def split_by_pattern(text: str, pattern: str) -> List[str]:
        """Split text by regex pattern.
        
        Args:
            text: Text to split.
            pattern: Regex pattern.
        
        Returns:
            List of text parts.
        """
        try:
            return re.split(pattern, text)
        except re.error:
            return [text]
    
    @staticmethod
    def partition_list(data: List[Any], predicate: Callable[[Any], bool]) -> tuple:
        """Partition list by predicate.
        
        Args:
            data: List to partition.
            predicate: Function that returns True for first partition.
        
        Returns:
            Tuple of (matching, non_matching).
        """
        matching = []
        non_matching = []
        
        for item in data:
            if predicate(item):
                matching.append(item)
            else:
                non_matching.append(item)
        
        return (matching, non_matching)
    
    @staticmethod
    def split_by_index(data: List[Any], indices: List[int]) -> List[List[Any]]:
        """Split list at specific indices.
        
        Args:
            data: List to split.
            indices: Split positions.
        
        Returns:
            List of split parts.
        """
        if not indices:
            return [data]
        
        indices = sorted(set(indices))
        result = []
        prev = 0
        
        for idx in indices:
            if 0 < idx < len(data):
                result.append(data[prev:idx])
                prev = idx
        
        result.append(data[prev:])
        
        return result
    
    @staticmethod
    def split_lines(text: str, strip: bool = True) -> List[str]:
        """Split text into lines.
        
        Args:
            text: Text to split.
            strip: Whether to strip whitespace.
        
        Returns:
            List of lines.
        """
        lines = text.split('\n')
        
        if strip:
            lines = [line.strip() for line in lines]
        
        return [line for line in lines if line]


class SplitListAction(BaseAction):
    """Split list into chunks."""
    action_type = "split_list"
    display_name = "拆分列表"
    description = "将列表拆分为多个块"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split list.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, size, allow_partial.
        
        Returns:
            ActionResult with split chunks.
        """
        data = params.get('data', [])
        size = params.get('size', 10)
        allow_partial = params.get('allow_partial', True)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        try:
            chunks = DataSplitter.split_list(data, size, allow_partial)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(chunks)} chunks",
                data={"chunks": chunks, "chunk_count": len(chunks), "chunk_size": size}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class SplitTextAction(BaseAction):
    """Split text by delimiter."""
    action_type = "split_text"
    display_name = "拆分文本"
    description = "按分隔符拆分文本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split text.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, delimiter, max_split.
        
        Returns:
            ActionResult with text parts.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', ',')
        max_split = params.get('max_split', -1)
        
        if not isinstance(text, str):
            return ActionResult(success=False, message="text must be a string")
        
        try:
            parts = DataSplitter.split_by_delimiter(text, delimiter, max_split)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(parts)} parts",
                data={"parts": parts, "count": len(parts)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class SplitByLengthAction(BaseAction):
    """Split text by fixed length."""
    action_type = "split_by_length"
    display_name = "按长度拆分"
    description = "按固定长度拆分文本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split by length.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, length.
        
        Returns:
            ActionResult with chunks.
        """
        text = params.get('text', '')
        length = params.get('length', 80)
        
        if not isinstance(text, str):
            return ActionResult(success=False, message="text must be a string")
        
        try:
            chunks = DataSplitter.split_by_length(text, length)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(chunks)} chunks",
                data={"chunks": chunks, "count": len(chunks), "chunk_length": length}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class SplitByPatternAction(BaseAction):
    """Split text by regex pattern."""
    action_type = "split_by_pattern"
    display_name = "按模式拆分"
    description = "按正则表达式拆分"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split by pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, pattern.
        
        Returns:
            ActionResult with parts.
        """
        text = params.get('text', '')
        pattern = params.get('pattern', r'\s+')
        
        if not isinstance(text, str):
            return ActionResult(success=False, message="text must be a string")
        
        try:
            parts = DataSplitter.split_by_pattern(text, pattern)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(parts)} parts",
                data={"parts": parts, "count": len(parts)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class PartitionListAction(BaseAction):
    """Partition list by predicate."""
    action_type = "partition_list"
    display_name = "分割列表"
    description = "按条件分割列表"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Partition list.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, predicate.
        
        Returns:
            ActionResult with partitioned lists.
        """
        data = params.get('data', [])
        predicate = params.get('predicate', 'lambda x: bool(x)')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        try:
            if isinstance(predicate, str):
                pred_func = eval(f"lambda x: {predicate}")
            else:
                pred_func = predicate
            
            matching, non_matching = DataSplitter.partition_list(data, pred_func)
            
            return ActionResult(
                success=True,
                message=f"Partitioned: {len(matching)} matching, {len(non_matching)} non-matching",
                data={
                    "matching": matching,
                    "non_matching": non_matching,
                    "matching_count": len(matching),
                    "non_matching_count": len(non_matching)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition error: {str(e)}")


class SplitLinesAction(BaseAction):
    """Split text into lines."""
    action_type = "split_lines"
    display_name = "拆分行"
    description = "将文本拆分为行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split lines.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, strip.
        
        Returns:
            ActionResult with lines.
        """
        text = params.get('text', '')
        strip = params.get('strip', True)
        
        if not isinstance(text, str):
            return ActionResult(success=False, message="text must be a string")
        
        try:
            lines = DataSplitter.split_lines(text, strip)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(lines)} lines",
                data={"lines": lines, "count": len(lines)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")
