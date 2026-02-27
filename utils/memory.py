import gc
import os
import sys
import weakref
from typing import Dict, Any, Optional, Callable
from functools import lru_cache
import threading


class MemoryManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._cache: Dict[str, Any] = {}
        self._weak_refs: Dict[str, weakref.ref] = {}
        self._max_cache_size = 50
    
    def get_cached(self, key: str, loader: Callable[[], Any]) -> Any:
        if key in self._cache:
            return self._cache[key]
        
        if key in self._weak_refs:
            ref = self._weak_refs[key]()
            if ref is not None:
                return ref
        
        value = loader()
        self._cache[key] = value
        self._cleanup_if_needed()
        return value
    
    def set_cached(self, key: str, value: Any, weak: bool = False) -> None:
        if weak:
            self._weak_refs[key] = weakref.ref(value)
        else:
            self._cache[key] = value
            self._cleanup_if_needed()
    
    def _cleanup_if_needed(self) -> None:
        if len(self._cache) > self._max_cache_size:
            keys_to_remove = list(self._cache.keys())[:len(self._cache) - self._max_cache_size // 2]
            for key in keys_to_remove:
                del self._cache[key]
    
    def clear_cache(self) -> None:
        self._cache.clear()
        self._weak_refs.clear()
        gc.collect()
    
    def get_memory_usage(self) -> Dict[str, int]:
        import psutil
        process = psutil.Process(os.getpid())
        return {
            'rss': process.memory_info().rss // 1024 // 1024,
            'vms': process.memory_info().vms // 1024 // 1024,
            'cache_size': len(self._cache),
        }
    
    def optimize(self) -> None:
        self._cleanup_if_needed()
        gc.collect()


memory_manager = MemoryManager()


class ImageCache:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._templates: Dict[str, Any] = {}
        self._screenshots: Dict[str, Any] = {}
        self._max_templates = 20
    
    def get_template(self, path: str) -> Optional[Any]:
        if path in self._templates:
            return self._templates[path]
        
        if not os.path.exists(path):
            return None
        
        try:
            import cv2
            template = cv2.imread(path)
            if template is not None:
                if len(self._templates) >= self._max_templates:
                    oldest_key = next(iter(self._templates))
                    del self._templates[oldest_key]
                self._templates[path] = template
            return template
        except Exception:
            return None
    
    def clear(self) -> None:
        self._templates.clear()
        self._screenshots.clear()


image_cache = ImageCache()
