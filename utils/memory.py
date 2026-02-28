import gc
import os
import sys
import weakref
from typing import Dict, Any, Optional, Callable
from functools import lru_cache
import threading
import time


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
        self._max_cache_size = 30
        self._last_cleanup = time.time()
        self._cleanup_interval = 60
    
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
        now = time.time()
        if len(self._cache) > self._max_cache_size:
            keys_to_remove = list(self._cache.keys())[:len(self._cache) - self._max_cache_size // 2]
            for key in keys_to_remove:
                del self._cache[key]
        
        if now - self._last_cleanup > self._cleanup_interval:
            self._periodic_cleanup()
            self._last_cleanup = now
    
    def _periodic_cleanup(self) -> None:
        dead_refs = [k for k, v in self._weak_refs.items() if v() is None]
        for key in dead_refs:
            del self._weak_refs[key]
        
        gc.collect(0)
    
    def clear_cache(self) -> None:
        self._cache.clear()
        self._weak_refs.clear()
        gc.collect()
    
    def get_memory_usage(self) -> Dict[str, int]:
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return {
                'rss': process.memory_info().rss // 1024 // 1024,
                'vms': process.memory_info().vms // 1024 // 1024,
                'cache_size': len(self._cache),
            }
        except ImportError:
            return {'rss': 0, 'vms': 0, 'cache_size': len(self._cache)}
        except Exception:
            return {'rss': 0, 'vms': 0, 'cache_size': 0}
    
    def optimize(self) -> Dict[str, Any]:
        before = self.get_memory_usage()['rss']
        
        self._cleanup_if_needed()
        self._periodic_cleanup()
        
        gc.collect(1)
        gc.collect(2)
        
        after = self.get_memory_usage()['rss']
        
        return {
            'before': before,
            'after': after,
            'freed': before - after
        }


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
        self._max_templates = 10
        self._access_times: Dict[str, float] = {}
    
    def get_template(self, path: str) -> Optional[Any]:
        if path in self._templates:
            self._access_times[path] = time.time()
            return self._templates[path]
        
        if not os.path.exists(path):
            return None
        
        try:
            import cv2
            template = cv2.imread(path)
            if template is not None:
                self._evict_oldest()
                self._templates[path] = template
                self._access_times[path] = time.time()
            return template
        except Exception:
            return None
    
    def _evict_oldest(self) -> None:
        if len(self._templates) >= self._max_templates:
            oldest_key = min(self._access_times, key=self._access_times.get)
            del self._templates[oldest_key]
            del self._access_times[oldest_key]
    
    def clear(self) -> None:
        self._templates.clear()
        self._screenshots.clear()
        self._access_times.clear()


image_cache = ImageCache()


class OCRCache:
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
        self._ocr_instance = None
        self._last_used = 0
        self._idle_timeout = 300
    
    def get_ocr(self, loader: Callable):
        self._last_used = time.time()
        return self._ocr_instance
    
    def set_ocr(self, instance):
        self._ocr_instance = instance
        self._last_used = time.time()
    
    def check_idle_cleanup(self):
        if self._ocr_instance and time.time() - self._last_used > self._idle_timeout:
            self._ocr_instance = None
            gc.collect()


ocr_cache = OCRCache()
