"""Memory management utilities for RabAI AutoClick.

Provides caching and memory optimization utilities:
- MemoryManager: General-purpose cache with LRU eviction
- ImageCache: Template image caching
- OCRCache: OCR engine instance caching
"""

import gc
import os
import threading
import time
import weakref
from typing import Any, Callable, Dict, List, Optional


class MemoryManager:
    """Singleton memory manager with cache and garbage collection.
    
    Provides a caching layer with automatic LRU eviction and
    periodic cleanup of dead weak references.
    """
    
    _instance: Optional['MemoryManager'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls) -> 'MemoryManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        
        self._initialized = True
        self._cache: Dict[str, Any] = {}
        self._weak_refs: Dict[str, weakref.ref] = {}
        self._max_cache_size: int = 30
        self._last_cleanup: float = time.time()
        self._cleanup_interval: float = 60
    
    def get_cached(
        self, 
        key: str, 
        loader: Callable[[], Any]
    ) -> Any:
        """Get a cached value, loading it if not present.
        
        Args:
            key: Cache key.
            loader: Function to call if key not in cache.
            
        Returns:
            The cached or newly loaded value.
        """
        # Check strong cache first
        if key in self._cache:
            return self._cache[key]
        
        # Check weak references
        if key in self._weak_refs:
            ref = self._weak_refs[key]()
            if ref is not None:
                return ref
        
        # Load and cache
        value = loader()
        self._cache[key] = value
        self._cleanup_if_needed()
        return value
    
    def set_cached(
        self, 
        key: str, 
        value: Any, 
        weak: bool = False
    ) -> None:
        """Set a cached value.
        
        Args:
            key: Cache key.
            value: Value to cache.
            weak: If True, use weak reference (object can be GC'd).
        """
        if weak:
            self._weak_refs[key] = weakref.ref(value)
        else:
            self._cache[key] = value
            self._cleanup_if_needed()
    
    def _cleanup_if_needed(self) -> None:
        """Evict oldest cache entries if over size limit."""
        now = time.time()
        
        # Evict oldest entries if cache is too large
        if len(self._cache) > self._max_cache_size:
            keys_to_remove = list(self._cache.keys())[
                :len(self._cache) - self._max_cache_size // 2
            ]
            for key in keys_to_remove:
                del self._cache[key]
        
        # Periodic cleanup of dead weak refs
        if now - self._last_cleanup > self._cleanup_interval:
            self._periodic_cleanup()
            self._last_cleanup = now
    
    def _periodic_cleanup(self) -> None:
        """Remove dead weak references and trigger GC."""
        dead_refs: List[str] = [
            k for k, v in self._weak_refs.items() if v() is None
        ]
        for key in dead_refs:
            del self._weak_refs[key]
        
        gc.collect(0)
    
    def clear_cache(self) -> None:
        """Clear all caches and run garbage collection."""
        self._cache.clear()
        self._weak_refs.clear()
        gc.collect()
    
    def get_memory_usage(self) -> Dict[str, int]:
        """Get current memory usage statistics.
        
        Returns:
            Dictionary with rss, vms (MB), and cache_size.
        """
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            return {
                'rss': mem_info.rss // 1024 // 1024,
                'vms': mem_info.vms // 1024 // 1024,
                'cache_size': len(self._cache),
            }
        except ImportError:
            return {'rss': 0, 'vms': 0, 'cache_size': len(self._cache)}
        except Exception:
            return {'rss': 0, 'vms': 0, 'cache_size': 0}
    
    def optimize(self) -> Dict[str, int]:
        """Run memory optimization.
        
        Returns:
            Dictionary with before/after RSS (MB) and freed amount.
        """
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


class ImageCache:
    """Singleton image template cache with LRU eviction.
    
    Caches loaded template images to avoid repeated file I/O
    and CV2 image loading.
    """
    
    _instance: Optional['ImageCache'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls) -> 'ImageCache':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        
        self._initialized = True
        self._templates: Dict[str, Any] = {}
        self._screenshots: Dict[str, Any] = {}
        self._max_templates: int = 10
        self._access_times: Dict[str, float] = {}
    
    def get_template(self, path: str) -> Optional[Any]:
        """Get a cached template image.
        
        Args:
            path: Path to the template image file.
            
        Returns:
            Loaded template image (numpy array), or None if not found.
        """
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
        """Evict the least recently accessed template if at capacity."""
        if len(self._templates) >= self._max_templates:
            oldest_key = min(
                self._access_times, 
                key=self._access_times.get
            )
            del self._templates[oldest_key]
            del self._access_times[oldest_key]
    
    def clear(self) -> None:
        """Clear all cached images."""
        self._templates.clear()
        self._screenshots.clear()
        self._access_times.clear()


class OCRCache:
    """Singleton OCR engine instance cache.
    
    Caches the OCR engine to avoid repeated initialization,
    with idle timeout for automatic cleanup.
    """
    
    _instance: Optional['OCRCache'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls) -> 'OCRCache':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        
        self._initialized = True
        self._ocr_instance: Optional[Any] = None
        self._last_used: float = 0
        self._idle_timeout: float = 300
    
    def get_ocr(self, loader: Callable[[], Any]) -> Optional[Any]:
        """Get the cached OCR instance.
        
        Args:
            loader: Function to create OCR instance (currently unused).
            
        Returns:
            Cached OCR instance, or None if idle timeout exceeded.
        """
        self._last_used = time.time()
        self.check_idle_cleanup()
        return self._ocr_instance
    
    def set_ocr(self, instance: Any) -> None:
        """Set the OCR instance to cache.
        
        Args:
            instance: OCR engine instance to cache.
        """
        self._ocr_instance = instance
        self._last_used = time.time()
    
    def check_idle_cleanup(self) -> None:
        """Clean up OCR instance if idle timeout exceeded."""
        if self._ocr_instance and time.time() - self._last_used > self._idle_timeout:
            self._ocr_instance = None
            gc.collect()


# Global singleton instances
memory_manager: MemoryManager = MemoryManager()
image_cache: ImageCache = ImageCache()
ocr_cache: OCRCache = OCRCache()
