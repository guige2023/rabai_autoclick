"""
API Session Manager Module.

Provides session lifecycle management, pooling, affinity,
and stateful request handling for API clients.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import deque
import logging
import uuid

logger = logging.getLogger(__name__)


class SessionState(Enum):
    """Session state."""
    NEW = "new"
    ACTIVE = "active"
    IDLE = "idle"
    EXPIRED = "expired"
    CLOSED = "closed"


@dataclass
class Session:
    """Container for an API session."""
    session_id: str
    user_id: Optional[str] = None
    state: SessionState = SessionState.NEW
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    request_count: int = 0
    data: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def age(self) -> float:
        """Get session age in seconds."""
        return time.time() - self.created_at
        
    @property
    def idle_time(self) -> float:
        """Get idle time in seconds."""
        return time.time() - self.last_used
        
    @property
    def is_valid(self) -> bool:
        """Check if session is valid."""
        return self.state not in (SessionState.EXPIRED, SessionState.CLOSED)


@dataclass
class SessionConfig:
    """Configuration for session management."""
    max_sessions: int = 1000
    max_idle_time: float = 300.0  # 5 minutes
    max_session_age: float = 3600.0  # 1 hour
    session_timeout: float = 30.0
    enable_pooling: bool = True
    pool_size: int = 100
    sticky_sessions: bool = False
    affinity_key: Optional[str] = None


@dataclass
class SessionStats:
    """Session statistics."""
    total_sessions: int = 0
    active_sessions: int = 0
    idle_sessions: int = 0
    expired_sessions: int = 0
    pooled_sessions: int = 0
    hits: int = 0
    misses: int = 0
    evictions: int = 0


class SessionManager:
    """
    Manages API sessions with pooling and affinity.
    
    Example:
        manager = SessionManager(SessionConfig(
            max_sessions=100,
            enable_pooling=True
        ))
        
        # Get or create session
        session = await manager.get_session("user_123")
        
        # Use session
        session.data["token"] = "abc"
        
        # Release session back to pool
        await manager.release_session(session.session_id)
    """
    
    def __init__(self, config: Optional[SessionConfig] = None) -> None:
        """
        Initialize session manager.
        
        Args:
            config: Session configuration.
        """
        self.config = config or SessionConfig()
        self._sessions: Dict[str, Session] = {}
        self._user_sessions: Dict[str, Set[str]] = {}
        self._pool: deque = deque()
        self._affinity_map: Dict[str, str] = {}  # affinity_key -> session_id
        self._lock = asyncio.Lock()
        self._stats = SessionStats()
        self._cleanup_task: Optional[asyncio.Task] = None
        
    async def get_session(
        self,
        user_id: Optional[str] = None,
        affinity_key: Optional[str] = None,
        create: bool = True,
    ) -> Optional[Session]:
        """
        Get a session, creating if necessary.
        
        Args:
            user_id: Optional user identifier.
            affinity_key: Optional affinity key for sticky sessions.
            create: Whether to create session if not found.
            
        Returns:
            Session object or None.
        """
        async with self._lock:
            # Check affinity first
            if affinity_key and self.config.sticky_sessions:
                if affinity_key in self._affinity_map:
                    session_id = self._affinity_map[affinity_key]
                    if session_id in self._sessions:
                        session = self._sessions[session_id]
                        if session.is_valid:
                            session.last_used = time.time()
                            self._stats.hits += 1
                            return session
                            
            # Check for existing user session
            if user_id and user_id in self._user_sessions:
                for session_id in self._user_sessions[user_id]:
                    if session_id in self._sessions:
                        session = self._sessions[session_id]
                        if session.is_valid:
                            session.last_used = time.time()
                            self._stats.hits += 1
                            return session
                            
            # Try to get from pool
            if self.config.enable_pooling and self._pool:
                session_id = self._pool.popleft()
                if session_id in self._sessions:
                    session = self._sessions[session_id]
                    session.state = SessionState.ACTIVE
                    session.last_used = time.time()
                    self._stats.hits += 1
                    self._stats.pooled_sessions -= 1
                    
                    if user_id:
                        session.user_id = user_id
                        if user_id not in self._user_sessions:
                            self._user_sessions[user_id] = set()
                        self._user_sessions[user_id].add(session_id)
                        
                    if affinity_key:
                        self._affinity_map[affinity_key] = session_id
                        
                    return session
                    
            # Create new session if allowed
            if create:
                if len(self._sessions) >= self.config.max_sessions:
                    await self._evict_oldest()
                    
                session = await self._create_session(user_id)
                
                if affinity_key:
                    self._affinity_map[affinity_key] = session.session_id
                    
                self._stats.misses += 1
                return session
                
            return None
            
    async def _create_session(
        self,
        user_id: Optional[str] = None,
    ) -> Session:
        """Create a new session."""
        session_id = str(uuid.uuid4())
        
        session = Session(
            session_id=session_id,
            user_id=user_id,
            state=SessionState.ACTIVE,
            created_at=time.time(),
            last_used=time.time(),
        )
        
        self._sessions[session_id] = session
        self._stats.total_sessions += 1
        self._stats.active_sessions += 1
        
        if user_id:
            if user_id not in self._user_sessions:
                self._user_sessions[user_id] = set()
            self._user_sessions[user_id].add(session_id)
            
        logger.debug(f"Created session: {session_id}")
        return session
        
    async def release_session(self, session_id: str) -> bool:
        """
        Release session back to pool.
        
        Args:
            session_id: Session to release.
            
        Returns:
            True if released successfully.
        """
        async with self._lock:
            if session_id not in self._sessions:
                return False
                
            session = self._sessions[session_id]
            session.last_used = time.time()
            
            if session.state == SessionState.ACTIVE:
                if self.config.enable_pooling and len(self._pool) < self.config.pool_size:
                    session.state = SessionState.IDLE
                    self._pool.append(session_id)
                    self._stats.active_sessions -= 1
                    self._stats.idle_sessions += 1
                    self._stats.pooled_sessions += 1
                else:
                    self._stats.active_sessions -= 1
                    
            return True
            
    async def close_session(self, session_id: str) -> bool:
        """
        Close and remove a session.
        
        Args:
            session_id: Session to close.
            
        Returns:
            True if closed successfully.
        """
        async with self._lock:
            if session_id not in self._sessions:
                return False
                
            session = self._sessions[session_id]
            session.state = SessionState.CLOSED
            
            # Remove from user sessions
            if session.user_id and session.user_id in self._user_sessions:
                self._user_sessions[session.user_id].discard(session_id)
                
            # Remove from affinity
            for key, sid in list(self._affinity_map.items()):
                if sid == session_id:
                    del self._affinity_map[key]
                    
            # Remove from pool
            if session_id in self._pool:
                self._pool.remove(session_id)
                
            # Update stats
            if session.state == SessionState.ACTIVE:
                self._stats.active_sessions -= 1
            elif session.state == SessionState.IDLE:
                self._stats.idle_sessions -= 1
                self._stats.pooled_sessions -= 1
                
            del self._sessions[session_id]
            logger.debug(f"Closed session: {session_id}")
            return True
            
    async def get_session_data(
        self,
        session_id: str,
        key: str,
        default: Any = None,
    ) -> Any:
        """Get data from session."""
        async with self._lock:
            if session_id in self._sessions:
                return self._sessions[session_id].data.get(key, default)
        return default
        
    async def set_session_data(
        self,
        session_id: str,
        key: str,
        value: Any,
    ) -> bool:
        """Set data in session."""
        async with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].data[key] = value
                return True
        return False
        
    async def _evict_oldest(self) -> None:
        """Evict oldest idle session."""
        oldest_id = None
        oldest_time = float("inf")
        
        for session_id in self._pool:
            if session_id in self._sessions:
                session = self._sessions[session_id]
                if session.last_used < oldest_time:
                    oldest_time = session.last_used
                    oldest_id = session_id
                    
        if oldest_id:
            await self.close_session(oldest_id)
            self._stats.evictions += 1
            
    async def cleanup_expired(self) -> int:
        """
        Clean up expired sessions.
        
        Returns:
            Number of sessions cleaned up.
        """
        async with self._lock:
            expired = []
            
            for session_id, session in self._sessions.items():
                if session.state == SessionState.CLOSED:
                    continue
                    
                if session.age > self.config.max_session_age:
                    expired.append(session_id)
                elif session.idle_time > self.config.max_idle_time:
                    expired.append(session_id)
                    
            for session_id in expired:
                await self.close_session(session_id)
                
            self._stats.expired_sessions = len(expired)
            return len(expired)
            
    def get_stats(self) -> SessionStats:
        """Get session statistics."""
        return SessionStats(
            total_sessions=len(self._sessions),
            active_sessions=self._stats.active_sessions,
            idle_sessions=self._stats.idle_sessions,
            expired_sessions=self._stats.expired_sessions,
            pooled_sessions=self._stats.pooled_sessions,
            hits=self._stats.hits,
            misses=self._stats.misses,
            evictions=self._stats.evictions,
        )
        
    async def start_cleanup_task(self, interval: float = 60.0) -> None:
        """Start background cleanup task."""
        async def cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(interval)
                    cleaned = await self.cleanup_expired()
                    if cleaned > 0:
                        logger.info(f"Cleaned up {cleaned} expired sessions")
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Cleanup task error: {e}")
                    
        self._cleanup_task = asyncio.create_task(cleanup_loop())
        
    async def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass


class SessionPool:
    """
    Connection pool with session affinity.
    
    Example:
        pool = SessionPool(max_size=10)
        
        async with pool.get_connection() as conn:
            await conn.execute("SELECT * FROM users")
    """
    
    def __init__(self, max_size: int = 10) -> None:
        """Initialize session pool."""
        self.max_size = max_size
        self._pool: List[Any] = []
        self._in_use: Set[Any] = set()
        self._lock = asyncio.Lock()
        
    async def get_connection(self) -> Any:
        """Get a connection from the pool."""
        async with self._lock:
            if self._pool:
                conn = self._pool.pop()
                self._in_use.add(conn)
                return conn
                
            if len(self._in_use) < self.max_size:
                conn = await self._create_connection()
                self._in_use.add(conn)
                return conn
                
        # Wait for available connection
        while True:
            await asyncio.sleep(0.1)
            async with self._lock:
                if self._pool:
                    conn = self._pool.pop()
                    self._in_use.add(conn)
                    return conn
                    
    async def release_connection(self, conn: Any) -> None:
        """Release connection back to pool."""
        async with self._lock:
            if conn in self._in_use:
                self._in_use.discard(conn)
                self._pool.append(conn)
                
    async def _create_connection(self) -> Any:
        """Create a new connection."""
        # Placeholder - actual implementation would create real connection
        return object()
        
    async def __aenter__(self) -> "SessionPool":
        return self
        
    async def __aexit__(self, *args: Any) -> None:
        async with self._lock:
            self._pool.extend(self._in_use)
            self._in_use.clear()
