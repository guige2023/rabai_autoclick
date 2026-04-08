"""Session management action module for RabAI AutoClick.

Provides session operations:
- SessionCreateAction: Create session
- SessionGetAction: Get session data
- SessionUpdateAction: Update session
- SessionDeleteAction: Delete session
- SessionListAction: List sessions
- SessionExpireAction: Expire session
- SessionExtendAction: Extend session TTL
- SessionTokenAction: Generate session token
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SessionStore:
    """In-memory session storage."""
    
    _sessions: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def create(cls, session_id: str, data: Dict[str, Any] = None, ttl: int = 3600) -> Dict[str, Any]:
        session = {
            "id": session_id,
            "data": data or {},
            "created_at": time.time(),
            "expires_at": time.time() + ttl,
            "last_accessed": time.time()
        }
        cls._sessions[session_id] = session
        return session
    
    @classmethod
    def get(cls, session_id: str) -> Optional[Dict[str, Any]]:
        if session_id not in cls._sessions:
            return None
        session = cls._sessions[session_id]
        if time.time() > session["expires_at"]:
            del cls._sessions[session_id]
            return None
        session["last_accessed"] = time.time()
        return session
    
    @classmethod
    def update(cls, session_id: str, data: Dict[str, Any]) -> bool:
        if session_id in cls._sessions:
            cls._sessions[session_id]["data"].update(data)
            cls._sessions[session_id]["last_accessed"] = time.time()
            return True
        return False
    
    @classmethod
    def delete(cls, session_id: str) -> bool:
        if session_id in cls._sessions:
            del cls._sessions[session_id]
            return True
        return False
    
    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        cls._cleanup()
        return list(cls._sessions.values())
    
    @classmethod
    def _cleanup(cls) -> None:
        now = time.time()
        expired = [k for k, v in cls._sessions.items() if now > v["expires_at"]]
        for k in expired:
            del cls._sessions[k]


class SessionCreateAction(BaseAction):
    """Create a new session."""
    action_type = "session_create"
    display_name = "创建会话"
    description = "创建新会话"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            session_id = params.get("session_id", f"session-{int(time.time())}")
            data = params.get("data", {})
            ttl = params.get("ttl", 3600)
            
            session = SessionStore.create(session_id, data, ttl)
            
            return ActionResult(
                success=True,
                message=f"Created session: {session_id}",
                data={"session": session}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session create failed: {str(e)}")


class SessionGetAction(BaseAction):
    """Get session data."""
    action_type = "session_get"
    display_name = "获取会话"
    description = "获取会话数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            session_id = params.get("session_id", "")
            
            if not session_id:
                return ActionResult(success=False, message="session_id required")
            
            session = SessionStore.get(session_id)
            
            if not session:
                return ActionResult(success=False, message=f"Session not found: {session_id}")
            
            return ActionResult(
                success=True,
                message=f"Got session: {session_id}",
                data={"session": session}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session get failed: {str(e)}")


class SessionUpdateAction(BaseAction):
    """Update session data."""
    action_type = "session_update"
    display_name = "更新会话"
    description = "更新会话数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            session_id = params.get("session_id", "")
            data = params.get("data", {})
            
            if not session_id:
                return ActionResult(success=False, message="session_id required")
            
            updated = SessionStore.update(session_id, data)
            
            if not updated:
                return ActionResult(success=False, message=f"Session not found: {session_id}")
            
            return ActionResult(
                success=True,
                message=f"Updated session: {session_id}",
                data={"session_id": session_id, "updated": True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session update failed: {str(e)}")


class SessionDeleteAction(BaseAction):
    """Delete session."""
    action_type = "session_delete"
    display_name = "删除会话"
    description = "删除会话"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            session_id = params.get("session_id", "")
            
            if not session_id:
                return ActionResult(success=False, message="session_id required")
            
            deleted = SessionStore.delete(session_id)
            
            return ActionResult(
                success=deleted,
                message=f"Deleted session: {session_id}" if deleted else f"Session not found: {session_id}",
                data={"session_id": session_id, "deleted": deleted}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session delete failed: {str(e)}")


class SessionListAction(BaseAction):
    """List all sessions."""
    action_type = "session_list"
    display_name = "会话列表"
    description = "列出所有会话"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            sessions = SessionStore.list_all()
            
            return ActionResult(
                success=True,
                message=f"Found {len(sessions)} sessions",
                data={"sessions": sessions, "count": len(sessions)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session list failed: {str(e)}")


class SessionExpireAction(BaseAction):
    """Expire a session."""
    action_type = "session_expire"
    display_name = "会话过期"
    description = "使会话过期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            session_id = params.get("session_id", "")
            
            if not session_id:
                return ActionResult(success=False, message="session_id required")
            
            if session_id in SessionStore._sessions:
                SessionStore._sessions[session_id]["expires_at"] = time.time()
                return ActionResult(
                    success=True,
                    message=f"Expired session: {session_id}",
                    data={"session_id": session_id}
                )
            
            return ActionResult(success=False, message=f"Session not found: {session_id}")
        except Exception as e:
            return ActionResult(success=False, message=f"Session expire failed: {str(e)}")


class SessionExtendAction(BaseAction):
    """Extend session TTL."""
    action_type = "session_extend"
    display_name = "延长会话"
    description = "延长会话TTL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            session_id = params.get("session_id", "")
            ttl = params.get("ttl", 3600)
            
            if not session_id:
                return ActionResult(success=False, message="session_id required")
            
            session = SessionStore.get(session_id)
            if not session:
                return ActionResult(success=False, message=f"Session not found: {session_id}")
            
            session["expires_at"] = time.time() + ttl
            return ActionResult(
                success=True,
                message=f"Extended session {session_id} by {ttl}s",
                data={"session_id": session_id, "ttl": ttl}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session extend failed: {str(e)}")


class SessionTokenAction(BaseAction):
    """Generate session token."""
    action_type = "session_token"
    display_name = "生成令牌"
    description = "生成会话令牌"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import hashlib
            import base64
            
            session_id = params.get("session_id", f"session-{int(time.time())}")
            secret = params.get("secret", "default-secret")
            
            data = f"{session_id}:{time.time()}:{secret}"
            token = base64.b64encode(hashlib.sha256(data.encode()).digest()).decode()[:32]
            
            return ActionResult(
                success=True,
                message=f"Generated token for session: {session_id}",
                data={"session_id": session_id, "token": token}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Session token failed: {str(e)}")
