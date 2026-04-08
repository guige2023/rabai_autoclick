"""Telegram action module for RabAI AutoClick.

Provides Telegram Bot API operations including sending messages,
media, stickers, locations, and managing groups.
"""

import os
import sys
import time
import json
import base64
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TelegramClient:
    """Telegram Bot API client.
    
    Provides methods for interacting with the Telegram Bot API.
    """
    
    API_BASE = "https://api.telegram.org/bot"
    
    def __init__(self, token: str) -> None:
        """Initialize Telegram client.
        
        Args:
            token: Telegram bot token from @BotFather.
        """
        self.token = token
        self.api_base = f"{self.API_BASE}{token}"
    
    def _request(
        self,
        method: str,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make an API request to Telegram.
        
        Args:
            method: Bot API method name.
            data: Optional method parameters.
            files: Optional files to upload.
            
        Returns:
            Parsed JSON response.
            
        Raises:
            Exception: If the API request fails.
        """
        url = f"{self.api_base}/{method}"
        
        if files:
            import multipart
            body, content_type = multipart.encode(url, data or {}, files)
            headers = {"Content-Type": content_type}
            req = Request(url, data=body, headers=headers, method="POST")
        else:
            body = json.dumps(data or {}).encode("utf-8") if data else None
            headers = {"Content-Type": "application/json"}
            req = Request(url, data=body, headers=headers, method="POST")
        
        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                
                if not result.get("ok", False):
                    raise Exception(f"Telegram API error: {result.get('description', 'Unknown error')}")
                
                return result.get("result", {})
        
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            try:
                error_data = json.loads(error_body)
                raise Exception(f"Telegram API error: {error_data.get('description', str(e))}")
            except json.JSONDecodeError:
                raise Exception(f"Telegram API HTTP error: {error_body[:500]}")
    
    def get_me(self) -> Dict[str, Any]:
        """Get information about the bot.
        
        Returns:
            Bot information dictionary.
        """
        return self._request("getMe")
    
    def get_updates(
        self,
        offset: Optional[int] = None,
        limit: int = 100,
        timeout: int = 0
    ) -> List[Dict[str, Any]]:
        """Get incoming updates.
        
        Args:
            offset: Update ID to start from.
            limit: Maximum number of updates to return.
            timeout: Timeout for long polling.
            
        Returns:
            List of updates.
        """
        data: Dict[str, Any] = {"limit": limit, "timeout": timeout}
        if offset:
            data["offset"] = offset
        
        return self._request("getUpdates", data=data)
    
    def send_message(
        self,
        chat_id: Union[int, str],
        text: str,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = False,
        disable_notification: bool = False,
        reply_to_message_id: Optional[int] = None,
        reply_markup: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Send a text message.
        
        Args:
            chat_id: Target chat ID.
            text: Message text.
            parse_mode: Parse mode ('MarkdownV2', 'HTML', 'Markdown').
            disable_web_page_preview: Disable link previews.
            disable_notification: Send silently.
            reply_to_message_id: Reply to message ID.
            reply_markup: Inline keyboard markup.
            
        Returns:
            Sent message object.
        """
        data: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
            "disable_notification": disable_notification
        }
        
        if parse_mode:
            data["parse_mode"] = parse_mode
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        if reply_markup:
            data["reply_markup"] = reply_markup
        
        return self._request("sendMessage", data=data)
    
    def send_photo(
        self,
        chat_id: Union[int, str],
        photo: Union[str, bytes],
        caption: Optional[str] = None,
        parse_mode: Optional[str] = None,
        disable_notification: bool = False,
        reply_to_message_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Send a photo.
        
        Args:
            chat_id: Target chat ID.
            photo: Photo file path or URL.
            caption: Optional photo caption.
            parse_mode: Parse mode.
            disable_notification: Send silently.
            reply_to_message_id: Reply to message ID.
            
        Returns:
            Sent message object.
        """
        data: Dict[str, Any] = {
            "chat_id": chat_id,
            "disable_notification": disable_notification
        }
        
        files: Optional[Dict[str, Any]] = None
        
        if caption:
            data["caption"] = caption
            if parse_mode:
                data["parse_mode"] = parse_mode
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        
        if isinstance(photo, str):
            if photo.startswith("http://") or photo.startswith("https://"):
                data["photo"] = photo
            elif os.path.exists(photo):
                data["photo"] = photo
            else:
                data["photo"] = photo
        else:
            files = {"photo": ("photo.jpg", photo, "image/jpeg")}
        
        if files:
            return self._request("sendPhoto", data=data, files=files)
        else:
            return self._request("sendPhoto", data=data)
    
    def send_document(
        self,
        chat_id: Union[int, str],
        document: Union[str, bytes],
        filename: Optional[str] = None,
        caption: Optional[str] = None,
        disable_notification: bool = False
    ) -> Dict[str, Any]:
        """Send a document.
        
        Args:
            chat_id: Target chat ID.
            document: Document file path or URL.
            filename: Optional filename.
            caption: Optional caption.
            disable_notification: Send silently.
            
        Returns:
            Sent message object.
        """
        data: Dict[str, Any] = {
            "chat_id": chat_id,
            "disable_notification": disable_notification
        }
        
        files: Optional[Dict[str, Any]] = None
        
        if caption:
            data["caption"] = caption
        if isinstance(document, str) and os.path.exists(document):
            fname = filename or os.path.basename(document)
            files = {"document": (fname, open(document, "rb"), "application/octet-stream")}
        elif isinstance(document, bytes):
            fname = filename or "document"
            files = {"document": (fname, document, "application/octet-stream")}
        else:
            data["document"] = document
        
        if files:
            return self._request("sendDocument", data=data, files=files)
        else:
            return self._request("sendDocument", data=data)
    
    def send_location(
        self,
        chat_id: Union[int, str],
        latitude: float,
        longitude: float,
        disable_notification: bool = False,
        reply_to_message_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Send a location.
        
        Args:
            chat_id: Target chat ID.
            latitude: Latitude coordinate.
            longitude: Longitude coordinate.
            disable_notification: Send silently.
            reply_to_message_id: Reply to message ID.
            
        Returns:
            Sent message object.
        """
        data: Dict[str, Any] = {
            "chat_id": chat_id,
            "latitude": latitude,
            "longitude": longitude,
            "disable_notification": disable_notification
        }
        
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        
        return self._request("sendLocation", data=data)
    
    def send_sticker(
        self,
        chat_id: Union[int, str],
        sticker: Union[str, bytes],
        disable_notification: bool = False
    ) -> Dict[str, Any]:
        """Send a sticker.
        
        Args:
            chat_id: Target chat ID.
            sticker: Sticker file path, URL, or file_id.
            disable_notification: Send silently.
            
        Returns:
            Sent message object.
        """
        data: Dict[str, Any] = {
            "chat_id": chat_id,
            "disable_notification": disable_notification
        }
        
        files: Optional[Dict[str, Any]] = None
        
        if isinstance(sticker, str) and os.path.exists(sticker):
            files = {"sticker": (os.path.basename(sticker), open(sticker, "rb"), "image/webp")}
        elif isinstance(sticker, bytes):
            files = {"sticker": ("sticker.webp", sticker, "image/webp")}
        else:
            data["sticker"] = sticker
        
        if files:
            return self._request("sendSticker", data=data, files=files)
        else:
            return self._request("sendSticker", data=data)
    
    def get_chat(self, chat_id: Union[int, str]) -> Dict[str, Any]:
        """Get chat information.
        
        Args:
            chat_id: Chat ID.
            
        Returns:
            Chat information.
        """
        return self._request("getChat", data={"chat_id": chat_id})
    
    def get_chat_members_count(self, chat_id: Union[int, str]) -> int:
        """Get the number of members in a chat.
        
        Args:
            chat_id: Chat ID.
            
        Returns:
            Member count.
        """
        return self._request("getChatMembersCount", data={"chat_id": chat_id})
    
    def leave_chat(self, chat_id: Union[int, str]) -> bool:
        """Leave a chat.
        
        Args:
            chat_id: Chat ID.
            
        Returns:
            True if left successfully.
        """
        return self._request("leaveChat", data={"chat_id": chat_id})
    
    def kick_chat_member(
        self,
        chat_id: Union[int, str],
        user_id: int,
        until_date: Optional[int] = None
    ) -> bool:
        """Kick a user from a chat.
        
        Args:
            chat_id: Chat ID.
            user_id: User ID to kick.
            until_date: Optional ban until timestamp.
            
        Returns:
            True if kicked successfully.
        """
        data: Dict[str, Any] = {"chat_id": chat_id, "user_id": user_id}
        if until_date:
            data["until_date"] = until_date
        
        return self._request("kickChatMember", data=data)
    
    def unban_chat_member(self, chat_id: Union[int, str], user_id: int) -> bool:
        """Unban a user from a chat.
        
        Args:
            chat_id: Chat ID.
            user_id: User ID to unban.
            
        Returns:
            True if unbanned successfully.
        """
        return self._request("unbanChatMember", data={
            "chat_id": chat_id,
            "user_id": user_id
        })
    
    def answer_callback_query(
        self,
        callback_query_id: str,
        text: Optional[str] = None,
        show_alert: bool = False
    ) -> bool:
        """Answer a callback query.
        
        Args:
            callback_query_id: Callback query ID.
            text: Optional response text.
            show_alert: Whether to show as alert.
            
        Returns:
            True if answered successfully.
        """
        data: Dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text
            data["show_alert"] = show_alert
        
        return self._request("answerCallbackQuery", data=data)


class TelegramAction(BaseAction):
    """Telegram action for bot operations.
    
    Supports sending messages, media, and chat management.
    """
    action_type: str = "telegram"
    display_name: str = "Telegram动作"
    description: str = "Telegram机器人操作，支持消息、媒体和群组管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[TelegramClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Telegram operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                self._client = None
                return ActionResult(
                    success=True,
                    message="Telegram client disconnected",
                    duration=time.time() - start_time
                )
            elif operation == "get_me":
                return self._get_me(start_time)
            elif operation == "send_message":
                return self._send_message(params, start_time)
            elif operation == "send_photo":
                return self._send_photo(params, start_time)
            elif operation == "send_document":
                return self._send_document(params, start_time)
            elif operation == "send_location":
                return self._send_location(params, start_time)
            elif operation == "send_sticker":
                return self._send_sticker(params, start_time)
            elif operation == "get_chat":
                return self._get_chat(params, start_time)
            elif operation == "get_chat_members":
                return self._get_chat_members(params, start_time)
            elif operation == "leave_chat":
                return self._leave_chat(params, start_time)
            elif operation == "kick_member":
                return self._kick_member(params, start_time)
            elif operation == "unban_member":
                return self._unban_member(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Telegram operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Telegram bot API."""
        token = params.get("token", "")
        
        if not token:
            return ActionResult(
                success=False,
                message="Telegram bot token is required",
                duration=time.time() - start_time
            )
        
        self._client = TelegramClient(token=token)
        
        try:
            me = self._client.get_me()
            username = me.get("username", "unknown")
            
            return ActionResult(
                success=True,
                message=f"Connected to Telegram bot @{username}",
                data={"username": username, "first_name": me.get("first_name")},
                duration=time.time() - start_time
            )
        except Exception as e:
            self._client = None
            return ActionResult(
                success=False,
                message=f"Failed to connect: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _require_client(self) -> TelegramClient:
        """Ensure a Telegram client exists."""
        if not self._client:
            raise RuntimeError("Not connected to Telegram. Use 'connect' operation first.")
        return self._client
    
    def _get_me(self, start_time: float) -> ActionResult:
        """Get bot information."""
        client = self._require_client()
        
        try:
            me = client.get_me()
            
            return ActionResult(
                success=True,
                message=f"Bot: @{me.get('username')}",
                data=me,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get bot info: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _send_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a text message."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        text = params.get("text", "")
        
        if not chat_id or not text:
            return ActionResult(
                success=False,
                message="chat_id and text are required",
                duration=time.time() - start_time
            )
        
        try:
            message = client.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=params.get("parse_mode"),
                disable_web_page_preview=params.get("disable_web_page_preview", False),
                disable_notification=params.get("disable_notification", False),
                reply_to_message_id=params.get("reply_to_message_id")
            )
            
            return ActionResult(
                success=True,
                message=f"Sent message to chat {chat_id}",
                data={"message_id": message.get("message_id")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to send message: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _send_photo(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a photo."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        photo = params.get("photo", "")
        
        if not chat_id or not photo:
            return ActionResult(
                success=False,
                message="chat_id and photo are required",
                duration=time.time() - start_time
            )
        
        try:
            message = client.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=params.get("caption"),
                parse_mode=params.get("parse_mode"),
                disable_notification=params.get("disable_notification", False),
                reply_to_message_id=params.get("reply_to_message_id")
            )
            
            return ActionResult(
                success=True,
                message=f"Sent photo to chat {chat_id}",
                data={"message_id": message.get("message_id")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to send photo: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _send_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a document."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        document = params.get("document", "")
        
        if not chat_id or not document:
            return ActionResult(
                success=False,
                message="chat_id and document are required",
                duration=time.time() - start_time
            )
        
        try:
            message = client.send_document(
                chat_id=chat_id,
                document=document,
                filename=params.get("filename"),
                caption=params.get("caption"),
                disable_notification=params.get("disable_notification", False)
            )
            
            return ActionResult(
                success=True,
                message=f"Sent document to chat {chat_id}",
                data={"message_id": message.get("message_id")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to send document: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _send_location(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a location."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        latitude = params.get("latitude", 0)
        longitude = params.get("longitude", 0)
        
        if not chat_id:
            return ActionResult(
                success=False,
                message="chat_id is required",
                duration=time.time() - start_time
            )
        
        try:
            message = client.send_location(
                chat_id=chat_id,
                latitude=latitude,
                longitude=longitude,
                disable_notification=params.get("disable_notification", False),
                reply_to_message_id=params.get("reply_to_message_id")
            )
            
            return ActionResult(
                success=True,
                message=f"Sent location to chat {chat_id}",
                data={"message_id": message.get("message_id")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to send location: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _send_sticker(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a sticker."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        sticker = params.get("sticker", "")
        
        if not chat_id or not sticker:
            return ActionResult(
                success=False,
                message="chat_id and sticker are required",
                duration=time.time() - start_time
            )
        
        try:
            message = client.send_sticker(
                chat_id=chat_id,
                sticker=sticker,
                disable_notification=params.get("disable_notification", False)
            )
            
            return ActionResult(
                success=True,
                message=f"Sent sticker to chat {chat_id}",
                data={"message_id": message.get("message_id")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to send sticker: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_chat(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get chat information."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        
        if not chat_id:
            return ActionResult(
                success=False,
                message="chat_id is required",
                duration=time.time() - start_time
            )
        
        try:
            chat = client.get_chat(chat_id)
            
            return ActionResult(
                success=True,
                message=f"Retrieved chat info: {chat.get('title', chat.get('username', chat_id))}",
                data=chat,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get chat: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_chat_members(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get chat member count."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        
        if not chat_id:
            return ActionResult(
                success=False,
                message="chat_id is required",
                duration=time.time() - start_time
            )
        
        try:
            count = client.get_chat_members_count(chat_id)
            
            return ActionResult(
                success=True,
                message=f"Chat {chat_id} has {count} members",
                data={"chat_id": chat_id, "count": count},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get member count: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _leave_chat(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Leave a chat."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        
        if not chat_id:
            return ActionResult(
                success=False,
                message="chat_id is required",
                duration=time.time() - start_time
            )
        
        try:
            client.leave_chat(chat_id)
            
            return ActionResult(
                success=True,
                message=f"Left chat {chat_id}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to leave chat: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _kick_member(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Kick a user from a chat."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        user_id = params.get("user_id")
        
        if not chat_id or not user_id:
            return ActionResult(
                success=False,
                message="chat_id and user_id are required",
                duration=time.time() - start_time
            )
        
        try:
            client.kick_chat_member(
                chat_id=chat_id,
                user_id=int(user_id),
                until_date=params.get("until_date")
            )
            
            return ActionResult(
                success=True,
                message=f"Kicked user {user_id} from chat {chat_id}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to kick member: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _unban_member(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Unban a user from a chat."""
        client = self._require_client()
        chat_id = params.get("chat_id", "")
        user_id = params.get("user_id")
        
        if not chat_id or not user_id:
            return ActionResult(
                success=False,
                message="chat_id and user_id are required",
                duration=time.time() - start_time
            )
        
        try:
            client.unban_chat_member(chat_id=chat_id, user_id=int(user_id))
            
            return ActionResult(
                success=True,
                message=f"Unbanned user {user_id} from chat {chat_id}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to unban member: {str(e)}",
                duration=time.time() - start_time
            )
