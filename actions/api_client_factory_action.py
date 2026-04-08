"""API Client Factory Action Module. Creates pre-configured API client instances."""
import sys, os, time, threading, base64
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class ClientConfig:
    base_url: str; timeout_seconds: float = 30.0; max_retries: int = 3
    retry_delay: float = 1.0; auth_type: str = "none"
    auth_credentials: dict = field(default_factory=dict)
    default_headers: dict = field(default_factory=dict)

class APIClientFactoryAction(BaseAction):
    action_type = "api_client_factory"; display_name = "API客户端工厂"
    description = "创建预配置的API客户端"
    def __init__(self) -> None:
        super().__init__(); self._lock = threading.Lock()
        self._clients = {}
    def register_client(self, name: str, config: ClientConfig) -> None:
        with self._lock: self._clients[name] = config
    def _build_headers(self, config: ClientConfig) -> dict:
        headers = dict(config.default_headers)
        if config.auth_type == "bearer": headers["Authorization"] = f"Bearer {config.auth_credentials.get('token','')}"
        elif config.auth_type == "api_key":
            header = config.auth_credentials.get("header", "X-API-Key")
            headers[header] = config.auth_credentials.get("key", "")
        elif config.auth_type == "basic":
            creds = f"{config.auth_credentials.get('user','')}:{config.auth_credentials.get('password','')}"
            headers["Authorization"] = f"Basic {base64.b64encode(creds.encode()).decode()}"
        return headers
    def _request(self, config: ClientConfig, method: str, path: str, **kwargs) -> tuple:
        import urllib.request, urllib.error
        url = config.base_url.rstrip("/") + "/" + path.lstrip("/")
        headers = self._build_headers(config)
        headers.update(kwargs.pop("headers", {}))
        body = kwargs.pop("body", None)
        encoded = json.dumps(body).encode() if body else None
        if body and "Content-Type" not in headers: headers["Content-Type"] = "application/json"
        last_error = None
        for attempt in range(config.max_retries):
            try:
                req = urllib.request.Request(url, data=encoded, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=config.timeout_seconds) as response:
                    try: body_resp = json.loads(response.read())
                    except: body_resp = response.read().decode()
                    return response.status, body_resp
            except urllib.error.HTTPError as e:
                last_error = e
                if e.code >= 500 and attempt < config.max_retries - 1:
                    time.sleep(config.retry_delay * (attempt+1)); continue
                return e.code, str(e)
            except Exception as e:
                last_error = e
                if attempt < config.max_retries - 1: time.sleep(config.retry_delay*(attempt+1)); continue
                return 0, str(e)
        return 0, str(last_error)
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "list")
        if mode == "list":
            with self._lock: names = list(self._clients.keys())
            return ActionResult(success=True, message=f"{len(names)} clients registered")
        if mode == "register":
            config = ClientConfig(base_url=params.get("base_url",""),
                                  timeout_seconds=params.get("timeout_seconds", 30.0),
                                  max_retries=params.get("max_retries", 3),
                                  retry_delay=params.get("retry_delay", 1.0),
                                  auth_type=params.get("auth_type","none"),
                                  auth_credentials=params.get("auth_credentials",{}),
                                  default_headers=params.get("default_headers",{}))
            self.register_client(params.get("name","default"), config)
            return ActionResult(success=True, message=f"Client '{params.get('name')}' registered")
        name = params.get("name", "default")
        with self._lock:
            if name not in self._clients: return ActionResult(success=False, message=f"Client '{name}' not registered")
            config = self._clients[name]
        import json
        status, response = self._request(config, params.get("method","GET").upper(),
                                         params.get("path","/"), body=params.get("body"))
        return ActionResult(success=200<=status<400, message=f"HTTP {status}", data={"status": status, "response": response})
