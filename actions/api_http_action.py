"""API HTTP action module for RabAI AutoClick.

Provides enhanced HTTP operations including redirects handling,
content negotiation, and conditional requests (ETag/Last-Modified).
"""

import time
import json
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from core.base_action import BaseAction, ActionResult


class HttpConditionalGetAction(BaseAction):
    """Perform conditional HTTP GET requests using ETag/If-None-Match.
    
    Skips body download when server returns 304 Not Modified.
    Supports Last-Modified/If-Modified-Since as fallback.
    """
    action_type = "http_conditional_get"
    display_name = "HTTP条件获取"
    description = "使用ETag执行条件HTTP GET请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform conditional GET request.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, etag, last_modified, headers, timeout.
        
        Returns:
            ActionResult with response or not-modified status.
        """
        url = params.get("url", "")
        etag = params.get("etag")
        last_modified = params.get("last_modified")
        headers = params.get("headers", {})
        timeout = params.get("timeout", 30)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        try:
            req_headers = {str(k): str(v) for k, v in headers.items()}
            
            if etag:
                req_headers["If-None-Match"] = etag
            if last_modified:
                req_headers["If-Modified-Since"] = last_modified
            
            request = Request(url, headers=req_headers)
            
            start_time = time.time()
            with urlopen(request, timeout=timeout) as response:
                elapsed = time.time() - start_time
                response_etag = response.headers.get("ETag")
                response_modified = response.headers.get("Last-Modified")
                status_code = response.status
                body = response.read().decode("utf-8")
                
                try:
                    body_json = json.loads(body)
                except Exception:
                    body_json = body
                
                return ActionResult(
                    success=True,
                    message=f"Full response ({status_code}) in {elapsed:.2f}s",
                    data={
                        "status_code": status_code,
                        "etag": response_etag,
                        "last_modified": response_modified,
                        "body": body_json,
                        "elapsed": elapsed,
                        "not_modified": False
                    }
                )
        except HTTPError as e:
            if e.code == 304:
                elapsed = time.time() - start_time
                return ActionResult(
                    success=True,
                    message="Not Modified (304)",
                    data={
                        "status_code": 304,
                        "not_modified": True,
                        "elapsed": elapsed
                    }
                )
            return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}")
        except Exception as e:
            return ActionResult(success=False, message=f"Conditional GET failed: {e}")


class HttpRedirectHandlerAction(BaseAction):
    """Handle HTTP redirects with configurable policy.
    
    Supports following redirects, limiting max hops, restricting
    to same domain, and tracking redirect chain.
    """
    action_type = "http_redirect_handler"
    display_name = "HTTP重定向处理"
    description = "处理HTTP重定向，跟踪重定向链"
    VALID_REDIRECT_MODES = ["follow", "manual", "error"]
    VALID_REDIRECT_POLICIES = ["default", "same_domain", "same_protocol", "no_loop"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle HTTP redirects.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, redirect_mode, max_hops,
                   policy, headers, body, timeout.
        
        Returns:
            ActionResult with final response and redirect chain.
        """
        url = params.get("url", "")
        method = params.get("method", "GET").upper()
        redirect_mode = params.get("redirect_mode", "follow")
        max_hops = params.get("max_hops", 10)
        policy = params.get("policy", "default")
        headers = params.get("headers", {})
        body = params.get("body")
        timeout = params.get("timeout", 30)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        from urllib.parse import urlparse
        
        redirect_chain = []
        current_url = url
        current_method = method
        current_body = body
        hops = 0
        
        try:
            while hops < max_hops:
                hops += 1
                
                req_headers = {str(k): str(v) for k, v in headers.items()}
                req_headers["User-Agent"] = "RabAI-AutoClick/1.0"
                
                body_bytes = None
                if current_body:
                    if isinstance(current_body, dict):
                        body_bytes = json.dumps(current_body).encode()
                    elif isinstance(current_body, str):
                        body_bytes = current_body.encode()
                    else:
                        body_bytes = current_body
                
                request = Request(
                    current_url,
                    data=body_bytes,
                    headers=req_headers,
                    method=current_method
                )
                
                with urlopen(request, timeout=timeout) as response:
                    status_code = response.status
                    location = response.headers.get("Location")
                    
                    if 300 <= status_code < 400 and location:
                        if redirect_mode == "error":
                            return ActionResult(
                                success=False,
                                message=f"Redirect encountered (not following)",
                                data={
                                    "redirect_chain": redirect_chain,
                                    "hops": hops
                                }
                            )
                        
                        if policy == "same_domain":
                            parsed_orig = urlparse(current_url)
                            parsed_new = urlparse(location if location.startswith("http") else f"{parsed_orig.scheme}://{parsed_orig.netloc}{location}")
                            if parsed_orig.netloc != parsed_new.netloc:
                                return ActionResult(
                                    success=False,
                                    message="Cross-domain redirect blocked by policy"
                                )
                        elif policy == "same_protocol":
                            parsed_orig = urlparse(current_url)
                            parsed_new = urlparse(location if location.startswith("http") else f"{parsed_orig.scheme}://{parsed_orig.netloc}{location}")
                            if parsed_orig.scheme != parsed_new.scheme:
                                return ActionResult(
                                    success=False,
                                    message="Protocol change redirect blocked by policy"
                                )
                        
                        redirect_chain.append({
                            "url": current_url,
                            "status": status_code,
                            "location": location
                        })
                        
                        current_url = location if location.startswith("http") else f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}{location}"
                        
                        if status_code in (307, 308):
                            pass
                        else:
                            current_method = "GET"
                            current_body = None
                    else:
                        response_body = response.read()
                        try:
                            body_json = json.loads(response_body.decode())
                        except Exception:
                            body_json = response_body.decode("utf-8", errors="replace")
                        
                        return ActionResult(
                            success=True,
                            message=f"Final response: {status_code} after {hops} hops",
                            data={
                                "status_code": status_code,
                                "body": body_json,
                                "redirect_chain": redirect_chain,
                                "hops": hops,
                                "final_url": current_url
                            }
                        )
            
            return ActionResult(
                success=False,
                message=f"Max redirect hops ({max_hops}) exceeded",
                data={
                    "redirect_chain": redirect_chain,
                    "hops": hops
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Redirect handling failed: {e}")


class HttpContentNegotiationAction(BaseAction):
    """Perform HTTP requests with content negotiation headers.
    
    Supports Accept, Accept-Language, Accept-Encoding headers
    for flexible content format handling.
    """
    action_type = "http_content_negotiation"
    display_name = "HTTP内容协商"
    description = "执行带内容协商头的HTTP请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute content-negotiated request.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, accept, accept_language,
                   accept_encoding, headers, timeout.
        
        Returns:
            ActionResult with negotiated response.
        """
        url = params.get("url", "")
        accept = params.get("accept", "application/json")
        accept_language = params.get("accept_language", "en")
        accept_encoding = params.get("accept_encoding", "gzip, deflate")
        headers = params.get("headers", {})
        timeout = params.get("timeout", 30)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        try:
            req_headers = {str(k): str(v) for k, v in headers.items()}
            req_headers["Accept"] = accept
            req_headers["Accept-Language"] = accept_language
            req_headers["Accept-Encoding"] = accept_encoding
            
            request = Request(url, headers=req_headers)
            
            start_time = time.time()
            with urlopen(request, timeout=timeout) as response:
                elapsed = time.time() - start_time
                status_code = response.status
                content_type = response.headers.get("Content-Type", "")
                content_encoding = response.headers.get("Content-Encoding", "")
                
                import gzip
                body = response.read()
                if content_encoding == "gzip":
                    body = gzip.decompress(body)
                
                try:
                    body_json = json.loads(body.decode("utf-8"))
                except Exception:
                    body_json = body.decode("utf-8", errors="replace")
                
                return ActionResult(
                    success=True,
                    message=f"Response: {status_code} ({content_type})",
                    data={
                        "status_code": status_code,
                        "content_type": content_type,
                        "content_encoding": content_encoding,
                        "body": body_json,
                        "elapsed": elapsed
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Content negotiation request failed: {e}")
