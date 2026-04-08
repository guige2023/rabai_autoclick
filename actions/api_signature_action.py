"""API Signature action module for RabAI AutoClick.

Request signing for API authentication (AWS, HMAC, OAuth1).
"""

import time
import hmac
import hashlib
import sys
import os
import urllib.parse
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiSignatureAction(BaseAction):
    """Sign API requests with various signature schemes.

    Supports AWS SigV4, HMAC-SHA256, and OAuth1 signatures.
    """
    action_type = "api_signature"
    display_name = "API签名"
    description = "为API请求生成签名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sign a request.

        Args:
            context: Execution context.
            params: Dict with keys: sign_type (aws/hmac/oauth1),
                   method, url, headers, body, secret_key,
                   access_key, region, service.

        Returns:
            ActionResult with signed headers.
        """
        start_time = time.time()
        try:
            sign_type = params.get('sign_type', 'hmac')
            method = params.get('method', 'GET').upper()
            url = params.get('url', '')
            headers_in = params.get('headers', {})
            body = params.get('body', '')
            secret_key = params.get('secret_key', '')
            access_key = params.get('access_key', '')

            parsed = urllib.parse.urlparse(url)
            signed_headers = dict(headers_in)

            if sign_type == 'hmac':
                message = f"{method}\n{parsed.path}\n{parsed.query}".encode()
                signature = hmac.new(secret_key.encode(), message, hashlib.sha256).hexdigest()
                signed_headers['X-Signature'] = signature

            elif sign_type == 'aws':
                region = params.get('region', 'us-east-1')
                service = params.get('service', 'execute-api')
                now = params.get('timestamp', time.strftime('%Y%m%dT%H%M%SZ'))
                date_scope = f"{now[:8]}/{region}/{service}/aws4_request"
                canonical = f"{method}\n{parsed.path}\n{parsed.query}\nhost:{parsed.netloc}\n\nhost\n{hashlib.sha256((body or '').encode()).hexdigest()}"
                string_to_sign = f"AWS4-HMAC-SHA256\n{now}\n{date_scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
                kdate = hmac.new(f"AWS4{secret_key}".encode(), now[:8].encode(), hashlib.sha256).digest()
                kregion = hmac.new(kdate, region.encode(), hashlib.sha256).digest()
                kservice = hmac.new(kregion, service.encode(), hashlib.sha256).digest()
                ksigning = hmac.new(kservice, b"aws4_request", hashlib.sha256).digest()
                signature = hmac.new(ksigning, string_to_sign.encode(), hashlib.sha256).hexdigest()
                signed_headers['Authorization'] = f"AWS4-HMAC-SHA256 Credential={access_key}/{date_scope}, SignedHeaders=host, Signature={signature}"

            elif sign_type == 'oauth1':
                import secrets
                nonce = secrets.token_hex(16)
                oauth_time = str(int(time.time()))
                base_str = f"{method}&{urllib.parse.quote(url, '')}&{urllib.parse.quote(f"oauth_consumer_key={access_key}&oauth_nonce={nonce}&oauth_signature_method=HMAC-SHA1&oauth_timestamp={oauth_time}&oauth_version=1.0", '')}"
                sig = hmac.new(f"{secret_key}&".encode(), base_str.encode(), hashlib.sha1).digest()
                signature = urllib.parse.quote(hashlib.b64encode(sig).decode())
                signed_headers['Authorization'] = f'OAuth oauth_consumer_key="{access_key}", oauth_signature="{signature}", oauth_signature_method="HMAC-SHA1", oauth_timestamp="{oauth_time}", oauth_nonce="{nonce}", oauth_version="1.0"'

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Generated {sign_type} signature",
                data={'signed_headers': signed_headers, 'sign_type': sign_type},
                duration=duration,
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Signature error: {str(e)}", duration=time.time() - start_time)
