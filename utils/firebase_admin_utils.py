"""
Firebase Admin SDK Utilities.

Helpers for server-side Firebase operations: FCM push notifications,
Firestore document management, Authentication, and Cloud Storage.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "")
FIREBASE_SERVICE_ACCOUNT = os.getenv("FIREBASE_SERVICE_ACCOUNT", "")
FIREBASE_API_BASE = f"https://firestore.googleapis.com/v1"


# --------------------------------------------------------------------------- #
# Auth Helpers
# --------------------------------------------------------------------------- #

def get_access_token() -> str:
    """
    Obtain a short-lived OAuth2 access token from a service account.

    The service account can be provided as a JSON string or a path
    to a JSON key file. Falls back to GOOGLE_APPLICATION_CREDENTIALS.

    Returns:
        Bearer access token string.
    """
    import time
    creds = FIREBASE_SERVICE_ACCOUNT
    if not creds:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT not set")
    try:
        sa_info = json.loads(creds)
    except json.JSONDecodeError:
        with open(creds, "r") as f:
            sa_info = json.load(f)
    client_email = sa_info["client_email"]
    private_key = sa_info["private_key"]
    token_uri = sa_info.get("token_uri", "https://oauth2.googleapis.com/token")
    now = int(time.time())
    import jwt
    payload = {
        "iss": client_email,
        "sub": client_email,
        "aud": token_uri,
        "iat": now,
        "exp": now + 3600,
        "scope": "https://www.googleapis.com/auth/firebase",
    }
    signed_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    # Exchange for access token
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": signed_jwt,
    }
    req = urllib.request.Request(
        token_uri,
        data=urllib.parse.urlencode(data).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    import urllib.parse
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            return result["access_token"]
    except urllib.error.HTTPError as exc:
        raise FirebaseAuthError(exc.read().decode()) from exc


class FirebaseAuthError(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(f"Firebase auth error: {msg}")


# --------------------------------------------------------------------------- #
# FCM Push Notifications
# --------------------------------------------------------------------------- #

def send_fcm(
    token: str,
    title: str,
    body: str,
    data: Optional[dict[str, str]] = None,
    badge: Optional[int] = None,
    sound: str = "default",
) -> dict[str, Any]:
    """
    Send a Firebase Cloud Messaging notification to a device token.

    Args:
        token: FCM device registration token.
        title: Notification title.
        body: Notification body text.
        data: Optional custom data payload.
        badge: Optional iOS badge count.
        sound: Notification sound name.

    Returns:
        FCM API response with message_id or error.
    """
    message: dict[str, Any] = {
        "token": token,
        "notification": {"title": title, "body": body},
        "android": {"priority": "high"},
        "apns": {
            "payload": {
                "aps": {
                    "sound": sound,
                }
            }
        },
    }
    if badge is not None:
        message["apns"]["payload"]["aps"]["badge"] = badge
    if data:
        message["data"] = data
    return _fcm_send(message)


def send_fcm_multicast(
    tokens: list[str],
    title: str,
    body: str,
    data: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Send the same notification to multiple device tokens."""
    message: dict[str, Any] = {
        "notification": {"title": title, "body": body},
        "tokens": tokens,
    }
    if data:
        message["data"] = data
    return _fcm_send(message)


def _fcm_send(message: dict[str, Any]) -> dict[str, Any]:
    """Internal FCM send helper."""
    if not FIREBASE_PROJECT_ID:
        raise ValueError("FIREBASE_PROJECT_ID not set")
    try:
        access_token = get_access_token()
    except Exception as exc:
        return {"error": str(exc)}
    url = f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send"
    data = json.dumps({"message": message}).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return {"error": exc.read().decode()}


# --------------------------------------------------------------------------- #
# Firestore
# --------------------------------------------------------------------------- #

def firestore_path(project: Optional[str] = None) -> str:
    p = project or FIREBASE_PROJECT_ID
    return f"projects/{p}/databases/(default)/documents"


def firestore_doc_path(project: Optional[str], collection: str, doc_id: str) -> str:
    p = project or FIREBASE_PROJECT_ID
    return f"projects/{p}/databases/(default)/documents/{collection}/{doc_id}"


def get_document(
    collection: str,
    doc_id: str,
    project: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Fetch a Firestore document by collection and ID."""
    try:
        access_token = get_access_token()
    except Exception:
        return None
    path = firestore_doc_path(project, collection, doc_id)
    url = f"{FIREBASE_API_BASE}/{path}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise FirebaseAPIError(exc.code, exc.read().decode()) from exc


def set_document(
    collection: str,
    doc_id: str,
    data: dict[str, Any],
    project: Optional[str] = None,
    merge: bool = False,
) -> dict[str, Any]:
    """
    Create or update a Firestore document.

    Args:
        collection: Collection name.
        doc_id: Document ID.
        data: Document fields.
        project: Override project ID.
        merge: If True, merge with existing document.

    Returns:
        API response with update_time.
    """
    access_token = get_access_token()
    path = firestore_doc_path(project, collection, doc_id)
    url = f"{FIREBASE_API_BASE}/{path}"
    body: dict[str, Any] = {"fields": _to_firestore_fields(data)}
    if merge:
        url += "?currentDocument.exists=true"
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="PATCH",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise FirebaseAPIError(exc.code, exc.read().decode()) from exc


def delete_document(collection: str, doc_id: str, project: Optional[str] = None) -> None:
    """Delete a Firestore document."""
    access_token = get_access_token()
    path = firestore_doc_path(project, collection, doc_id)
    url = f"{FIREBASE_API_BASE}/{path}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            pass
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise FirebaseAPIError(exc.code, exc.read().decode()) from exc


def query_collection(
    collection: str,
    filters: Optional[list[dict[str, str]]] = None,
    order_by: Optional[str] = None,
    limit: int = 100,
    project: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Query a Firestore collection with filters.

    Args:
        collection: Collection name.
        filters: List of {'field': str, 'op': str, 'value': Any}.
        order_by: Field to order results by.
        limit: Maximum documents to return.
        project: Override project ID.

    Returns:
        List of document dicts.
    """
    access_token = get_access_token()
    p = project or FIREBASE_PROJECT_ID
    base_path = f"projects/{p}/databases/(default)/documents"
    url = f"{FIREBASE_API_BASE}/{base_path}/{collection}"
    structured_query: dict[str, Any] = {"limit": limit}
    if filters:
        structured_query["where"] = {
            "compositeFilter": {
                "op": "AND",
                "filters": [
                    {
                        "fieldFilter": {
                            "field": {"fieldPath": f["field"]},
                            "op": f["op"],
                            "value": _to_firestore_value(f["value"]),
                        }
                    }
                    for f in filters
                ],
            }
        }
    if order_by:
        structured_query["orderBy"] = [
            {"field": {"fieldPath": order_by}, "direction": "ASCENDING"}
        ]
    body = {"structuredQuery": structured_query, "parent": f"{base_path}"}
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url + ":runQuery",
        data=data,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            results = json.loads(resp.read())
            docs = []
            for r in results:
                doc = r.get("document", {})
                if doc:
                    docs.append(doc)
            return docs
    except urllib.error.HTTPError as exc:
        raise FirebaseAPIError(exc.code, exc.read().decode()) from exc


# --------------------------------------------------------------------------- #
# Field Conversion
# --------------------------------------------------------------------------- #

def _to_firestore_value(value: Any) -> dict[str, Any]:
    """Convert a Python value to a Firestore Value proto."""
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"integerValue": value}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_to_firestore_value(v) for v in value]}}
    if isinstance(value, dict):
        return {"mapValue": {"fields": _to_firestore_fields(value)}}
    if value is None:
        return {"nullValue": None}
    return {"stringValue": str(value)}


def _to_firestore_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Convert a dict of Python values to Firestore fields."""
    return {k: _to_firestore_value(v) for k, v in data.items()}


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #

def get_storage_download_url(
    bucket: str,
    path: str,
) -> str:
    """
    Get a signed download URL for a Cloud Storage object.

    Note: Full implementation requires the google-cloud-storage library
    or service account key with Storage Object Admin role.

    Returns:
        A download URL string.
    """
    return f"https://firebasestorage.googleapis.com/v0/b/{bucket}/o/{path}?alt=media"


import urllib.parse
