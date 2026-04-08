"""
Regex utilities - pattern matching, extraction, substitution, validation, splitting.
"""
from typing import Any, Dict, List, Optional, Pattern
import re
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class RegexAction(BaseAction):
    """Regex operations.

    Provides pattern matching, extraction, substitution, validation, splitting.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "match")
        pattern = params.get("pattern", "")
        text = params.get("text", "")

        try:
            flags = 0
            if params.get("ignore_case"):
                flags |= re.IGNORECASE
            if params.get("multiline"):
                flags |= re.MULTILINE
            if params.get("dotall"):
                flags |= re.DOTALL

            if operation == "match":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                compiled = re.compile(pattern, flags)
                m = compiled.match(text)
                if m:
                    return {"success": True, "matched": True, "group": m.group(), "groups": m.groups()}
                return {"success": True, "matched": False}

            elif operation == "search":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                compiled = re.compile(pattern, flags)
                m = compiled.search(text)
                if m:
                    return {"success": True, "matched": True, "match": m.group(), "start": m.start(), "end": m.end()}
                return {"success": True, "matched": False}

            elif operation == "findall":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                compiled = re.compile(pattern, flags)
                matches = compiled.findall(text)
                return {"success": True, "matches": matches, "count": len(matches)}

            elif operation == "finditer":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                compiled = re.compile(pattern, flags)
                results = []
                for m in compiled.finditer(text):
                    results.append({"match": m.group(), "start": m.start(), "end": m.end(), "groups": m.groups()})
                return {"success": True, "matches": results, "count": len(results)}

            elif operation == "split":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                compiled = re.compile(pattern, flags)
                parts = compiled.split(text)
                return {"success": True, "parts": parts, "count": len(parts)}

            elif operation == "sub":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                replacement = params.get("replacement", "")
                count = int(params.get("count", 0))
                compiled = re.compile(pattern, flags)
                result = compiled.sub(replacement, text, count=count)
                return {"success": True, "result": result}

            elif operation == "validate":
                if not pattern:
                    return {"success": False, "error": "pattern required"}
                try:
                    compiled = re.compile(pattern, flags)
                    return {"success": True, "valid": True, "pattern": pattern}
                except re.error as e:
                    return {"success": True, "valid": False, "error": str(e), "pattern": pattern}

            elif operation == "groups":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                compiled = re.compile(pattern, flags)
                m = compiled.search(text)
                if m:
                    named = m.groupdict() if m.groupdict() else {}
                    return {"success": True, "groups": m.groups(), "named_groups": named, "count": len(m.groups())}
                return {"success": True, "groups": None}

            elif operation == "extract_emails":
                if not text:
                    return {"success": False, "error": "text required"}
                pattern_email = r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b"
                emails = re.findall(pattern_email, text)
                return {"success": True, "emails": emails, "count": len(emails)}

            elif operation == "extract_phones":
                if not text:
                    return {"success": False, "error": "text required"}
                pattern_phone = r"\+?[\d\s\-\(\)]{10,}"
                phones = re.findall(pattern_phone, text)
                cleaned = [re.sub(r"[^\d+]", "", p) for p in phones]
                return {"success": True, "phones": cleaned, "count": len(cleaned)}

            elif operation == "extract_urls":
                if not text:
                    return {"success": False, "error": "text required"}
                pattern_url = r"https?://\S+"
                urls = re.findall(pattern_url, text)
                return {"success": True, "urls": urls, "count": len(urls)}

            elif operation == "replace_all":
                if not pattern or text is None:
                    return {"success": False, "error": "pattern and text required"}
                replacement = params.get("replacement", "")
                result = text.replace(pattern, replacement)
                return {"success": True, "result": result, "replacements": text.count(pattern)}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except re.error as e:
            return {"success": False, "error": f"Regex error: {e}"}
        except Exception as e:
            logger.error(f"RegexAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return RegexAction().execute(context, params)
