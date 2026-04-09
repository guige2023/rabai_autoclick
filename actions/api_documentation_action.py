"""API documentation action module for RabAI AutoClick.

Provides API documentation operations:
- OpenAPIGeneratorAction: Generate OpenAPI specs
- EndpointDocGeneratorAction: Generate endpoint documentation
- RequestExampleGeneratorAction: Generate request examples
- ResponseExampleGeneratorAction: Generate response examples
- MarkdownDocGeneratorAction: Generate Markdown documentation
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import json

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OpenAPIGeneratorAction(BaseAction):
    """Generate OpenAPI specifications."""
    action_type = "openapi_generator"
    display_name = "OpenAPI生成"
    description = "生成OpenAPI规范文档"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            title = params.get("title", "API Documentation")
            version = params.get("version", "1.0.0")
            endpoints = params.get("endpoints", [])
            base_url = params.get("base_url", "/api/v1")
            
            openapi_spec = {
                "openapi": "3.0.0",
                "info": {
                    "title": title,
                    "version": version,
                    "description": params.get("description", ""),
                    "contact": params.get("contact", {})
                },
                "servers": params.get("servers", [{"url": base_url}]),
                "paths": {},
                "components": {
                    "schemas": {},
                    "securitySchemes": params.get("security_schemes", {})
                }
            }
            
            for endpoint in endpoints:
                path = endpoint.get("path")
                methods = endpoint.get("methods", ["get"])
                
                if not path:
                    continue
                
                openapi_spec["paths"][path] = {}
                
                for method in methods:
                    openapi_spec["paths"][path][method] = {
                        "summary": endpoint.get("summary", ""),
                        "description": endpoint.get("description", ""),
                        "operationId": endpoint.get("operation_id", f"{method}_{path}"),
                        "tags": endpoint.get("tags", []),
                        "parameters": endpoint.get("parameters", []),
                        "requestBody": endpoint.get("request_body"),
                        "responses": endpoint.get("responses", {
                            "200": {"description": "Success"},
                            "400": {"description": "Bad Request"},
                            "500": {"description": "Internal Server Error"}
                        }),
                        "security": endpoint.get("security")
                    }
            
            format_type = params.get("format", "dict")
            
            if format_type == "json":
                return ActionResult(
                    success=True,
                    message="OpenAPI spec generated (JSON)",
                    data={"spec": json.dumps(openapi_spec, indent=2)}
                )
            
            return ActionResult(
                success=True,
                message="OpenAPI spec generated",
                data={"spec": openapi_spec}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class EndpointDocGeneratorAction(BaseAction):
    """Generate endpoint documentation."""
    action_type = "endpoint_doc_generator"
    display_name = "端点文档生成"
    description = "生成API端点文档"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", {})
            include_examples = params.get("include_examples", True)
            format_type = params.get("format", "markdown")
            
            method = endpoint.get("method", "GET").upper()
            path = endpoint.get("path", "/")
            summary = endpoint.get("summary", "")
            description = endpoint.get("description", "")
            parameters = endpoint.get("parameters", [])
            request_body = endpoint.get("request_body", {})
            responses = endpoint.get("responses", {})
            
            if format_type == "markdown":
                doc = self._generate_markdown(method, path, summary, description, 
                                             parameters, request_body, responses,
                                             include_examples)
            else:
                doc = self._generate_dict(method, path, summary, description,
                                        parameters, request_body, responses)
            
            return ActionResult(
                success=True,
                message="Endpoint documentation generated",
                data={
                    "method": method,
                    "path": path,
                    "documentation": doc
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_markdown(self, method: str, path: str, summary: str, description: str,
                          parameters: List, request_body: Dict, responses: Dict,
                          include_examples: bool) -> str:
        lines = [
            f"### {method} {path}",
            "",
            summary,
            "",
        ]
        
        if description:
            lines.extend([description, ""])
        
        if parameters:
            lines.append("**Parameters:**")
            lines.append("")
            lines.append("| Name | Location | Type | Required | Description |")
            lines.append("|------|----------|------|----------|-------------|")
            
            for param in parameters:
                lines.append(f"| {param.get('name')} | {param.get('in')} | {param.get('type', 'string')} | {param.get('required', False)} | {param.get('description', '')} |")
            lines.append("")
        
        if request_body:
            lines.append("**Request Body:**")
            lines.append("```json")
            lines.append(json.dumps(request_body, indent=2))
            lines.append("```")
            lines.append("")
        
        if responses:
            lines.append("**Responses:**")
            lines.append("")
            for status_code, response in responses.items():
                lines.append(f"- `{status_code}`: {response.get('description', '')}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_dict(self, method: str, path: str, summary: str, description: str,
                      parameters: List, request_body: Dict, responses: Dict) -> Dict:
        return {
            "method": method,
            "path": path,
            "summary": summary,
            "description": description,
            "parameters": parameters,
            "request_body": request_body,
            "responses": responses
        }


class RequestExampleGeneratorAction(BaseAction):
    """Generate request examples."""
    action_type = "request_example_generator"
    display_name = "请求示例生成"
    description = "生成API请求示例"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", {})
            format_type = params.get("format", "curl")
            base_url = params.get("base_url", "https://api.example.com")
            
            method = endpoint.get("method", "GET").upper()
            path = endpoint.get("path", "/")
            parameters = endpoint.get("parameters", [])
            request_body = endpoint.get("request_body")
            headers = params.get("headers", {"Content-Type": "application/json"})
            
            if format_type == "curl":
                example = self._generate_curl(method, f"{base_url}{path}", 
                                             parameters, request_body, headers)
            elif format_type == "python":
                example = self._generate_python(method, f"{base_url}{path}",
                                               parameters, request_body, headers)
            elif format_type == "javascript":
                example = self._generate_javascript(method, f"{base_url}{path}",
                                                   parameters, request_body, headers)
            else:
                return ActionResult(success=False, message=f"Unknown format: {format_type}")
            
            return ActionResult(
                success=True,
                message=f"Request example generated ({format_type})",
                data={
                    "format": format_type,
                    "example": example
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_curl(self, method: str, url: str, parameters: List, 
                      request_body: Optional[Dict], headers: Dict) -> str:
        lines = [f"curl -X {method} \\"]
        lines.append(f'  "{url}" \\')
        
        for name, value in headers.items():
            lines.append(f'  -H "{name}: {value}" \\')
        
        if request_body:
            lines.append(f'  -d \'{json.dumps(request_body, indent=2)}\' \\')
        
        lines[-1] = lines[-1].rstrip(" \\")
        
        return "\n".join(lines)
    
    def _generate_python(self, method: str, url: str, parameters: List,
                        request_body: Optional[Dict], headers: Dict) -> str:
        lines = [
            "import requests",
            "",
            f"url = \"{url}\"",
            "",
            f"headers = {json.dumps(headers, indent=4)}",
            ""
        ]
        
        if request_body:
            lines.append(f"payload = {json.dumps(request_body, indent=4)}")
            lines.append("")
            lines.append(f'response = requests.{method.lower()}(url, headers=headers, json=payload)')
        else:
            lines.append(f'response = requests.{method.lower()}(url, headers=headers)')
        
        lines.extend([
            "",
            "print(response.status_code)",
            "print(response.json())"
        ])
        
        return "\n".join(lines)
    
    def _generate_javascript(self, method: str, url: str, parameters: List,
                           request_body: Optional[Dict], headers: Dict) -> str:
        lines = [
            f"const url = '{url}';",
            "",
            f"const headers = {json.dumps(headers, indent=2)};",
            ""
        ]
        
        if request_body:
            lines.append(f"const payload = {json.dumps(request_body, indent=2)};")
            lines.append("")
            lines.append(f"const response = await fetch(url, {{")
            lines.append(f"  method: '{method}',")
            lines.append(f"  headers,")
            lines.append(f"  body: JSON.stringify(payload)")
            lines.append(f"}});")
        else:
            lines.append(f"const response = await fetch(url, {{")
            lines.append(f"  method: '{method}',")
            lines.append(f"  headers")
            lines.append(f"}});")
        
        lines.extend([
            "",
            "const data = await response.json();",
            "console.log(data);"
        ])
        
        return "\n".join(lines)


class ResponseExampleGeneratorAction(BaseAction):
    """Generate response examples."""
    action_type = "response_example_generator"
    display_name = "响应示例生成"
    description = "生成API响应示例"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema = params.get("schema", {})
            status_code = params.get("status_code", 200)
            include_error = params.get("include_error_examples", True)
            format_type = params.get("format", "json")
            
            success_response = self._generate_from_schema(schema)
            
            examples = {
                "success": {
                    "status_code": status_code,
                    "body": success_response
                }
            }
            
            if include_error:
                examples["error_400"] = {
                    "status_code": 400,
                    "body": {"error": "Bad Request", "message": "Invalid parameters", "code": "INVALID_PARAMS"}
                }
                examples["error_401"] = {
                    "status_code": 401,
                    "body": {"error": "Unauthorized", "message": "Invalid or missing authentication", "code": "AUTH_REQUIRED"}
                }
                examples["error_500"] = {
                    "status_code": 500,
                    "body": {"error": "Internal Server Error", "message": "An unexpected error occurred", "code": "SERVER_ERROR"}
                }
            
            if format_type == "json":
                formatted = json.dumps(examples, indent=2)
                return ActionResult(
                    success=True,
                    message="Response examples generated",
                    data={"examples": formatted}
                )
            
            return ActionResult(
                success=True,
                message="Response examples generated",
                data={"examples": examples}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_from_schema(self, schema: Dict, depth: int = 0) -> Any:
        if depth > 5:
            return "..."
        
        schema_type = schema.get("type")
        
        if schema_type == "object":
            properties = schema.get("properties", {})
            return {key: self._generate_from_schema(prop, depth + 1) 
                   for key, prop in properties.items()}
        elif schema_type == "array":
            items = schema.get("items", {})
            return [self._generate_from_schema(items, depth + 1)]
        elif schema_type == "string":
            example = schema.get("example", "example_string")
            return example
        elif schema_type == "integer" or schema_type == "number":
            example = schema.get("example", 123)
            return example
        elif schema_type == "boolean":
            return True
        elif schema_type == "null":
            return None
        else:
            return schema.get("example", "value")


class MarkdownDocGeneratorAction(BaseAction):
    """Generate Markdown documentation."""
    action_type = "markdown_doc_generator"
    display_name = "Markdown文档生成"
    description = "生成Markdown格式API文档"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            title = params.get("title", "API Documentation")
            description = params.get("description", "")
            base_url = params.get("base_url", "")
            endpoints = params.get("endpoints", [])
            include_toc = params.get("include_toc", True)
            
            lines = [
                f"# {title}",
                ""
            ]
            
            if description:
                lines.extend([description, ""])
            
            if include_toc:
                lines.append("## Table of Contents")
                lines.append("")
                for i, endpoint in enumerate(endpoints):
                    method = endpoint.get("method", "GET").upper()
                    path = endpoint.get("path", "/")
                    summary = endpoint.get("summary", "")
                    lines.append(f"- [{method} {path}](#{method.lower()}-{path.replace('/', '-').replace('{', '').replace('}', '')})")
                lines.append("")
            
            lines.append("## Endpoints")
            lines.append("")
            
            for endpoint in endpoints:
                method = endpoint.get("method", "GET").upper()
                path = endpoint.get("path", "/")
                summary = endpoint.get("summary", "")
                desc = endpoint.get("description", "")
                params_list = endpoint.get("parameters", [])
                request_body = endpoint.get("request_body", {})
                responses = endpoint.get("responses", {})
                
                anchor = f"{method.lower()}-{path.replace('/', '-').replace('{', '').replace('}', '')}"
                
                lines.append(f"### <a name=\"{anchor}\"></a>{method} {path}")
                lines.append("")
                lines.append(summary)
                lines.append("")
                
                if desc:
                    lines.extend([desc, ""])
                
                if params_list:
                    lines.append("**Parameters:**")
                    lines.append("")
                    lines.append("| Name | Type | Required | Description |")
                    lines.append("|------|------|----------|-------------|")
                    for p in params_list:
                        lines.append(f"| {p.get('name')} | {p.get('type', 'string')} | {p.get('required', False)} | {p.get('description', '')} |")
                    lines.append("")
                
                if request_body:
                    lines.append("**Request Body:**")
                    lines.append("```json")
                    lines.append(json.dumps(request_body, indent=2))
                    lines.append("```")
                    lines.append("")
                
                if responses:
                    lines.append("**Responses:**")
                    for code, resp in responses.items():
                        lines.append(f"- `{code}`: {resp.get('description', '')}")
                    lines.append("")
                
                lines.append("---")
                lines.append("")
            
            return ActionResult(
                success=True,
                message="Markdown documentation generated",
                data={
                    "title": title,
                    "endpoints_count": len(endpoints),
                    "documentation": "\n".join(lines)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
