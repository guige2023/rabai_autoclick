"""
LLM Prompt engineering utilities - template rendering, chain-of-thought, few-shot examples.
"""
from typing import Any, Dict, List, Optional, Callable
import re
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _render_template(template: str, variables: Dict[str, Any]) -> str:
    """Simple {{variable}} template renderer."""
    result = template
    for key, value in variables.items():
        result = result.replace(f"{{{{{key}}}}}", str(value))
        result = result.replace(f"{{{{ {key} }}}}", str(value))
    unmatched = re.findall(r"\{\{\{?\s*[\w]+\s*\}?\}\}", result)
    if unmatched:
        logger.warning(f"Unmatched template variables: {unmatched}")
    return result


def _chain_of_thought(prompt: str, steps: List[str]) -> str:
    """Build a chain-of-thought prompt."""
    lines = [prompt, "\nLet's think step by step:"]
    for i, step in enumerate(steps, 1):
        lines.append(f"{i}. {step}")
    lines.append("\nTherefore, the answer is:")
    return "\n".join(lines)


def _few_shot_prompt(
    instruction: str,
    examples: List[Dict[str, str]],
    input_text: str,
    input_key: str = "input",
    output_key: str = "output"
) -> str:
    """Build a few-shot prompt with examples."""
    lines = [instruction, "\nExamples:"]
    for ex in examples:
        lines.append(f"Input: {ex.get(input_key, '')}")
        lines.append(f"Output: {ex.get(output_key, '')}")
        lines.append("")
    lines.append(f"Input: {input_text}")
    lines.append("Output:")
    return "\n".join(lines)


def _json_to_markdown_table(data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> str:
    """Convert JSON list to markdown table."""
    if not data:
        return ""
    cols = columns or list(data[0].keys())
    header = "| " + " | ".join(cols) + " |"
    separator = "| " + " | ".join("---" for _ in cols) + " |"
    rows = []
    for row in data:
        rows.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")
    return "\n".join([header, separator] + rows)


def _extract_json_objects(text: str) -> List[Dict[str, Any]]:
    """Extract JSON objects from text that may contain markdown code blocks."""
    import json
    results = []
    patterns = [
        r"```json\s*(\{.*?\})\s*```",
        r"```\s*(\{.*?\})\s*```",
        r"(\{.*\})",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, re.DOTALL)
        for match in matches:
            try:
                results.append(json.loads(match))
            except json.JSONDecodeError:
                continue
    return results


class LLMPromptAction(BaseAction):
    """LLM prompt engineering operations.

    Provides template rendering, chain-of-thought, few-shot, prompt chaining, extraction.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "render")
        template = params.get("template", "")
        variables = params.get("variables", {})
        instruction = params.get("instruction", "")
        examples = params.get("examples", [])
        input_text = params.get("input_text", "")
        steps = params.get("steps", [])

        try:
            if operation == "render":
                if not template:
                    return {"success": False, "error": "template required"}
                result = _render_template(template, variables)
                return {"success": True, "prompt": result}

            elif operation == "chain_of_thought":
                if not instruction:
                    return {"success": False, "error": "instruction required"}
                if not steps:
                    return {"success": False, "error": "steps required"}
                prompt = _chain_of_thought(instruction, steps)
                return {"success": True, "prompt": prompt}

            elif operation == "few_shot":
                if not instruction or not examples or not input_text:
                    return {"success": False, "error": "instruction, examples, and input_text required"}
                prompt = _few_shot_prompt(instruction, examples, input_text)
                return {"success": True, "prompt": prompt}

            elif operation == "system_message":
                role = params.get("role", "assistant")
                content = params.get("content", instruction)
                return {"success": True, "message": {"role": role, "content": content}}

            elif operation == "messages":
                msgs = params.get("messages", [])
                return {"success": True, "messages": msgs, "count": len(msgs)}

            elif operation == "add_example":
                input_val = params.get("input", "")
                output_val = params.get("output", "")
                examples.append({"input": input_val, "output": output_val})
                return {"success": True, "examples": examples, "count": len(examples)}

            elif operation == "json_to_table":
                data = params.get("data", [])
                columns = params.get("columns")
                table = _json_to_markdown_table(data, columns)
                return {"success": True, "table": table}

            elif operation == "extract_json":
                text = params.get("text", "")
                objects = _extract_json_objects(text)
                return {"success": True, "objects": objects, "count": len(objects)}

            elif operation == "prompt_with_context":
                system = params.get("system", "")
                context_text = params.get("context", "")
                user_prompt = params.get("prompt", instruction)
                messages = []
                if system:
                    messages.append({"role": "system", "content": system})
                if context_text:
                    messages.append({"role": "system", "content": f"Context:\n{context_text}"})
                messages.append({"role": "user", "content": user_prompt})
                return {"success": True, "messages": messages, "count": len(messages)}

            elif operation == "compare_prompts":
                prompts = params.get("prompts", [])
                if not prompts:
                    return {"success": False, "error": "prompts list required"}
                rendered = [_render_template(p, variables) for p in prompts]
                return {"success": True, "prompts": rendered, "count": len(rendered)}

            elif operation == "count_tokens_estimate":
                text = template or instruction or input_text
                words = len(text.split())
                chars = len(text)
                estimate = int(chars / 4)
                return {"success": True, "words": words, "chars": chars, "token_estimate": estimate}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"LLMPromptAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for LLM prompt operations."""
    return LLMPromptAction().execute(context, params)
