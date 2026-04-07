"""Markdown action module for RabAI AutoClick.

Provides markdown processing operations:
- MarkdownParseAction: Parse markdown to AST
- MarkdownToHtmlAction: Convert markdown to HTML
- MarkdownToPdfAction: Convert markdown to PDF
- MarkdownExtractLinksAction: Extract all links from markdown
- MarkdownExtractImagesAction: Extract all images from markdown
- MarkdownExtractHeadingsAction: Extract all headings
- MarkdownExtractCodeBlocksAction: Extract code blocks
- MarkdownRenderAction: Render markdown for display
- MarkdownTocAction: Generate table of contents
- MarkdownWordCountAction: Count words and characters
"""

import re
import os
import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MarkdownParseAction(BaseAction):
    """Parse markdown to structured data."""
    action_type = "markdown_parse"
    display_name = "解析Markdown"
    description = "解析Markdown内容为结构化数据"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with parsed markdown.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'markdown_parsed')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            # Simple parsing - extract basic structure
            lines = resolved_content.split('\n')
            parsed = {
                'headings': [],
                'links': [],
                'images': [],
                'code_blocks': [],
                'lists': [],
                'tables': [],
                'paragraphs': []
            }

            in_code_block = False
            code_block_content = []
            code_block_lang = ''

            for line in lines:
                if line.startswith('```'):
                    if not in_code_block:
                        in_code_block = True
                        code_block_lang = line[3:].strip()
                        code_block_content = []
                    else:
                        in_code_block = False
                        parsed['code_blocks'].append({
                            'language': code_block_lang,
                            'content': '\n'.join(code_block_content)
                        })
                    continue

                if in_code_block:
                    code_block_content.append(line)
                    continue

                # Headings
                m = re.match(r'^(#{1,6})\s+(.+)$', line)
                if m:
                    parsed['headings'].append({
                        'level': len(m.group(1)),
                        'text': m.group(2).strip()
                    })
                    continue

                # Links
                for match in re.finditer(r'\[([^\]]+)\]\(([^\)]+)\)', line):
                    parsed['links'].append({
                        'text': match.group(1),
                        'url': match.group(2)
                    })

                # Images
                for match in re.finditer(r'!\[([^\]]*)\]\(([^\)]+)\)', line):
                    parsed['images'].append({
                        'alt': match.group(1),
                        'url': match.group(2)
                    })

                # List items
                if re.match(r'^[\-\*\+]\s+', line) or re.match(r'^\d+\.\s+', line):
                    parsed['lists'].append(line.strip())

            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message=f"已解析Markdown: {len(parsed['headings'])} 标题, {len(parsed['links'])} 链接",
                data={
                    'headings': len(parsed['headings']),
                    'links': len(parsed['links']),
                    'images': len(parsed['images']),
                    'code_blocks': len(parsed['code_blocks']),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析Markdown失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'markdown_parsed'}


class MarkdownToHtmlAction(BaseAction):
    """Convert markdown to HTML."""
    action_type = "markdown_to_html"
    display_name = "Markdown转HTML"
    description = "将Markdown转换为HTML"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with HTML.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'html_content')
        output_file = params.get('output_file', '')

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            # Try to use mistune or commonmark
            html_output = ''
            try:
                import mistune
                md = mistune.create_markdown()
                html_output = md(resolved_content)
            except ImportError:
                try:
                    import commonmark
                    parser = commonmark.Parser()
                    ast = parser.parse(resolved_content)
                    renderer = commonmark.HtmlRenderer()
                    html_output = renderer.render(ast)
                except ImportError:
                    # Fallback: simple regex-based conversion
                    html_output = self._simple_convert(resolved_content)

            context.set(output_var, html_output)

            if output_file:
                resolved_out = context.resolve_value(output_file)
                with open(resolved_out, 'w', encoding='utf-8') as f:
                    f.write(html_output)

            return ActionResult(
                success=True,
                message=f"已转换为HTML ({len(html_output)} 字符)",
                data={'length': len(html_output), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Markdown转HTML失败: {str(e)}"
            )

    def _simple_convert(self, md: str) -> str:
        html = md
        # Headings
        for i in range(6, 0, -1):
            pattern = r'^' + '#' * i + r'\s+(.+)$'
            html = re.sub(pattern, f'<h{i}>\\1</h{i}>', html, flags=re.MULTILINE)
        # Bold
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
        html = re.sub(r'__(.+?)__', r'<strong>\1</strong>', html)
        # Italic
        html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)
        html = re.sub(r'_(.+?)_', r'<em>\1</em>', html)
        # Links
        html = re.sub(r'\[([^\]]+)\]\(([^\)]+)\)', r'<a href="\2">\1</a>', html)
        # Images
        html = re.sub(r'!\[([^\]]*)\]\(([^\)]+)\)', r'<img src="\2" alt="\1"/>', html)
        # Code blocks
        html = re.sub(r'```(\w*)\n(.+?)```', r'<pre><code class="\1">\2</code></pre>', html, flags=re.DOTALL)
        # Inline code
        html = re.sub(r'`([^`]+)`', r'<code>\1</code>', html)
        # Line breaks
        html = html.replace('\n\n', '</p><p>')
        html = '<p>' + html + '</p>'
        return html

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'html_content', 'output_file': ''}


class MarkdownExtractLinksAction(BaseAction):
    """Extract all links from markdown."""
    action_type = "markdown_extract_links"
    display_name = "提取链接"
    description = "从Markdown中提取所有链接"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with links.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'markdown_links')

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            links = []
            for match in re.finditer(r'\[([^\]]+)\]\(([^\)]+)\)', resolved_content):
                links.append({
                    'text': match.group(1),
                    'url': match.group(2)
                })

            context.set(output_var, links)

            return ActionResult(
                success=True,
                message=f"提取到 {len(links)} 个链接",
                data={'count': len(links), 'links': links, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取链接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'markdown_links'}


class MarkdownExtractImagesAction(BaseAction):
    """Extract all images from markdown."""
    action_type = "markdown_extract_images"
    display_name = "提取图片"
    description = "从Markdown中提取所有图片"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with images.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'markdown_images')

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            images = []
            for match in re.finditer(r'!\[([^\]]*)\]\(([^\)]+)\)', resolved_content):
                images.append({
                    'alt': match.group(1),
                    'url': match.group(2)
                })

            context.set(output_var, images)

            return ActionResult(
                success=True,
                message=f"提取到 {len(images)} 张图片",
                data={'count': len(images), 'images': images, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'markdown_images'}


class MarkdownExtractHeadingsAction(BaseAction):
    """Extract all headings from markdown."""
    action_type = "markdown_extract_headings"
    display_name = "提取标题"
    description = "从Markdown中提取所有标题"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with headings.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'markdown_headings')

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            headings = []
            for match in re.finditer(r'^(#{1,6})\s+(.+)$', resolved_content, re.MULTILINE):
                headings.append({
                    'level': len(match.group(1)),
                    'text': match.group(2).strip()
                })

            context.set(output_var, headings)

            return ActionResult(
                success=True,
                message=f"提取到 {len(headings)} 个标题",
                data={'count': len(headings), 'headings': headings, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取标题失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'markdown_headings'}


class MarkdownExtractCodeBlocksAction(BaseAction):
    """Extract all code blocks from markdown."""
    action_type = "markdown_extract_code_blocks"
    display_name = "提取代码块"
    description = "从Markdown中提取所有代码块"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with code blocks.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'markdown_code_blocks')

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            code_blocks = []
            for match in re.finditer(r'```(\w*)\n(.*?)```', resolved_content, re.DOTALL):
                code_blocks.append({
                    'language': match.group(1),
                    'content': match.group(2).strip()
                })

            context.set(output_var, code_blocks)

            return ActionResult(
                success=True,
                message=f"提取到 {len(code_blocks)} 个代码块",
                data={'count': len(code_blocks), 'code_blocks': code_blocks, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取代码块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'markdown_code_blocks'}


class MarkdownTocAction(BaseAction):
    """Generate table of contents."""
    action_type = "markdown_toc"
    display_name = "生成目录"
    description = "从Markdown生成目录"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute TOC.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var, max_level.

        Returns:
            ActionResult with TOC.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'markdown_toc')
        max_level = params.get('max_level', 3)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            resolved_max = context.resolve_value(max_level)
            headings = []
            for match in re.finditer(r'^(#{1,6})\s+(.+)$', resolved_content, re.MULTILINE):
                level = len(match.group(1))
                if level <= resolved_max:
                    headings.append({
                        'level': level,
                        'text': match.group(2).strip()
                    })

            # Generate TOC markdown
            toc_lines = []
            for h in headings:
                indent = '  ' * (h['level'] - 1)
                anchor = h['text'].lower().replace(' ', '-')
                toc_lines.append(f"{indent}- [{h['text']}](#{anchor})")

            toc = '\n'.join(toc_lines)
            context.set(output_var, toc)

            return ActionResult(
                success=True,
                message=f"已生成目录 ({len(headings)} 项)",
                data={'toc': toc, 'count': len(headings), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'markdown_toc', 'max_level': 3}


class MarkdownWordCountAction(BaseAction):
    """Count words and characters."""
    action_type = "markdown_word_count"
    display_name = "统计字数"
    description = "统计Markdown的字数和字符数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with content or file_path, output_var.

        Returns:
            ActionResult with counts.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'word_count')

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            # Strip markdown syntax for accurate count
            stripped = re.sub(r'```[\s\S]*?```', '', resolved_content)
            stripped = re.sub(r'!\[([^\]]*)\]\([^\)]+\)', '', stripped)
            stripped = re.sub(r'\[[^\]]+\]\([^\)]+\)', '', stripped)
            stripped = re.sub(r'[#*_`~\[\]]', '', stripped)

            chars = len(stripped)
            words = len(stripped.split())
            lines = len(resolved_content.split('\n'))

            counts = {'chars': chars, 'words': words, 'lines': lines}
            context.set(output_var, counts)

            return ActionResult(
                success=True,
                message=f"字数统计: {words} 词, {chars} 字符, {lines} 行",
                data=counts
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计字数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'word_count'}
