"""
Markdown processing and formatting actions.
"""
from __future__ import annotations

import re
from typing import Dict, Any, Optional, List
from html import escape as html_escape


def render_markdown(markdown: str) -> str:
    """
    Render Markdown to HTML.

    Args:
        markdown: Markdown text.

    Returns:
        HTML string.
    """
    html = markdown

    html = re.sub(r'^###### (.+)$', r'<h6>\1</h6>', html, flags=re.MULTILINE)
    html = re.sub(r'^##### (.+)$', r'<h5>\1</h5>', html, flags=re.MULTILINE)
    html = re.sub(r'^#### (.+)$', r'<h4>\1</h4>', html, flags=re.MULTILINE)
    html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
    html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
    html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)

    html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
    html = re.sub(r'__(.+?)__', r'<strong>\1</strong>', html)
    html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)
    html = re.sub(r'_(.+?)_', r'<em>\1</em>', html)

    html = re.sub(r'~~(.+?)~~', r'<del>\1</del>', html)

    html = re.sub(r'`(.+?)`', r'<code>\1</code>', html)

    lines = html.split('\n')
    in_list = False
    result_lines = []

    for line in lines:
        list_match = re.match(r'^[\-\*] (.+)$', line)
        if list_match:
            if not in_list:
                result_lines.append('<ul>')
                in_list = True
            result_lines.append(f'  <li>{list_match.group(1)}</li>')
        else:
            if in_list:
                result_lines.append('</ul>')
                in_list = False
            result_lines.append(line)

    if in_list:
        result_lines.append('</ul>')

    html = '\n'.join(result_lines)

    html = re.sub(
        r'\[(.+?)\]\((.+?)\)',
        r'<a href="\2">\1</a>',
        html
    )

    html = re.sub(r'!\[(.+?)\]\((.+?)\)', r'<img src="\2" alt="\1">', html)

    html = re.sub(r'^> (.+)$', r'<blockquote>\1</blockquote>', html, flags=re.MULTILINE)

    html = re.sub(r'^---$', '<hr>', html, flags=re.MULTILINE)

    html = html.replace('\n\n', '</p><p>')
    html = f'<p>{html}</p>'

    return html


def extract_headings(markdown: str) -> List[Dict[str, Any]]:
    """
    Extract all headings from Markdown.

    Args:
        markdown: Markdown text.

    Returns:
        List of heading dictionaries.
    """
    headings: List[Dict[str, Any]] = []

    lines = markdown.split('\n')

    for line in lines:
        match = re.match(r'^(#{1,6}) (.+)$', line)
        if match:
            headings.append({
                'level': len(match.group(1)),
                'text': match.group(2).strip(),
            })

    return headings


def extract_links(markdown: str) -> List[Dict[str, str]]:
    """
    Extract all links from Markdown.

    Args:
        markdown: Markdown text.

    Returns:
        List of link dictionaries.
    """
    links: List[Dict[str, str]] = []

    for match in re.finditer(r'\[(.+?)\]\((.+?)\)', markdown):
        links.append({
            'text': match.group(1),
            'url': match.group(2),
        })

    return links


def extract_code_blocks(markdown: str) -> List[Dict[str, str]]:
    """
    Extract all code blocks from Markdown.

    Args:
        markdown: Markdown text.

    Returns:
        List of code block dictionaries.
    """
    blocks: List[Dict[str, str]] = []

    pattern = r'```(\w*)\n(.*?)```'

    for match in re.finditer(pattern, markdown, re.DOTALL):
        blocks.append({
            'language': match.group(1),
            'code': match.group(2).strip(),
        })

    return blocks


def strip_markdown(markdown: str) -> str:
    """
    Remove all Markdown formatting from text.

    Args:
        markdown: Markdown text.

    Returns:
        Plain text.
    """
    text = markdown

    text = re.sub(r'^#{1,6} ', '', text, flags=re.MULTILINE)

    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'__(.+?)__', r'\1', text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'_(.+?)_', r'\1', text)
    text = re.sub(r'~~(.+?)~~', r'\1', text)

    text = re.sub(r'`(.+?)`', r'\1', text)

    text = re.sub(r'```.*?\n(.*?)```', r'\1', text, flags=re.DOTALL)

    text = re.sub(r'\[(.+?)\]\(.+?\)', r'\1', text)

    text = re.sub(r'!\[.*?\]\(.+?\)', '', text)

    text = re.sub(r'^> ', '', text, flags=re.MULTILINE)

    text = re.sub(r'^[\-\*] ', '', text, flags=re.MULTILINE)

    text = re.sub(r'^---$', '', text, flags=re.MULTILINE)

    return text.strip()


def create_markdown_table(
    headers: List[str],
    rows: List[List[str]]
) -> str:
    """
    Create a Markdown table.

    Args:
        headers: Column headers.
        rows: Data rows.

    Returns:
        Markdown table string.
    """
    lines = []

    lines.append('| ' + ' | '.join(headers) + ' |')

    lines.append('| ' + ' | '.join(['---'] * len(headers)) + ' |')

    for row in rows:
        lines.append('| ' + ' | '.join(str(cell) for cell in row) + ' |')

    return '\n'.join(lines)


def markdown_to_plain_text(markdown: str) -> str:
    """
    Convert Markdown to plain text.

    Args:
        markdown: Markdown text.

    Returns:
        Plain text.
    """
    return strip_markdown(markdown)


def word_count(markdown: str) -> int:
    """
    Count words in Markdown (excluding formatting).

    Args:
        markdown: Markdown text.

    Returns:
        Word count.
    """
    text = strip_markdown(markdown)
    words = re.findall(r'\b\w+\b', text)
    return len(words)


def extract_images(markdown: str) -> List[Dict[str, str]]:
    """
    Extract all images from Markdown.

    Args:
        markdown: Markdown text.

    Returns:
        List of image dictionaries.
    """
    images: List[Dict[str, str]] = []

    for match in re.finditer(r'!\[(.+?)\]\((.+?)\)', markdown):
        images.append({
            'alt': match.group(1),
            'url': match.group(2),
        })

    return images


def add_syntax_highlighting(
    code: str,
    language: str
) -> str:
    """
    Wrap code in a fenced code block with language.

    Args:
        code: Code content.
        language: Programming language.

    Returns:
        Markdown code block.
    """
    return f'```{language}\n{code}\n```'


def create_link(text: str, url: str) -> str:
    """
    Create a Markdown link.

    Args:
        text: Link text.
        url: Link URL.

    Returns:
        Markdown link.
    """
    return f'[{text}]({url})'


def create_image(alt: str, url: str) -> str:
    """
    Create a Markdown image.

    Args:
        alt: Alt text.
        url: Image URL.

    Returns:
        Markdown image.
    """
    return f'![{alt}]({url})'


def create_heading(text: str, level: int = 1) -> str:
    """
    Create a Markdown heading.

    Args:
        text: Heading text.
        level: Heading level (1-6).

    Returns:
        Markdown heading.
    """
    level = max(1, min(6, level))
    return f"{'#' * level} {text}"


def create_task_list(items: List[Dict[str, Any]]) -> str:
    """
    Create a Markdown task list.

    Args:
        items: List of items with 'text' and 'checked' keys.

    Returns:
        Markdown task list.
    """
    lines = []
    for item in items:
        checkbox = '[x]' if item.get('checked', False) else '[ ]'
        lines.append(f'- {checkbox} {item.get("text", "")}')
    return '\n'.join(lines)


def extract_table_data(markdown_table: str) -> Dict[str, Any]:
    """
    Parse a Markdown table into data.

    Args:
        markdown_table: Markdown table.

    Returns:
        Dictionary with headers and rows.
    """
    lines = markdown_table.strip().split('\n')

    if len(lines) < 2:
        return {'headers': [], 'rows': []}

    headers = [h.strip() for h in lines[0].split('|')[1:-1]]

    rows = []
    for line in lines[2:]:
        cells = [c.strip() for c in line.split('|')[1:-1]]
        if cells:
            rows.append(cells)

    return {'headers': headers, 'rows': rows}


def is_valid_markdown_link(text: str) -> bool:
    """
    Check if text is a valid Markdown link.

    Args:
        text: Text to check.

    Returns:
        True if valid link format.
    """
    return bool(re.match(r'\[.+?\]\(.+?\)', text))


def markdown_to_github_issue(body: str) -> str:
    """
    Format Markdown for GitHub issues.

    Args:
        body: Markdown body.

    Returns:
        GitHub-formatted Markdown.
    """
    lines = body.split('\n')
    result = []

    for line in lines:
        if re.match(r'^#{1,6} ', line):
            result.append(line)
        elif re.match(r'^\- \[ \]', line):
            result.append(line)
        elif re.match(r'^\- \[x\]', line):
            result.append(line)
        else:
            result.append(line)

    return '\n'.join(result)
