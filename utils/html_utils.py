"""HTML utilities for RabAI AutoClick.

Provides:
- HTML parsing and manipulation
- HTML entity encoding/decoding
- HTML sanitization
"""

import re
from typing import List, Optional


def escape_html(text: str) -> str:
    """Escape HTML special characters.

    Args:
        text: Text to escape.

    Returns:
        Escaped text.
    """
    html_escape_table = {
        "&": "&amp;",
        '"': "&quot;",
        "'": "&#x27;",
        ">": "&gt;",
        "<": "&lt;",
    }
    return "".join(html_escape_table.get(c, c) for c in text)


def unescape_html(text: str) -> str:
    """Unescape HTML entities.

    Args:
        text: HTML text to unescape.

    Returns:
        Unescaped text.
    """
    html_unescape_table = {
        "&amp;": "&",
        "&quot;": '"',
        "&#x27;": "'",
        "&gt;": ">",
        "&lt;": "<",
        "&nbsp;": " ",
        "&#39;": "'",
        "&apos;": "'",
    }
    result = text
    for entity, char in html_unescape_table.items():
        result = result.replace(entity, char)
    return result


def strip_tags(html: str) -> str:
    """Remove HTML tags from text.

    Args:
        html: HTML string.

    Returns:
        Plain text.
    """
    return re.sub(r'<[^>]+>', '', html)


def extract_text(html: str) -> str:
    """Extract text content from HTML.

    Args:
        html: HTML string.

    Returns:
        Extracted text.
    """
    text = strip_tags(html)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def get_tag_content(html: str, tag: str) -> List[str]:
    """Extract content from HTML tags.

    Args:
        html: HTML string.
        tag: Tag name to extract.

    Returns:
        List of tag contents.
    """
    pattern = rf'<{tag}[^>]*>([^<]*)</{tag}>'
    matches = re.findall(pattern, html, re.IGNORECASE)
    return [m.strip() for m in matches if m.strip()]


def get_attribute(html: str, tag: str, attribute: str) -> List[str]:
    """Extract attribute values from HTML tags.

    Args:
        html: HTML string.
        tag: Tag name.
        attribute: Attribute name.

    Returns:
        List of attribute values.
    """
    pattern = rf'<{tag}[^>]*\s{attribute}="([^"]*)"[^>]*>'
    matches = re.findall(pattern, html, re.IGNORECASE)
    return matches


def is_valid_html(text: str) -> bool:
    """Check if text contains valid HTML.

    Args:
        text: Text to check.

    Returns:
        True if valid HTML.
    """
    if '<' not in text or '>' not in text:
        return False
    open_tags = len(re.findall(r'<[a-zA-Z][^>]*\s[^>]*>', text))
    close_tags = len(re.findall(r'</[^>]+>', text))
    self_closing = len(re.findall(r'<[^>]*/>', text))
    simple_tags = len(re.findall(r'<[a-zA-Z]+>', text))
    return (open_tags + simple_tags) >= close_tags or self_closing > 0


def add_class(html: str, tag: str, class_name: str) -> str:
    """Add class to HTML tag.

    Args:
        html: HTML string.
        tag: Tag name.
        class_name: Class to add.

    Returns:
        Modified HTML.
    """
    def replacement(match):
        tag_content = match.group(1)
        if 'class="' in tag_content:
            return match.group(0)
        return f'{tag_content} class="{class_name}">'

    pattern = rf'(<{tag}[^>]*)(>)'
    return re.sub(pattern, replacement, html, flags=re.IGNORECASE)


def remove_class(html: str, tag: str, class_name: str) -> str:
    """Remove class from HTML tag.

    Args:
        html: HTML string.
        tag: Tag name.
        class_name: Class to remove.

    Returns:
        Modified HTML.
    """
    pattern = rf'class="[^"]*\b{class_name}\b[^"]*"'
    return re.sub(pattern, '', html, flags=re.IGNORECASE)


def sanitize_html(html: str, allowed_tags: List[str] = None) -> str:
    """Sanitize HTML by removing disallowed tags.

    Args:
        html: HTML to sanitize.
        allowed_tags: List of allowed tag names.

    Returns:
        Sanitized HTML.
    """
    if allowed_tags is None:
        allowed_tags = ['p', 'br', 'b', 'i', 'u', 'em', 'strong', 'a', 'ul', 'ol', 'li']

    def replace_tag(match):
        tag = match.group(1)
        if tag.lower() in allowed_tags:
            return match.group(0)
        return ''

    pattern = r'<(\w+)[^>]*>'
    return re.sub(pattern, replace_tag, html)


def make_links_clickable(text: str) -> str:
    """Convert URLs in text to clickable links.

    Args:
        text: Text with URLs.

    Returns:
        Text with clickable links.
    """
    def replace_url(match):
        url = match.group(0)
        return f'<a href="{url}">{url}</a>'
    url_pattern = r'https?://[^\s<]+'
    return re.sub(url_pattern, replace_url, text)


def linkify(text: str, target: str = None) -> str:
    """Convert URLs to anchor tags.

    Args:
        text: Text with URLs.
        target: Optional target attribute.

    Returns:
        HTML with links.
    """
    def make_link(match):
        url = match.group(0)
        target_attr = f' target="{target}"' if target else ''
        return f'<a href="{url}"{target_attr}>{url}</a>'
    return re.sub(r'https?://[^\s<]+', make_link, text)


def html_to_text_lines(html: str) -> List[str]:
    """Convert HTML to text lines.

    Args:
        html: HTML string.

    Returns:
        List of text lines.
    """
    text = re.sub(r'</p>|<br\s*/?>|<li>', '\n', html, flags=re.IGNORECASE)
    text = strip_tags(text)
    lines = text.split('\n')
    return [line.strip() for line in lines if line.strip()]


def create_tag(tag: str, content: str = "", attributes: dict = None) -> str:
    """Create HTML tag.

    Args:
        tag: Tag name.
        content: Tag content.
        attributes: Optional attributes dict.

    Returns:
        HTML tag string.
    """
    attrs = ""
    if attributes:
        attr_strs = [f'{k}="{v}"' for k, v in attributes.items()]
        attrs = " " + " ".join(attr_strs)
    if content:
        return f"<{tag}{attrs}>{content}</{tag}>"
    return f"<{tag}{attrs} />"


def create_link(url: str, text: str = None, attributes: dict = None) -> str:
    """Create anchor tag.

    Args:
        url: Link URL.
        text: Link text.
        attributes: Optional attributes dict.

    Returns:
        HTML anchor tag.
    """
    if text is None:
        text = url
    if attributes is None:
        attributes = {}
    attributes['href'] = url
    return create_tag('a', text, attributes)


def create_image(src: str, alt: str = "", attributes: dict = None) -> str:
    """Create image tag.

    Args:
        src: Image source URL.
        alt: Alt text.
        attributes: Optional attributes dict.

    Returns:
        HTML image tag.
    """
    if attributes is None:
        attributes = {}
    attributes['src'] = src
    attributes['alt'] = alt
    return create_tag('img', "", attributes)


def create_paragraph(text: str, attributes: dict = None) -> str:
    """Create paragraph tag.

    Args:
        text: Paragraph text.
        attributes: Optional attributes dict.

    Returns:
        HTML paragraph tag.
    """
    return create_tag('p', text, attributes)


def wrap_in_tag(text: str, tag: str, attributes: dict = None) -> str:
    """Wrap text in tag.

    Args:
        text: Text to wrap.
        tag: Tag name.
        attributes: Optional attributes dict.

    Returns:
        Wrapped text.
    """
    return create_tag(tag, text, attributes)


def indent_html(html: str, spaces: int = 2) -> str:
    """Indent HTML for readability.

    Args:
        html: HTML to indent.
        spaces: Number of spaces to indent.

    Returns:
        Indented HTML.
    """
    indent_str = ' ' * spaces
    lines = []
    depth = 0

    tokens = re.split(r'(<[^>]+>)', html)
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token.startswith('</'):
            depth = max(0, depth - 1)
            lines.append(indent_str * depth + token)
        elif token.startswith('<') and not token.endswith('/>'):
            lines.append(indent_str * depth + token)
            if not token.startswith('<!'):
                depth += 1
        else:
            if token.strip():
                lines.append(indent_str * depth + token)
    return '\n'.join(lines)


def minify_html(html: str) -> str:
    """Minify HTML by removing whitespace.

    Args:
        html: HTML to minify.

    Returns:
        Minified HTML.
    """
    html = re.sub(r'<!--.*?-->', '', html)
    html = re.sub(r'\s+', ' ', html)
    html = re.sub(r'>\s+<', '><', html)
    return html.strip()


def build_table_row(cells: List[str], is_header: bool = False) -> str:
    """Build HTML table row.

    Args:
        cells: List of cell contents.
        is_header: Whether to use th tags.

    Returns:
        HTML table row.
    """
    tag = 'th' if is_header else 'td'
    cell_tags = [f'<{tag}>{cell}</{tag}>' for cell in cells]
    return f'<tr>{"".join(cell_tags)}</tr>'


def build_table(headers: List[str], rows: List[List[str]]) -> str:
    """Build HTML table.

    Args:
        headers: List of header texts.
        rows: List of row cell lists.

    Returns:
        HTML table.
    """
    table = ['<table>']
    table.append(f'<thead>{build_table_row(headers, True)}</thead>')
    table.append('<tbody>')
    for row in rows:
        table.append(build_table_row(row))
    table.append('</tbody>')
    table.append('</table>')
    return ''.join(table)


def build_unordered_list(items: List[str]) -> str:
    """Build unordered list.

    Args:
        items: List item texts.

    Returns:
        HTML unordered list.
    """
    list_items = [f'<li>{item}</li>' for item in items]
    return f'<ul>{"".join(list_items)}</ul>'


def build_ordered_list(items: List[str]) -> str:
    """Build ordered list.

    Args:
        items: List item texts.

    Returns:
        HTML ordered list.
    """
    list_items = [f'<li>{item}</li>' for item in items]
    return f'<ol>{"".join(list_items)}</ol>'


def extract_links(html: str) -> List[str]:
    """Extract all links from HTML.

    Args:
        html: HTML string.

    Returns:
        List of URLs.
    """
    pattern = r'<a[^>]+href="([^"]*)"[^>]*>'
    return re.findall(pattern, html, re.IGNORECASE)


def extract_images(html: str) -> List[str]:
    """Extract all image sources from HTML.

    Args:
        html: HTML string.

    Returns:
        List of image sources.
    """
    pattern = r'<img[^>]+src="([^"]*)"[^>]*>'
    return re.findall(pattern, html, re.IGNORECASE)


def extract_meta_tags(html: str) -> dict:
    """Extract meta tags from HTML.

    Args:
        html: HTML string.

    Returns:
        Dict of meta tag name -> content.
    """
    result = {}
    pattern = r'<meta[^>]+name="([^"]*)"[^>]+content="([^"]*)"[^>]*>'
    matches = re.findall(pattern, html, re.IGNORECASE)
    for name, content in matches:
        result[name] = content
    pattern2 = r'<meta[^>]+content="([^"]*)"[^>]+name="([^"]*)"[^>]*>'
    matches2 = re.findall(pattern2, html, re.IGNORECASE)
    for content, name in matches2:
        result[name] = content
    return result


def get_page_title(html: str) -> Optional[str]:
    """Extract page title from HTML.

    Args:
        html: HTML string.

    Returns:
        Page title or None.
    """
    titles = get_tag_content(html, 'title')
    return titles[0] if titles else None


def is_email(email: str) -> bool:
    """Check if string is valid email format.

    Args:
        email: Email string.

    Returns:
        True if valid email format.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def is_url(url: str) -> bool:
    """Check if string is valid URL format.

    Args:
        url: URL string.

    Returns:
        True if valid URL format.
    """
    pattern = r'^https?://[^\s<]+'
    return bool(re.match(pattern, url))
