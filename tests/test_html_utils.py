"""Tests for HTML utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.html_utils import (
    escape_html,
    unescape_html,
    strip_tags,
    extract_text,
    get_tag_content,
    get_attribute,
    is_valid_html,
    add_class,
    remove_class,
    sanitize_html,
    make_links_clickable,
    linkify,
    html_to_text_lines,
    create_tag,
    create_link,
    create_image,
    create_paragraph,
    wrap_in_tag,
    indent_html,
    minify_html,
    build_table_row,
    build_table,
    build_unordered_list,
    build_ordered_list,
    extract_links,
    extract_images,
    extract_meta_tags,
    get_page_title,
    is_email,
    is_url,
)


class TestEscapeHtml:
    """Tests for escape_html function."""

    def test_escape_html(self) -> None:
        """Test escaping HTML."""
        result = escape_html("<div>Hello & World</div>")
        assert "&lt;div&gt;" in result
        assert "&amp;" in result


class TestUnescapeHtml:
    """Tests for unescape_html function."""

    def test_unescape_html(self) -> None:
        """Test unescaping HTML."""
        result = unescape_html("&lt;div&gt;Hello &amp; World&lt;/div&gt;")
        assert "<div>" in result


class TestStripTags:
    """Tests for strip_tags function."""

    def test_strip_tags(self) -> None:
        """Test stripping HTML tags."""
        result = strip_tags("<p>Hello <b>World</b></p>")
        assert result == "Hello World"


class TestExtractText:
    """Tests for extract_text function."""

    def test_extract_text(self) -> None:
        """Test extracting text from HTML."""
        result = extract_text("<p>Hello   World</p>")
        assert result == "Hello World"


class TestGetTagContent:
    """Tests for get_tag_content function."""

    def test_get_tag_content(self) -> None:
        """Test extracting tag content."""
        html = "<p>First</p><p>Second</p>"
        result = get_tag_content(html, "p")
        assert result == ["First", "Second"]


class TestGetAttribute:
    """Tests for get_attribute function."""

    def test_get_attribute(self) -> None:
        """Test extracting attribute."""
        html = '<a href="http://example.com">Link</a>'
        result = get_attribute(html, "a", "href")
        assert result == ["http://example.com"]


class TestIsValidHtml:
    """Tests for is_valid_html function."""

    def test_is_valid_html(self) -> None:
        """Test validating HTML."""
        assert is_valid_html("<p>Hello</p>")
        assert is_valid_html("<p>Hello</p><div>World</div>")


class TestAddClass:
    """Tests for add_class function."""

    def test_add_class(self) -> None:
        """Test adding class to HTML."""
        result = add_class('<div>Test</div>', 'div', 'my-class')
        assert 'class="my-class"' in result


class TestRemoveClass:
    """Tests for remove_class function."""

    def test_remove_class(self) -> None:
        """Test removing class from HTML."""
        result = remove_class('<div class="my-class">Test</div>', 'div', 'my-class')
        assert 'class' not in result


class TestSanitizeHtml:
    """Tests for sanitize_html function."""

    def test_sanitize_html(self) -> None:
        """Test sanitizing HTML."""
        html = "<p>Hello</p><script>alert('xss')</script>"
        result = sanitize_html(html, allowed_tags=['p'])
        assert '<script>' not in result


class TestMakeLinksClickable:
    """Tests for make_links_clickable function."""

    def test_make_links_clickable(self) -> None:
        """Test converting links to clickable."""
        result = make_links_clickable("Visit http://example.com")
        assert '<a href="http://example.com">' in result


class TestLinkify:
    """Tests for linkify function."""

    def test_linkify(self) -> None:
        """Test linkifying URLs."""
        result = linkify("Visit http://example.com")
        assert '<a href="http://example.com">' in result


class TestHtmlToTextLines:
    """Tests for html_to_text_lines function."""

    def test_html_to_text_lines(self) -> None:
        """Test converting HTML to text lines."""
        html = "<p>Line 1</p><p>Line 2</p>"
        result = html_to_text_lines(html)
        assert "Line 1" in result
        assert "Line 2" in result


class TestCreateTag:
    """Tests for create_tag function."""

    def test_create_tag(self) -> None:
        """Test creating HTML tag."""
        result = create_tag("p", "Hello")
        assert result == "<p>Hello</p>"

    def test_create_tag_with_attrs(self) -> None:
        """Test creating HTML tag with attributes."""
        result = create_tag("a", "Link", {"href": "http://example.com"})
        assert 'href="http://example.com"' in result


class TestCreateLink:
    """Tests for create_link function."""

    def test_create_link(self) -> None:
        """Test creating anchor tag."""
        result = create_link("http://example.com", "Example")
        assert '<a href="http://example.com">Example</a>' in result


class TestCreateImage:
    """Tests for create_image function."""

    def test_create_image(self) -> None:
        """Test creating image tag."""
        result = create_image("image.png", "Alt text")
        assert 'src="image.png"' in result
        assert 'alt="Alt text"' in result


class TestCreateParagraph:
    """Tests for create_paragraph function."""

    def test_create_paragraph(self) -> None:
        """Test creating paragraph."""
        result = create_paragraph("Hello")
        assert result == "<p>Hello</p>"


class TestWrapInTag:
    """Tests for wrap_in_tag function."""

    def test_wrap_in_tag(self) -> None:
        """Test wrapping in tag."""
        result = wrap_in_tag("Hello", "strong")
        assert "<strong>Hello</strong>" in result


class TestIndentHtml:
    """Tests for indent_html function."""

    def test_indent_html(self) -> None:
        """Test indenting HTML."""
        html = "<div><p>Hello</p></div>"
        result = indent_html(html)
        assert '\n' in result


class TestMinifyHtml:
    """Tests for minify_html function."""

    def test_minify_html(self) -> None:
        """Test minifying HTML."""
        html = "<div>\n  <p>Hello</p>\n</div>"
        result = minify_html(html)
        assert '\n' not in result


class TestBuildTableRow:
    """Tests for build_table_row function."""

    def test_build_table_row(self) -> None:
        """Test building table row."""
        result = build_table_row(["A", "B"])
        assert "<td>A</td>" in result
        assert "<td>B</td>" in result


class TestBuildTable:
    """Tests for build_table function."""

    def test_build_table(self) -> None:
        """Test building table."""
        result = build_table(["A", "B"], [["1", "2"]])
        assert "<table>" in result
        assert "<thead>" in result
        assert "<tbody>" in result


class TestBuildUnorderedList:
    """Tests for build_unordered_list function."""

    def test_build_unordered_list(self) -> None:
        """Test building unordered list."""
        result = build_unordered_list(["A", "B"])
        assert "<ul>" in result
        assert "<li>A</li>" in result


class TestBuildOrderedList:
    """Tests for build_ordered_list function."""

    def test_build_ordered_list(self) -> None:
        """Test building ordered list."""
        result = build_ordered_list(["A", "B"])
        assert "<ol>" in result
        assert "<li>A</li>" in result


class TestExtractLinks:
    """Tests for extract_links function."""

    def test_extract_links(self) -> None:
        """Test extracting links."""
        html = '<a href="http://example.com">Link</a>'
        result = extract_links(html)
        assert "http://example.com" in result


class TestExtractImages:
    """Tests for extract_images function."""

    def test_extract_images(self) -> None:
        """Test extracting images."""
        html = '<img src="image.png" alt="test">'
        result = extract_images(html)
        assert "image.png" in result


class TestExtractMetaTags:
    """Tests for extract_meta_tags function."""

    def test_extract_meta_tags(self) -> None:
        """Test extracting meta tags."""
        html = '<meta name="description" content="Test description">'
        result = extract_meta_tags(html)
        assert "description" in result


class TestGetPageTitle:
    """Tests for get_page_title function."""

    def test_get_page_title(self) -> None:
        """Test extracting page title."""
        html = "<title>Page Title</title>"
        result = get_page_title(html)
        assert result == "Page Title"


class TestIsEmail:
    """Tests for is_email function."""

    def test_is_email_valid(self) -> None:
        """Test valid email."""
        assert is_email("test@example.com")

    def test_is_email_invalid(self) -> None:
        """Test invalid email."""
        assert not is_email("not-an-email")


class TestIsUrl:
    """Tests for is_url function."""

    def test_is_url_valid(self) -> None:
        """Test valid URL."""
        assert is_url("http://example.com")

    def test_is_url_invalid(self) -> None:
        """Test invalid URL."""
        assert not is_url("not-a-url")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
