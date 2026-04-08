"""Jupyter Notebook integration for notebook operations.

Handles Jupyter operations including executing code,
managing cells, and notebook manipulation.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
import json
import base64
import zlib

try:
    import nbformat
    from nbformat import v4 as nbf
except ImportError:
    nbformat = None
    nbf = None

logger = logging.getLogger(__name__)


@dataclass
class JupyterConfig:
    """Configuration for Jupyter operations."""
    kernel_name: str = "python3"
    timeout: int = 300
    allow_errors: bool = False


@dataclass
class CellOutput:
    """Represents a cell output."""
    output_type: str
    text: str
    data: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)


@dataclass
class NotebookCell:
    """Represents a notebook cell."""
    cell_type: str  # code, markdown, raw
    source: str
    outputs: list[CellOutput] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


class JupyterAPIError(Exception):
    """Raised when Jupyter operations fail."""
    pass


class JupyterAction:
    """Jupyter Notebook client for notebook operations."""

    def __init__(self, config: Optional[JupyterConfig] = None):
        """Initialize Jupyter processor with configuration.

        Args:
            config: JupyterConfig with settings

        Raises:
            ImportError: If nbformat is not installed
        """
        if nbformat is None:
            raise ImportError("nbformat required: pip install nbformat")

        self.config = config or JupyterConfig()
        self._notebook = None

    def create_notebook(self, kernel_name: Optional[str] = None) -> dict:
        """Create a new notebook.

        Args:
            kernel_name: Kernel name (uses config default if None)

        Returns:
            Notebook dict
        """
        nb = nbf.new_notebook()
        kernel = kernel_name or self.config.kernel_name
        nb.metadata["kernelspec"] = {
            "display_name": kernel,
            "language": "python",
            "name": kernel
        }

        self._notebook = nb
        return self._notebook_to_dict(nb)

    def parse_notebook(self, file_path: str) -> dict:
        """Parse a notebook from file.

        Args:
            file_path: Path to .ipynb file

        Returns:
            Parsed notebook dict
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                nb = nbformat.read(f, as_version=4)

            self._notebook = nb
            return self._notebook_to_dict(nb)

        except Exception as e:
            raise JupyterAPIError(f"Parse notebook failed: {e}")

    def parse_notebook_string(self, nb_string: str) -> dict:
        """Parse a notebook from string.

        Args:
            nb_string: Notebook JSON as string

        Returns:
            Parsed notebook dict
        """
        try:
            nb = nbformat.reads(nb_string, as_version=4)
            self._notebook = nb
            return self._notebook_to_dict(nb)

        except Exception as e:
            raise JupyterAPIError(f"Parse notebook string failed: {e}")

    def save_notebook(self, file_path: str,
                     notebook: Optional[dict] = None) -> bool:
        """Save a notebook to file.

        Args:
            file_path: Output path
            notebook: Notebook dict (uses internal if None)

        Returns:
            True if successful
        """
        nb = notebook or self._notebook

        if nb is None:
            raise JupyterAPIError("No notebook to save")

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                nbformat.write(nb, f)

            return True

        except Exception as e:
            logger.error(f"Save notebook failed: {e}")
            return False

    def add_code_cell(self, source: str,
                     outputs: Optional[list[dict]] = None,
                     metadata: Optional[dict] = None) -> dict:
        """Add a code cell to the notebook.

        Args:
            source: Code source
            outputs: Optional cell outputs
            metadata: Optional cell metadata

        Returns:
            Cell dict
        """
        if self._notebook is None:
            self.create_notebook()

        cell = nbf.new_code_cell(source)

        if outputs:
            cell.outputs = [self._parse_output(o) for o in outputs]

        if metadata:
            cell.metadata.update(metadata)

        self._notebook.cells.append(cell)
        return self._cell_to_dict(cell)

    def add_markdown_cell(self, source: str,
                         metadata: Optional[dict] = None) -> dict:
        """Add a markdown cell to the notebook.

        Args:
            source: Markdown content
            metadata: Optional cell metadata

        Returns:
            Cell dict
        """
        if self._notebook is None:
            self.create_notebook()

        cell = nbf.new_markdown_cell(source)

        if metadata:
            cell.metadata.update(metadata)

        self._notebook.cells.append(cell)
        return self._cell_to_dict(cell)

    def add_raw_cell(self, source: str,
                    metadata: Optional[dict] = None) -> dict:
        """Add a raw cell to the notebook.

        Args:
            source: Raw content
            metadata: Optional cell metadata

        Returns:
            Cell dict
        """
        if self._notebook is None:
            self.create_notebook()

        cell = nbformat.v4.new_raw_cell(source)

        if metadata:
            cell.metadata.update(metadata)

        self._notebook.cells.append(cell)
        return self._cell_to_dict(cell)

    def insert_cell(self, index: int, cell_type: str, source: str) -> dict:
        """Insert a cell at a specific index.

        Args:
            index: Position to insert at
            cell_type: Cell type (code, markdown, raw)
            source: Cell content

        Returns:
            Cell dict
        """
        if self._notebook is None:
            self.create_notebook()

        if cell_type == "code":
            cell = nbf.new_code_cell(source)
        elif cell_type == "markdown":
            cell = nbf.new_markdown_cell(source)
        else:
            cell = nbformat.v4.new_raw_cell(source)

        self._notebook.cells.insert(index, cell)
        return self._cell_to_dict(cell)

    def delete_cell(self, index: int) -> bool:
        """Delete a cell by index.

        Args:
            index: Cell index to delete

        Returns:
            True if deleted
        """
        if self._notebook is None:
            return False

        if 0 <= index < len(self._notebook.cells):
            del self._notebook.cells[index]
            return True

        return False

    def update_cell(self, index: int, source: str,
                   outputs: Optional[list[dict]] = None) -> dict:
        """Update a cell's content.

        Args:
            index: Cell index
            source: New source
            outputs: Optional new outputs

        Returns:
            Updated cell dict
        """
        if self._notebook is None:
            raise JupyterAPIError("No notebook loaded")

        if index < 0 or index >= len(self._notebook.cells):
            raise JupyterAPIError(f"Cell index {index} out of range")

        cell = self._notebook.cells[index]
        cell.source = source

        if outputs is not None:
            cell.outputs = [self._parse_output(o) for o in outputs]

        return self._cell_to_dict(cell)

    def get_cell(self, index: int) -> Optional[dict]:
        """Get a cell by index.

        Args:
            index: Cell index

        Returns:
            Cell dict or None
        """
        if self._notebook is None:
            return None

        if 0 <= index < len(self._notebook.cells):
            return self._cell_to_dict(self._notebook.cells[index])

        return None

    def get_all_cells(self) -> list[dict]:
        """Get all cells in the notebook.

        Returns:
            List of cell dicts
        """
        if self._notebook is None:
            return []

        return [self._cell_to_dict(cell) for cell in self._notebook.cells]

    def execute_notebook(self, kernel_name: Optional[str] = None,
                       timeout: Optional[int] = None) -> dict:
        """Execute all code cells in the notebook.

        Note: Requires jupyter client runtime. Returns notebook
        with placeholder outputs if execution unavailable.

        Args:
            kernel_name: Kernel to use
            timeout: Execution timeout

        Returns:
            Notebook dict with executed outputs
        """
        if self._notebook is None:
            raise JupyterAPIError("No notebook to execute")

        timeout = timeout or self.config.timeout

        try:
            from nbclient import NotebookClient
            import asyncio

            kernel = kernel_name or self.config.kernel_name
            self._notebook.metadata["kernelspec"]["name"] = kernel

            client = NotebookClient(
                self._notebook,
                timeout=timeout,
                allow_errors=self.config.allow_errors
            )
            client.execute()

            return self._notebook_to_dict(self._notebook)

        except ImportError:
            logger.warning("nbclient not available, notebook not executed")
            return self._notebook_to_dict(self._notebook)
        except Exception as e:
            logger.error(f"Execute notebook failed: {e}")
            raise JupyterAPIError(f"Execute failed: {e}")

    def execute_cell(self, index: int,
                    kernel_name: Optional[str] = None) -> dict:
        """Execute a single cell.

        Args:
            index: Cell index
            kernel_name: Kernel to use

        Returns:
            Cell dict with outputs
        """
        if self._notebook is None:
            raise JupyterAPIError("No notebook loaded")

        if index < 0 or index >= len(self._notebook.cells):
            raise JupyterAPIError(f"Cell index {index} out of range")

        cell = self._notebook.cells[index]

        if cell.cell_type != "code":
            return self._cell_to_dict(cell)

        try:
            from nbclient import NotebookClient

            kernel = kernel_name or self.config.kernel_name
            self._notebook.metadata["kernelspec"]["name"] = kernel

            client = NotebookClient(
                self._notebook,
                timeout=self.config.timeout,
                allow_errors=self.config.allow_errors
            )
            client.execute_cell(cell)

            return self._cell_to_dict(cell)

        except ImportError:
            logger.warning("nbclient not available")
            return self._cell_to_dict(cell)
        except Exception as e:
            logger.error(f"Execute cell failed: {e}")
            raise JupyterAPIError(f"Execute cell failed: {e}")

    def clear_outputs(self, index: Optional[int] = None) -> bool:
        """Clear cell outputs.

        Args:
            index: Cell index (all if None)

        Returns:
            True if cleared
        """
        if self._notebook is None:
            return False

        if index is not None:
            if 0 <= index < len(self._notebook.cells):
                self._notebook.cells[index].outputs = []
                return True
            return False
        else:
            for cell in self._notebook.cells:
                if cell.cell_type == "code":
                    cell.outputs = []
            return True

    def merge_notebooks(self, notebooks: list[dict]) -> dict:
        """Merge multiple notebooks into one.

        Args:
            notebooks: List of notebook dicts

        Returns:
            Merged notebook dict
        """
        if self._notebook is None:
            self.create_notebook()

        for nb_dict in notebooks:
            nb = nbformat.from_dict(nb_dict)
            self._notebook.cells.extend(nb.cells)

        return self._notebook_to_dict(self._notebook)

    def split_notebook(self, split_indices: list[int]) -> list[dict]:
        """Split a notebook into multiple notebooks.

        Args:
            split_indices: Indices where to split

        Returns:
            List of notebook dicts
        """
        if self._notebook is None:
            raise JupyterAPIError("No notebook to split")

        notebooks = []
        prev_idx = 0

        for idx in sorted(split_indices):
            nb = nbf.new_notebook()
            nb.cells = self._notebook.cells[prev_idx:idx]
            notebooks.append(self._notebook_to_dict(nb))
            prev_idx = idx

        nb = nbf.new_notebook()
        nb.cells = self._notebook.cells[prev_idx:]
        notebooks.append(self._notebook_to_dict(nb))

        return notebooks

    def to_html(self, notebook: Optional[dict] = None) -> str:
        """Convert notebook to HTML.

        Args:
            notebook: Notebook dict (uses internal if None)

        Returns:
            HTML string
        """
        nb = notebook or self._notebook

        if nb is None:
            raise JupyterAPIError("No notebook to convert")

        try:
            from nbconvert import HTMLExporter
            exporter = HTMLExporter()
            html, _ = exporter.from_notebook_node(nb)
            return html

        except ImportError:
            logger.warning("nbconvert not available")
            return "<pre>HTML export not available</pre>"
        except Exception as e:
            logger.error(f"HTML export failed: {e}")
            return f"<pre>Error: {e}</pre>"

    def to_markdown(self, notebook: Optional[dict] = None) -> str:
        """Convert notebook to Markdown.

        Args:
            notebook: Notebook dict (uses internal if None)

        Returns:
            Markdown string
        """
        nb = notebook or self._notebook

        if nb is None:
            raise JupyterAPIError("No notebook to convert")

        lines = []
        lines.append(f"# {nb.metadata.get('name', 'Notebook')}")
        lines.append("")

        for cell in nb.cells:
            if cell.cell_type == "markdown":
                lines.append(cell.source)
                lines.append("")
            elif cell.cell_type == "code":
                lines.append("```python")
                lines.append(cell.source)
                lines.append("```")
                lines.append("")

                for output in cell.outputs:
                    if output.output_type == "stream":
                        lines.append(f"```\n{output.text}\n```")
                        lines.append("")

        return "\n".join(lines)

    def _notebook_to_dict(self, nb) -> dict:
        """Convert notebook node to dict."""
        return nbformat.to_dict(nb)

    def _cell_to_dict(self, cell) -> dict:
        """Convert cell node to dict."""
        return {
            "cell_type": cell.cell_type,
            "source": cell.source,
            "outputs": [self._output_to_dict(o) for o in getattr(cell, "outputs", [])],
            "metadata": dict(cell.metadata)
        }

    def _output_to_dict(self, output) -> dict:
        """Convert output to dict."""
        return {
            "output_type": output.output_type,
            "text": getattr(output, "text", ""),
            "data": dict(getattr(output, "data", {})),
            "metadata": dict(getattr(output, "metadata", {}))
        }

    def _parse_output(self, output: dict) -> Any:
        """Parse output dict to output object."""
        output_type = output.get("output_type", "stream")

        if output_type == "stream":
            out = nbformat.v4.new_output("stream", text=output.get("text", ""))
        elif output_type == "execute_result":
            out = nbformat.v4.new_output(
                "execute_result",
                data=output.get("data", {}),
                execution_count=output.get("execution_count")
            )
        elif output_type == "error":
            out = nbformat.v4.new_output(
                "error",
                ename=output.get("ename", ""),
                evalue=output.get("evalue", ""),
                traceback=output.get("traceback", [])
            )
        else:
            out = nbformat.v4.new_output(output_type)

        return out
