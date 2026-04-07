"""Tests for history utilities."""

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Mock PyQt5 before any imports
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtWidgets'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()
sys.modules['PyQt5.QtGui'] = MagicMock()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly to avoid utils/__init__.py issues
import importlib.util


def load_module_from_file(module_name: str, file_path: str):
    """Load a module directly from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict('sys.modules', {
        'PyQt5': MagicMock(),
        'PyQt5.QtWidgets': MagicMock(),
        'PyQt5.QtCore': MagicMock(),
        'PyQt5.QtGui': MagicMock(),
    }):
        spec.loader.exec_module(module)
    return module


history_module = load_module_from_file(
    "history",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "history.py")
)

WorkflowHistoryManager = history_module.WorkflowHistoryManager


class TestWorkflowHistoryManager:
    """Tests for WorkflowHistoryManager."""

    @pytest.fixture
    def temp_dir(self) -> str:
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as td:
            yield td

    @pytest.fixture
    def manager(self, temp_dir: str) -> WorkflowHistoryManager:
        """Create a WorkflowHistoryManager with temp directory."""
        return WorkflowHistoryManager(history_dir=temp_dir)

    def test_init_creates_directory(self, temp_dir: str) -> None:
        """Test initialization creates history directory."""
        mgr = WorkflowHistoryManager(history_dir=temp_dir)
        assert os.path.exists(temp_dir)

    def test_save_workflow(self, manager: WorkflowHistoryManager) -> None:
        """Test saving a workflow."""
        workflow = {"steps": [{"action": "click"}]}
        path = manager.save_workflow("Test Workflow", workflow)
        assert os.path.exists(path)
        assert "Test_Workflow" in path or "Test Workflow" in open(path).read()

    def test_save_workflow_with_tags(self, manager: WorkflowHistoryManager) -> None:
        """Test saving workflow with tags."""
        workflow = {"steps": []}
        path = manager.save_workflow("Tagged", workflow, tags=["test", "demo"])
        assert os.path.exists(path)

    def test_load_workflow(self, manager: WorkflowHistoryManager) -> None:
        """Test loading a workflow."""
        workflow = {"steps": [{"action": "click", "target": "button"}]}
        path = manager.save_workflow("Load Test", workflow)
        filename = os.path.basename(path)
        loaded = manager.load_workflow(filename)
        assert loaded is not None
        assert "steps" in loaded

    def test_load_workflow_not_found(self, manager: WorkflowHistoryManager) -> None:
        """Test loading nonexistent workflow returns None."""
        result = manager.load_workflow("nonexistent_file.json")
        assert result is None

    def test_delete_workflow(self, manager: WorkflowHistoryManager) -> None:
        """Test deleting a workflow."""
        workflow = {"steps": []}
        path = manager.save_workflow("Delete Me", workflow)
        filename = os.path.basename(path)
        assert os.path.exists(path)
        result = manager.delete_workflow(filename)
        assert result is True
        assert not os.path.exists(path)

    def test_delete_workflow_not_found(self, manager: WorkflowHistoryManager) -> None:
        """Test deleting nonexistent workflow returns False."""
        result = manager.delete_workflow("nonexistent.json")
        assert result is False

    def test_rename_workflow(self, manager: WorkflowHistoryManager) -> None:
        """Test renaming a workflow."""
        workflow = {"steps": []}
        path = manager.save_workflow("Old Name", workflow)
        filename = os.path.basename(path)
        result = manager.rename_workflow(filename, "New Name")
        assert result is True

    def test_get_all_workflows(self, manager: WorkflowHistoryManager) -> None:
        """Test getting all workflows."""
        manager.save_workflow("WF1", {"steps": []})
        manager.save_workflow("WF2", {"steps": []})
        workflows = manager.get_all_workflows()
        assert len(workflows) == 2

    def test_search_workflows(self, manager: WorkflowHistoryManager) -> None:
        """Test searching workflows."""
        manager.save_workflow("Alpha Workflow", {"steps": []}, tags=["test"])
        manager.save_workflow("Beta Workflow", {"steps": []}, tags=["demo"])
        results = manager.search_workflows("alpha")
        assert len(results) == 1
        assert "Alpha" in results[0]["name"]

    def test_search_workflows_by_tag(self, manager: WorkflowHistoryManager) -> None:
        """Test searching workflows by tag."""
        manager.save_workflow("WF1", {"steps": []}, tags=["test"])
        manager.save_workflow("WF2", {"steps": []}, tags=["demo"])
        results = manager.search_workflows("test")
        assert len(results) == 1

    def test_save_workflow_updates_existing(self, manager: WorkflowHistoryManager) -> None:
        """Test saving workflow with same name updates existing."""
        workflow1 = {"steps": [{"action": "click"}]}
        workflow2 = {"steps": [{"action": "type"}]}
        manager.save_workflow("Same Name", workflow1)
        manager.save_workflow("Same Name", workflow2)
        workflows = manager.get_all_workflows()
        assert len(workflows) == 1

    def test_index_file_created_after_save(self, temp_dir: str) -> None:
        """Test index file is created after first save."""
        mgr = WorkflowHistoryManager(history_dir=temp_dir)
        mgr.save_workflow("WF", {"steps": []})
        index_path = os.path.join(temp_dir, 'index.json')
        assert os.path.exists(index_path)

    def test_load_index_creates_default(self, temp_dir: str) -> None:
        """Test loading nonexistent index creates default."""
        mgr = WorkflowHistoryManager(history_dir=temp_dir)
        assert mgr.index == {'workflows': []}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
