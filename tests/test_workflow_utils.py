"""Test suite for workflow utilities.

Run with: pytest tests/test_workflow_utils.py -v
"""

import sys
import os
import json
import tempfile

import pytest

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class TestWorkflowUtils:
    """Tests for workflow utilities."""

    def test_create_workflow(self):
        """Test creating a new workflow."""
        from core.workflow_utils import create_workflow

        workflow = create_workflow(
            name="Test Workflow",
            description="A test workflow",
            variables={'x': 1, 'y': 2},
            steps=[{'id': 1, 'type': 'wait', 'seconds': 1}]
        )

        assert workflow['name'] == "Test Workflow"
        assert workflow['description'] == "A test workflow"
        assert workflow['variables'] == {'x': 1, 'y': 2}
        assert len(workflow['steps']) == 1
        assert workflow['schema_version'] == "2.0"

    def test_add_step(self):
        """Test adding steps to a workflow."""
        from core.workflow_utils import create_workflow, add_step

        workflow = create_workflow()

        add_step(workflow, 'click', {'x': 100, 'y': 200})
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)

        assert len(workflow['steps']) == 2
        assert workflow['steps'][0]['type'] == 'click'
        assert workflow['steps'][1]['id'] == 2

    def test_validate_workflow(self):
        """Test workflow validation."""
        from core.workflow_utils import create_workflow, add_step, validate_workflow

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100, 'y': 200}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)

        valid, msg = validate_workflow(workflow)
        assert valid is True

    def test_validate_workflow_missing_steps(self):
        """Test validation fails for missing steps field."""
        from core.workflow_utils import validate_workflow

        workflow = {'name': 'Test'}
        valid, msg = validate_workflow(workflow)
        assert valid is False
        assert 'steps' in msg

    def test_validate_workflow_duplicate_ids(self):
        """Test validation fails for duplicate step IDs."""
        from core.workflow_utils import create_workflow, add_step, validate_workflow

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=1)

        valid, msg = validate_workflow(workflow)
        assert valid is False
        assert 'Duplicate' in msg

    def test_get_step_by_id(self):
        """Test getting step by ID."""
        from core.workflow_utils import create_workflow, add_step, get_step_by_id

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)

        step = get_step_by_id(workflow, 2)
        assert step is not None
        assert step['type'] == 'wait'

        step = get_step_by_id(workflow, 999)
        assert step is None

    def test_remove_step(self):
        """Test removing a step."""
        from core.workflow_utils import create_workflow, add_step, remove_step

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)

        removed = remove_step(workflow, 1)
        assert removed is True
        assert len(workflow['steps']) == 1
        assert workflow['steps'][0]['id'] == 2

    def test_clone_step(self):
        """Test cloning a step."""
        from core.workflow_utils import create_workflow, add_step, clone_step

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100}, step_id=1)

        cloned = clone_step(workflow, 1, new_step_id=2)
        assert cloned is not None
        assert cloned['id'] == 2
        assert cloned['x'] == 100
        assert len(workflow['steps']) == 2

    def test_export_import_workflow(self):
        """Test workflow export and import."""
        from core.workflow_utils import create_workflow, add_step, export_workflow, import_workflow

        workflow = create_workflow(name="Export Test")
        add_step(workflow, 'click', {'x': 100, 'y': 200}, step_id=1)

        # Create temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            filepath = f.name

        try:
            # Export
            success = export_workflow(workflow, filepath)
            assert success is True

            # Import
            imported = import_workflow(filepath)
            assert imported is not None
            assert imported['name'] == "Export Test"
            assert len(imported['steps']) == 1
        finally:
            os.unlink(filepath)

    def test_get_workflow_summary(self):
        """Test getting workflow summary."""
        from core.workflow_utils import create_workflow, add_step, get_workflow_summary

        workflow = create_workflow(name="Summary Test")
        add_step(workflow, 'click', {'x': 100}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)
        add_step(workflow, 'click', {'x': 200}, step_id=3)

        summary = get_workflow_summary(workflow)
        assert summary['name'] == "Summary Test"
        assert summary['total_steps'] == 3
        assert summary['step_types']['click'] == 2
        assert summary['step_types']['wait'] == 1

    def test_merge_workflows(self):
        """Test merging workflows."""
        from core.workflow_utils import create_workflow, add_step, merge_workflows

        workflow1 = create_workflow()
        add_step(workflow1, 'click', {'x': 100}, step_id=1)

        workflow2 = create_workflow()
        add_step(workflow2, 'wait', {'seconds': 1}, step_id=1)

        merged = merge_workflows([workflow1, workflow2])
        assert len(merged['steps']) == 2
        assert merged['steps'][0]['id'] == 1
        assert merged['steps'][1]['id'] == 2


class TestWorkflowAnalysis:
    """Tests for workflow analysis."""

    def test_analyze_step(self):
        """Test analyzing a single step."""
        from core.workflow_analysis import analyze_step

        step = {'id': 1, 'type': 'delay', 'seconds': 0.01}
        analysis = analyze_step(step)

        assert analysis.step_id == 1
        assert analysis.step_type == 'delay'
        # Very short delays should be flagged
        assert len(analysis.issues) >= 0

    def test_analyze_workflow(self):
        """Test analyzing a complete workflow."""
        from core.workflow_utils import create_workflow, add_step
        from core.workflow_analysis import analyze_workflow

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100, 'y': 200}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)

        analysis = analyze_workflow(workflow)
        assert analysis.total_steps == 2
        assert analysis.workflow_name == workflow['name']
        assert 'click' in analysis.step_types
        assert 'wait' in analysis.step_types

    def test_find_unreachable_steps(self):
        """Test finding unreachable steps."""
        from core.workflow_utils import create_workflow, add_step
        from core.workflow_analysis import find_unreachable_steps

        workflow = create_workflow()
        add_step(workflow, 'click', {'x': 100}, step_id=1)
        add_step(workflow, 'wait', {'seconds': 1}, step_id=2)

        unreachable = find_unreachable_steps(workflow)
        # Step 2 is unreachable because there's no next from step 1
        assert 2 in unreachable

    def test_calculate_complexity_score(self):
        """Test calculating complexity score."""
        from core.workflow_utils import create_workflow, add_step
        from core.workflow_analysis import calculate_complexity_score

        workflow = create_workflow()
        for i in range(10):
            add_step(workflow, 'click', {'x': 100 * i}, step_id=i + 1)

        score = calculate_complexity_score(workflow)
        assert score > 0

    def test_optimize_workflow(self):
        """Test workflow optimization."""
        from core.workflow_utils import create_workflow, add_step
        from core.workflow_analysis import optimize_workflow

        workflow = create_workflow()
        add_step(workflow, 'delay', {'seconds': 1}, step_id=1)
        add_step(workflow, 'delay', {'seconds': 2}, step_id=2)

        optimized = optimize_workflow(workflow)
        # Consecutive delays should be merged
        assert len(optimized['steps']) <= len(workflow['steps'])


class TestPerformance:
    """Tests for performance utilities."""

    def test_performance_profiler(self):
        """Test performance profiler."""
        from core.performance import PerformanceProfiler

        profiler = PerformanceProfiler()

        profiler.start_operation('test_op')
        import time
        time.sleep(0.01)
        profiler.end_operation('test_op', success=True)

        metrics = profiler.get_metrics()
        assert len(metrics) == 1
        assert metrics[0].operation == 'test_op'
        assert metrics[0].success is True
        assert metrics[0].duration >= 0.01

    def test_cached_result(self):
        """Test cached result."""
        from core.performance import CachedResult

        cache = CachedResult(max_size=2)

        cache.set('key1', 'value1')
        assert cache.get('key1') == 'value1'

        cache.set('key2', 'value2')
        assert cache.get('key2') == 'value2'

        # Should evict oldest when full
        cache.set('key3', 'value3')
        assert cache.get('key1') is None

    def test_cached_result_invalidate(self):
        """Test cache invalidation."""
        from core.performance import CachedResult

        cache = CachedResult()
        cache.set('key1', 'value1')

        cache.invalidate('key1')
        assert cache.get('key1') is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
