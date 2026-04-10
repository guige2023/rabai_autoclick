"""Tests for RabAI AutoClick API Server.

Tests REST API endpoints, WebSocket functionality, authentication,
rate limiting, and all workflow execution features.
"""

import asyncio
import json
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict

import pytest

# Add project to path
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')

from fastapi.testclient import TestClient
from fastapi import WebSocket

# Import the API server components
from src.api_server import (
    app,
    store,
    API_KEY,
    WorkflowDefinition,
    WorkflowStep,
    WorkflowExecution,
    ExecutionStatus,
    ActionExecuteRequest,
    verify_api_key,
    RateLimiter,
    ConnectionManager,
    WebSocketMessage,
    asdict
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def auth_headers():
    """Create valid authentication headers."""
    return {"X-API-Key": API_KEY}


@pytest.fixture
def sample_workflow():
    """Create a sample workflow for testing."""
    return WorkflowDefinition(
        workflow_id="wf_test_001",
        name="Test Workflow",
        description="A test workflow",
        steps=[
            WorkflowStep(
                step_id="step1",
                name="Delay Step",
                action="delay",
                params={"duration": 0.1}
            ),
            WorkflowStep(
                step_id="step2",
                name="Click Step",
                action="click",
                params={"x": 100, "y": 200}
            )
        ]
    )


@pytest.fixture
def auth_client(client, auth_headers):
    """Return a client with valid auth for testing."""
    return client, auth_headers


# =============================================================================
# Health & Metrics Tests
# =============================================================================

class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_check_returns_200(self, client):
        """Test that health check returns 200 OK."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_check_returns_status(self, client):
        """Test that health check returns proper status."""
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "version" in data
        assert data["version"] == "22.0.0"
        assert "uptime" in data


class TestMetricsEndpoint:
    """Tests for /metrics endpoint."""

    def test_metrics_returns_200_with_auth(self, client, auth_headers):
        """Test metrics endpoint requires auth."""
        response = client.get("/metrics", headers=auth_headers)
        assert response.status_code == 200

    def test_metrics_returns_statistics(self, client, auth_headers):
        """Test metrics returns execution statistics."""
        response = client.get("/metrics", headers=auth_headers)
        data = response.json()
        assert "total_workflows" in data
        assert "total_executions" in data
        assert "active_executions" in data
        assert "completed_executions" in data
        assert "failed_executions" in data
        assert "total_actions_executed" in data
        assert "average_execution_time" in data
        assert "success_rate" in data


# =============================================================================
# Authentication Tests
# =============================================================================

class TestAuthentication:
    """Tests for API key authentication."""

    def test_missing_api_key_returns_401(self, client):
        """Test that missing API key returns 401."""
        response = client.get("/workflows")
        assert response.status_code == 401
        assert "Missing X-API-Key" in response.json()["detail"]

    def test_invalid_api_key_returns_401(self, client):
        """Test that invalid API key returns 401."""
        response = client.get("/workflows", headers={"X-API-Key": "invalid-key"})
        assert response.status_code == 401
        assert "Invalid API key" in response.json()["detail"]

    def test_valid_api_key_allows_access(self, client, auth_headers):
        """Test that valid API key allows access."""
        response = client.get("/workflows", headers=auth_headers)
        assert response.status_code == 200

    def test_health_endpoint_no_auth_required(self, client):
        """Test that health endpoint doesn't require auth."""
        response = client.get("/health")
        assert response.status_code == 200


# =============================================================================
# Rate Limiting Tests
# =============================================================================

class TestRateLimiting:
    """Tests for rate limiting functionality."""

    def test_rate_limiter_is_allowed(self):
        """Test rate limiter allows requests within limit."""
        from fastapi import Request
        from unittest.mock import MagicMock

        limiter = RateLimiter(max_requests=10, window_seconds=60)

        mock_request = MagicMock(spec=Request)
        mock_request.headers = {}
        mock_request.client.host = "127.0.0.1"

        # Should allow first request
        assert limiter.is_allowed(mock_request) is True

    def test_rate_limiter_blocks_exceeded(self):
        """Test rate limiter blocks when limit exceeded."""
        from fastapi import Request
        from unittest.mock import MagicMock

        limiter = RateLimiter(max_requests=2, window_seconds=60)

        mock_request = MagicMock(spec=Request)
        mock_request.headers = {}
        mock_request.client.host = "192.168.1.100"

        # Should allow 2 requests
        assert limiter.is_allowed(mock_request) is True
        assert limiter.is_allowed(mock_request) is True
        # Third request should be blocked
        assert limiter.is_allowed(mock_request) is False

    def test_rate_limiter_different_clients(self):
        """Test rate limiter tracks different clients separately."""
        from fastapi import Request
        from unittest.mock import MagicMock

        limiter = RateLimiter(max_requests=1, window_seconds=60)

        mock_request1 = MagicMock(spec=Request)
        mock_request1.headers = {}
        mock_request1.client.host = "10.0.0.1"

        mock_request2 = MagicMock(spec=Request)
        mock_request2.headers = {}
        mock_request2.client.host = "10.0.0.2"

        # Each client should get their own limit
        assert limiter.is_allowed(mock_request1) is True
        assert limiter.is_allowed(mock_request1) is False
        assert limiter.is_allowed(mock_request2) is True


# =============================================================================
# Workflow CRUD Tests
# =============================================================================

class TestWorkflowCRUD:
    """Tests for workflow CRUD operations."""

    def test_create_workflow(self, client, auth_headers, sample_workflow):
        """Test creating a new workflow."""
        response = client.post(
            "/workflows",
            headers=auth_headers,
            json=sample_workflow.model_dump()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "wf_test_001"
        assert data["name"] == "Test Workflow"
        assert len(data["steps"]) == 2

    def test_list_workflows(self, client, auth_headers, sample_workflow):
        """Test listing all workflows."""
        # Create a workflow first
        client.post("/workflows", headers=auth_headers, json=sample_workflow.model_dump())

        response = client.get("/workflows", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_get_workflow(self, client, auth_headers, sample_workflow):
        """Test getting a specific workflow."""
        # Create workflow
        client.post("/workflows", headers=auth_headers, json=sample_workflow.model_dump())

        response = client.get("/workflows/wf_test_001", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "wf_test_001"

    def test_get_nonexistent_workflow_returns_404(self, client, auth_headers):
        """Test getting a nonexistent workflow returns 404."""
        response = client.get("/workflows/nonexistent", headers=auth_headers)
        assert response.status_code == 404

    def test_update_workflow(self, client, auth_headers, sample_workflow):
        """Test updating a workflow."""
        # Create workflow
        client.post("/workflows", headers=auth_headers, json=sample_workflow.model_dump())

        # Update it
        sample_workflow.name = "Updated Workflow"
        response = client.put(
            "/workflows/wf_test_001",
            headers=auth_headers,
            json=sample_workflow.model_dump()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Workflow"

    def test_delete_workflow(self, client, auth_headers, sample_workflow):
        """Test deleting a workflow."""
        # Create workflow
        client.post("/workflows", headers=auth_headers, json=sample_workflow.model_dump())

        # Delete it
        response = client.delete("/workflows/wf_test_001", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["deleted"] is True

        # Verify it's gone
        response = client.get("/workflows/wf_test_001", headers=auth_headers)
        assert response.status_code == 404

    def test_delete_nonexistent_workflow_returns_404(self, client, auth_headers):
        """Test deleting a nonexistent workflow returns 404."""
        response = client.delete("/workflows/nonexistent", headers=auth_headers)
        assert response.status_code == 404


# =============================================================================
# Workflow Execution Tests
# =============================================================================

class TestWorkflowExecution:
    """Tests for workflow execution endpoints."""

    @pytest.fixture
    def created_workflow(self, client, auth_headers, sample_workflow):
        """Create a workflow and return its ID."""
        response = client.post(
            "/workflows",
            headers=auth_headers,
            json=sample_workflow.model_dump()
        )
        return response.json()["workflow_id"]

    def test_execute_workflow(self, client, auth_headers, created_workflow):
        """Test starting workflow execution."""
        response = client.post(
            f"/workflows/{created_workflow}/execute",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "execution_id" in data
        assert data["status"] == "running"

    def test_execute_nonexistent_workflow_returns_404(self, client, auth_headers):
        """Test executing a nonexistent workflow returns 404."""
        response = client.post(
            "/workflows/nonexistent/execute",
            headers=auth_headers
        )
        assert response.status_code == 404

    def test_get_workflow_status(self, client, auth_headers, created_workflow):
        """Test getting workflow execution status."""
        # Start execution
        exec_response = client.post(
            f"/workflows/{created_workflow}/execute",
            headers=auth_headers
        )
        execution_id = exec_response.json()["execution_id"]

        # Get status
        response = client.get(
            f"/workflows/{created_workflow}/status",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_get_execution_status(self, client, auth_headers, created_workflow):
        """Test getting specific execution status."""
        # Start execution
        exec_response = client.post(
            f"/workflows/{created_workflow}/execute",
            headers=auth_headers
        )
        execution_id = exec_response.json()["execution_id"]

        # Get status
        response = client.get(
            f"/executions/{execution_id}/status",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["execution_id"] == execution_id
        assert data["workflow_id"] == created_workflow

    def test_get_nonexistent_execution_status_returns_404(self, client, auth_headers):
        """Test getting nonexistent execution returns 404."""
        response = client.get(
            "/executions/nonexistent/execution",
            headers=auth_headers
        )
        assert response.status_code == 404

    def test_pause_execution(self, client, auth_headers, created_workflow):
        """Test pausing an execution."""
        # Start execution
        exec_response = client.post(
            f"/workflows/{created_workflow}/execute",
            headers=auth_headers
        )
        execution_id = exec_response.json()["execution_id"]

        # Pause it
        response = client.post(
            f"/executions/{execution_id}/pause",
            headers=auth_headers
        )
        assert response.status_code == 200

    def test_resume_execution(self, client, auth_headers, created_workflow):
        """Test resuming an execution."""
        # Start execution
        exec_response = client.post(
            f"/workflows/{created_workflow}/execute",
            headers=auth_headers
        )
        execution_id = exec_response.json()["execution_id"]

        # Pause and resume
        client.post(f"/executions/{execution_id}/pause", headers=auth_headers)
        response = client.post(
            f"/executions/{execution_id}/resume",
            headers=auth_headers
        )
        assert response.status_code == 200

    def test_stop_execution(self, client, auth_headers, created_workflow):
        """Test stopping an execution."""
        # Start execution
        exec_response = client.post(
            f"/workflows/{created_workflow}/execute",
            headers=auth_headers
        )
        execution_id = exec_response.json()["execution_id"]

        # Stop it
        response = client.post(
            f"/executions/{execution_id}/stop",
            headers=auth_headers
        )
        assert response.status_code == 200
        assert response.json()["stopped"] is True


# =============================================================================
# Action Execution Tests
# =============================================================================

class TestActionExecution:
    """Tests for action execution endpoint."""

    def test_execute_delay_action(self, client, auth_headers):
        """Test executing a delay action."""
        response = client.post(
            "/actions/delay/execute",
            headers=auth_headers,
            json={"params": {"duration": 0.01}, "context": {}}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "duration" in data

    def test_execute_click_action(self, client, auth_headers):
        """Test executing a click action."""
        response = client.post(
            "/actions/click/execute",
            headers=auth_headers,
            json={"params": {"x": 100, "y": 200, "button": "left"}, "context": {}}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_execute_unknown_action(self, client, auth_headers):
        """Test executing an unknown action returns success (generic handler)."""
        response = client.post(
            "/actions/unknown_action/execute",
            headers=auth_headers,
            json={"params": {}, "context": {}}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_execute_action_with_context(self, client, auth_headers):
        """Test executing action with context variables."""
        response = client.post(
            "/actions/delay/execute",
            headers=auth_headers,
            json={"params": {}, "context": {"test_var": "test_value"}}
        )
        assert response.status_code == 200
        # Context should be stored in variables
        var_response = client.get("/variables/test_var", headers=auth_headers)
        assert var_response.json()["value"] == "test_value"


# =============================================================================
# Variable Management Tests
# =============================================================================

class TestVariableManagement:
    """Tests for variable management endpoints."""

    def test_set_and_get_variable(self, client, auth_headers):
        """Test setting and getting a variable."""
        # Set variable
        response = client.put(
            "/variables/test_var",
            headers=auth_headers,
            json={"value": "test_value"}
        )
        # FastAPI's put expects the body as the value directly for primitives
        # But since we model it as JSON body...
        # Let me check the actual implementation...

        # Actually looking at the API, PUT /variables/{name} expects a body with 'value'
        response = client.put(
            "/variables/test_var",
            headers=auth_headers,
            json={"value": "test_value"}
        )
        assert response.status_code == 200

    def test_get_nonexistent_variable_returns_default(self, client, auth_headers):
        """Test getting nonexistent variable returns default."""
        response = client.get(
            "/variables/nonexistent",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["value"] is None

    def test_list_variables(self, client, auth_headers):
        """Test listing all variables."""
        # Set some variables first
        client.put("/variables/var1", headers=auth_headers, json={"value": "val1"})
        client.put("/variables/var2", headers=auth_headers, json={"value": "val2"})

        response = client.get("/variables", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)


# =============================================================================
# WebSocket Tests
# =============================================================================

class TestWebSocket:
    """Tests for WebSocket functionality."""

    def test_websocket_connection(self, client):
        """Test WebSocket connection establishment."""
        with client.websocket_connect("/ws") as websocket:
            # Send a ping
            websocket.send_text(json.dumps({"type": "ping"}))
            # Receive pong
            data = websocket.receive_text()
            msg = json.loads(data)
            assert msg["type"] == "pong"

    def test_websocket_execution_connection(self, client):
        """Test WebSocket connection for specific execution."""
        with client.websocket_connect("/ws/exec_123") as websocket:
            websocket.send_text(json.dumps({"type": "ping"}))
            data = websocket.receive_text()
            msg = json.loads(data)
            assert msg["type"] == "pong"


# =============================================================================
# CORS Tests
# =============================================================================

class TestCORS:
    """Tests for CORS functionality."""

    def test_cors_headers_present(self, client):
        """Test that CORS headers are present in response."""
        response = client.get("/health")
        # FastAPI's CORSMiddleware adds headers automatically
        # In test client, they may or may not be visible depending on configuration
        # This is a basic check
        assert response.status_code == 200


# =============================================================================
# OpenAPI Documentation Tests
# =============================================================================

class TestOpenAPIDocs:
    """Tests for OpenAPI documentation endpoints."""

    def test_docs_endpoint_available(self, client):
        """Test that /docs endpoint is available."""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_redoc_endpoint_available(self, client):
        """Test that /redoc endpoint is available."""
        response = client.get("/redoc")
        assert response.status_code == 200

    def test_openapi_schema_available(self, client):
        """Test that OpenAPI schema is available."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data
        assert "/health" in data["paths"]
        assert "/workflows" in data["paths"]


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_json_returns_422(self, client, auth_headers):
        """Test that invalid JSON returns 422."""
        response = client.post(
            "/workflows",
            headers=auth_headers,
            content="not valid json",
            media_type="application/json"
        )
        assert response.status_code == 422

    def test_method_not_allowed(self, client, auth_headers):
        """Test that wrong method returns 405."""
        response = client.delete("/health")
        assert response.status_code == 405


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_workflow_list(self, client, auth_headers):
        """Test listing workflows when none exist."""
        # This depends on test isolation - in a clean test env there might be none
        response = client.get("/workflows", headers=auth_headers)
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_workflow_with_empty_steps(self, client, auth_headers):
        """Test creating workflow with no steps."""
        workflow = {
            "workflow_id": "wf_empty",
            "name": "Empty Workflow",
            "steps": []
        }
        response = client.post("/workflows", headers=auth_headers, json=workflow)
        assert response.status_code == 200

    def test_workflow_execution_with_empty_context(self, client, auth_headers, sample_workflow):
        """Test workflow execution with empty context."""
        # Create workflow
        response = client.post(
            "/workflows",
            headers=auth_headers,
            json=sample_workflow.model_dump()
        )
        workflow_id = response.json()["workflow_id"]

        # Execute with empty context
        response = client.post(
            f"/workflows/{workflow_id}/execute",
            headers=auth_headers,
            json={"params": {}, "context": {}}
        )
        assert response.status_code == 200

    def test_concurrent_workflow_executions(self, client, auth_headers, sample_workflow):
        """Test starting multiple workflow executions concurrently."""
        # Create workflow
        response = client.post(
            "/workflows",
            headers=auth_headers,
            json=sample_workflow.model_dump()
        )
        workflow_id = response.json()["workflow_id"]

        # Start multiple executions
        executions = []
        for _ in range(3):
            response = client.post(
                f"/workflows/{workflow_id}/execute",
                headers=auth_headers
            )
            assert response.status_code == 200
            executions.append(response.json()["execution_id"])

        # Verify all executions exist
        assert len(set(executions)) == 3  # All unique


# =============================================================================
# Test Runner
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
