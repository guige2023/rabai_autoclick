"""CircleCI integration for RabAI AutoClick.

Provides actions to trigger pipelines, monitor workflows, and manage CircleCI projects.
"""

import json
import time
import sys
import os
import base64
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircleCIPipelineAction(BaseAction):
    """Trigger and manage CircleCI pipelines.

    Supports triggering pipelines with custom parameters and monitoring status.
    """
    action_type = "circleci_pipeline"
    display_name = "CircleCI流水线"
    description = "触发和管理CircleCI流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Trigger and manage CircleCI pipelines.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: CircleCI API token
                - project_slug: Project slug (org/repo)
                - operation: trigger | get | list
                - branch: Branch name (for trigger)
                - tag: Tag name (for trigger)
                - parameters: Dict of pipeline parameters
                - pipeline_number: Pipeline number (for get)

        Returns:
            ActionResult with pipeline data.
        """
        api_token = params.get('api_token') or os.environ.get('CIRCLECI_API_TOKEN')
        project_slug = params.get('project_slug')
        operation = params.get('operation', 'list')

        if not api_token:
            return ActionResult(success=False, message="CIRCLECI_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Circle-Token': api_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'trigger':
                if not project_slug:
                    return ActionResult(success=False, message="project_slug is required for trigger")

                payload = {
                    'branch': params.get('branch', 'main'),
                }
                if params.get('tag'):
                    payload['tag'] = params['tag']
                if params.get('parameters'):
                    payload['parameters'] = params['parameters']

                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/pipeline',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Pipeline triggered", data=result)

            elif operation == 'get':
                if not project_slug or not params.get('pipeline_number'):
                    return ActionResult(success=False, message="project_slug and pipeline_number required for get")

                pipeline_num = params.get('pipeline_number')
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/pipeline/{pipeline_num}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Pipeline retrieved", data=result)

            elif operation == 'list':
                if not project_slug:
                    return ActionResult(success=False, message="project_slug is required for list")

                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/pipeline',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                pipelines = result.get('items', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(pipelines)} pipelines",
                    data={'pipelines': pipelines, 'next_token': result.get('next_page_token')}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"CircleCI API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"CircleCI error: {str(e)}")


class CircleCIWorkflowAction(BaseAction):
    """Manage CircleCI workflows - trigger, monitor, cancel workflows.

    Provides workflow-level operations for CI/CD management.
    """
    action_type = "circleci_workflow"
    display_name = "CircleCI工作流"
    description = "管理CircleCI工作流：触发、监控、取消"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage CircleCI workflows.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: CircleCI API token
                - project_slug: Project slug (org/repo)
                - operation: trigger | get | list | cancel | approve | list_jobs
                - workflow_id: Workflow ID (for get/cancel/approve)
                - branch: Branch name (for trigger)
                - workflow_name: Workflow name to trigger

        Returns:
            ActionResult with workflow data.
        """
        api_token = params.get('api_token') or os.environ.get('CIRCLECI_API_TOKEN')
        project_slug = params.get('project_slug')
        operation = params.get('operation', 'list')

        if not api_token:
            return ActionResult(success=False, message="CIRCLECI_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Circle-Token': api_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'trigger':
                if not project_slug:
                    return ActionResult(success=False, message="project_slug is required")
                workflow_name = params.get('workflow_name', 'build')
                branch = params.get('branch', 'main')

                # First trigger pipeline, then get workflow
                payload = {'branch': branch, 'parameters': {'workflow_name': workflow_name}}
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/pipeline',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    pipeline = json.loads(resp.read().decode('utf-8'))

                pipeline_url = pipeline.get('browser_download_url', '')
                return ActionResult(success=True, message="Workflow triggered", data={'pipeline': pipeline})

            elif operation == 'list':
                if not project_slug:
                    return ActionResult(success=False, message="project_slug is required")
                url = f'https://circleci.com/api/v2/workflow/{project_slug}'
                if params.get('branch'):
                    url += f"?branch={params['branch']}"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                workflows = result.get('items', [])
                return ActionResult(success=True, message=f"Found {len(workflows)} workflows", data={'workflows': workflows})

            elif operation == 'get':
                workflow_id = params.get('workflow_id')
                if not workflow_id:
                    return ActionResult(success=False, message="workflow_id is required")
                req = urllib.request.Request(f'https://circleci.com/api/v2/workflow/{workflow_id}', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Workflow retrieved", data=result)

            elif operation == 'cancel':
                workflow_id = params.get('workflow_id')
                if not workflow_id:
                    return ActionResult(success=False, message="workflow_id is required")
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/workflow/{workflow_id}/cancel',
                    data=b'{}',
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Workflow cancelled", data=result)

            elif operation == 'list_jobs':
                workflow_id = params.get('workflow_id')
                if not workflow_id:
                    return ActionResult(success=False, message="workflow_id is required")
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/workflow/{workflow_id}/job',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                jobs = result.get('items', [])
                return ActionResult(success=True, message=f"Found {len(jobs)} jobs", data={'jobs': jobs})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"CircleCI API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"CircleCI error: {str(e)}")


class CircleCIJobAction(BaseAction):
    """Monitor and manage CircleCI job runs.

    Provides job-level artifact retrieval and log access.
    """
    action_type = "circleci_job"
    display_name = "CircleCI任务"
    description = "监控和管理CircleCI任务运行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage CircleCI jobs.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: CircleCI API token
                - project_slug: Project slug
                - operation: get | list_artifacts | get_tests | retry
                - job_number: Job number
                - workflow_id: Workflow ID

        Returns:
            ActionResult with job data.
        """
        api_token = params.get('api_token') or os.environ.get('CIRCLECI_API_TOKEN')
        project_slug = params.get('project_slug')
        operation = params.get('operation', 'get')

        if not api_token:
            return ActionResult(success=False, message="CIRCLECI_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {'Circle-Token': api_token, 'Accept': 'application/json'}

        try:
            if operation == 'get':
                if not project_slug or not params.get('job_number'):
                    return ActionResult(success=False, message="project_slug and job_number required")
                job_num = params.get('job_number')
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/job/{job_num}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Job retrieved", data=result)

            elif operation == 'list_artifacts':
                if not project_slug or not params.get('job_number'):
                    return ActionResult(success=False, message="project_slug and job_number required")
                job_num = params.get('job_number')
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/{job_num}/artifacts',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    artifacts = json.loads(resp.read().decode('utf-8'))
                return ActionResult(
                    success=True,
                    message=f"Found {len(artifacts)} artifacts",
                    data={'artifacts': artifacts}
                )

            elif operation == 'get_tests':
                if not project_slug or not params.get('job_number'):
                    return ActionResult(success=False, message="project_slug and job_number required")
                job_num = params.get('job_number')
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/{job_num}/tests',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                tests = result if isinstance(result, list) else result.get('tests', [])
                return ActionResult(success=True, message=f"Found {len(tests)} tests", data={'tests': tests})

            elif operation == 'retry':
                if not project_slug or not params.get('job_number'):
                    return ActionResult(success=False, message="project_slug and job_number required")
                job_num = params.get('job_number')
                req = urllib.request.Request(
                    f'https://circleci.com/api/v2/project/{project_slug}/{job_num}/retry',
                    data=b'{}',
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Job retry initiated", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"CircleCI API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"CircleCI error: {str(e)}")


class CircleCIInsightsAction(BaseAction):
    """Fetch CircleCI workflow and job insights/metrics.

    Provides performance metrics and success rates.
    """
    action_type = "circleci_insights"
    display_name = "CircleCI洞察"
    description = "获取CircleCI工作流和任务的性能洞察"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get CircleCI insights.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: CircleCI API token
                - project_slug: Project slug
                - operation: workflow_summary | job_runs | metrics
                - workflow_name: Workflow name (for workflow_summary)
                - branch: Branch filter
                - metrics_date_end: End date for metrics window

        Returns:
            ActionResult with insights data.
        """
        api_token = params.get('api_token') or os.environ.get('CIRCLECI_API_TOKEN')
        project_slug = params.get('project_slug')

        if not api_token:
            return ActionResult(success=False, message="CIRCLECI_API_TOKEN is required")
        if not project_slug:
            return ActionResult(success=False, message="project_slug is required")

        import urllib.request
        import urllib.error

        headers = {'Circle-Token': api_token, 'Accept': 'application/json'}

        try:
            operation = params.get('operation', 'workflow_summary')

            if operation == 'workflow_summary':
                workflow_name = params.get('workflow_name')
                if not workflow_name:
                    return ActionResult(success=False, message="workflow_name is required")
                url = f'https://circleci.com/api/v2/insights/{project_slug}/workflows/{workflow_name}'
                query_params = []
                if params.get('branch'):
                    query_params.append(f"branch={params['branch']}")
                if params.get('metrics_date_end'):
                    query_params.append(f"metrics-date-end={params['metrics_date_end']}")
                if query_params:
                    url += '?' + '&'.join(query_params)
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Insights retrieved", data=result)

            elif operation == 'job_runs':
                workflow_name = params.get('workflow_name')
                if not workflow_name:
                    return ActionResult(success=False, message="workflow_name is required")
                url = f'https://circleci.com/api/v2/insights/{project_slug}/workflows/{workflow_name}/jobs'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Job runs retrieved", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"CircleCI API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"CircleCI error: {str(e)}")
