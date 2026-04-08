"""Tekton Pipelines integration for RabAI AutoClick.

Provides actions to manage Tekton PipelineRuns, TaskRuns, and pipeline resources.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TektonPipelineRunAction(BaseAction):
    """Manage Tekton PipelineRuns and TaskRuns.

    Handles pipeline execution and monitoring on Kubernetes.
    """
    action_type = "tekton_pipelinerun"
    display_name = "Tekton流水线"
    description = "管理Tekton流水线运行和任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Tekton PipelineRuns.

        Args:
            context: Execution context.
            params: Dict with keys:
                - kubeconfig: Path to kubeconfig
                - namespace: Namespace (default: tekton-pipelines)
                - operation: create_pipeline | list_pipeline | delete_pipeline | get_pipeline
                - pipelinerun_name: PipelineRun name
                - pipeline_name: Pipeline name
                - params: Dict of pipeline parameters
                - service_account: Service account name
                - timeout: Timeout in minutes

        Returns:
            ActionResult with pipelinerun data.
        """
        namespace = params.get('namespace', 'tekton-pipelines')

        try:
            from kubernetes import client, config
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")

        try:
            if params.get('kubeconfig'):
                config.load_kube_config(config_file=params['kubeconfig'])
            else:
                try:
                    config.load_incluster_config()
                except Exception:
                    config.load_kube_config()

            custom_api = client.CustomObjectsApi()
            operation = params.get('operation', 'list_pipeline')

            if operation == 'create_pipeline':
                pipelinerun_name = params.get('pipelinerun_name')
                pipeline_name = params.get('pipeline_name')

                if not pipelinerun_name or not pipeline_name:
                    return ActionResult(success=False, message="pipelinerun_name and pipeline_name are required")

                params_list = params.get('params', {})
                param_specs = [{'name': k, 'value': v} for k, v in params_list.items()]

                pipelinerun = {
                    'apiVersion': 'tekton.dev/v1beta1',
                    'kind': 'PipelineRun',
                    'metadata': {'name': pipelinerun_name, 'namespace': namespace},
                    'spec': {
                        'pipelineRef': {'name': pipeline_name},
                        'serviceAccountName': params.get('service_account', 'default'),
                        'timeout': f"{params.get('timeout', 60)}m",
                        'params': param_specs,
                    }
                }

                result = custom_api.create_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'pipelineruns', pipelinerun
                )
                return ActionResult(success=True, message=f"PipelineRun {pipelinerun_name} created", data={'name': result['metadata']['name']})

            elif operation == 'get_pipeline':
                pipelinerun_name = params.get('pipelinerun_name')
                if not pipelinerun_name:
                    return ActionResult(success=False, message="pipelinerun_name is required")

                result = custom_api.get_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'pipelineruns', pipelinerun_name
                )
                status = result.get('status', {})
                return ActionResult(success=True, message="PipelineRun retrieved", data={
                    'name': result['metadata']['name'],
                    'status': status.get('conditions', [{}])[0].get('reason', 'Unknown'),
                    'pipeline_spec': result.get('spec', {}).get('pipelineRef', {}).get('name'),
                })

            elif operation == 'list_pipeline':
                results = custom_api.list_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'pipelineruns'
                )
                items = results.get('items', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(items)} PipelineRuns",
                    data={'pipelineruns': [{'name': r['metadata']['name']} for r in items]}
                )

            elif operation == 'delete_pipeline':
                pipelinerun_name = params.get('pipelinerun_name')
                if not pipelinerun_name:
                    return ActionResult(success=False, message="pipelinerun_name is required")

                custom_api.delete_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'pipelineruns', pipelinerun_name
                )
                return ActionResult(success=True, message=f"PipelineRun {pipelinerun_name} deleted")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Tekton error: {str(e)}")


class TektonTaskRunAction(BaseAction):
    """Manage Tekton TaskRuns.

    Handles standalone task execution.
    """
    action_type = "tekton_taskrun"
    display_name = "Tekton任务"
    description = "管理Tekton独立任务运行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Tekton TaskRuns.

        Args:
            context: Execution context.
            params: Dict with keys:
                - kubeconfig: Path to kubeconfig
                - namespace: Namespace
                - operation: create_task | list_task | get_task | delete_task
                - taskrun_name: TaskRun name
                - task_name: Task name
                - params: Dict of task parameters
                - timeout: Timeout in minutes

        Returns:
            ActionResult with taskrun data.
        """
        namespace = params.get('namespace', 'tekton-pipelines')

        try:
            from kubernetes import client, config
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")

        try:
            if params.get('kubeconfig'):
                config.load_kube_config(config_file=params['kubeconfig'])
            else:
                try:
                    config.load_incluster_config()
                except Exception:
                    config.load_kube_config()

            custom_api = client.CustomObjectsApi()
            operation = params.get('operation', 'list_task')

            if operation == 'create_task':
                taskrun_name = params.get('taskrun_name')
                task_name = params.get('task_name')

                if not taskrun_name or not task_name:
                    return ActionResult(success=False, message="taskrun_name and task_name are required")

                params_list = params.get('params', {})
                param_specs = [{'name': k, 'value': v} for k, v in params_list.items()]

                taskrun = {
                    'apiVersion': 'tekton.dev/v1beta1',
                    'kind': 'TaskRun',
                    'metadata': {'name': taskrun_name, 'namespace': namespace},
                    'spec': {
                        'taskRef': {'name': task_name},
                        'serviceAccountName': params.get('service_account', 'default'),
                        'timeout': f"{params.get('timeout', 60)}m",
                        'params': param_specs,
                    }
                }

                result = custom_api.create_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'taskruns', taskrun
                )
                return ActionResult(success=True, message=f"TaskRun {taskrun_name} created", data={'name': result['metadata']['name']})

            elif operation == 'get_task':
                taskrun_name = params.get('taskrun_name')
                if not taskrun_name:
                    return ActionResult(success=False, message="taskrun_name is required")

                result = custom_api.get_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'taskruns', taskrun_name
                )
                status = result.get('status', {})
                return ActionResult(success=True, message="TaskRun retrieved", data={
                    'name': result['metadata']['name'],
                    'status': status.get('conditions', [{}])[0].get('reason', 'Unknown'),
                })

            elif operation == 'list_task':
                results = custom_api.list_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'taskruns'
                )
                items = results.get('items', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(items)} TaskRuns",
                    data={'taskruns': [{'name': r['metadata']['name']} for r in items]}
                )

            elif operation == 'delete_task':
                taskrun_name = params.get('taskrun_name')
                if not taskrun_name:
                    return ActionResult(success=False, message="taskrun_name is required")

                custom_api.delete_namespaced_custom_object(
                    'tekton.dev', 'v1beta1', namespace, 'taskruns', taskrun_name
                )
                return ActionResult(success=True, message=f"TaskRun {taskrun_name} deleted")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Tekton error: {str(e)}")
