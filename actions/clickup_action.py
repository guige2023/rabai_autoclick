"""ClickUp integration for RabAI AutoClick.

Provides actions to manage tasks, lists, folders, spaces, and teams in ClickUp.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClickUpTaskAction(BaseAction):
    """Create, update, and manage ClickUp tasks.

    Supports rich task properties: priority, assignees, due dates, tags, checklists.
    """
    action_type = "clickup_task"
    display_name = "ClickUp任务"
    description = "创建、更新、管理ClickUp任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage ClickUp tasks.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ClickUp API token
                - team_id: Workspace team ID
                - operation: create | update | get | delete | archive
                - list_id: List ID (for create)
                - task_id: Task ID (for update/get/delete)
                - name: Task name (for create)
                - content: Task description
                - priority: 1-4 (1=urgent, 4=low)
                - due_date: Unix timestamp in ms
                - assignees: List of assignee IDs
                - tags: List of tag names
                - status: Status name

        Returns:
            ActionResult with task data.
        """
        api_key = params.get('api_key') or os.environ.get('CLICKUP_API_KEY')
        team_id = params.get('team_id')
        operation = params.get('operation', 'create')

        if not api_key:
            return ActionResult(success=False, message="CLICKUP_API_KEY is required")

        import urllib.request
        import urllib.error

        base_headers = {
            'Authorization': api_key,
            'Content-Type': 'application/json'
        }

        def make_request(url, method='GET', data=None):
            req_data = json.dumps(data).encode('utf-8') if data else None
            req = urllib.request.Request(url, data=req_data, method=method, headers=base_headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode('utf-8'))

        try:
            if operation == 'create':
                list_id = params.get('list_id')
                if not list_id:
                    return ActionResult(success=False, message="list_id is required for create")

                url = f'https://api.clickup.com/api/v2/list/{list_id}/task'
                task_data = {
                    'name': params.get('name', 'New Task'),
                    'content': params.get('content', ''),
                    'priority': params.get('priority'),
                    'due_date': params.get('due_date'),
                    'assignees': params.get('assignees', []),
                    'tags': params.get('tags', []),
                }
                task_data = {k: v for k, v in task_data.items() if v is not None}
                result = make_request(url, 'POST', task_data)

            elif operation == 'update':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required for update")

                url = f'https://api.clickup.com/api/v2/task/{task_id}'
                update_data = {}
                for key in ['name', 'content', 'priority', 'due_date', 'status']:
                    if key in params:
                        update_data[key] = params[key]
                if 'assignees' in params:
                    update_data['assignees'] = {str(a): 'add' for a in params['assignees']}
                result = make_request(url, 'PUT', update_data if update_data else None)

            elif operation == 'get':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required for get")
                result = make_request(f'https://api.clickup.com/api/v2/task/{task_id}')

            elif operation == 'delete':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required for delete")
                result = make_request(f'https://api.clickup.com/api/v2/task/{task_id}', 'DELETE')
                return ActionResult(success=True, message="Task deleted", data=result)

            elif operation == 'archive':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required for archive")
                result = make_request(f'https://api.clickup.com/api/v2/task/{task_id}/archive', 'POST')
                return ActionResult(success=True, message="Task archived", data=result)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(
                success=True,
                message=f"Task '{operation}' completed",
                data=result
            )
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"ClickUp API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"ClickUp error: {str(e)}")


class ClickUpListAction(BaseAction):
    """Manage ClickUp lists - create, get, update lists.

    Lists are contained within Folders and Spaces.
    """
    action_type = "clickup_list"
    display_name = "ClickUp列表"
    description = "创建、获取、更新ClickUp列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage ClickUp lists.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ClickUp API token
                - operation: create | get | update | delete
                - folder_id: Folder ID (for create)
                - space_id: Space ID (for create without folder)
                - list_id: List ID (for get/update/delete)
                - name: List name
                - content: List description
                - status: Default status

        Returns:
            ActionResult with list data.
        """
        api_key = params.get('api_key') or os.environ.get('CLICKUP_API_KEY')
        operation = params.get('operation', 'get')

        if not api_key:
            return ActionResult(success=False, message="CLICKUP_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': api_key, 'Content-Type': 'application/json'}

        def make_request(url, method='GET', data=None):
            req_data = json.dumps(data).encode('utf-8') if data else None
            req = urllib.request.Request(url, data=req_data, method=method, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode('utf-8'))

        try:
            if operation == 'create':
                folder_id = params.get('folder_id')
                space_id = params.get('space_id')

                if folder_id:
                    url = f'https://api.clickup.com/api/v2/folder/{folder_id}/list'
                elif space_id:
                    url = f'https://api.clickup.com/api/v2/space/{space_id}/list'
                else:
                    return ActionResult(success=False, message="folder_id or space_id required for create")

                result = make_request(url, 'POST', {
                    'name': params.get('name', 'New List'),
                    'content': params.get('content', ''),
                    'status': params.get('status'),
                })

            elif operation == 'get':
                list_id = params.get('list_id')
                if not list_id:
                    return ActionResult(success=False, message="list_id is required")
                result = make_request(f'https://api.clickup.com/api/v2/list/{list_id}')

            elif operation == 'update':
                list_id = params.get('list_id')
                if not list_id:
                    return ActionResult(success=False, message="list_id is required")
                update_data = {}
                for key in ['name', 'content', 'status']:
                    if key in params:
                        update_data[key] = params[key]
                result = make_request(f'https://api.clickup.com/api/v2/list/{list_id}', 'PUT', update_data or None)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"List '{operation}' completed", data=result)
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"ClickUp API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"ClickUp error: {str(e)}")


class ClickUpTeamAction(BaseAction):
    """Manage ClickUp teams, spaces, and folders.

    Provides workspace-level organization operations.
    """
    action_type = "clickup_team"
    display_name = "ClickUp团队空间"
    description = "管理ClickUp团队空间和文件夹"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage ClickUp teams/spaces/folders.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ClickUp API token
                - operation: list_teams | list_spaces | list_folders
                - team_id: Team ID (required for spaces/folders)
                - space_id: Space ID (required for folders)

        Returns:
            ActionResult with organizational data.
        """
        api_key = params.get('api_key') or os.environ.get('CLICKUP_API_KEY')
        operation = params.get('operation', 'list_teams')

        if not api_key:
            return ActionResult(success=False, message="CLICKUP_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': api_key}
        result = {}

        try:
            if operation == 'list_teams':
                req = urllib.request.Request('https://api.clickup.com/api/v2/team', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                teams = result.get('teams', [])
                return ActionResult(success=True, message=f"Found {len(teams)} teams", data={'teams': teams})

            elif operation == 'list_spaces':
                team_id = params.get('team_id')
                if not team_id:
                    return ActionResult(success=False, message="team_id is required")
                req = urllib.request.Request(f'https://api.clickup.com/api/v2/team/{team_id}/space', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                spaces = result.get('spaces', [])
                return ActionResult(success=True, message=f"Found {len(spaces)} spaces", data={'spaces': spaces})

            elif operation == 'list_folders':
                space_id = params.get('space_id')
                if not space_id:
                    return ActionResult(success=False, message="space_id is required")
                req = urllib.request.Request(f'https://api.clickup.com/api/v2/space/{space_id}/folder', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                folders = result.get('folders', [])
                return ActionResult(success=True, message=f"Found {len(folders)} folders", data={'folders': folders})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"ClickUp API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"ClickUp error: {str(e)}")


class ClickUpChecklistAction(BaseAction):
    """Manage ClickUp task checklists.

    Allows adding and managing checklist items within tasks.
    """
    action_type = "clickup_checklist"
    display_name = "ClickUp检查清单"
    description = "管理ClickUp任务检查清单"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage ClickUp checklists.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ClickUp API token
                - operation: create_checklist | add_item | update_item | delete_item
                - task_id: Task ID
                - checklist_id: Checklist ID (for item operations)
                - item_id: Item ID (for update/delete)
                - name: Checklist/item name
                - resolved: Boolean for item completion

        Returns:
            ActionResult with checklist data.
        """
        api_key = params.get('api_key') or os.environ.get('CLICKUP_API_KEY')
        operation = params.get('operation', 'create_checklist')

        if not api_key:
            return ActionResult(success=False, message="CLICKUP_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': api_key, 'Content-Type': 'application/json'}

        try:
            task_id = params.get('task_id')
            if not task_id:
                return ActionResult(success=False, message="task_id is required")

            if operation == 'create_checklist':
                url = f'https://api.clickup.com/api/v2/task/{task_id}/checklist'
                data = {'name': params.get('name', 'Checklist')}
                req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), method='POST', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Checklist created", data=result)

            elif operation in ('add_item', 'update_item', 'delete_item'):
                checklist_id = params.get('checklist_id')
                item_id = params.get('item_id')
                if not checklist_id:
                    return ActionResult(success=False, message="checklist_id is required")

                if operation == 'add_item':
                    url = f'https://api.clickup.com/api/v2/task/{task_id}/checklist/{checklist_id}/checklist_item'
                    data = {'name': params.get('name', 'Item')}
                    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), method='POST', headers=headers)
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        result = json.loads(resp.read().decode('utf-8'))
                    return ActionResult(success=True, message="Item added", data=result)

                elif operation == 'update_item' and item_id:
                    url = f'https://api.clickup.com/api/v2/task/{task_id}/checklist/{checklist_id}/checklist_item/{item_id}'
                    data = {'name': params.get('name'), 'resolved': params.get('resolved')}
                    data = {k: v for k, v in data.items() if v is not None}
                    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), method='PUT', headers=headers)
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        result = json.loads(resp.read().decode('utf-8'))
                    return ActionResult(success=True, message="Item updated", data=result)

                elif operation == 'delete_item' and item_id:
                    url = f'https://api.clickup.com/api/v2/task/{task_id}/checklist/{checklist_id}/checklist_item/{item_id}'
                    req = urllib.request.Request(url, method='DELETE', headers=headers)
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        result = json.loads(resp.read().decode('utf-8'))
                    return ActionResult(success=True, message="Item deleted", data=result)

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"ClickUp API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"ClickUp error: {str(e)}")


class ClickUpTimeTrackingAction(BaseAction):
    """Manage ClickUp time tracking entries.

    Allows starting/stopping timers and managing time entries.
    """
    action_type = "clickup_time_tracking"
    display_name = "ClickUp时间跟踪"
    description = "管理ClickUp时间跟踪条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage ClickUp time tracking.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ClickUp API token
                - operation: start | stop | get_entries | create_entry
                - task_id: Task ID for time entry
                - start_date: Start timestamp in ms
                - duration: Duration in ms
                - description: Entry description

        Returns:
            ActionResult with time tracking data.
        """
        api_key = params.get('api_key') or os.environ.get('CLICKUP_API_KEY')
        operation = params.get('operation', 'get_entries')

        if not api_key:
            return ActionResult(success=False, message="CLICKUP_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': api_key, 'Content-Type': 'application/json'}

        try:
            if operation == 'get_entries':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required")
                req = urllib.request.Request(
                    f'https://api.clickup.com/api/v2/task/{task_id}/time_entries',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Time entries retrieved", data=result)

            elif operation == 'start':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required")
                req = urllib.request.Request(
                    f'https://api.clickup.com/api/v2/task/{task_id}/time_entries/start',
                    data=b'{}',
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Timer started", data=result)

            elif operation == 'stop':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required")
                req = urllib.request.Request(
                    f'https://api.clickup.com/api/v2/task/{task_id}/time_entries/stop',
                    data=b'{}',
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Timer stopped", data=result)

            elif operation == 'create_entry':
                task_id = params.get('task_id')
                if not task_id:
                    return ActionResult(success=False, message="task_id is required")
                data = {
                    'start_date': params.get('start_date'),
                    'duration': params.get('duration'),
                    'description': params.get('description', ''),
                }
                req = urllib.request.Request(
                    f'https://api.clickup.com/api/v2/task/{task_id}/time_entries',
                    data=json.dumps(data).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Time entry created", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"ClickUp API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"ClickUp error: {str(e)}")
