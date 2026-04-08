"""Jenkins action module for RabAI AutoClick.

Provides Jenkins CI/CD operations for builds, jobs, and nodes.
"""

import base64
import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JenkinsAction(BaseAction):
    """Jenkins CI/CD operations.
    
    Supports triggering builds, monitoring job status,
    managing nodes, and retrieving build artifacts.
    """
    action_type = "jenkins"
    display_name = "Jenkins CI/CD"
    description = "Jenkins构建、任务与节点管理"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Jenkins operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'build', 'status', 'list_jobs', 'log', 'node_info', 'create_job'
                - url: Jenkins server URL
                - username: Jenkins username
                - token: Jenkins API token (or password)
                - job_name: Job name for operations
                - build_number: Build number (for status/log)
                - parameters: Build parameters dict
        
        Returns:
            ActionResult with operation result.
        """
        command = params.get('command', 'list_jobs')
        url = params.get('url')
        username = params.get('username')
        token = params.get('token')
        job_name = params.get('job_name')
        build_number = params.get('build_number')
        parameters = params.get('parameters', {})
        
        if not url:
            return ActionResult(success=False, message="Jenkins URL required")
        
        auth = None
        if username and token:
            creds = f"{username}:{token}"
            auth = base64.b64encode(creds.encode()).decode()
        
        headers: Dict[str, str] = {}
        if auth:
            headers['Authorization'] = f'Basic {auth}'
        headers['Accept'] = 'application/json'
        
        url = url.rstrip('/')
        
        if command == 'build':
            if not job_name:
                return ActionResult(success=False, message="job_name required for build")
            return self._jenkins_build(url, headers, job_name, parameters)
        
        if command == 'status':
            if not job_name:
                return ActionResult(success=False, message="job_name required for status")
            return self._jenkins_status(url, headers, job_name, build_number)
        
        if command == 'list_jobs':
            return self._jenkins_list_jobs(url, headers)
        
        if command == 'log':
            if not job_name or not build_number:
                return ActionResult(success=False, message="job_name and build_number required for log")
            return self._jenkins_log(url, headers, job_name, build_number)
        
        if command == 'node_info':
            return self._jenkins_node_info(url, headers, params.get('node_name'))
        
        if command == 'create_job':
            if not job_name:
                return ActionResult(success=False, message="job_name required for create_job")
            return self._jenkins_create_job(url, headers, job_name, params.get('config_xml', self._default_config_xml()))
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _jenkins_request(self, url: str, headers: Dict, endpoint: str, method: str = 'GET', data: Optional[str] = None) -> Dict:
        """Make HTTP request to Jenkins API."""
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        
        full_url = f"{url}/{endpoint}"
        req_data = data.encode('utf-8') if data else None
        request = Request(full_url, data=req_data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=20) as resp:
                content = resp.read().decode('utf-8')
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    return {'raw': content}
        except HTTPError as e:
            try:
                error_body = e.read().decode('utf-8')
                return {'error': f"HTTP {e.code}: {error_body[:500]}"}
            except Exception:
                return {'error': f"HTTP {e.code}"}
        except Exception as e:
            return {'error': str(e)}
    
    def _jenkins_build(self, url: str, headers: Dict, job_name: str, parameters: Dict) -> ActionResult:
        """Trigger Jenkins build."""
        import urllib.parse
        
        job_path = f'job/{urllib.parse.quote(job_name, safe="")}/build'
        if parameters:
            form_data = '&'.join(f'param.{k}={urllib.parse.quote(str(v))}' for k, v in parameters.items())
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
            result = self._jenkins_request(url, headers, job_path, 'POST', form_data)
        else:
            result = self._jenkins_request(url, headers, job_path, 'POST')
        
        if 'error' in result:
            return ActionResult(success=False, message=result['error'])
        return ActionResult(
            success=True,
            message=f"Triggered build for {job_name}",
            data={'job': job_name, 'parameters': parameters}
        )
    
    def _jenkins_status(self, url: str, headers: Dict, job_name: str, build_number: Optional[int]) -> ActionResult:
        """Get build or job status."""
        import urllib.parse
        
        job_path = f'job/{urllib.parse.quote(job_name, safe="")}'
        if build_number:
            job_path += f'/{build_number}/api/json'
            result = self._jenkins_request(url, headers, job_path)
            if 'error' in result:
                return ActionResult(success=False, message=result['error'])
            return ActionResult(
                success=True,
                message=f"Build #{build_number} status: {result.get('result', 'UNKNOWN')}",
                data={
                    'number': result.get('number'),
                    'result': result.get('result'),
                    'duration': result.get('duration'),
                    'building': result.get('building'),
                    'url': result.get('url')
                }
            )
        else:
            job_path += '/lastBuild/api/json'
            result = self._jenkins_request(url, headers, job_path)
            if 'error' in result:
                return ActionResult(success=False, message=result['error'])
            return ActionResult(
                success=True,
                message=f"Last build: {result.get('result', 'UNKNOWN')}",
                data={
                    'number': result.get('number'),
                    'result': result.get('result'),
                    'url': result.get('url')
                }
            )
    
    def _jenkins_list_jobs(self, url: str, headers: Dict) -> ActionResult:
        """List all Jenkins jobs."""
        result = self._jenkins_request(url, headers, 'api/json')
        if 'error' in result:
            return ActionResult(success=False, message=result['error'])
        jobs = result.get('jobs', [])
        return ActionResult(
            success=True,
            message=f"Found {len(jobs)} jobs",
            data={'jobs': [{'name': j.get('name'), 'url': j.get('url'), 'color': j.get('color')} for j in jobs], 'count': len(jobs)}
        )
    
    def _jenkins_log(self, url: str, headers: Dict, job_name: str, build_number: int) -> ActionResult:
        """Get build console log."""
        import urllib.parse
        
        log_path = f"job/{urllib.parse.quote(job_name, safe='')}/{build_number}/consoleText"
        result = self._jenkins_request(url, headers, log_path)
        if 'error' in result:
            return ActionResult(success=False, message=result['error'])
        log_text = result.get('raw', '')
        return ActionResult(
            success=True,
            message=f"Build #{build_number} log ({len(log_text)} chars)",
            data={'log': log_text[:5000], 'truncated': len(log_text) > 5000}
        )
    
    def _jenkins_node_info(self, url: str, headers: Dict, node_name: Optional[str]) -> ActionResult:
        """Get node/computer info."""
        if node_name:
            endpoint = f"computer/{node_name}/api/json"
        else:
            endpoint = "computer/api/json"
        result = self._jenkins_request(url, headers, endpoint)
        if 'error' in result:
            return ActionResult(success=False, message=result['error'])
        nodes = result.get('computer', [])
        return ActionResult(
            success=True,
            message=f"Found {len(nodes)} nodes",
            data={'nodes': [{'displayName': n.get('displayName'), 'offline': n.get('offline'), 'executors': n.get('numExecutors')} for n in nodes]}
        )
    
    def _jenkins_create_job(self, url: str, headers: Dict, job_name: str, config_xml: str) -> ActionResult:
        """Create a new Jenkins job."""
        import urllib.parse
        
        headers['Content-Type'] = 'application/xml'
        job_path = f"createItem?name={urllib.parse.quote(job_name, safe='')}"
        result = self._jenkins_request(url, headers, job_path, 'POST', config_xml)
        if 'error' in result:
            return ActionResult(success=False, message=result['error'])
        return ActionResult(success=True, message=f"Created job: {job_name}")
    
    def _default_config_xml(self) -> str:
        """Return default Jenkins job config XML."""
        return """<?xml version='1.1' encoding='UTF-8'?>
<project>
  <description>Auto-created job</description>
  <keepDependencies>false</keepDependencies>
  <properties/>
  <scm class="hudson.scm.NullSCM"/>
  <canRoam>true</canRoam>
  <disabled>false</disabled>
  <blockBuildWhenDownstreamBuilding>false</blockBuildWhenDownstreamBuilding>
  <blockBuildWhenUpstreamBuilding>false</blockBuildWhenUpstreamBuilding>
  <triggers/>
  <concurrentBuild>false</concurrentBuild>
  <builders/>
  <publishers/>
  <buildWrappers/>
</project>"""
