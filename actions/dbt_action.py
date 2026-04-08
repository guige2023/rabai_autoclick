"""dbt action module for RabAI AutoClick.

Provides dbt (data build tool) operations for
data transformation and analytics engineering.
"""

import os
import sys
import time
import subprocess
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DbtClient:
    """dbt client for data transformation.
    
    Provides methods for running dbt commands,
    managing models, and data transformations.
    """
    
    def __init__(
        self,
        project_dir: str = ".",
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        version: Optional[str] = None
    ) -> None:
        """Initialize dbt client.
        
        Args:
            project_dir: Path to dbt project directory.
            profiles_dir: Path to profiles directory.
            target: Target environment name.
            version: dbt version to use.
        """
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.version = version
    
    def _run_dbt(self, args: List[str], timeout: int = 600) -> subprocess.CompletedProcess:
        """Run a dbt command.
        
        Args:
            args: Command arguments.
            timeout: Command timeout.
            
        Returns:
            CompletedProcess result.
        """
        cmd = ["dbt"] + args
        
        if self.profiles_dir:
            cmd.extend(["--profiles-dir", self.profiles_dir])
        
        if self.target:
            cmd.extend(["--target", self.target])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=self.project_dir,
                timeout=timeout
            )
            return result
        except subprocess.TimeoutExpired:
            raise Exception(f"dbt command timed out after {timeout}s")
        except Exception as e:
            raise Exception(f"dbt command failed: {str(e)}")
    
    def connect(self) -> bool:
        """Test if dbt is available.
        
        Returns:
            True if dbt is available, False otherwise.
        """
        try:
            result = self._run_dbt(["--version"], timeout=30)
            return result.returncode == 0
        except Exception:
            return False
    
    def debug(self) -> Dict[str, Any]:
        """Run dbt debug to check configuration.
        
        Returns:
            Debug results.
        """
        try:
            result = self._run_dbt(["debug"], timeout=60)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run(
        self,
        models: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        full_refresh: bool = False
    ) -> Dict[str, Any]:
        """Run dbt models.
        
        Args:
            models: Optional model names to run.
            exclude: Optional models to exclude.
            full_refresh: Full refresh mode.
            
        Returns:
            Run results.
        """
        try:
            args = ["run"]
            
            if models:
                args.extend(["--models"] + models)
            
            if exclude:
                args.extend(["--exclude"] + exclude)
            
            if full_refresh:
                args.append("--full-refresh")
            
            result = self._run_dbt(args)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def test(
        self,
        models: Optional[List[str]] = None,
        data: bool = True,
        schema: bool = True
    ) -> Dict[str, Any]:
        """Run dbt tests.
        
        Args:
            models: Optional model names to test.
            data: Run data tests.
            schema: Run schema tests.
            
        Returns:
            Test results.
        """
        try:
            args = ["test"]
            
            if models:
                args.extend(["--models"] + models)
            
            if data and not schema:
                args.append("--data")
            elif schema and not data:
                args.append("--schema")
            
            result = self._run_dbt(args)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def compile(
        self,
        models: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Compile dbt SQL.
        
        Args:
            models: Optional model names to compile.
            
        Returns:
            Compile results.
        """
        try:
            args = ["compile"]
            
            if models:
                args.extend(["--models"] + models)
            
            result = self._run_dbt(args)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def ls(self, models: bool = True, sources: bool = False, macros: bool = False) -> List[str]:
        """List dbt resources.
        
        Args:
            models: List models.
            sources: List sources.
            macros: List macros.
            
        Returns:
            List of resource names.
        """
        try:
            args = ["ls"]
            
            resource_type = "models"
            if sources:
                resource_type = "sources"
            elif macros:
                resource_type = "macros"
            
            args.extend(["--resource-type", resource_type])
            
            result = self._run_dbt(args)
            
            if result.returncode == 0:
                return [line.strip() for line in result.stdout.split("\n") if line.strip()]
            
            return []
        
        except Exception:
            return []
    
    def seed(self, show: bool = False) -> Dict[str, Any]:
        """Load seed files.
        
        Args:
            show: Show seed results.
            
        Returns:
            Seed results.
        """
        try:
            args = ["seed"]
            
            if show:
                args.append("--show")
            
            result = self._run_dbt(args)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def snapshot(self) -> Dict[str, Any]:
        """Run dbt snapshots.
        
        Returns:
            Snapshot results.
        """
        try:
            result = self._run_dbt(["snapshot"])
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def docs_generate(self) -> Dict[str, Any]:
        """Generate dbt documentation.
        
        Returns:
            Docs generation results.
        """
        try:
            result = self._run_dbt(["docs", "generate"])
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def deps(self) -> Dict[str, Any]:
        """Install dbt dependencies.
        
        Returns:
            Deps installation results.
        """
        try:
            result = self._run_dbt(["deps"])
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def clean(self) -> Dict[str, Any]:
        """Clean dbt artifacts.
        
        Returns:
            Clean results.
        """
        try:
            result = self._run_dbt(["clean"])
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def build(
        self,
        models: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Run dbt build (run, test, seed, docs-generate).
        
        Args:
            models: Optional model names.
            exclude: Optional models to exclude.
            
        Returns:
            Build results.
        """
        try:
            args = ["build"]
            
            if models:
                args.extend(["--models"] + models)
            
            if exclude:
                args.extend(["--exclude"] + exclude)
            
            result = self._run_dbt(args)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run_operation(
        self,
        macro_name: str,
        args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Run a dbt macro.
        
        Args:
            macro_name: Name of macro to run.
            args: Optional macro arguments.
            
        Returns:
            Operation results.
        """
        try:
            cmd = ["run-operation", macro_name]
            
            if args:
                for key, value in args.items():
                    cmd.extend(["--args", f"{key}={value}"])
            
            result = self._run_dbt(cmd)
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr
            }
        
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_manifest(self) -> Optional[Dict[str, Any]]:
        """Get dbt manifest.json content.
        
        Returns:
            Manifest data or None.
        """
        import json
        
        manifest_path = os.path.join(self.project_dir, "target", "manifest.json")
        
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r") as f:
                    return json.load(f)
            except Exception:
                return None
        
        return None
    
    def get_run_results(self) -> Optional[Dict[str, Any]]:
        """Get dbt run_results.json content.
        
        Returns:
            Run results data or None.
        """
        import json
        
        results_path = os.path.join(self.project_dir, "target", "run_results.json")
        
        if os.path.exists(results_path):
            try:
                with open(results_path, "r") as f:
                    return json.load(f)
            except Exception:
                return None
        
        return None


class DbtAction(BaseAction):
    """dbt action for data transformation.
    
    Supports model running, testing, and analytics engineering.
    """
    action_type: str = "dbt"
    display_name: str = "dbt动作"
    description: str = "dbt数据转换和Analytics工程操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[DbtClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute dbt operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "debug":
                return self._debug(start_time)
            elif operation == "run":
                return self._run(params, start_time)
            elif operation == "test":
                return self._test(params, start_time)
            elif operation == "compile":
                return self._compile(params, start_time)
            elif operation == "ls":
                return self._ls(params, start_time)
            elif operation == "seed":
                return self._seed(params, start_time)
            elif operation == "snapshot":
                return self._snapshot(start_time)
            elif operation == "docs_generate":
                return self._docs_generate(start_time)
            elif operation == "deps":
                return self._deps(start_time)
            elif operation == "clean":
                return self._clean(start_time)
            elif operation == "build":
                return self._build(params, start_time)
            elif operation == "run_operation":
                return self._run_operation(params, start_time)
            elif operation == "get_manifest":
                return self._get_manifest(start_time)
            elif operation == "get_run_results":
                return self._get_run_results(start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except Exception as e:
            return ActionResult(success=False, message=f"dbt operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize dbt client."""
        project_dir = params.get("project_dir", ".")
        profiles_dir = params.get("profiles_dir")
        target = params.get("target")
        
        self._client = DbtClient(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=target
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message="dbt is available" if success else "dbt not available",
            duration=time.time() - start_time
        )
    
    def _debug(self, start_time: float) -> ActionResult:
        """Run dbt debug."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.debug()
            return ActionResult(
                success=result.get("success", False),
                message="Debug complete" if result.get("success") else "Debug failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run dbt models."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.run(
                models=params.get("models"),
                exclude=params.get("exclude"),
                full_refresh=params.get("full_refresh", False)
            )
            return ActionResult(
                success=result.get("success", False),
                message="Run complete" if result.get("success") else "Run failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _test(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run dbt tests."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.test(
                models=params.get("models"),
                data=params.get("data", True),
                schema=params.get("schema", True)
            )
            return ActionResult(
                success=result.get("success", False),
                message="Tests complete" if result.get("success") else "Tests failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _compile(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Compile dbt SQL."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.compile(models=params.get("models"))
            return ActionResult(
                success=result.get("success", False),
                message="Compile complete" if result.get("success") else "Compile failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _ls(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List dbt resources."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            items = self._client.ls(
                models=params.get("models", True),
                sources=params.get("sources", False),
                macros=params.get("macros", False)
            )
            return ActionResult(
                success=True,
                message=f"Found {len(items)} resources",
                data={"resources": items},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _seed(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Load seed files."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.seed(show=params.get("show", False))
            return ActionResult(
                success=result.get("success", False),
                message="Seed complete" if result.get("success") else "Seed failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _snapshot(self, start_time: float) -> ActionResult:
        """Run dbt snapshots."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.snapshot()
            return ActionResult(
                success=result.get("success", False),
                message="Snapshot complete" if result.get("success") else "Snapshot failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _docs_generate(self, start_time: float) -> ActionResult:
        """Generate dbt documentation."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.docs_generate()
            return ActionResult(
                success=result.get("success", False),
                message="Docs generated" if result.get("success") else "Docs generation failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _deps(self, start_time: float) -> ActionResult:
        """Install dbt dependencies."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.deps()
            return ActionResult(
                success=result.get("success", False),
                message="Deps installed" if result.get("success") else "Deps installation failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _clean(self, start_time: float) -> ActionResult:
        """Clean dbt artifacts."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.clean()
            return ActionResult(
                success=result.get("success", False),
                message="Clean complete" if result.get("success") else "Clean failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _build(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run dbt build."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.build(
                models=params.get("models"),
                exclude=params.get("exclude")
            )
            return ActionResult(
                success=result.get("success", False),
                message="Build complete" if result.get("success") else "Build failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _run_operation(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run a dbt macro."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        macro_name = params.get("macro_name", "")
        if not macro_name:
            return ActionResult(success=False, message="macro_name is required", duration=time.time() - start_time)
        
        try:
            result = self._client.run_operation(macro_name, params.get("args"))
            return ActionResult(
                success=result.get("success", False),
                message="Operation complete" if result.get("success") else "Operation failed",
                data={"output": result.get("output"), "error": result.get("error")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_manifest(self, start_time: float) -> ActionResult:
        """Get dbt manifest."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            manifest = self._client.get_manifest()
            return ActionResult(
                success=manifest is not None,
                message="Manifest retrieved" if manifest else "Manifest not found",
                data={"manifest": manifest},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_run_results(self, start_time: float) -> ActionResult:
        """Get dbt run results."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            results = self._client.get_run_results()
            return ActionResult(
                success=results is not None,
                message="Run results retrieved" if results else "Run results not found",
                data={"run_results": results},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
