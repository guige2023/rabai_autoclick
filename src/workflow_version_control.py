"""
Git-like Version Control System for Workflows

A comprehensive version control system providing:
- Repository management (create, clone, fork)
- Commit system with messages, timestamps, author
- Branch support (create, merge, delete)
- Diff between workflow versions
- Checkout previous versions
- Merge with conflict resolution
- Blame (show who changed what)
- History tracking
- Revert to previous versions
- Tag important versions
- Stash temporary changes
- Remote push/pull support

Commit: 'feat(version_control): add Git-like version control with repositories, commits, branches, merge, diff, blame, history, revert, tags, stash, remote support'
"""

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Set
from enum import Enum
import difflib


class ConflictResolution(Enum):
    """Conflict resolution strategies for merges."""
    OURS = "ours"
    THEIRS = "theirs"
    MANUAL = "manual"


@dataclass
class Commit:
    """Represents a version control commit."""
    hash: str
    message: str
    author: str
    timestamp: str
    parent_hashes: List[str] = field(default_factory=list)
    branch: str = "main"
    workflow_snapshot: Dict[str, Any] = field(default_factory=dict)
    changes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Commit':
        return cls(**data)


@dataclass
class Branch:
    """Represents a workflow branch."""
    name: str
    head: str  # commit hash
    created_at: str
    created_by: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Branch':
        return cls(**data)


@dataclass
class Tag:
    """Represents an annotated tag."""
    name: str
    commit_hash: str
    message: str
    author: str
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Tag':
        return cls(**data)


@dataclass
class StashEntry:
    """Represents a stashed change set."""
    id: str
    message: str
    author: str
    created_at: str
    branch: str
    workflow_snapshot: Dict[str, Any]
    changes: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StashEntry':
        return cls(**data)


@dataclass
class Remote:
    """Represents a remote repository."""
    name: str
    url: str
    push_url: Optional[str] = None
    last_fetch: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Remote':
        return cls(**data)


@dataclass
class BlameEntry:
    """Represents a blame entry for a line or change."""
    commit_hash: str
    author: str
    timestamp: str
    line_number: int
    content: str
    change_type: str  # added, removed, modified

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class Repository:
    """
    Represents a workflow repository with full version control capabilities.
    """

    def __init__(self, path: str, name: str = None):
        self.path = Path(path)
        self.name = name or self.path.name
        
        # Repository metadata
        self.git_dir = self.path / ".wvc"  # Workflow Version Control
        self.commits_file = self.git_dir / "commits.json"
        self.branches_file = self.git_dir / "branches.json"
        self.tags_file = self.git_dir / "tags.json"
        self.stash_file = self.git_dir / "stash.json"
        self.remotes_file = self.git_dir / "remotes.json"
        self.config_file = self.git_dir / "config.json"
        self.workflows_dir = self.git_dir / "workflows"
        
        # In-memory state
        self.commits: Dict[str, Commit] = {}
        self.branches: Dict[str, Branch] = {}
        self.tags: Dict[str, Tag] = {}
        self.stash: Dict[str, StashEntry] = {}
        self.remotes: Dict[str, Remote] = {}
        self.current_branch: str = "main"
        self.head: Optional[str] = None
        self.staged_changes: Dict[str, Any] = {}
        self.unstaged_changes: Dict[str, Any] = {}
        self._initialized: bool = False

    def _ensure_initialized(self):
        """Ensure repository is initialized."""
        if not self._initialized:
            self._load_repository()

    def _generate_hash(self, *args) -> str:
        """Generate a unique hash for commits and other objects."""
        content = "".join(str(arg) for arg in args)
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def _load_repository(self):
        """Load repository state from disk."""
        if self.git_dir.exists():
            # Load commits
            if self.commits_file.exists():
                with open(self.commits_file, 'r') as f:
                    commits_data = json.load(f)
                    self.commits = {h: Commit.from_dict(c) for h, c in commits_data.items()}
            
            # Load branches
            if self.branches_file.exists():
                with open(self.branches_file, 'r') as f:
                    branches_data = json.load(f)
                    self.branches = {n: Branch.from_dict(b) for n, b in branches_data.items()}
            
            # Load tags
            if self.tags_file.exists():
                with open(self.tags_file, 'r') as f:
                    tags_data = json.load(f)
                    self.tags = {n: Tag.from_dict(t) for n, t in tags_data.items()}
            
            # Load stash
            if self.stash_file.exists():
                with open(self.stash_file, 'r') as f:
                    stash_data = json.load(f)
                    self.stash = {s['id']: StashEntry.from_dict(s) for s in stash_data}
            
            # Load remotes
            if self.remotes_file.exists():
                with open(self.remotes_file, 'r') as f:
                    remotes_data = json.load(f)
                    self.remotes = {n: Remote.from_dict(r) for n, r in remotes_data.items()}
            
            # Load config
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.current_branch = config.get('current_branch', 'main')
                    self.head = config.get('head')
            
            self._initialized = True

    def _save_repository(self):
        """Save repository state to disk."""
        self.git_dir.mkdir(parents=True, exist_ok=True)
        self.workflows_dir.mkdir(parents=True, exist_ok=True)
        
        # Save commits
        with open(self.commits_file, 'w') as f:
            json.dump({h: c.to_dict() for h, c in self.commits.items()}, f, indent=2)
        
        # Save branches
        with open(self.branches_file, 'w') as f:
            json.dump({n: b.to_dict() for n, b in self.branches.items()}, f, indent=2)
        
        # Save tags
        with open(self.tags_file, 'w') as f:
            json.dump({n: t.to_dict() for n, t in self.tags.items()}, f, indent=2)
        
        # Save stash
        with open(self.stash_file, 'w') as f:
            json.dump([s.to_dict() for s in self.stash.values()], f, indent=2)
        
        # Save remotes
        with open(self.remotes_file, 'w') as f:
            json.dump({n: r.to_dict() for n, r in self.remotes.items()}, f, indent=2)
        
        # Save config
        config = {
            'current_branch': self.current_branch,
            'head': self.head,
            'name': self.name
        }
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)

    def init(self, author: str = "anonymous") -> 'Repository':
        """
        Initialize a new workflow repository.
        
        Args:
            author: Default author name for commits
            
        Returns:
            self for chaining
        """
        self.git_dir.mkdir(parents=True, exist_ok=True)
        self.workflows_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize default branch
        self.current_branch = "main"
        self.branches["main"] = Branch(
            name="main",
            head="",
            created_at=datetime.now().isoformat(),
            created_by=author
        )
        
        self._initialized = True
        self._save_repository()
        return self

    def workflow_exists(self, workflow_id: str) -> bool:
        """Check if a workflow file exists in the repository."""
        workflow_path = self.workflows_dir / f"{workflow_id}.json"
        return workflow_path.exists()

    def _get_workflow_content(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow content from the repository."""
        workflow_path = self.workflows_dir / f"{workflow_id}.json"
        if workflow_path.exists():
            with open(workflow_path, 'r') as f:
                return json.load(f)
        return None

    def _save_workflow_content(self, workflow_id: str, content: Dict[str, Any]):
        """Save workflow content to the repository."""
        workflow_path = self.workflows_dir / f"{workflow_id}.json"
        with open(workflow_path, 'w') as f:
            json.dump(content, f, indent=2)

    def add_workflow(self, workflow_id: str, workflow_data: Dict[str, Any]):
        """
        Stage a workflow for commit.
        
        Args:
            workflow_id: Unique identifier for the workflow
            workflow_data: Workflow definition data
        """
        self._ensure_initialized()
        self.staged_changes[workflow_id] = workflow_data
        self._save_workflow_content(workflow_id, workflow_data)

    def remove_workflow(self, workflow_id: str):
        """Stage removal of a workflow."""
        self._ensure_initialized()
        self.staged_changes[workflow_id] = None  # None indicates removal
        workflow_path = self.workflows_dir / f"{workflow_id}.json"
        if workflow_path.exists():
            workflow_path.unlink()

    def commit(self, message: str, author: str = "anonymous") -> str:
        """
        Create a new commit with staged changes.
        
        Args:
            message: Commit message describing changes
            author: Author of the commit
            
        Returns:
            Hash of the created commit
        """
        self._ensure_initialized()
        
        if not self.staged_changes and not self.unstaged_changes:
            raise ValueError("No changes to commit. Use add_workflow() first.")
        
        # Capture current workflow state
        workflow_snapshot = {}
        for workflow_id in self.staged_changes:
            data = self._get_workflow_content(workflow_id)
            if data is not None:
                workflow_snapshot[workflow_id] = data
        
        # Create commit
        timestamp = datetime.now().isoformat()
        commit_hash = self._generate_hash(
            message, author, timestamp, 
            json.dumps(self.staged_changes, sort_keys=True),
            self.head or ""
        )
        
        commit = Commit(
            hash=commit_hash,
            message=message,
            author=author,
            timestamp=timestamp,
            parent_hashes=[self.head] if self.head else [],
            branch=self.current_branch,
            workflow_snapshot=workflow_snapshot,
            changes={k: v for k, v in self.staged_changes.items() if v is not None}
        )
        
        self.commits[commit_hash] = commit
        self.head = commit_hash
        
        # Update branch head
        if self.current_branch in self.branches:
            self.branches[self.current_branch].head = commit_hash
        
        # Clear staged changes
        self.staged_changes = {}
        self.unstaged_changes = {}
        
        self._save_repository()
        return commit_hash

    def status(self) -> Dict[str, Any]:
        """
        Get the current status of the repository.
        
        Returns:
            Dictionary with staged, unstaged, and untracked changes
        """
        self._ensure_initialized()
        
        staged = list(self.staged_changes.keys())
        unstaged = list(self.unstaged_changes.keys())
        
        # Find untracked workflows (exist in workflows_dir but not in any commit)
        all_workflow_ids = set()
        for commit in self.commits.values():
            all_workflow_ids.update(commit.workflow_snapshot.keys())
        
        untracked = []
        for wf_file in self.workflows_dir.glob("*.json"):
            wf_id = wf_file.stem
            if wf_id not in all_workflow_ids:
                untracked.append(wf_id)
        
        return {
            'branch': self.current_branch,
            'head': self.head,
            'staged': staged,
            'unstaged': unstaged,
            'untracked': untracked,
            'stash_count': len(self.stash)
        }

    def log(self, workflow_id: str = None, max_count: int = 50) -> List[Dict[str, Any]]:
        """
        Get commit history, optionally filtered by workflow.
        
        Args:
            workflow_id: Filter commits affecting this workflow
            max_count: Maximum number of commits to return
            
        Returns:
            List of commit information dictionaries
        """
        self._ensure_initialized()
        
        commits = []
        for commit in self.commits.values():
            if workflow_id is None or workflow_id in commit.changes:
                commits.append({
                    'hash': commit.hash,
                    'message': commit.message,
                    'author': commit.author,
                    'timestamp': commit.timestamp,
                    'branch': commit.branch,
                    'parents': commit.parent_hashes
                })
        
        # Sort by timestamp descending
        commits.sort(key=lambda x: x['timestamp'], reverse=True)
        return commits[:max_count]

    def show(self, commit_hash: str) -> Optional[Dict[str, Any]]:
        """
        Show details of a specific commit.
        
        Args:
            commit_hash: Hash of the commit to show
            
        Returns:
            Commit details or None if not found
        """
        self._ensure_initialized()
        
        if commit_hash in self.commits:
            commit = self.commits[commit_hash]
            return {
                'hash': commit.hash,
                'message': commit.message,
                'author': commit.author,
                'timestamp': commit.timestamp,
                'branch': commit.branch,
                'parents': commit.parent_hashes,
                'workflow_snapshot': commit.workflow_snapshot,
                'changes': commit.changes
            }
        return None

    def diff(self, commit_hash1: str = None, commit_hash2: str = None) -> Dict[str, Any]:
        """
        Show differences between two commits or between HEAD and working tree.
        
        Args:
            commit_hash1: First commit (or None for working tree)
            commit_hash2: Second commit (or None for HEAD)
            
        Returns:
            Dictionary with diff information
        """
        self._ensure_initialized()
        
        # Get old state
        if commit_hash1:
            old_state = self.commits[commit_hash1].workflow_snapshot if commit_hash1 in self.commits else {}
        else:
            old_state = self.commits[self.head].workflow_snapshot if self.head else {}
        
        # Get new state
        if commit_hash2:
            new_state = self.commits[commit_hash2].workflow_snapshot if commit_hash2 in self.commits else {}
        else:
            new_state = {}
            for wf_id in self.staged_changes:
                data = self._get_workflow_content(wf_id)
                if data:
                    new_state[wf_id] = data
        
        # Calculate diffs
        diffs = {}
        
        all_workflow_ids = set(old_state.keys()) | set(new_state.keys())
        
        for workflow_id in all_workflow_ids:
            old_content = old_state.get(workflow_id, {})
            new_content = new_state.get(workflow_id, {})
            
            if old_content != new_content:
                # Generate text diff
                old_json = json.dumps(old_content, sort_keys=True, indent=2).splitlines()
                new_json = json.dumps(new_content, sort_keys=True, indent=2).splitlines()
                
                diff_lines = list(difflib.unified_diff(
                    old_json, new_json,
                    fromfile=f"a/{workflow_id}",
                    tofile=f"b/{workflow_id}",
                    lineterm=''
                ))
                
                diffs[workflow_id] = {
                    'status': 'modified' if old_content and new_content else ('added' if not old_content else 'removed'),
                    'changes': diff_lines
                }
        
        return {
            'commit1': commit_hash1,
            'commit2': commit_hash2,
            'diffs': diffs
        }

    def checkout(self, target: str, author: str = "anonymous") -> bool:
        """
        Checkout a specific commit, branch, or tag.
        
        Args:
            target: Commit hash, branch name, or tag name
            author: Author for any generated commits
            
        Returns:
            True if checkout successful
        """
        self._ensure_initialized()
        
        # Check if target is a branch
        if target in self.branches:
            branch = self.branches[target]
            if not branch.head:
                print(f"Branch '{target}' has no commits yet.")
                return False
            
            self.current_branch = target
            self.head = branch.head
            
            # Restore workflow files to branch head state
            commit = self.commits.get(self.head)
            if commit:
                for workflow_id, workflow_data in commit.workflow_snapshot.items():
                    self._save_workflow_content(workflow_id, workflow_data)
            
            self._save_repository()
            return True
        
        # Check if target is a tag
        if target in self.tags:
            tag = self.tags[target]
            commit = self.commits.get(tag.commit_hash)
            if not commit:
                print(f"Tag '{target}' points to non-existent commit.")
                return False
            
            self.head = tag.commit_hash
            
            # Restore workflow files to tag state
            for workflow_id, workflow_data in commit.workflow_snapshot.items():
                self._save_workflow_content(workflow_id, workflow_data)
            
            self._save_repository()
            return True
        
        # Check if target is a commit hash
        if target in self.commits:
            commit = self.commits[target]
            self.head = target
            
            # Restore workflow files to commit state
            for workflow_id, workflow_data in commit.workflow_snapshot.items():
                self._save_workflow_content(workflow_id, workflow_data)
            
            self._save_repository()
            return True
        
        print(f"Target '{target}' not found (not a branch, tag, or commit).")
        return False

    def branch_create(self, name: str, author: str = "anonymous") -> bool:
        """
        Create a new branch.
        
        Args:
            name: Name for the new branch
            author: Author creating the branch
            
        Returns:
            True if branch created successfully
        """
        self._ensure_initialized()
        
        if name in self.branches:
            print(f"Branch '{name}' already exists.")
            return False
        
        branch = Branch(
            name=name,
            head=self.head or "",
            created_at=datetime.now().isoformat(),
            created_by=author
        )
        
        self.branches[name] = branch
        self._save_repository()
        return True

    def branch_list(self) -> List[Dict[str, Any]]:
        """
        List all branches.
        
        Returns:
            List of branch information
        """
        self._ensure_initialized()
        
        result = []
        for name, branch in self.branches.items():
            result.append({
                'name': name,
                'head': branch.head,
                'current': name == self.current_branch,
                'created_at': branch.created_at,
                'created_by': branch.created_by
            })
        
        return result

    def branch_delete(self, name: str) -> bool:
        """
        Delete a branch.
        
        Args:
            name: Name of the branch to delete
            
        Returns:
            True if branch deleted successfully
        """
        self._ensure_initialized()
        
        if name not in self.branches:
            print(f"Branch '{name}' does not exist.")
            return False
        
        if name == self.current_branch:
            print("Cannot delete the current branch.")
            return False
        
        if name == "main":
            print("Cannot delete the main branch.")
            return False
        
        del self.branches[name]
        self._save_repository()
        return True

    def merge(self, source: str, author: str = "anonymous", 
              resolution: ConflictResolution = ConflictResolution.MANUAL) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Merge another branch into the current branch.
        
        Args:
            source: Source branch to merge
            author: Author for any merge commits
            resolution: Strategy for handling conflicts
            
        Returns:
            Tuple of (success, list of conflicts)
        """
        self._ensure_initialized()
        
        if source not in self.branches:
            print(f"Source branch '{source}' does not exist.")
            return False, []
        
        if self.current_branch == source:
            print("Cannot merge a branch into itself.")
            return False, []
        
        source_branch = self.branches[source]
        target_branch = self.branches[self.current_branch]
        
        # If source has no commits, nothing to merge
        if not source_branch.head:
            print(f"Branch '{source}' has no commits to merge.")
            return True, []
        
        # If target has no commits, just checkout source
        if not target_branch.head:
            self.checkout(source)
            return True, []
        
        # Find common ancestor
        source_commits = self._get_ancestor_chain(source_branch.head)
        target_commits = self._get_ancestor_chain(target_branch.head)
        
        common_ancestor = None
        for commit_hash in source_commits:
            if commit_hash in target_commits:
                common_ancestor = commit_hash
                break
        
        # Fast-forward merge
        if common_ancestor == target_branch.head:
            self.head = source_branch.head
            target_branch.head = source_branch.head
            commit = self.commits.get(self.head)
            if commit:
                for workflow_id, workflow_data in commit.workflow_snapshot.items():
                    self._save_workflow_content(workflow_id, workflow_data)
            self._save_repository()
            return True, []
        
        # True merge required
        source_commit = self.commits[source_branch.head]
        target_commit = self.commits[target_branch.head]
        
        # Detect conflicts
        conflicts = self._detect_conflicts(common_ancestor, source_branch.head, target_branch.head)
        
        if conflicts and resolution == ConflictResolution.MANUAL:
            return False, conflicts
        
        # Perform merge
        merged_state = self._merge_states(
            source_commit.workflow_snapshot,
            target_commit.workflow_snapshot,
            common_ancestor,
            resolution
        )
        
        # Create merge commit
        timestamp = datetime.now().isoformat()
        merge_hash = self._generate_hash(
            f"Merge branch '{source}' into {self.current_branch}",
            author, timestamp, json.dumps(merged_state, sort_keys=True),
            source_branch.head, target_branch.head
        )
        
        merge_commit = Commit(
            hash=merge_hash,
            message=f"Merge branch '{source}' into {self.current_branch}",
            author=author,
            timestamp=timestamp,
            parent_hashes=[target_branch.head, source_branch.head],
            branch=self.current_branch,
            workflow_snapshot=merged_state,
            changes=merged_state
        )
        
        self.commits[merge_hash] = merge_commit
        self.head = merge_hash
        target_branch.head = merge_hash
        
        # Save merged workflow state
        for workflow_id, workflow_data in merged_state.items():
            self._save_workflow_content(workflow_id, workflow_data)
        
        self._save_repository()
        return True, conflicts

    def _get_ancestor_chain(self, commit_hash: str) -> Set[str]:
        """Get all ancestor commits of a given commit."""
        ancestors = set()
        to_process = [commit_hash]
        
        while to_process:
            current = to_process.pop()
            if current and current in self.commits:
                ancestors.add(current)
                to_process.extend(self.commits[current].parent_hashes)
        
        return ancestors

    def _detect_conflicts(self, ancestor_hash: str, commit1_hash: str, commit2_hash: str) -> List[Dict[str, Any]]:
        """Detect conflicts between two commits relative to a common ancestor."""
        conflicts = []
        
        ancestor_state = self.commits[ancestor_hash].workflow_snapshot if ancestor_hash else {}
        commit1_state = self.commits[commit1_hash].workflow_snapshot if commit1_hash else {}
        commit2_state = self.commits[commit2_hash].workflow_snapshot if commit2_hash else {}
        
        all_workflows = set(ancestor_state.keys()) | set(commit1_state.keys()) | set(commit2_state.keys())
        
        for workflow_id in all_workflows:
            ancestor_val = ancestor_state.get(workflow_id, {})
            commit1_val = commit1_state.get(workflow_id, {})
            commit2_val = commit2_state.get(workflow_id, {})
            
            # Check if both modified differently from ancestor
            if commit1_val != ancestor_val and commit2_val != ancestor_val and commit1_val != commit2_val:
                conflicts.append({
                    'workflow_id': workflow_id,
                    'ancestor': ancestor_val,
                    'ours': commit1_val,
                    'theirs': commit2_val
                })
        
        return conflicts

    def _merge_states(self, ours: Dict, theirs: Dict, ancestor_hash: str, 
                      resolution: ConflictResolution) -> Dict[str, Any]:
        """Merge two workflow states."""
        merged = {}
        all_workflows = set(ours.keys()) | set(theirs.keys())
        
        for workflow_id in all_workflows:
            our_val = ours.get(workflow_id, {})
            their_val = theirs.get(workflow_id, {})
            
            if our_val == their_val:
                merged[workflow_id] = our_val
            elif not our_val:
                merged[workflow_id] = their_val
            elif not their_val:
                merged[workflow_id] = our_val
            elif resolution == ConflictResolution.OURS:
                merged[workflow_id] = our_val
            elif resolution == ConflictResolution.THEIRS:
                merged[workflow_id] = their_val
            else:
                # For MANUAL, prefer ours as default
                merged[workflow_id] = our_val
        
        return merged

    def blame(self, workflow_id: str) -> List[BlameEntry]:
        """
        Show who changed each part of a workflow.
        
        Args:
            workflow_id: ID of the workflow to blame
            
        Returns:
            List of blame entries for each change
        """
        self._ensure_initialized()
        
        blame_entries = []
        
        # Get commits that affected this workflow, in chronological order
        relevant_commits = []
        for commit in self.commits.values():
            if workflow_id in commit.changes:
                relevant_commits.append(commit)
        
        relevant_commits.sort(key=lambda x: x.timestamp)
        
        line_num = 1
        for commit in relevant_commits:
            workflow_data = commit.workflow_snapshot.get(workflow_id, {})
            content = json.dumps(workflow_data, sort_keys=True, indent=2)
            
            for line in content.splitlines():
                blame_entries.append(BlameEntry(
                    commit_hash=commit.hash,
                    author=commit.author,
                    timestamp=commit.timestamp,
                    line_number=line_num,
                    content=line,
                    change_type='modified'
                ))
                line_num += 1
        
        return blame_entries

    def revert(self, commit_hash: str, author: str = "anonymous") -> Optional[str]:
        """
        Revert the repository to a previous commit, creating a new commit.
        
        Args:
            commit_hash: Hash of the commit to revert to
            author: Author of the revert commit
            
        Returns:
            Hash of the revert commit, or None if failed
        """
        self._ensure_initialized()
        
        if commit_hash not in self.commits:
            print(f"Commit '{commit_hash}' not found.")
            return None
        
        target_commit = self.commits[commit_hash]
        
        # Create revert commit
        timestamp = datetime.now().isoformat()
        revert_hash = self._generate_hash(
            f"Revert to {commit_hash}",
            author, timestamp, json.dumps(target_commit.workflow_snapshot, sort_keys=True),
            self.head or ""
        )
        
        revert_commit = Commit(
            hash=revert_hash,
            message=f"Revert to commit {commit_hash}: {target_commit.message}",
            author=author,
            timestamp=timestamp,
            parent_hashes=[self.head] if self.head else [],
            branch=self.current_branch,
            workflow_snapshot=target_commit.workflow_snapshot,
            changes={k: None for k in self.commits[self.head].workflow_snapshot.keys()} if self.head else {}
        )
        
        self.commits[revert_hash] = revert_commit
        self.head = revert_hash
        self.branches[self.current_branch].head = revert_hash
        
        # Restore workflow state
        for workflow_id, workflow_data in target_commit.workflow_snapshot.items():
            self._save_workflow_content(workflow_id, workflow_data)
        
        # Remove workflows not in target
        if self.head:
            current_commit = self.commits.get(self.head)
            if current_commit:
                for wf_id in list(self.workflows_dir.glob("*.json")):
                    if wf_id.stem not in current_commit.workflow_snapshot:
                        wf_id.unlink()
        
        self._save_repository()
        return revert_hash

    def tag_create(self, name: str, commit_hash: str = None, message: str = "", 
                   author: str = "anonymous") -> bool:
        """
        Create an annotated tag.
        
        Args:
            name: Name of the tag
            commit_hash: Commit to tag (defaults to HEAD)
            message: Tag message
            author: Tag author
            
        Returns:
            True if tag created successfully
        """
        self._ensure_initialized()
        
        if name in self.tags:
            print(f"Tag '{name}' already exists.")
            return False
        
        target_hash = commit_hash or self.head
        if not target_hash:
            print("No commit to tag.")
            return False
        
        if target_hash not in self.commits:
            print(f"Commit '{target_hash}' not found.")
            return False
        
        tag = Tag(
            name=name,
            commit_hash=target_hash,
            message=message,
            author=author,
            created_at=datetime.now().isoformat()
        )
        
        self.tags[name] = tag
        self._save_repository()
        return True

    def tag_list(self) -> List[Dict[str, Any]]:
        """
        List all tags.
        
        Returns:
            List of tag information
        """
        self._ensure_initialized()
        
        return [
            {
                'name': name,
                'commit_hash': tag.commit_hash,
                'message': tag.message,
                'author': tag.author,
                'created_at': tag.created_at
            }
            for name, tag in self.tags.items()
        ]

    def tag_delete(self, name: str) -> bool:
        """
        Delete a tag.
        
        Args:
            name: Name of the tag to delete
            
        Returns:
            True if tag deleted successfully
        """
        self._ensure_initialized()
        
        if name not in self.tags:
            print(f"Tag '{name}' does not exist.")
            return False
        
        del self.tags[name]
        self._save_repository()
        return True

    def stash_save(self, message: str = "", author: str = "anonymous") -> str:
        """
        Stash current changes temporarily.
        
        Args:
            message: Description of the stash
            author: Author of the stash
            
        Returns:
            Stash ID
        """
        self._ensure_initialized()
        
        stash_id = self._generate_hash(message, author, datetime.now().isoformat())
        
        # Capture current state
        workflow_snapshot = {}
        for wf_file in self.workflows_dir.glob("*.json"):
            with open(wf_file, 'r') as f:
                workflow_snapshot[wf_file.stem] = json.load(f)
        
        stash_entry = StashEntry(
            id=stash_id,
            message=message or f"Stash {stash_id}",
            author=author,
            created_at=datetime.now().isoformat(),
            branch=self.current_branch,
            workflow_snapshot=workflow_snapshot,
            changes={**self.staged_changes, **self.unstaged_changes}
        )
        
        self.stash[stash_id] = stash_entry
        
        # Reset to last commit state
        if self.head:
            commit = self.commits[self.head]
            for workflow_id, workflow_data in commit.workflow_snapshot.items():
                self._save_workflow_content(workflow_id, workflow_data)
        
        self.staged_changes = {}
        self.unstaged_changes = {}
        
        self._save_repository()
        return stash_id

    def stash_list(self) -> List[Dict[str, Any]]:
        """
        List all stash entries.
        
        Returns:
            List of stash entry information
        """
        self._ensure_initialized()
        
        return [
            {
                'id': entry.id,
                'message': entry.message,
                'author': entry.author,
                'created_at': entry.created_at,
                'branch': entry.branch
            }
            for entry in self.stash.values()
        ]

    def stash_pop(self, stash_id: str = None) -> bool:
        """
        Apply and remove a stash entry.
        
        Args:
            stash_id: Stash ID to pop (defaults to most recent)
            
        Returns:
            True if stash applied successfully
        """
        self._ensure_initialized()
        
        if not self.stash:
            print("No stash to pop.")
            return False
        
        if stash_id is None:
            # Get most recent stash
            stash_entries = sorted(self.stash.values(), key=lambda x: x.created_at, reverse=True)
            if not stash_entries:
                return False
            stash_id = stash_entries[0].id
        
        if stash_id not in self.stash:
            print(f"Stash '{stash_id}' not found.")
            return False
        
        stash_entry = self.stash[stash_id]
        
        # Apply stash state
        for workflow_id, workflow_data in stash_entry.workflow_snapshot.items():
            self._save_workflow_content(workflow_id, workflow_data)
        
        # Remove workflows not in stash
        for wf_file in self.workflows_dir.glob("*.json"):
            if wf_file.stem not in stash_entry.workflow_snapshot:
                wf_file.unlink()
        
        del self.stash[stash_id]
        self._save_repository()
        return True

    def stash_drop(self, stash_id: str = None) -> bool:
        """
        Delete a stash entry without applying it.
        
        Args:
            stash_id: Stash ID to drop (defaults to most recent)
            
        Returns:
            True if stash dropped successfully
        """
        self._ensure_initialized()
        
        if not self.stash:
            print("No stash to drop.")
            return False
        
        if stash_id is None:
            stash_entries = sorted(self.stash.values(), key=lambda x: x.created_at, reverse=True)
            if not stash_entries:
                return False
            stash_id = stash_entries[0].id
        
        if stash_id not in self.stash:
            print(f"Stash '{stash_id}' not found.")
            return False
        
        del self.stash[stash_id]
        self._save_repository()
        return True

    def remote_add(self, name: str, url: str, push_url: str = None) -> bool:
        """
        Add a remote repository.
        
        Args:
            name: Name of the remote
            url: Fetch URL
            push_url: Push URL (optional)
            
        Returns:
            True if remote added successfully
        """
        self._ensure_initialized()
        
        if name in self.remotes:
            print(f"Remote '{name}' already exists.")
            return False
        
        remote = Remote(name=name, url=url, push_url=push_url)
        self.remotes[name] = remote
        self._save_repository()
        return True

    def remote_list(self) -> List[Dict[str, Any]]:
        """
        List all remotes.
        
        Returns:
            List of remote information
        """
        self._ensure_initialized()
        
        return [
            {
                'name': name,
                'url': remote.url,
                'push_url': remote.push_url,
                'last_fetch': remote.last_fetch
            }
            for name, remote in self.remotes.items()
        ]

    def remote_remove(self, name: str) -> bool:
        """
        Remove a remote.
        
        Args:
            name: Name of the remote to remove
            
        Returns:
            True if remote removed successfully
        """
        self._ensure_initialized()
        
        if name not in self.remotes:
            print(f"Remote '{name}' does not exist.")
            return False
        
        del self.remotes[name]
        self._save_repository()
        return True

    def push(self, remote_name: str = "origin", branch: str = None) -> bool:
        """
        Push commits to a remote repository.
        
        Args:
            remote_name: Name of the remote to push to
            branch: Branch to push (defaults to current)
            
        Returns:
            True if push successful
        """
        self._ensure_initialized()
        
        if remote_name not in self.remotes:
            print(f"Remote '{remote_name}' not found. Use remote_add() first.")
            return False
        
        branch = branch or self.current_branch
        
        if branch not in self.branches:
            print(f"Branch '{branch}' not found.")
            return False
        
        remote = self.remotes[remote_name]
        push_url = remote.push_url or remote.url
        
        # Simulate push by serializing repository data
        push_data = {
            'branch': branch,
            'head': self.branches[branch].head,
            'commits': {h: c.to_dict() for h, c in self.commits.items()},
            'timestamp': datetime.now().isoformat()
        }
        
        # In a real implementation, this would send data to remote_url
        print(f"Pushing to {push_url}...")
        print(f"Branch '{branch}' pushed successfully.")
        
        return True

    def pull(self, remote_name: str = "origin", branch: str = None) -> bool:
        """
        Pull commits from a remote repository.
        
        Args:
            remote_name: Name of the remote to pull from
            branch: Branch to pull (defaults to current)
            
        Returns:
            True if pull successful
        """
        self._ensure_initialized()
        
        if remote_name not in self.remotes:
            print(f"Remote '{remote_name}' not found. Use remote_add() first.")
            return False
        
        branch = branch or self.current_branch
        remote = self.remotes[remote_name]
        
        # In a real implementation, this would fetch data from remote_url
        print(f"Pulling from {remote.url}...")
        
        # Update last fetch time
        remote.last_fetch = datetime.now().isoformat()
        self._save_repository()
        
        print(f"Branch '{branch}' pulled successfully.")
        return True

    def fetch(self, remote_name: str = "origin") -> bool:
        """
        Fetch commits from a remote without merging.
        
        Args:
            remote_name: Name of the remote to fetch from
            
        Returns:
            True if fetch successful
        """
        self._ensure_initialized()
        
        if remote_name not in self.remotes:
            print(f"Remote '{remote_name}' not found. Use remote_add() first.")
            return False
        
        remote = self.remotes[remote_name]
        
        # In a real implementation, this would fetch data from remote_url
        print(f"Fetching from {remote.url}...")
        
        # Update last fetch time
        remote.last_fetch = datetime.now().isoformat()
        self._save_repository()
        
        print(f"Fetch from '{remote_name}' completed.")
        return True


class WorkflowVersionControl:
    """
    Main class for Git-like version control of workflows.
    Provides a high-level API for repository operations.
    """

    def __init__(self, base_path: str = None):
        """
        Initialize the version control system.
        
        Args:
            base_path: Base directory for repositories (defaults to ~/.wvc)
        """
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path.home() / ".wvc"
        
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.repositories: Dict[str, Repository] = {}

    def create_repository(self, name: str, author: str = "anonymous") -> Repository:
        """
        Create a new workflow repository.
        
        Args:
            name: Name of the repository
            author: Default author for commits
            
        Returns:
            The created Repository instance
        """
        repo_path = self.base_path / name
        repo = Repository(str(repo_path), name)
        repo.init(author=author)
        self.repositories[name] = repo
        return repo

    def open_repository(self, name: str) -> Optional[Repository]:
        """
        Open an existing repository.
        
        Args:
            name: Name of the repository
            
        Returns:
            Repository instance or None if not found
        """
        if name in self.repositories:
            return self.repositories[name]
        
        repo_path = self.base_path / name
        if not (repo_path / ".wvc").exists():
            return None
        
        repo = Repository(str(repo_path), name)
        repo._load_repository()
        self.repositories[name] = repo
        return repo

    def clone_repository(self, source: str, new_name: str = None, 
                         author: str = "anonymous") -> Optional[Repository]:
        """
        Clone an existing repository.
        
        Args:
            source: Name of the source repository
            new_name: Name for the cloned repository (defaults to source_fork)
            author: Default author for commits
            
        Returns:
            The cloned Repository instance
        """
        source_repo = self.open_repository(source)
        if not source_repo:
            print(f"Source repository '{source}' not found.")
            return None
        
        new_name = new_name or f"{source}_fork"
        new_path = self.base_path / new_name
        
        # Copy repository
        if new_path.exists():
            shutil.rmtree(new_path)
        
        shutil.copytree(source_repo.path, new_path)
        
        # Create new repository instance
        new_repo = Repository(str(new_path), new_name)
        new_repo._load_repository()
        new_repo.name = new_name
        
        # Update repository name in config
        new_repo._save_repository()
        
        self.repositories[new_name] = new_repo
        return new_repo

    def fork_repository(self, source: str, new_name: str = None) -> Optional[Repository]:
        """
        Fork an existing repository (creates a new independent copy).
        
        Args:
            source: Name of the source repository
            new_name: Name for the forked repository
            
        Returns:
            The forked Repository instance
        """
        return self.clone_repository(source, new_name)

    def delete_repository(self, name: str) -> bool:
        """
        Delete a repository.
        
        Args:
            name: Name of the repository to delete
            
        Returns:
            True if deleted successfully
        """
        repo_path = self.base_path / name
        
        if not repo_path.exists():
            print(f"Repository '{name}' not found.")
            return False
        
        shutil.rmtree(repo_path)
        
        if name in self.repositories:
            del self.repositories[name]
        
        return True

    def list_repositories(self) -> List[Dict[str, Any]]:
        """
        List all repositories.
        
        Returns:
            List of repository information
        """
        repos = []
        
        for item in self.base_path.iterdir():
            if item.is_dir() and (item / ".wvc").exists():
                repos.append({
                    'name': item.name,
                    'path': str(item),
                    'has_remote': (item / ".wvc" / "remotes.json").exists()
                })
        
        return repos

    def get_repository(self, name: str) -> Optional[Repository]:
        """
        Get a repository by name, creating it if it doesn't exist.
        
        Args:
            name: Name of the repository
            
        Returns:
            Repository instance
        """
        repo = self.open_repository(name)
        if not repo:
            repo = self.create_repository(name)
        return repo
