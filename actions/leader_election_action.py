"""Leader Election Action Module.

Provides leader election for distributed coordination
using various consensus algorithms.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ElectionState(Enum):
    """Election state."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class Node:
    """Cluster node."""
    node_id: str
    name: str
    last_heartbeat: float = field(default_factory=time.time)
    state: ElectionState = ElectionState.FOLLOWER


class LeaderElection:
    """Leader election manager."""

    def __init__(self, cluster_id: str, heartbeat_timeout: float = 30.0):
        self.cluster_id = cluster_id
        self.heartbeat_timeout = heartbeat_timeout
        self._nodes: Dict[str, Node] = {}
        self._leader: Optional[str] = None
        self._lock = threading.RLock()
        self._current_node_id: Optional[str] = None

    def join(self, node_id: str, name: str) -> ElectionState:
        """Node joins cluster."""
        with self._lock:
            if node_id not in self._nodes:
                self._nodes[node_id] = Node(node_id=node_id, name=name)

            self._current_node_id = node_id
            self._update_leader()
            return self._nodes[node_id].state

    def leave(self, node_id: str) -> None:
        """Node leaves cluster."""
        with self._lock:
            if node_id in self._nodes:
                del self._nodes[node_id]
            if self._leader == node_id:
                self._leader = None
                self._start_election()

    def heartbeat(self, node_id: str) -> Optional[str]:
        """Update node heartbeat."""
        with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].last_heartbeat = time.time()
            return self._leader

    def propose_leader(self, node_id: str) -> bool:
        """Propose node as leader."""
        with self._lock:
            if node_id not in self._nodes:
                return False

            self._leader = node_id
            self._nodes[node_id].state = ElectionState.LEADER

            for nid, node in self._nodes.items():
                if nid != node_id:
                    node.state = ElectionState.FOLLOWER

            return True

    def get_leader(self) -> Optional[Node]:
        """Get current leader."""
        with self._lock:
            self._check_leader_health()
            if self._leader and self._leader in self._nodes:
                return self._nodes[self._leader]
            return None

    def _update_leader(self) -> None:
        """Update leader based on health."""
        self._check_leader_health()

        if self._leader is None:
            self._start_election()

    def _check_leader_health(self) -> None:
        """Check if leader is still healthy."""
        if self._leader and self._leader in self._nodes:
            leader = self._nodes[self._leader]
            if time.time() - leader.last_heartbeat > self.heartbeat_timeout:
                self._leader = None

    def _start_election(self) -> None:
        """Start leader election."""
        if not self._nodes:
            return

        candidates = sorted(
            self._nodes.values(),
            key=lambda n: n.node_id
        )

        if candidates:
            new_leader = candidates[0].node_id
            self.propose_leader(new_leader)

    def get_state(self, node_id: str) -> Optional[ElectionState]:
        """Get node state."""
        node = self._nodes.get(node_id)
        return node.state if node else None


class LeaderElectionAction(BaseAction):
    """Action for leader election operations."""

    def __init__(self):
        super().__init__("leader_election")
        self._elections: Dict[str, LeaderElection] = {}

    def execute(self, params: Dict) -> ActionResult:
        """Execute leader election action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "join":
                return self._join(params)
            elif operation == "leave":
                return self._leave(params)
            elif operation == "heartbeat":
                return self._heartbeat(params)
            elif operation == "leader":
                return self._leader(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create election."""
        cluster_id = params.get("cluster_id", "")
        election = LeaderElection(
            cluster_id=cluster_id,
            heartbeat_timeout=params.get("heartbeat_timeout", 30.0)
        )
        self._elections[cluster_id] = election
        return ActionResult(success=True, data={"cluster_id": cluster_id})

    def _join(self, params: Dict) -> ActionResult:
        """Join cluster."""
        cluster_id = params.get("cluster_id", "")
        node_id = params.get("node_id", "")
        election = self._elections.get(cluster_id)
        if not election:
            return ActionResult(success=False, message="Cluster not found")

        state = election.join(node_id, params.get("name", node_id))
        return ActionResult(success=True, data={"state": state.value})

    def _leave(self, params: Dict) -> ActionResult:
        """Leave cluster."""
        cluster_id = params.get("cluster_id", "")
        node_id = params.get("node_id", "")
        election = self._elections.get(cluster_id)
        if election:
            election.leave(node_id)
        return ActionResult(success=True)

    def _heartbeat(self, params: Dict) -> ActionResult:
        """Send heartbeat."""
        cluster_id = params.get("cluster_id", "")
        node_id = params.get("node_id", "")
        election = self._elections.get(cluster_id)
        if not election:
            return ActionResult(success=False, message="Cluster not found")

        leader_id = election.heartbeat(node_id)
        return ActionResult(success=True, data={"leader_id": leader_id})

    def _leader(self, params: Dict) -> ActionResult:
        """Get leader."""
        cluster_id = params.get("cluster_id", "")
        election = self._elections.get(cluster_id)
        if not election:
            return ActionResult(success=False, message="Cluster not found")

        leader = election.get_leader()
        if not leader:
            return ActionResult(success=True, data={"leader": None})

        return ActionResult(success=True, data={
            "leader": {
                "node_id": leader.node_id,
                "name": leader.name
            }
        })
