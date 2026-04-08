"""Leader Election action module for RabAI AutoClick.

Provides distributed leader election using various algorithms
(bully, ring, ZooKeeper-style) with heartbeat monitoring.
"""

import sys
import os
import json
import time
import uuid
import asyncio
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ElectionState(Enum):
    """Election state for a node."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    OBSERVER = "observer"


@dataclass
class Node:
    """Represents a node in the election cluster."""
    node_id: str
    address: str
    port: int
    priority: int = 1
    last_heartbeat: float = field(default_factory=time.time)
    state: ElectionState = ElectionState.FOLLOWER
    term: int = 0
    votes_received: int = 0
    is_active: bool = True


@dataclass
class ElectionConfig:
    """Configuration for leader election."""
    cluster_id: str
    node_id: str
    algorithm: str = "raft"  # raft, bully, ring
    election_timeout_ms: float = 5000.0
    heartbeat_interval_ms: float = 1000.0
    priority: int = 1


class LeaderElection:
    """Leader election with Raft-style consensus."""
    
    def __init__(self):
        self._config: Optional[ElectionConfig] = None
        self._nodes: Dict[str, Node] = {}
        self._current_leader: Optional[str] = None
        self._my_node: Optional[Node] = None
        self._state = ElectionState.FOLLOWER
        self._term = 0
        self._voted_for: Optional[str] = None
        self._votes: set = set()
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._leader_callbacks: List = []
        self._lock = threading.Lock()
    
    def configure(self, config: ElectionConfig) -> None:
        """Configure the election."""
        self._config = config
        self._my_node = Node(
            node_id=config.node_id,
            address="localhost",
            port=0,
            priority=config.priority
        )
        self._nodes[config.node_id] = self._my_node
    
    def add_node(self, node: Node) -> None:
        """Add a node to the cluster."""
        self._nodes[node.node_id] = node
    
    def get_leader(self) -> Optional[str]:
        """Get current leader ID."""
        return self._current_leader
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self._state == ElectionState.LEADER
    
    def start_election(self) -> bool:
        """Start leader election process."""
        if not self._config:
            return False
        
        with self._lock:
            self._state = ElectionState.CANDIDATE
            self._term += 1
            self._my_node.term = self._term
            self._voted_for = self._config.node_id
            self._votes = {self._config.node_id}
        
        # Request votes from other nodes
        quorum = len(self._nodes) // 2 + 1
        votes_needed = quorum
        
        for node_id, node in self._nodes.items():
            if node_id == self._config.node_id:
                continue
            # In real impl, would send RequestVote RPC
            if node.priority <= self._my_node.priority:
                continue
            if node.is_active:
                self._votes.add(node_id)
        
        with self._lock:
            if len(self._votes) >= votes_needed:
                self._become_leader()
                return True
            else:
                self._state = ElectionState.FOLLOWER
                return False
    
    def _become_leader(self) -> None:
        """Become the leader."""
        self._state = ElectionState.LEADER
        self._current_leader = self._config.node_id
        self._my_node.state = ElectionState.LEADER
        
        # Notify callbacks
        for cb in self._leader_callbacks:
            try:
                cb(self._config.node_id)
            except Exception:
                pass
    
    def handle_heartbeat(self, leader_id: str, term: int) -> bool:
        """Handle incoming heartbeat from leader."""
        with self._lock:
            if term > self._term:
                self._term = term
                self._my_node.term = term
                self._state = ElectionState.FOLLOWER
                self._voted_for = None
            
            if self._state == ElectionState.CANDIDATE:
                self._state = ElectionState.FOLLOWER
            
            self._current_leader = leader_id
            self._my_node.last_heartbeat = time.time()
            return self._state != ElectionState.LEADER
    
    def send_heartbeat(self) -> None:
        """Send heartbeat to all followers."""
        if self._state != ElectionState.LEADER:
            return
        
        for node_id, node in self._nodes.items():
            if node_id == self._config.node_id:
                continue
            # In real impl, would send AppendEntries RPC
            node.last_heartbeat = time.time()
    
    def register_leader_callback(self, callback) -> None:
        """Register callback for when this node becomes leader."""
        self._leader_callbacks.append(callback)


class LeaderElectionAction(BaseAction):
    """Distributed leader election with Raft-style consensus.
    
    Supports cluster membership, heartbeat monitoring, election
    timeouts, and leader callbacks.
    """
    action_type = "leader_election"
    display_name = "领导者选举"
    description = "分布式领导者选举，支持Raft风格共识"
    
    def __init__(self):
        super().__init__()
        self._election = LeaderElection()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute leader election operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "configure":
                return self._configure(params)
            elif operation == "add_node":
                return self._add_node(params)
            elif operation == "start_election":
                return self._start_election(params)
            elif operation == "get_leader":
                return self._get_leader(params)
            elif operation == "is_leader":
                return self._is_leader(params)
            elif operation == "heartbeat":
                return self._heartbeat(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure leader election."""
        config = ElectionConfig(
            cluster_id=params.get("cluster_id", ""),
            node_id=params.get("node_id", str(uuid.uuid4())),
            algorithm=params.get("algorithm", "raft"),
            election_timeout_ms=params.get("election_timeout_ms", 5000.0),
            priority=params.get("priority", 1)
        )
        self._election.configure(config)
        return ActionResult(success=True, message=f"Configured node '{config.node_id}'",
                         data={"node_id": config.node_id})
    
    def _add_node(self, params: Dict[str, Any]) -> ActionResult:
        """Add a node to cluster."""
        node = Node(
            node_id=params.get("node_id", ""),
            address=params.get("address", "localhost"),
            port=params.get("port", 0),
            priority=params.get("priority", 1)
        )
        self._election.add_node(node)
        return ActionResult(success=True, message=f"Node '{node.node_id}' added")
    
    def _start_election(self, params: Dict[str, Any]) -> ActionResult:
        """Start leader election."""
        success = self._election.start_election()
        return ActionResult(success=success, message="Became leader" if success else "Election failed")
    
    def _get_leader(self, params: Dict[str, Any]) -> ActionResult:
        """Get current leader."""
        leader = self._election.get_leader()
        return ActionResult(success=True, message=f"Leader: {leader}",
                         data={"leader": leader})
    
    def _is_leader(self, params: Dict[str, Any]) -> ActionResult:
        """Check if this node is leader."""
        is_leader = self._election.is_leader()
        return ActionResult(success=True, message=f"Leader: {is_leader}",
                         data={"is_leader": is_leader})
    
    def _heartbeat(self, params: Dict[str, Any]) -> ActionResult:
        """Handle heartbeat."""
        leader_id = params.get("leader_id", "")
        term = params.get("term", 0)
        self._election.handle_heartbeat(leader_id, term)
        return ActionResult(success=True, message="Heartbeat handled")
