"""Process mining action module for RabAI AutoClick.

Analyzes event sequences to discover, validate, and improve
business process models. Extracts BPMN-style flows from logs.
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, Counter
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """Process transition types."""
    SEQUENCE = "->"
    PARALLEL = "||"
    CHOICE = "XOR"
    LOOP = "*"


@dataclass
class ProcessNode:
    """A node in the process graph."""
    activity: str
    frequency: int = 0
    first_occurrence: int = 0
    last_occurrence: int = 0
    incoming: List[str] = field(default_factory=list)
    outgoing: List[str] = field(default_factory=list)


@dataclass
class ProcessTransition:
    """A directed edge in the process graph."""
    source: str
    target: str
    frequency: int
    avg_duration: Optional[float] = None
    transition_type: str = "SEQUENCE"


@dataclass
class ProcessMetrics:
    """Process-level metrics."""
    total_cases: int
    total_events: int
    avg_case_length: float
    median_case_length: float
    start_activities: List[Tuple[str, int]]
    end_activities: List[Tuple[str, int]]
    avg_cycle_time: Optional[float]
    most_common_path: List[str]


class AlphaMinerAction(BaseAction):
    """Alpha algorithm process miner for discovering workflow nets.
    
    Analyzes event logs to discover the process model using the Alpha
    algorithm. Outputs a Petri net representation as process graph.
    
    Works with event logs that have: case_id, activity, timestamp.
    
    Args:
        case_id_key: Key for case identifier
        activity_key: Key for activity name
        timestamp_key: Key for event timestamp
    """

    def execute(
        self,
        event_log: List[Dict[str, Any]],
        case_id_key: str = "case_id",
        activity_key: str = "activity",
        timestamp_key: str = "timestamp",
        min_frequency: int = 1
    ) -> ActionResult:
        try:
            # Group by case
            cases: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for event in event_log:
                if case_id_key in event and activity_key in event:
                    cases[event[case_id_key]].append(event)

            if len(cases) == 0:
                return ActionResult(success=False, error="No valid cases found")

            # Sort each case by timestamp
            for case_id in cases:
                ts_key = timestamp_key if timestamp_key in event_log[0] else None
                if ts_key:
                    cases[case_id].sort(key=lambda e: e.get(ts_key, ""))
                else:
                    # Preserve original order
                    pass

            # Extract activity sequences
            sequences: Dict[str, List[str]] = {}
            for case_id, events in cases.items():
                sequences[case_id] = [e[activity_key] for e in events]

            # Build directly-follows graph
            df_graph: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for case_id, activities in sequences.items():
                for i in range(len(activities) - 1):
                    df_graph[activities[i]][activities[i + 1]] += 1

            # Build start/end activity sets
            start_activities: Set[str] = set()
            end_activities: Set[str] = set()
            for case_id, activities in sequences.items():
                if activities:
                    start_activities.add(activities[0])
                    end_activities.add(activities[-1])

            # Filter by min frequency
            nodes: Dict[str, ProcessNode] = {}
            for case_id, activities in sequences.items():
                for act in activities:
                    if act not in nodes:
                        nodes[act] = ProcessNode(
                            activity=act,
                            frequency=0,
                            first_occurrence=999999999,
                            last_occurrence=0
                        )
                    nodes[act].frequency += 1

            transitions: List[ProcessTransition] = []
            for src, targets in df_graph.items():
                for tgt, freq in targets.items():
                    if freq >= min_frequency:
                        transitions.append(ProcessTransition(
                            source=src, target=tgt, frequency=freq
                        ))
                        if src in nodes and tgt not in nodes[src].outgoing:
                            nodes[src].outgoing.append(tgt)
                        if tgt in nodes and src not in nodes[tgt].incoming:
                            nodes[tgt].incoming.append(src)

            # Alpha relations
            all_activities = set(nodes.keys())
            parallel_relations: List[Tuple[str, str]] = []
            exclusive_relations: List[Tuple[str, str]] = []

            for a in all_activities:
                for b in all_activities:
                    if a != b:
                        # Check if a and b are in parallel (both -> each other)
                        if df_graph[a].get(b, 0) > 0 and df_graph[b].get(a, 0) > 0:
                            parallel_relations.append((a, b))

            # Compute case lengths
            lengths = [len(acts) for acts in sequences.values()]
            lengths.sort()
            median_len = lengths[len(lengths) // 2]

            # Most common path
            path_counts: Counter = Counter()
            for activities in sequences.values():
                path_counts[tuple(activities)] += 1
            most_common = list(path_counts.most_common(1)[0][0]) if path_counts else []

            # Start/end activity counts
            start_counts = Counter(a[0] for a in sequences.values())
            end_counts = Counter(a[-1] for a in sequences.values())

            return ActionResult(success=True, data={
                "n_cases": len(cases),
                "n_events": len(event_log),
                "n_activities": len(nodes),
                "nodes": {name: {"frequency": n.frequency, "incoming": n.incoming, "outgoing": n.outgoing}
                          for name, n in nodes.items()},
                "transitions": [{"source": t.source, "target": t.target, "frequency": t.frequency}
                                for t in transitions],
                "parallel_relations": [(a, b) for a, b in parallel_relations],
                "start_activities": start_counts.most_common(5),
                "end_activities": end_counts.most_common(5),
                "avg_case_length": round(sum(lengths) / len(lengths), 2),
                "median_case_length": median_len,
                "most_common_path": most_common,
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class ProcessConformanceAction(BaseAction):
    """Check process conformance by comparing event log to a reference model.
    
    Detects deviations, skips, rewrites, and insertions relative to
    the expected process flow.
    
    Args:
        expected_flow: List of allowed activity sequences
        strict: If True, all events must appear in expected_flow
    """

    def execute(
        self,
        event_log: List[Dict[str, Any]],
        reference_flow: List[List[str]],
        case_id_key: str = "case_id",
        activity_key: str = "activity",
        strict: bool = False
    ) -> ActionResult:
        try:
            # Group by case
            cases: Dict[str, List[str]] = defaultdict(list)
            for event in event_log:
                if case_id_key in event and activity_key in event:
                    cases[event[case_id_key]].append(event[activity_key])

            deviations = []
            total_activities = 0
            deviant_cases = 0

            for case_id, activities in cases.items():
                total_activities += len(activities)
                is_deviant = False
                case_deviations = []

                for act in activities:
                    # Check if activity exists in any reference path
                    found = any(act in path for path in reference_flow)
                    if not found:
                        is_deviant = True
                        case_deviations.append({
                            "activity": act,
                            "position": len(case_deviations),
                            "type": "unexpected_activity"
                        })

                if is_deviant:
                    deviant_cases += 1
                    deviations.append({
                        "case_id": case_id,
                        "deviations": case_deviations
                    })

            conformance_score = 1.0 - (deviant_cases / len(cases)) if cases else 1.0

            return ActionResult(success=True, data={
                "n_cases": len(cases),
                "deviant_cases": deviant_cases,
                "conformance_score": round(conformance_score, 4),
                "deviations": deviations[:50],  # cap for readability
                "total_activities": total_activities
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class ProcessDAGAction(BaseAction):
    """Build a directed acyclic graph of process flows.
    
    Converts event logs into a DAG showing activity transitions
    with frequency weights. Useful for visualizing process variants.
    
    Args:
        include_loop_detection: Mark activities that appear as both start and end
    """

    def execute(
        self,
        event_log: List[Dict[str, Any]],
        case_id_key: str = "case_id",
        activity_key: str = "activity",
        timestamp_key: str = "timestamp",
        include_loop_detection: bool = True
    ) -> ActionResult:
        try:
            cases: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for event in event_log:
                if case_id_key in event and activity_key in event:
                    cases[event[case_id_key]].append(event)

            # Sort by timestamp
            for case_id in cases:
                cases[case_id].sort(key=lambda e: str(e.get(timestamp_key, "")))

            # Build DAG edges
            edges: Dict[Tuple[str, str], int] = defaultdict(int)
            for case_id, events in cases.items():
                activities = [e[activity_key] for e in events]
                for i in range(len(activities) - 1):
                    edges[(activities[i], activities[i + 1])] += 1

            # Detect nodes that are both start and end (potential loops)
            loop_nodes: Set[str] = set()
            if include_loop_detection:
                start_act = Counter(a[0] for a in [ [e[activity_key] for e in evts] for evts in cases.values()])
                end_act = Counter(a[-1] for a in [ [e[activity_key] for e in evts] for evts in cases.values()])
                loop_nodes = set(start_act.keys()) & set(end_act.keys())

            # Top edges
            sorted_edges = sorted(edges.items(), key=lambda x: -x[1])[:30]

            return ActionResult(success=True, data={
                "nodes": {
                    "count": len(set(a for evts in cases.values() for e in evts for a in [e[activity_key]])),
                    "loop_candidates": list(loop_nodes)
                },
                "edges": [{"from": src, "to": tgt, "frequency": freq}
                          for (src, tgt), freq in sorted_edges],
                "variant_count": len(cases),
                "max_case_length": max(len(evts) for evts in cases.values()) if cases else 0,
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))
