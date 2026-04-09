"""
Data sampler action for statistical sampling and data reduction.

Provides random, stratified, and reservoir sampling algorithms.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import random
import heapq
import itertools


class DataSamplerAction:
    """Statistical data sampling with multiple algorithms."""

    def __init__(self, random_seed: Optional[int] = None) -> None:
        """
        Initialize data sampler.

        Args:
            random_seed: Random seed for reproducibility
        """
        if random_seed is not None:
            random.seed(random_seed)
        self._reservoir_state: Dict[str, List[Any]] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute sampling operation.

        Args:
            params: Dictionary containing:
                - operation: 'random', 'stratified', 'reservoir', 'systematic'
                - data: Data to sample from
                - sample_size: Number of samples
                - population: Population for sampling
                - strata: Strata definitions (for stratified)

        Returns:
            Dictionary with sampling result
        """
        operation = params.get("operation", "random")

        if operation == "random":
            return self._random_sample(params)
        elif operation == "stratified":
            return self._stratified_sample(params)
        elif operation == "reservoir":
            return self._reservoir_sample(params)
        elif operation == "systematic":
            return self._systematic_sample(params)
        elif operation == "cluster":
            return self._cluster_sample(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _random_sample(self, params: dict[str, Any]) -> dict[str, Any]:
        """Simple random sampling without replacement."""
        population = params.get("population", [])
        sample_size = params.get("sample_size", 10)
        with_replacement = params.get("with_replacement", False)

        if not population:
            return {"success": False, "error": "Population is required"}

        if sample_size > len(population) and not with_replacement:
            sample_size = len(population)

        if with_replacement:
            sample = [random.choice(population) for _ in range(sample_size)]
        else:
            sample = random.sample(population, sample_size)

        return {
            "success": True,
            "sample_size": len(sample),
            "population_size": len(population),
            "sample": sample,
            "method": "random",
        }

    def _stratified_sample(self, params: dict[str, Any]) -> dict[str, Any]:
        """Stratified sampling with proportional allocation."""
        population = params.get("population", [])
        strata = params.get("strata", {})
        sample_size = params.get("sample_size", 10)
        proportional = params.get("proportional", True)

        if not population or not strata:
            return {"success": False, "error": "Population and strata are required"}

        stratified_samples = {}
        total_allocated = 0

        for stratum_name, stratum_data in strata.items():
            stratum_size = len(stratum_data)
            if proportional:
                allocation = int((stratum_size / len(population)) * sample_size)
            else:
                allocation = stratum_data.get("allocation", sample_size // len(strata))

            allocation = min(allocation, stratum_size)
            samples = random.sample(stratum_data, allocation)

            stratified_samples[stratum_name] = {
                "samples": samples,
                "size": allocation,
                "stratum_size": stratum_size,
            }
            total_allocated += allocation

        remaining = sample_size - total_allocated
        if remaining > 0 and proportional:
            largest_stratum = max(strata.keys(), key=lambda k: len(strata[k]))
            additional = random.sample(strata[largest_stratum], min(remaining, len(strata[largest_stratum])))
            stratified_samples[largest_stratum]["samples"].extend(additional)
            stratified_samples[largest_stratum]["size"] += len(additional)

        all_samples = []
        for stratum in stratified_samples.values():
            all_samples.extend(stratum["samples"])

        return {
            "success": True,
            "sample_size": len(all_samples),
            "strata": stratified_samples,
            "method": "stratified",
        }

    def _reservoir_sample(self, params: dict[str, Any]) -> dict[str, Any]:
        """Reservoir sampling for large streaming data."""
        stream_id = params.get("stream_id", "default")
        sample_size = params.get("sample_size", 10)
        incoming_item = params.get("item")

        if stream_id not in self._reservoir_state:
            self._reservoir_state[stream_id] = {
                "reservoir": [],
                "count": 0,
            }

        state = self._reservoir_state[stream_id]

        if incoming_item is not None:
            state["count"] += 1
            n = state["count"]

            if len(state["reservoir"]) < sample_size:
                state["reservoir"].append(incoming_item)
            else:
                j = random.randint(1, n)
                if j <= sample_size:
                    state["reservoir"][j - 1] = incoming_item

            return {
                "success": True,
                "reservoir_size": len(state["reservoir"]),
                "stream_count": state["count"],
                "method": "reservoir",
            }

        return {
            "success": True,
            "sample": state["reservoir"],
            "reservoir_size": len(state["reservoir"]),
            "stream_count": state["count"],
            "method": "reservoir",
        }

    def _systematic_sample(self, params: dict[str, Any]) -> dict[str, Any]:
        """Systematic sampling with fixed interval."""
        population = params.get("population", [])
        sample_size = params.get("sample_size", 10)

        if not population:
            return {"success": False, "error": "Population is required"}

        population_size = len(population)
        if sample_size >= population_size:
            return {
                "success": True,
                "sample": population,
                "sample_size": population_size,
                "method": "systematic",
            }

        interval = population_size // sample_size
        start = random.randint(0, interval - 1)

        sample = [population[start + i * interval] for i in range(sample_size)]

        return {
            "success": True,
            "sample_size": len(sample),
            "interval": interval,
            "sample": sample,
            "method": "systematic",
        }

    def _cluster_sample(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cluster sampling where entire clusters are selected."""
        population = params.get("population", [])
        cluster_size = params.get("cluster_size", 10)
        num_clusters = params.get("num_clusters", 2)

        if not population:
            return {"success": False, "error": "Population is required"}

        num_items = len(population)
        num_possible_clusters = num_items // cluster_size

        if num_possible_clusters < num_clusters:
            num_clusters = num_possible_clusters

        cluster_indices = random.sample(range(num_possible_clusters), num_clusters)

        sample = []
        for cluster_idx in cluster_indices:
            start = cluster_idx * cluster_size
            end = min(start + cluster_size, num_items)
            sample.extend(population[start:end])

        return {
            "success": True,
            "sample_size": len(sample),
            "clusters_selected": num_clusters,
            "cluster_size": cluster_size,
            "sample": sample,
            "method": "cluster",
        }
