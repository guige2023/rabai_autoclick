"""
Genetic algorithm and evolutionary computation utilities.

Provides genetic algorithm, crossover operators, mutation operators,
selection methods, and NSGA-II for multi-objective optimization.
"""

from __future__ import annotations

import math
import random
from typing import Any, Callable


Chromosome = list[Any]
FitnessFunc = Callable[[Chromosome], float]
Population = list[Chromosome]


def tournament_selection(
    population: Population,
    fitness_scores: list[float],
    tournament_size: int = 3,
    seed: int | None = None,
) -> Chromosome:
    """
    Tournament selection operator.

    Randomly selects tournament_size individuals and picks the fittest.
    """
    rng = random.Random(seed)
    indices = rng.sample(range(len(population)), min(tournament_size, len(population)))
    best_idx = max(indices, key=lambda i: fitness_scores[i])
    return list(population[best_idx])


def roulette_wheel_selection(
    population: Population,
    fitness_scores: list[float],
    seed: int | None = None,
) -> Chromosome:
    """Roulette wheel (fitness proportional) selection."""
    rng = random.Random(seed)
    min_f = min(fitness_scores)
    adjusted = [f - min_f + 0.001 for f in fitness_scores]
    total = sum(adjusted)
    r = rng.random() * total
    cumulative = 0.0
    for i, f in enumerate(adjusted):
        cumulative += f
        if r <= cumulative:
            return list(population[i])
    return list(population[-1])


def rank_selection(
    population: Population,
    fitness_scores: list[float],
    seed: int | None = None,
) -> Chromosome:
    """Rank-based selection."""
    rng = random.Random(seed)
    sorted_pairs = sorted(zip(fitness_scores, population), key=lambda x: x[0])
    ranks = list(range(1, len(sorted_pairs) + 1))
    total_rank = sum(ranks)
    r = rng.random() * total_rank
    cumulative = 0.0
    for rank, (_, chrom) in zip(ranks, sorted_pairs):
        cumulative += rank
        if r <= cumulative:
            return list(chrom)
    return list(sorted_pairs[-1][1])


def one_point_crossover(
    parent1: Chromosome,
    parent2: Chromosome,
    seed: int | None = None,
) -> tuple[Chromosome, Chromosome]:
    """Single-point crossover."""
    rng = random.Random(seed)
    if len(parent1) < 2:
        return list(parent1), list(parent2)
    point = rng.randint(1, len(parent1) - 1)
    child1 = parent1[:point] + parent2[point:]
    child2 = parent2[:point] + parent1[point:]
    return child1, child2


def two_point_crossover(
    parent1: Chromosome,
    parent2: Chromosome,
    seed: int | None = None,
) -> tuple[Chromosome, Chromosome]:
    """Two-point crossover."""
    rng = random.Random(seed)
    n = min(len(parent1), len(parent2))
    if n < 3:
        return one_point_crossover(parent1, parent2, seed)
    points = sorted(rng.sample(range(1, n), 2))
    p1, p2 = points
    child1 = parent1[:p1] + parent2[p1:p2] + parent1[p2:]
    child2 = parent2[:p1] + parent1[p1:p2] + parent2[p2:]
    return child1, child2


def uniform_crossover(
    parent1: Chromosome,
    parent2: Chromosome,
    seed: int | None = None,
) -> tuple[Chromosome, Chromosome]:
    """Uniform crossover (each gene randomly chosen from parents)."""
    rng = random.Random(seed)
    n = min(len(parent1), len(parent2))
    child1 = [rng.choice([parent1[i], parent2[i]]) for i in range(n)]
    child2 = [rng.choice([parent1[i], parent2[i]]) for i in range(n)]
    return child1, child2


def swapMutation(chromosome: Chromosome, rate: float = 0.1, seed: int | None = None) -> Chromosome:
    """Swap mutation: randomly swap two positions."""
    rng = random.Random(seed)
    child = list(chromosome)
    if rng.random() < rate and len(child) >= 2:
        i, j = rng.sample(range(len(child)), 2)
        child[i], child[j] = child[j], child[i]
    return child


def bitFlipMutation(chromosome: Chromosome, rate: float = 0.1, seed: int | None = None) -> Chromosome:
    """Bit flip mutation for binary chromosomes."""
    rng = random.Random(seed)
    child = list(chromosome)
    for i in range(len(child)):
        if isinstance(child[i], bool):
            if rng.random() < rate:
                child[i] = not child[i]
        elif isinstance(child[i], int):
            if rng.random() < rate:
                child[i] = 1 - child[i]
    return child


def gaussianMutation(
    chromosome: Chromosome,
    sigma: float = 0.1,
    rate: float = 0.1,
    seed: int | None = None,
) -> Chromosome:
    """Gaussian mutation for real-valued chromosomes."""
    rng = random.Random(seed)
    child = list(chromosome)
    for i in range(len(child)):
        if isinstance(child[i], (int, float)):
            if rng.random() < rate:
                child[i] = child[i] + rng.gauss(0, sigma)
    return child


def polynomial_bounded_mutation(
    chromosome: Chromosome,
    eta: float = 20.0,
    bounds: tuple[float, float] = (0.0, 1.0),
    rate: float = 0.1,
    seed: int | None = None,
) -> Chromosome:
    """
    Polynomial mutation for real-encoded chromosomes.

    Args:
        chromosome: Real-valued chromosome
        eta: Distribution index (higher = smaller perturbation)
        bounds: (lower, upper) bounds
        rate: Mutation probability per gene
    """
    rng = random.Random(seed)
    child = list(chromosome)
    lo, hi = bounds
    delta1 = (child[0] - lo) / (hi - lo) if hi > lo else 0.0
    delta2 = (hi - child[0]) / (hi - lo) if hi > lo else 0.0
    for i in range(len(child)):
        if rng.random() < rate:
            rnd = rng.random()
            if rnd < 0.5:
                delta = (2 * rnd) ** (1 / (eta + 1)) - 1
                child[i] = child[i] + delta * (child[i] - lo) if hi > lo else child[i]
            else:
                delta = 1 - (2 * (1 - rnd)) ** (1 / (eta + 1))
                child[i] = child[i] + delta * (hi - child[i]) if hi > lo else child[i]
            child[i] = max(lo, min(hi, child[i]))
    return child


class GeneticAlgorithm:
    """Generic genetic algorithm solver."""

    def __init__(
        self,
        fitness_fn: FitnessFunc,
        chromosome_size: int,
        gene_space: Callable[[], Any] | list[Any] | None = None,
        population_size: int = 50,
        generations: int = 100,
        crossover_rate: float = 0.8,
        mutation_rate: float = 0.1,
        elitism: int = 2,
        selection_fn: Callable = tournament_selection,
        crossover_fn: Callable = one_point_crossover,
        mutation_fn: Callable = bitFlipMutation,
        maximize: bool = True,
        seed: int | None = None,
    ):
        self.fitness_fn = fitness_fn
        self.chromosome_size = chromosome_size
        self.gene_space = gene_space
        self.pop_size = population_size
        self.generations = generations
        self.cx_rate = crossover_rate
        self.mut_rate = mutation_rate
        self.elitism = elitism
        self.selection_fn = selection_fn
        self.crossover_fn = crossover_fn
        self.mutation_fn = mutation_fn
        self.maximize = maximize
        self.rng = random.Random(seed)

    def _random_chromosome(self) -> Chromosome:
        if callable(self.gene_space):
            return [self.gene_space() for _ in range(self.chromosome_size)]
        elif isinstance(self.gene_space, list):
            return [self.rng.choice(self.gene_space) for _ in range(self.chromosome_size)]
        else:
            return [self.rng.random() for _ in range(self.chromosome_size)]

    def _evaluate(self, population: Population) -> list[float]:
        return [self.fitness_fn(chrom) for chrom in population]

    def _select_parents(self, population: Population, fitness: list[float]) -> list[Chromosome]:
        parents = []
        for _ in range(self.pop_size):
            parents.append(self.selection_fn(population, fitness, seed=self.rng.randint(0, 2**31)))
        return parents

    def run(self) -> tuple[Chromosome, float]:
        """
        Run the genetic algorithm.

        Returns:
            Tuple of (best_chromosome, best_fitness).
        """
        # Initialize population
        population = [self._random_chromosome() for _ in range(self.pop_size)]
        best = None
        best_fitness = float("-inf") if self.maximize else float("inf")

        for gen in range(self.generations):
            fitness = self._evaluate(population)

            # Track best
            for chrom, fit in zip(population, fitness):
                if self.maximize:
                    if fit > best_fitness:
                        best_fitness = fit
                        best = list(chrom)
                else:
                    if fit < best_fitness:
                        best_fitness = fit
                        best = list(chrom)

            # Elites
            sorted_pop = sorted(zip(population, fitness), key=lambda x: x[1], reverse=self.maximize)
            elites = [list(c) for c, _ in sorted_pop[:self.elitism]]

            # Selection
            parents = self._select_parents(population, fitness)

            # Crossover and mutation
            offspring = []
            for i in range(0, self.pop_size - self.elitism, 2):
                p1 = parents[i]
                p2 = parents[i + 1] if i + 1 < len(parents) else parents[0]
                if self.rng.random() < self.cx_rate:
                    c1, c2 = self.crossover_fn(p1, p2, seed=self.rng.randint(0, 2**31))
                else:
                    c1, c2 = list(p1), list(p2)
                offspring.append(self.mutation_fn(c1, self.mut_rate, seed=self.rng.randint(0, 2**31)))
                offspring.append(self.mutation_fn(c2, self.mut_rate, seed=self.rng.randint(0, 2**31)))

            # Next generation
            population = elites + offspring[:self.pop_size - self.elitism]

        return best or [], best_fitness


def simulated_annealing(
    initial_solution: Chromosome,
    fitness_fn: FitnessFunc,
    neighbor_fn: Callable[[Chromosome], Chromosome],
    initial_temp: float = 1000.0,
    cooling_rate: float = 0.995,
    min_temp: float = 1.0,
    max_iter: int = 1000,
    maximize: bool = True,
    seed: int | None = None,
) -> tuple[Chromosome, float]:
    """
    Simulated annealing optimization.

    Args:
        initial_solution: Starting solution
        fitness_fn: Fitness function
        neighbor_fn: Function to generate neighbor from chromosome
        initial_temp: Starting temperature
        cooling_rate: Temperature multiplier per iteration
        min_temp: Stopping temperature
        max_iter: Maximum iterations
        maximize: True to maximize, False to minimize
        seed: Random seed

    Returns:
        Tuple of (best_solution, best_fitness).
    """
    rng = random.Random(seed)
    current = list(initial_solution)
    current_fitness = fitness_fn(current)
    best = list(current)
    best_fitness = current_fitness
    temp = initial_temp

    for _ in range(max_iter):
        if temp < min_temp:
            break
        neighbor = neighbor_fn(current)
        neighbor_fitness = fitness_fn(neighbor)
        delta = neighbor_fitness - current_fitness
        if maximize:
            if delta > 0 or rng.random() < math.exp(delta / temp):
                current = neighbor
                current_fitness = neighbor_fitness
                if current_fitness > best_fitness:
                    best = list(current)
                    best_fitness = current_fitness
        else:
            if delta < 0 or rng.random() < math.exp(-delta / temp):
                current = neighbor
                current_fitness = neighbor_fitness
                if current_fitness < best_fitness:
                    best = list(current)
                    best_fitness = current_fitness
        temp *= cooling_rate

    return best, best_fitness


def particle_swarm_optimization(
    fitness_fn: FitnessFunc,
    dim: int,
    n_particles: int = 30,
    max_iter: int = 100,
    w: float = 0.729,
    c1: float = 1.494,
    c2: float = 1.494,
    bounds: tuple[float, float] = (-100.0, 100.0),
    seed: int | None = None,
) -> tuple[list[float], float]:
    """
    Particle Swarm Optimization (PSO).

    Args:
        fitness_fn: Fitness function
        dim: Problem dimension
        n_particles: Number of particles
        max_iter: Maximum iterations
        w: Inertia weight
        c1: Cognitive coefficient
        c2: Social coefficient
        bounds: (min, max) for particle positions
        seed: Random seed

    Returns:
        Tuple of (best_position, best_fitness).
    """
    rng = random.Random(seed)
    lo, hi = bounds

    # Initialize
    positions = [[lo + rng.random() * (hi - lo) for _ in range(dim)] for _ in range(n_particles)]
    velocities = [[0.0] * dim for _ in range(n_particles)]
    personal_best = [list(p) for p in positions]
    personal_best_fitness = [fitness_fn(p) for p in positions]
    global_best = personal_best[max(range(n_particles), key=lambda i: personal_best_fitness[i])]
    global_best_fitness = max(personal_best_fitness)

    for _ in range(max_iter):
        for i in range(n_particles):
            for d in range(dim):
                r1 = rng.random()
                r2 = rng.random()
                velocities[i][d] = (
                    w * velocities[i][d]
                    + c1 * r1 * (personal_best[i][d] - positions[i][d])
                    + c2 * r2 * (global_best[d] - positions[i][d])
                )
                positions[i][d] = max(lo, min(hi, positions[i][d] + velocities[i][d]))

            fitness = fitness_fn(positions[i])
            if fitness > personal_best_fitness[i]:
                personal_best[i] = list(positions[i])
                personal_best_fitness[i] = fitness
                if fitness > global_best_fitness:
                    global_best = list(positions[i])
                    global_best_fitness = fitness

    return global_best, global_best_fitness
