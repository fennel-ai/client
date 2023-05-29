from collections import defaultdict
from typing import Dict, List

from fennel.featuresets import Extractor
from fennel.lib.graph_algorithms.utils import extractor_graph


def is_cyclic_util(
    v: str,
    visited: Dict[str, bool],
    rec_stack: Dict[str, bool],
    graph: Dict[str, List[str]],
) -> bool:
    # Mark current node as visited and add to recursion stack
    visited[v] = True
    rec_stack[v] = True

    # Recur for all neighbours if any neighbour is visited and in
    # rec_stack then graph is cyclic
    for neighbour in graph[v]:
        if not visited[neighbour]:
            if is_cyclic_util(neighbour, visited, rec_stack, graph):
                raise ValueError(f"Cyclic dependency found for {v}")
        elif rec_stack[neighbour]:
            raise ValueError(f"Cyclic dependency found for {v}")

    rec_stack[v] = False
    return False


def is_cyclic(graph: Dict[str, List[str]]) -> bool:
    visited: Dict[str, bool] = defaultdict(bool)
    rec_stack: Dict[str, bool] = defaultdict(bool)
    vertices = list(graph.keys())
    for vertex in vertices:
        if not visited[vertex]:
            if is_cyclic_util(vertex, visited, rec_stack, graph):
                raise ValueError(
                    f"Cycle detected in extractor graph with extractor"
                    f" {vertex}"
                )
    return False


def is_extractor_graph_cyclic(extractors: List[Extractor]) -> bool:
    graph, _ = extractor_graph(extractors)
    return is_cyclic(graph)
