from collections import defaultdict
from typing import cast, List, Union, Dict, Set, Tuple

from fennel.featuresets import Extractor, Featureset, Feature
from fennel.internal_lib.ascii_visualizer import draw_graph
from fennel.internal_lib.graph_algorithms.utils import extractor_graph


def _get_features(feature: Union[Feature, Featureset, str]) -> set:
    if isinstance(feature, Feature):
        return {feature.fqn()}
    elif isinstance(feature, Featureset):
        return {f.fqn() for f in feature.features}
    elif isinstance(feature, str):
        return {feature}
    elif type(feature) is tuple:
        return {f.fqn() for f in feature}
    else:
        raise ValueError(
            f"Unknown type for feature/featureset {feature} of"
            f"type {type(feature)}"
        )


def _topological_sort_util(
    extractor_name: str,
    visited: Dict[str, bool],
    stack: List[str],
    graph: Dict[str, List[str]],
):
    visited[extractor_name] = True
    for extractor_neighbour in graph[extractor_name]:
        if not visited[extractor_neighbour]:
            _topological_sort_util(extractor_neighbour, visited, stack, graph)

    stack.insert(0, extractor_name)


def _topological_sort(
    extractors: List[Extractor],
) -> Tuple[List[str], Dict[str, Extractor]]:
    """
    Topologically sort the extractors.

    :param extractors: List of extractors
    :return: Tuple of (topically sorted extractors, map of output
    feature to the extractor that produces it)
    """
    visited: Dict[str, bool] = defaultdict(bool)
    stack: List[str] = []
    graph, feature_to_extractor_map = extractor_graph(extractors)
    for extractor in extractors:
        if not visited[extractor.fqn()]:
            _topological_sort_util(extractor.fqn(), visited, stack, graph)

    return stack, feature_to_extractor_map


def get_vertices_and_eges(
    extractors: List[Extractor],
) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    """
    Get the vertices and edges from the graph.

    :param graph: Graph
    :return: Tuple of (vertices, edges)
    """

    def get_extractor_vertex(extractor: Extractor) -> str:
        featureset_name = extractor.fqn().split(".")[0]
        function_name = extractor.fqn().split(".")[1]
        caps_only = "".join([c for c in featureset_name if c.isupper()])
        return f"{caps_only}.{function_name}"

    def get_feature_vertex(f: Union[Feature, Featureset, str]) -> str:
        """Returns name of the vertex corresponding to the feature in the extractor DAG."""
        if isinstance(f, Feature):
            featureset = f.featureset_name
            caps_only = "".join([c for c in featureset if c.isupper()])
            return f"F({caps_only}.{f.name})"
        elif isinstance(f, Featureset):
            raise ValueError(
                "Featureset is not supported as an input to an extractor"
            )
        elif type(f) is tuple:
            return "DF(" + ",".join([get_feature_vertex(f) for f in f]) + ")"
        elif isinstance(f, str):
            featureset = f.split(".")[0]
            caps_only = "".join([c for c in featureset if c.isupper()])
            feature_name = f.split(".")[1]
            return f"F({caps_only}.{feature_name})"
        raise ValueError(f"Unknown type {type(f)}")

    vertices = set()
    edges = set()
    for extractor in extractors:
        vertices.add(get_extractor_vertex(extractor))
        for inp in extractor.inputs:
            vertices.add(get_feature_vertex(inp))
            edges.add(
                (get_extractor_vertex(extractor), get_feature_vertex(inp))
            )
        for output in extractor.fqn_output_features():
            vertices.add(get_feature_vertex(output))
            edges.add(
                (get_feature_vertex(output), get_extractor_vertex(extractor))
            )
    if len(vertices) == 0:
        raise ValueError("No extractors found.")

    return vertices, edges


def get_extractor_order(
    input_features: List[Union[Feature, str]],
    output_features: List[Union[Feature, Featureset, str]],
    extractors: List[Extractor],
) -> List[Extractor]:
    """
    Given a list of input features and output features find the most optimal
    order to run the extractors.
    :param input_features: List of features provided by user
    :param output_features: List of features that need to be extracted
    :param extractors: List of extractors available
    :return: List of extractor functions in order
    """
    sorted_extractors, feature_to_extractor_map = _topological_sort(extractors)
    resolved_features = set()
    for f in input_features:
        resolved_features.update(_get_features(f))
    to_find: Set[str] = set()
    for f in output_features:  # type: ignore
        to_find.update(_get_features(f))
    # Find the extractors that need to be run to produce the output features.
    extractor_names: Set[str] = set()
    # Run a BFS from resolved_features to to_find features.
    while len(to_find) > 0:
        next_to_find = set()
        for feature in to_find:
            if feature in resolved_features:
                continue
            # Find an extractor for this feature
            if feature not in feature_to_extractor_map:
                # Draw DAG
                vertices, edges = get_vertices_and_eges(extractors)
                draw_graph(vertices, edges)
                raise ValueError(f"No extractor found for feature `{feature}`.")
            extractor = feature_to_extractor_map[feature]

            if extractor.fqn() in extractor_names:
                continue

            extractor_names.add(extractor.fqn())
            for output in extractor.fqn_output_features():
                resolved_features.add(output)
            for inp in extractor.inputs:
                if isinstance(inp, Feature):
                    if inp.fqn() not in resolved_features:
                        next_to_find.add(inp.fqn())
                elif isinstance(inp, Featureset):
                    for f in inp.features:
                        f = cast(Feature, f)
                        if f.fqn() not in resolved_features:
                            next_to_find.add(f.fqn())
                elif isinstance(inp, tuple):
                    for f in inp:
                        f = cast(Feature, f)
                        if f.fqn() not in resolved_features:
                            next_to_find.add(f.fqn())
        to_find = next_to_find

    ret: List[Extractor] = []
    extractor_map = {e.fqn(): e for e in extractors}
    for extractor_name in sorted_extractors:
        if extractor_name in extractor_names:
            ret.append(extractor_map[extractor_name])
    return ret
