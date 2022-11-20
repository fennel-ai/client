from collections import defaultdict
from typing import List, Union, Dict, Set, Tuple

from fennel.featuresets import Extractor, Featureset, Feature
from fennel.lib.graph_algorithms.utils import extractor_graph


def _get_features(feature: Union[Feature, Featureset]) -> set:
    if isinstance(feature, Feature):
        return {str(feature)}
    elif isinstance(feature, Featureset):
        return {str(f) for f in feature.features}
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
        if not visited[extractor.name]:
            _topological_sort_util(extractor.name, visited, stack, graph)

    return stack, feature_to_extractor_map


def get_extractor_order(
    input_features: List[Union[Feature, Featureset]],
    output_features: List[Union[Feature, Featureset]],
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

    to_find = set()
    for f in output_features:
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
                raise ValueError(f"No extractor found for feature {feature}")
            extractor = feature_to_extractor_map[feature]

            if extractor.name in extractor_names:
                continue

            extractor_names.add(extractor.name)
            for output in extractor.output_features:
                resolved_features.add(output)
            for inp in extractor.inputs:
                fqn = str(inp)
                if fqn not in resolved_features:
                    next_to_find.add(fqn)
        to_find = next_to_find

    ret: List[Extractor] = []
    extractor_map = {e.name: e for e in extractors}
    for extractor_name in sorted_extractors:
        if extractor_name in extractor_names:
            ret.append(extractor_map[extractor_name])
    return ret
