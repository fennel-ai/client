from collections import defaultdict
from typing import List, Dict, Tuple

from fennel.featuresets import Extractor, Feature, Featureset


def extractor_graph(
    extractors: List[Extractor],
) -> Tuple[Dict[str, List[str]], Dict[str, Extractor]]:
    """
    Create a graph of the extractors. It is a directed graph from an
    extractor that can produce a feature to the extractor that depends on it.
    """
    feature_to_extractor_map = {}
    for extractor in extractors:
        for output in extractor.fqn_output_features():
            feature_to_extractor_map[output] = extractor
    graph: Dict[str, List[str]] = defaultdict(list)

    # Create a graph, using adjacency list representation
    for extractor in extractors:
        for inp in extractor.inputs:
            if isinstance(inp, Feature):
                # If the given input feature doesn't have an extractor, then
                # it is a user-resolved feature.
                if inp.fqn() not in feature_to_extractor_map:
                    continue
                extractor_producer = feature_to_extractor_map[inp.fqn()]
                if extractor.fqn() not in graph[extractor_producer.fqn()]:
                    graph[extractor_producer.fqn()].append(extractor.fqn())
            elif type(inp) is tuple:
                for feature in inp:
                    if feature.fqn() not in feature_to_extractor_map:
                        continue
                    extractor_producer = feature_to_extractor_map[feature.fqn()]
                    if extractor.fqn() not in graph[extractor_producer.fqn()]:
                        graph[extractor_producer.fqn()].append(extractor.fqn())
            elif isinstance(inp, Featureset):
                raise ValueError(
                    "Featureset is not supported as an input to an extractor"
                )

    return graph, feature_to_extractor_map
