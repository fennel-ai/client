from collections import defaultdict
from typing import List, Dict, Tuple

from fennel.featuresets import Extractor


def extractor_graph(
    extractors: List[Extractor],
) -> Tuple[Dict[str, List[str]], Dict[str, Extractor]]:
    feature_to_extractor_map = {}
    for extractor in extractors:
        for output in extractor.output_features:
            feature_to_extractor_map[output] = extractor

    graph: Dict[str, List[str]] = defaultdict(list)

    # Create a graph, using adjacency list representation
    for extractor in extractors:
        for inp in extractor.inputs:
            # If the given input feature doesn't have an extractor, then
            # it is a user-resolved feature.
            if str(inp) not in feature_to_extractor_map:
                continue
            extractor_producer = feature_to_extractor_map[str(inp)]
            graph[extractor_producer.name].append(extractor.name)

    return graph, feature_to_extractor_map
