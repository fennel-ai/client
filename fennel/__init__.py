import sys

sys.setrecursionlimit(3000)

import fennel.client
import fennel.sources
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.aggregate import Count, Sum, Max, Min, Average, TopK, CF
from fennel.lib.metadata import meta
from fennel.lib.window import Window
