class FennelConcept:
    """
    Datasets, Pipes, FeatureSets and Extractors, Views are all FennelConcepts
    and need to implement the following methods:
    """
    name: str

    def __init__(self, name: str):
        self.name = name

    def signature(self):
        raise NotImplementedError

    def to_proto(self):
        raise NotImplementedError
