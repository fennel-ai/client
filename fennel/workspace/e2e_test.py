class Test:
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


"""
    Unit
        # - stream's transform function for a populator works -- no mocks
        # - preaggrgate works (df -> df) -- this needs mock for aggregates
        # - df -> aggregated data (talk to server, mock)
        - everything works in env specified
        # - feature computation works given input streams (mock of aggregates/features)
    
    Integration
        - e2e == read from stream, e2e
        - e2e == artificial stream data, e2e aggregate
        - e2e == read from stream, populate aggregates, read features
"""
