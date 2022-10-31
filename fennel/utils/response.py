from fennel.gen.status_pb2 import Status


def check_response(response: Status):
    """Check the response from the server and raise an exception if the response is not OK"""
    if response.code != 0:
        raise Exception(response.message)
