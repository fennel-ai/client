from fennel.utils import fhash, to_columnar_json


def test_fhash_Callable():
    def f(x: int, y: int) -> int:
        x = x + 1
        return x + y

    hash_code = "baae07d4aa0291b3ba2758f66817133c"
    assert fhash(f) == hash_code

    def f(x: int, y: int) -> int:
        x = x + 1
        # this is a comment
        return x + y

    assert fhash(f) == hash_code

    def f(x: int, y: int) -> int:
        x = x + 1

        # this is a comment
        return x + y

    assert fhash(f) == hash_code

    def f(x: int, y: int) -> int:
        x = x + 1

        # this is a comment

        return x + y

    assert fhash(f) == hash_code


def test_to_columnar_json():
    raise NotImplementedError("TODO zaki do this test")