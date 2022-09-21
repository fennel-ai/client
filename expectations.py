import typing


# TODO(aditya): can we just use a wrapper over Great expectations?
# TODO: do we need 'mostly' arg? If yes, should this be kwarg for readability?
class Expectation:
    def __int__(self, type_: str, value: typing.Any, mostly:float):
        if mostly < 0 or mostly > 1.0:
            raise Exception('invalid expectation: mostly not between [0,1]')
        self.type_ = type_
        self.value = value
        self.mostly = mostly


def expect_values_between(low: typing.Any, high: typing.Any, mostly:float) -> Expectation:
    return Expectation('values_between', [low, high], mostly=mostly)


def expect_values_equal_to(expected: typing.Any, mostly: float) -> Expectation:
    return Expectation('values_equal_to', expected, mostly=mostly)


def expect_values_positive(mostly: float) -> Expectation:
    return Expectation('vaues_positive', None, mostly)