import pytest
from fennel._vendor.pydantic import ValidationError  # type: ignore

from fennel.lib.includes import EnvSelector


def test_env_selector():
    with pytest.raises(ValidationError):
        EnvSelector(envs="")

    with pytest.raises(ValidationError):
        EnvSelector(envs="a b")

    with pytest.raises(ValidationError):
        EnvSelector(envs=["~gold", "silver"])

    try:
        EnvSelector(envs=["~gold", "~silver"])
    except ValidationError:
        pytest.fail("ValidationError should not be raised")

    try:
        EnvSelector(envs=["gold", "silver"])
    except ValidationError:
        pytest.fail("ValidationError should not be raised")

    try:
        EnvSelector(envs=None)
    except ValidationError:
        pytest.fail("ValidationError should not be raised")


def test_is_entity_selected():
    selector = EnvSelector(envs=None)
    assert selector.is_entity_selected("gold") is True

    selector = EnvSelector(envs="~gold")
    assert selector.is_entity_selected("silver") is True
    with pytest.raises(ValueError):
        selector.is_entity_selected("~silver")

    selector = EnvSelector(envs="gold")
    assert selector.is_entity_selected("silver") is False

    selector = EnvSelector(envs=["gold", "silver"])
    assert selector.is_entity_selected("gold") is True

    selector = EnvSelector(envs=["~gold", "~silver"])
    assert selector.is_entity_selected("gold") is False
    assert selector.is_entity_selected("bronze") is True
