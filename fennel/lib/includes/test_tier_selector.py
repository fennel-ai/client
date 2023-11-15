import pytest
from fennel._vendor.pydantic import ValidationError  # type: ignore

from fennel.lib.includes import TierSelector


def test_tier_selector():
    with pytest.raises(ValidationError):
        TierSelector(tiers="")

    with pytest.raises(ValidationError):
        TierSelector(tiers="a b")

    with pytest.raises(ValidationError):
        TierSelector(tiers=["~gold", "silver"])

    try:
        TierSelector(tiers=["~gold", "~silver"])
    except ValidationError:
        pytest.fail("ValidationError should not be raised")

    try:
        TierSelector(tiers=["gold", "silver"])
    except ValidationError:
        pytest.fail("ValidationError should not be raised")

    try:
        TierSelector(tiers=None)
    except ValidationError:
        pytest.fail("ValidationError should not be raised")


def test_is_entity_selected():
    selector = TierSelector(tiers=None)
    assert selector.is_entity_selected("gold") is True

    selector = TierSelector(tiers="~gold")
    assert selector.is_entity_selected("silver") is True
    with pytest.raises(ValueError):
        selector.is_entity_selected("~silver")

    selector = TierSelector(tiers="gold")
    assert selector.is_entity_selected("silver") is False

    selector = TierSelector(tiers=["gold", "silver"])
    assert selector.is_entity_selected("gold") is True

    selector = TierSelector(tiers=["~gold", "~silver"])
    assert selector.is_entity_selected("gold") is False
    assert selector.is_entity_selected("bronze") is True
