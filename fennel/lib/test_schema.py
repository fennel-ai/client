import pytest

from fennel.errors import SchemaException
from fennel.lib import Field, Schema
from fennel.lib.schema import Array, Bool, F_Double, Int, Map, String


def validate_schema(s):
    errors = s.validate()
    if errors:
        raise SchemaException(errors)


def test_ValidSchema():
    Schema(
        Field("actor_id", dtype=Int, default=1),
        Field("target_id", dtype=F_Double(), default=2.0),
        Field("action_type", dtype=String, default="wassup"),
        Field("bool_field", dtype=Bool, default=True),
    )
    Schema(
        Field("actor_id", dtype=Array(Int), default=[2, 3, 4, 5]),
        Field("target_id", dtype=Array(String), default=["a", "b", "c"]),
        Field(
            "action_type",
            dtype=Array(Map(Int, String)),
            default=[{1: "a", 2: "b"}, {3: "c"}],
        ),
        Field(
            "complex_map",
            dtype=Map(String, Map(String, Map(String, String))),
            default={"a": {"b": {"c": "d"}}},
        ),
        Field(
            "complex_map2",
            dtype=Map(String, Map(String, Map(String, Array(String)))),
            default={"a": {"b": {"c": ["d", "e"]}}},
        ),
    )


def test_RepeatedFields():
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Int, default=1),
                Field("actor_id", dtype=F_Double(), default=2.0),
                Field("action_type", dtype=String, default="wassup"),
                Field("bool_field", dtype=Bool, default=True),
            )
        )
    assert (
        str(e.value) == "Schema errors: [Exception('field actor_id provided "
        "multiple times')]"
    )


def test_InvalidMap():
    with pytest.raises(TypeError) as e:
        Schema(
            Field(
                "complex_map",
                dtype=Map(
                    Map(String, String), Map(String, Map(String, Array(String)))
                ),
                default={{"a": "a1"}, {"b": {"c": ["d", "e"]}}},
            ),
        )
    assert str(e.value) == "unhashable type: 'dict'"
    with pytest.raises(TypeError) as e:
        Schema(
            Field(
                "complex_map",
                dtype=Map(
                    Array(String), Map(String, Map(String, Array(String)))
                ),
                default={["a", "b", "c"], {"b": {"c": ["d", "e"]}}},
            ),
        )
    assert str(e.value) == "unhashable type: 'list'"


def test_InvalidDType():
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Array, default=1),
                Field("target_id", dtype=Int, default=2),
                Field("action_type", dtype=Int, default=3),
                Field("timestamp", dtype=Int, default=0),
            )
        )
    assert (
        str(e.value)
        == "Schema errors: [TypeError('Type for actor_id should be a "
        "Fennel Type object such as Int() and not a class such as "
        "Int/int')]"
    )
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=int, default=1),
                Field("target_id", dtype=Int, default=2),
                Field("action_type", dtype=Int, default=3),
                Field("timestamp", dtype=Int, default=0),
            )
        )
    assert (
        str(e.value)
        == "Schema errors: [TypeError('Type for actor_id should be a "
        "Fennel Type object such as Int() and not a class such as "
        "Int/int')]"
    )
    with pytest.raises(TypeError) as e:
        Schema(
            Field("actor_id", dtype=Array(), default=1),
            Field("timestamp", dtype=Int, default=0),
        )
    assert str(e.value) == "invalid array type: None"
    with pytest.raises(TypeError) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Map(), default=1),
                Field("timestamp", dtype=Int, default=0),
            )
        )
    assert str(e.value) == "invalid key type in map: None"
    with pytest.raises(TypeError) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Map(String), default=1),
                Field("timestamp", dtype=Int, default=0),
            )
        )
    assert str(e.value) == "invalid value type in map: None"
    with pytest.raises(TypeError) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Map(String, Array()), default=1),
                Field("timestamp", dtype=Int, default=0),
            )
        )
    assert str(e.value) == "invalid array type: None"


def test_InvalidDefaultValue():
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Int, default=1.0),
                Field("actor_name", dtype=Int, default="string"),
            )
        )
    assert (
        str(e.value) == "Schema errors: [TypeError('Expected default value for "
        "field actor_id to be int, got 1.0'), "
        "TypeError('Expected default value for field actor_name "
        "to be int, got string')]"
    )
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=String, default=1),
            )
        )
    assert (
        str(e.value) == "Schema errors: [TypeError('Expected default value for "
        "field actor_id to be str, got 1')]"
    )
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field("actor_id", dtype=Array(String), default=1),
            )
        )
    assert (
        str(e.value) == "Schema errors: [TypeError('Expected default value for "
        "field actor_id to be list, got 1')]"
    )
    with pytest.raises(SchemaException) as e:
        validate_schema(
            Schema(
                Field(
                    "actor_id",
                    dtype=Map(String, Array(String)),
                    default={"a": [6, 2, 3]},
                ),
            )
        )
    assert (
        str(e.value) == "Schema errors: [TypeError('Expected default value for "
        "field actor_id to be str, got 6'), "
        "TypeError('Expected default value for field actor_id to "
        "be str, got 2'), TypeError('Expected "
        "default value for field actor_id to be str, got 3')]"
    )
