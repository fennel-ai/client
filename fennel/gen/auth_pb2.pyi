"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.enum_type_wrapper
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _OrgPermission:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _OrgPermissionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_OrgPermission.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    ORG_ALL: _OrgPermission.ValueType  # 0
    INVITE: _OrgPermission.ValueType  # 1
    """Can invite users to the organization."""
    CREATE_ROLE: _OrgPermission.ValueType  # 2
    """Can create roles for the organization."""
    EDIT_ROLE: _OrgPermission.ValueType  # 3
    """Can edit roles for the organization."""
    VIEW_ROLE: _OrgPermission.ValueType  # 4
    """Can view roles for the organization."""
    ASSIGN_ROLE: _OrgPermission.ValueType  # 5
    """Can assign roles to users for the organization."""
    PROVISION_TIER: _OrgPermission.ValueType  # 6
    """Can provision tiers for the organization."""
    DELETE_TIER: _OrgPermission.ValueType  # 7
    """Can delete tiers for the organization."""
    SET_DEFAULT_ROLE: _OrgPermission.ValueType  # 8
    """Can choose a default role for the organization."""
    ASSUME_IDENTITY: _OrgPermission.ValueType  # 9
    """Can assume a user's identity for the organization and perform actions as them."""

class OrgPermission(_OrgPermission, metaclass=_OrgPermissionEnumTypeWrapper): ...

ORG_ALL: OrgPermission.ValueType  # 0
INVITE: OrgPermission.ValueType  # 1
"""Can invite users to the organization."""
CREATE_ROLE: OrgPermission.ValueType  # 2
"""Can create roles for the organization."""
EDIT_ROLE: OrgPermission.ValueType  # 3
"""Can edit roles for the organization."""
VIEW_ROLE: OrgPermission.ValueType  # 4
"""Can view roles for the organization."""
ASSIGN_ROLE: OrgPermission.ValueType  # 5
"""Can assign roles to users for the organization."""
PROVISION_TIER: OrgPermission.ValueType  # 6
"""Can provision tiers for the organization."""
DELETE_TIER: OrgPermission.ValueType  # 7
"""Can delete tiers for the organization."""
SET_DEFAULT_ROLE: OrgPermission.ValueType  # 8
"""Can choose a default role for the organization."""
ASSUME_IDENTITY: OrgPermission.ValueType  # 9
"""Can assume a user's identity for the organization and perform actions as them."""
global___OrgPermission = OrgPermission

class _TierPermission:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _TierPermissionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_TierPermission.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    TIER_ALL: _TierPermission.ValueType  # 0
    MODIFY_ENVIRONMENT: _TierPermission.ValueType  # 1
    """Can modify the tier's environment."""
    ADD_TAGS: _TierPermission.ValueType  # 2
    """Can add new tags to the tier."""
    DELETE_TIER_ONLY: _TierPermission.ValueType  # 3
    """Can delete ONLY the given tier."""
    VIEW_TIER: _TierPermission.ValueType  # 4
    """Can view the tier."""
    TIER_ACCESS: _TierPermission.ValueType  # 5
    """Can give other user's access to the tier."""
    VIEW_ENTITY_DEFINITION: _TierPermission.ValueType  # 6
    """Can view definitions of an entity that has a given tag."""
    EDIT_ENTITY_DEFINITION: _TierPermission.ValueType  # 7
    """Can edit definitions of an entity that has a given tag."""
    READ_ENTITY_DATA: _TierPermission.ValueType  # 8
    """Can read data for a given entity that has a given tag."""
    WRITE_ENTITY_DATA: _TierPermission.ValueType  # 9
    """Can write data for a given entity that has a given tag."""
    EXTRACT_HISTORICAL_FEATURES: _TierPermission.ValueType  # 10
    """Can run extract_historical_features on the tier."""

class TierPermission(_TierPermission, metaclass=_TierPermissionEnumTypeWrapper): ...

TIER_ALL: TierPermission.ValueType  # 0
MODIFY_ENVIRONMENT: TierPermission.ValueType  # 1
"""Can modify the tier's environment."""
ADD_TAGS: TierPermission.ValueType  # 2
"""Can add new tags to the tier."""
DELETE_TIER_ONLY: TierPermission.ValueType  # 3
"""Can delete ONLY the given tier."""
VIEW_TIER: TierPermission.ValueType  # 4
"""Can view the tier."""
TIER_ACCESS: TierPermission.ValueType  # 5
"""Can give other user's access to the tier."""
VIEW_ENTITY_DEFINITION: TierPermission.ValueType  # 6
"""Can view definitions of an entity that has a given tag."""
EDIT_ENTITY_DEFINITION: TierPermission.ValueType  # 7
"""Can edit definitions of an entity that has a given tag."""
READ_ENTITY_DATA: TierPermission.ValueType  # 8
"""Can read data for a given entity that has a given tag."""
WRITE_ENTITY_DATA: TierPermission.ValueType  # 9
"""Can write data for a given entity that has a given tag."""
EXTRACT_HISTORICAL_FEATURES: TierPermission.ValueType  # 10
"""Can run extract_historical_features on the tier."""
global___TierPermission = TierPermission
