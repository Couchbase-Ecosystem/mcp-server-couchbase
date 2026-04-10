"""Relationship rule classes."""

from .foreign_key_relationship import ForeignKeyRelationshipRule
from .one_to_many_relationship import OneToManyRelationshipRule
from .one_to_one_relationship import OneToOneRelationshipRule
from .primary_key_alternative_relationship import PrimaryKeyAlternativeRelationshipRule
from .primary_key_relationship import PrimaryKeyRelationshipRule

__all__ = [
    "PrimaryKeyRelationshipRule",
    "PrimaryKeyAlternativeRelationshipRule",
    "ForeignKeyRelationshipRule",
    "OneToOneRelationshipRule",
    "OneToManyRelationshipRule",
]
