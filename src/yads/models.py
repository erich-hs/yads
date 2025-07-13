"""
This module defines the data models for yads specifications.

These classes represent the fundamental building blocks of a data specification,
such as tables, columns, and databases.
"""

from typing import Any, List

from .base import BaseObject


class Constraint(BaseObject):
    """
    Represents a constraint on a column.
    """

    pass


class NotNullConstraint(Constraint):
    """
    Represents a NOT NULL constraint.
    """

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.not_null = True


class Column(BaseObject):
    """
    Represents a column in a table schema.
    """

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.constraints: List[Constraint] = []
        if "constraints" in data:
            for constraint_data in data["constraints"]:
                if "not_null" in constraint_data and constraint_data["not_null"]:
                    self.constraints.append(NotNullConstraint(**constraint_data))
