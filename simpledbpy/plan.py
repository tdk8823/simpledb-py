from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from simpledbpy.record import Schema

if TYPE_CHECKING:
    from simpledbpy.query import Scan


class Plan(ABC):
    """The interface implemented by each query plan.
    There is a Plan class for each relational algebra operator.
    """

    @abstractmethod
    def open(self) -> Scan:
        """Opens a scan corresponding to this plan. The scan will be positioned before its first record.

        Returns:
            Scan: a scan
        """

        raise NotImplementedError

    @abstractmethod
    def blocks_accessed(self) -> int:
        """Returns an estimate of the number of block accesses that will occur when the scan is read to completion.

        Returns:
            int: the estimated number of block accesses
        """

        raise NotImplementedError

    @abstractmethod
    def records_output(self) -> int:
        """Returns an estimate of the number of records in the query's output table.

        Returns:
            int: the estimated number of output records
        """

        raise NotImplementedError

    @abstractmethod
    def distinct_values(self, field_name: str) -> int:
        """Returns an estimate of the number of distinct values for the specified field in the query's output table.

        Args:
            field_name (str): the name of a field

        Returns:
            int: the estimated number of distinct field values in the output
        """

        raise NotImplementedError

    @abstractmethod
    @property
    def schema(self) -> Schema:
        """Returns the schema of the query.

        Returns:
            Schema: the query's schema
        """

        raise NotImplementedError
