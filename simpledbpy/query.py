from abc import ABC, abstractmethod
from typing import Optional


class RID:
    """An identifier for a record within a file.
    A RID consists of the block number in the file, and the location of the record in that block.
    """

    _block_number: int
    _slot: int

    def __init__(self, block_number: int, slot: int) -> None:
        """Create a RID for the record having the specified location in the specified block.

        Args:
            block_number (int): the block number where the record lives
            slot (int): the record's location in that block
        """
        self._block_number = block_number
        self._slot = slot

    @property
    def block_number(self) -> int:
        """Return the block number associated with this RID.

        Returns:
            int: the block number
        """
        return self._block_number

    @property
    def slot(self) -> int:
        """Return the slot associated with this RID.

        Returns:
            int: the slot
        """
        return self._slot

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RID):
            return NotImplemented
        return self._block_number == other.block_number and self._slot == other.slot

    def __str__(self) -> str:
        return f"[{self._block_number}, {self._slot}]"

    def __repr__(self) -> str:
        return f"RID({self._block_number}, {self._slot})"


class Constant:
    """The class that denotes values stored in the database."""

    _integer_value: Optional[int] = None
    _string_value: Optional[str] = None

    def __init__(self, value: int | str) -> None:
        if isinstance(value, int):
            self._integer_value = value
        elif isinstance(value, str):
            self._string_value = value
        else:
            raise ValueError("Invalid value")

    def as_int(self) -> int:
        if self._integer_value is not None:
            return self._integer_value
        raise ValueError("This constant is not an integer")

    def as_string(self) -> str:
        if self._string_value is not None:
            return self._string_value
        raise ValueError("This constant is not a string")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Constant):
            return False

        if self._integer_value is not None:
            return self._integer_value == other.as_int()
        else:
            return self._string_value == other.as_string()

    def __lt__(self, other: "Constant") -> bool:
        if self._integer_value is not None and other._integer_value is not None:
            return self._integer_value < other._integer_value
        if self._string_value is not None and other._string_value is not None:
            return self._string_value < other._string_value
        raise TypeError("Cannot compare constants of different types")

    def __hash__(self) -> int:
        return hash(self._integer_value) if self._integer_value is not None else hash(self._string_value)

    def __str__(self) -> str:
        if self._integer_value is not None:
            return str(self._integer_value)
        else:
            assert self._string_value is not None
            return self._string_value

    def __repr__(self) -> str:
        return (
            f"Constant({self._integer_value})" if self._integer_value is not None else f"Constant({self._string_value})"
        )


class Scan(ABC):
    """The interface will be implemented by each query scan.
    There is a Scan class for each relational algebra operator.
    """

    @abstractmethod
    def before_first(self) -> None:
        """Position the scan before its first record.
        A subsequent call to next() will return the first record.
        """

        raise NotImplementedError

    @abstractmethod
    def next(self) -> bool:
        """Move the scan to the next record.

        Returns:
            bool: false if there is no next record
        """

        raise NotImplementedError

    @abstractmethod
    def get_int(self, field_name: str) -> int:
        """Return the value of the specified integer field in the current record.

        Args:
            field_name (str): the name of the field.

        Returns:
            int: the field's integer value in the current record.
        """

        raise NotImplementedError

    @abstractmethod
    def get_string(self, field_name: str) -> str:
        """Return the value of the specified string field in the current record.

        Args:
            field_name (str): the name of the field.

        Returns:
            str: the field's string value in the current record.
        """

        raise NotImplementedError

    @abstractmethod
    def get_val(self, field_name: str) -> Constant:
        """Return the value of the specified field in the current record.

        Args:
            field_name (str): the name of the field.

        Returns:
            Constant: the value of that field, expressed as a Constant.
        """

        raise NotImplementedError

    @abstractmethod
    def has_field(self, field_name: str) -> bool:
        """Return true if the scan has the specified field.

        Args:
            field_name (str): the name of the field.

        Returns:
            bool: true if the scan has that field.
        """

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Closes the scan and its subscans, if any."""

        raise NotImplementedError


class UpdateScan(Scan):
    """The interface implemented by all updateable scans."""

    @abstractmethod
    def set_value(self, field_name: str, value: Constant) -> None:
        """Modify the field value of the current record.

        Args:
            field_name (str): the name of the field.
            value (Constant): the new value, expressed as a Constant.
        """

        raise NotImplementedError

    @abstractmethod
    def set_int(self, field_name: str, value: int) -> None:
        """Modify the field value of the current record.

        Args:
            field_name (str): the name of the field.
            value (int): the new value.
        """

        raise NotImplementedError

    @abstractmethod
    def set_string(self, field_name: str, value: str) -> None:
        """Modify the field value of the current record.

        Args:
            field_name (str): the name of the field.
            value (str): the new value.
        """

        raise NotImplementedError

    @abstractmethod
    def insert(self) -> None:
        """Insert a new record somewhere in the scan."""

        raise NotImplementedError

    @abstractmethod
    def delete(self) -> None:
        """Delete the current record from the scan."""

        raise NotImplementedError

    @abstractmethod
    def get_rid(self) -> RID:
        """Returns the RID of the current record.

        Returns:
            RID: the ID of the current record.
        """

        raise NotImplementedError

    @abstractmethod
    def move_to_rid(self, rid: RID) -> None:
        """Position the scan so that the current record has the specified RID.

        Args:
            rid (RID): the RID of the desired record.
        """

        raise NotImplementedError
