from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

from simpledbpy.file import BlockId
from simpledbpy.record import Layout, RecordPage, Schema, Types
from simpledbpy.tx.transaction import Transaction

if TYPE_CHECKING:
    from simpledbpy.plan import Plan


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


class TableScan(UpdateScan):
    _tx: Transaction
    _layout: Layout
    _file_name: str
    _current_slot: int
    _record_page: Optional[RecordPage]

    def __init__(self, tx: Transaction, table_name: str, layout: Layout) -> None:
        self._tx = tx
        self._layout = layout
        self._file_name = table_name + ".tbl"
        self._record_page = None
        if tx.size(self._file_name) == 0:
            self._move_to_new_block()
        else:
            self._move_to_block(0)

    def before_first(self) -> None:
        self._move_to_block(0)

    def next(self) -> bool:
        assert self._record_page is not None
        self._current_slot = self._record_page.next_after(self._current_slot)
        while self._current_slot < 0:
            if self._at_last_block():
                return False
            self._move_to_block(self._record_page.block_id.block_number + 1)
            self._current_slot = self._record_page.next_after(self._current_slot)
        return True

    def get_int(self, field_name: str) -> int:
        assert self._record_page is not None
        return self._record_page.get_int(self._current_slot, field_name)

    def get_string(self, field_name: str) -> str:
        assert self._record_page is not None
        return self._record_page.get_string(self._current_slot, field_name)

    def get_val(self, field_name: str) -> Constant:
        if self._layout.schema.type(field_name) == Types.INTEGER:
            return Constant(self.get_int(field_name))
        else:
            return Constant(self.get_string(field_name))

    def has_field(self, field_name: str) -> bool:
        return self._layout.schema.has_field(field_name)

    def close(self) -> None:
        if self._record_page is not None:
            self._tx.unpin(self._record_page.block_id)

    def set_int(self, field_name: str, value: int) -> None:
        assert self._record_page is not None
        self._record_page.set_int(self._current_slot, field_name, value)

    def set_string(self, field_name: str, value: str) -> None:
        assert self._record_page is not None
        self._record_page.set_string(self._current_slot, field_name, value)

    def set_value(self, field_name: str, val: Constant) -> None:
        if self._layout.schema.type(field_name) == Types.INTEGER:
            self.set_int(field_name, val.as_int())
        else:
            self.set_string(field_name, val.as_string())

    def insert(self) -> None:
        assert self._record_page is not None
        self._current_slot = self._record_page.insert_after(self._current_slot)
        while self._current_slot < 0:
            if self._at_last_block():
                self._move_to_new_block()
            else:
                self._move_to_block(self._record_page.block_id.block_number + 1)
            self._current_slot = self._record_page.insert_after(self._current_slot)

    def delete(self) -> None:
        assert self._record_page is not None
        self._record_page.delete(self._current_slot)

    def move_to_rid(self, rid: RID) -> None:
        self.close()
        block_id = BlockId(self._file_name, rid.block_number)
        self._record_page = RecordPage(self._tx, block_id, self._layout)
        self._current_slot = rid.slot

    def get_rid(self) -> RID:
        assert self._record_page is not None
        return RID(self._record_page.block_id.block_number, self._current_slot)

    def _move_to_block(self, block_number: int) -> None:
        self.close()
        block_id = BlockId(self._file_name, block_number)
        self._record_page = RecordPage(self._tx, block_id, self._layout)
        self._current_slot = -1

    def _move_to_new_block(self) -> None:
        self.close()
        block_id = self._tx.append(self._file_name)
        self._record_page = RecordPage(self._tx, block_id, self._layout)
        self._record_page.format()
        self._current_slot = -1

    def _at_last_block(self) -> bool:
        assert self._record_page is not None
        return self._record_page.block_id.block_number == self._tx.size(self._file_name) - 1


class ProductScan(Scan):
    """The scan class corresponding to the <i>product</i> relational algebra operator."""

    _scan1: Scan
    _scan2: Scan

    def __init__(self, scan1: Scan, scan2: Scan) -> None:
        """Create a product scan having the two underlying scans.

        Args:
            scan1 (Scan): the LHS scan
            scan2 (Scan): the RHS scan
        """
        self._scan1 = scan1
        self._scan2 = scan2
        self.before_first()

    def before_first(self) -> None:
        """Position the scan before its first record. In particular, the LHS scan is positioned at its first record,
        and the RHS scan is positioned before its first record.
        """
        self._scan1.before_first()
        self._scan1.next()
        self._scan2.before_first()

    def next(self) -> bool:
        """Move the scan to the next record. The method moves to the next RHS record, if possible. Otherwise, it moves
        to the next LHS record and the first RHS record. If there are no more LHS records, the method returns false.

        Returns:
            bool: True if there is anoter record in the LHS scan
        """
        if self._scan2.next():
            return True
        else:
            self._scan2.before_first()
            return self._scan1.next() and self._scan2.next()

    def get_int(self, field_name: str) -> int:
        """Return the integer value of the specified field.
        The value is obtained from whichever scan contains the field.

        Args:
            field_name (str): the name of the field

        Returns:
            int: the field's integer value
        """
        if self._scan1.has_field(field_name):
            return self._scan1.get_int(field_name)
        else:
            return self._scan2.get_int(field_name)

    def get_string(self, field_name: str) -> str:
        """Return the string value of the specified field.
        The value is obtained from whichever scan contains the field.

        Args:
            field_name (str): the name of the field

        Returns:
            str: the field's string value
        """
        if self._scan1.has_field(field_name):
            return self._scan1.get_string(field_name)
        else:
            return self._scan2.get_string(field_name)

    def get_val(self, field_name: str) -> Constant:
        """Return the value of the specified field.
        The value is obtained from whichever scan contains the field.

        Args:
            field_name (str): the name of the field

        Returns:
            Constant: the field's value
        """
        if self._scan1.has_field(field_name):
            return self._scan1.get_val(field_name)
        else:
            return self._scan2.get_val(field_name)

    def has_field(self, field_name: str) -> bool:
        """Returns True if the specified field is in either of the underlying scans.

        Args:
            field_name (str): the name of the field

        Returns:
            bool: True if the field is in either scan
        """
        return self._scan1.has_field(field_name) or self._scan2.has_field(field_name)

    def close(self) -> None:
        """Close both underlying scans."""
        self._scan1.close()
        self._scan2.close()


class Expression:
    _val: Optional[Constant]
    _field_name: Optional[str]

    def __init__(self, val: Constant | str) -> None:
        if isinstance(val, Constant):
            self._val = val
            self._field_name = None
        elif isinstance(val, str):
            self._field_name = val
            self._val = None
        else:
            raise ValueError("Invalid value")

    def evaluate(self, scan: Scan) -> Constant:
        """Evaluate the expression with respect to the current record of the specified scan.

        Args:
            scan (Scan): the stan

        Returns:
            Constant: the value of the expression, as a Constant
        """
        if self._val is not None:
            return self._val
        else:
            assert self._field_name is not None
            return scan.get_val(self._field_name)

    def is_field_name(self) -> bool:
        """Return true if the expression is a field reference.

        Returns:
            bool: True if the expression denotes a field
        """
        return self._field_name is not None

    def as_constant(self) -> Constant:
        """Return the constant corresponding to a constant expression,
        or null if the expression does not denote a constant.

        Returns:
            str: the expression as a constant
        """
        assert self._val is not None
        return self._val

    def as_field_name(self) -> str:
        """Return the field name corresponding to a constant expression,
        or null if the expression does not denote a field.

        Returns:
            str: the expression as a field name
        """
        assert self._field_name is not None
        return self._field_name

    def applies_to(self, schema: Schema) -> bool:
        """Determine if all of the fields mentioned in this expression are contained in the specified schema.

        Args:
            schema (Schema): the schema

        Returns:
            bool: True is all field in the expression are in the schema
        """
        if self._val is not None:
            return True
        assert self._field_name is not None
        return schema.has_field(self._field_name)

    def __str__(self) -> str:
        if self._val is not None:
            return str(self._val)
        else:
            assert self._field_name is not None
            return self._field_name


class Term:
    """A term is a comparison between two expressions."""

    _lhs: Expression
    _rhs: Expression

    def __init__(self, lhs: Expression, rhs: Expression) -> None:
        """Create a new term that compares two expressions for equality.

        Args:
            lhs (Expression): the LHS expression
            rhs (Expression): the RHS expression
        """
        self._lhs = lhs
        self._rhs = rhs

    def is_satisfied(self, scan: Scan) -> bool:
        """Return true if both of the term's expressions evaluate to the same constant,
        with respect to the specified scan.

        Args:
            scan (Scan): the scan

        Returns:
            bool: True if both expressions have the same value in the scan
        """
        lhs_val = self._lhs.evaluate(scan)
        rhs_val = self._rhs.evaluate(scan)
        return lhs_val == rhs_val

    def reduction_factor(self, plan: Plan) -> int:
        """Calculate the extent to which selecting on the term reduces the number of records output by a query.
        For example if the reduction factor is 2, then the term cuts the size of the output in half.

        Args:
            plan (Plan): the query's plan

        Returns:
            int: the integer reduction factor
        """
        if self._lhs.is_field_name() and self._rhs.is_field_name():
            lhs_name = self._lhs.as_field_name()
            rhs_name = self._rhs.as_field_name()
            return max(plan.distinct_values(lhs_name), plan.distinct_values(rhs_name))
        if self._lhs.is_field_name():
            lhs_name = self._lhs.as_field_name()
            return plan.distinct_values(lhs_name)
        if self._rhs.is_field_name():
            rhs_name = self._rhs.as_field_name()
            return plan.distinct_values(rhs_name)
        if self._lhs.as_constant() == self._rhs.as_constant():
            return 1
        else:
            # return Integer.MAX_VALUE
            return 2147483647

    def equal_with_constant(self, field_name: str) -> Optional[Constant]:
        """Determine if this term is of the form "F=c" where F is the specified field and c is some constant.
        If so, the method returns that constant. If not, the method returns null.

        Args:
            field_name (str): the name of the field

        Returns:
            Optional[Constant]: either the constant or None
        """
        if self._lhs.is_field_name() and self._lhs.as_field_name() == field_name and not self._rhs.is_field_name():
            return self._rhs.as_constant()
        elif self._rhs.is_field_name() and self._rhs.as_field_name() == field_name and not self._lhs.is_field_name():
            return self._lhs.as_constant()
        return None

    def equal_with_field(self, field_name: str) -> Optional[str]:
        """Determine if this term is of the form "F1=F2" where F1 is the specified field and F2 is another field.
        If so, the method returns the name of that field. If not, the method returns null.

        Args:
            field_name (str): the name of the field

        Returns:
            Optional[str]: either the field name or None
        """
        if self._lhs.is_field_name() and self._lhs.as_field_name() == field_name and self._rhs.is_field_name():
            return self._rhs.as_field_name()
        elif self._rhs.is_field_name() and self._rhs.as_field_name() == field_name and self._lhs.is_field_name():
            return self._lhs.as_field_name()
        else:
            return None

    def applies_to(self, schema: Schema) -> bool:
        """Return true if both of the term's expressions apply to the specified schema.

        Args:
            schema (Schema): the schema

        Returns:
            bool: True if both expressions apply to the schema
        """
        return self._lhs.applies_to(schema) and self._rhs.applies_to(schema)

    def __str__(self) -> str:
        return f"{self._lhs} = {self._rhs}"


class Predicate:
    """A predicate is a Boolean combination of terms."""

    _terms: List[Term]

    def __init__(self, term: Optional[Term] = None) -> None:
        """Create a predicate containing a single term.

        Args:
            term (Term): the term
        """
        self._terms = []
        if term is not None:
            self._terms.append(term)

    def conjoin_with(self, predication: "Predicate") -> None:
        """Modifies the predicate to be the conjunction of itself and the specified predicate.
        Args:
            predication (Predicate): the other predicate
        """
        self._terms.extend(predication._terms)

    def is_satisfied(self, scan: Scan) -> bool:
        """Returns true if the predicate evaluates to true with respect to the specified scan.

        Args:
            scan (Scan): the scan

        Returns:
            bool: True if the predicate is True in the scan
        """
        for term in self._terms:
            if not term.is_satisfied(scan):
                return False
        return True

    def reduction_factor(self, plan: Plan) -> int:
        """Calculate the extent to which selecting on the predicate reduces the number of records output by a query.
        For example if the reduction factor is 2, then the predicate cuts the size of the output in half.

        Args:
            plan (Plan): the query's plan

        Returns:
            int: the integer reduction factor
        """
        factor = 1
        for term in self._terms:
            factor *= term.reduction_factor(plan)
        return factor

    def select_sub_predicate(self, schema: Schema) -> Optional["Predicate"]:
        """Return the subpredicate that applies to the specified schema.

        Args:
            schema (Schema): the schema

        Returns:
            Predicate: the subpredicate applying to the schema
        """
        result = Predicate()
        for term in self._terms:
            if term.applies_to(schema):
                result._terms.append(term)
        if len(result._terms) == 0:
            return None
        return result

    def join_sub_predicate(self, schema1: Schema, schema2: Schema) -> Optional["Predicate"]:
        """Return the subpredicate consisting of terms that apply to the union of the two specified schemas,
        but not to either schema separately.

        Args:
            schema1 (Schema): the first schema
            schema2 (Schema): the second schema

        Returns:
            the subpredicate whose terms apply to the union of two schemas but not either schema separately
        """
        result = Predicate()
        new_schema = Schema()
        new_schema.add_all(schema1)
        new_schema.add_all(schema2)
        for term in self._terms:
            if not term.applies_to(schema1) and not term.applies_to(schema2) and term.applies_to(new_schema):
                result._terms.append(term)
        if len(result._terms) == 0:
            return None
        return result

    def equal_with_constant(self, field_name: str) -> Optional[Constant]:
        """Determine if there is a term of the form "F=c" where F is the specified field and c is some constant.
        If so, the method returns that constant. If not, the method returns null.

        Args:
            field_name (str): the name of the field

        Returns:
            Optional[Constant]: either the constant or None
        """
        for term in self._terms:
            result = term.equal_with_constant(field_name)
            if result is not None:
                return result
        return None

    def equal_with_field(self, field_name: str) -> Optional[str]:
        """Determine if there is a term of the form "F1=F2" where F1 is the specified field and F2 is another field.
         If so, the method returns the name of that field. If not, the method returns null.

        Args:
            field_name (str): the name of the field

        Returns:
            Optional[str]: the name of the other field or None
        """
        for term in self._terms:
            result = term.equal_with_field(field_name)
            if result is not None:
                return result
        return None

    def __str__(self) -> str:
        if not self._terms:
            return ""
        return " and ".join(str(term) for term in self._terms)


class SelectScan(Scan):
    """The scan class corresponding to the <i>select</i> relational algebra operator.
    All methods except next delegate their work to the underlying scan.
    """

    _scan: Scan
    _predication: Predicate

    def __init__(self, scan: Scan, predication: Predicate) -> None:
        """Create a select scan having the specified underlying scan and predicate.

        Args:
            scan (Scan): the scan of the underlying query
            predication (Predicate): the selection predicate
        """
        self._scan = scan
        self._predication = predication

    # Scan methods
    def before_first(self) -> None:
        self._scan.before_first()

    def next(self) -> bool:
        while self._scan.next():
            if self._predication.is_satisfied(self._scan):
                return True
        return False

    def get_int(self, field_name: str) -> int:
        return self._scan.get_int(field_name)

    def get_string(self, field_name: str) -> str:
        return self._scan.get_string(field_name)

    def get_val(self, field_name: str) -> Constant:
        return self._scan.get_val(field_name)

    def has_field(self, field_name: str) -> bool:
        return self._scan.has_field(field_name)

    def close(self) -> None:
        self._scan.close()

    # UpdateScan methods
    def set_int(self, field_name: str, value: int) -> None:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't set values of a non-update scan")
        self._scan.set_int(field_name, value)

    def set_string(self, field_name: str, value: str) -> None:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't set values of a non-update scan")
        self._scan.set_string(field_name, value)

    def set_value(self, field_name: str, value: Constant) -> None:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't set values of a non-update scan")
        self._scan.set_value(field_name, value)

    def delete(self) -> None:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't delete records of a non-update scan")
        self._scan.delete()

    def insert(self) -> None:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't insert records of a non-update scan")
        self._scan.insert()

    def get_rid(self) -> RID:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't get RIDs of a non-update scan")
        return self._scan.get_rid()

    def move_to_rid(self, rid: RID) -> None:
        if not isinstance(self._scan, UpdateScan):
            raise RuntimeError("Can't move to RIDs of a non-update scan")
        self._scan.move_to_rid(rid)


class ProjectScan(Scan):
    """The scan class corresponding to the <i>project</i> relational algebra operator.
    All methods except hasField delegate their work to the underlying scan.
    """

    _scan: Scan
    _field_list: List[str]

    def __init__(self, scan: Scan, field_list: List[str]) -> None:
        """Create a project scan having the specified underlying scan and field list.

        Args:
            scan (Scan): the underlying scan
            field_list (List[str]): the list of field names
        """
        self._scan = scan
        self._field_list = field_list

    def before_first(self) -> None:
        self._scan.before_first()

    def next(self) -> bool:
        return self._scan.next()

    def get_int(self, field_name: str) -> int:
        if self.has_field(field_name):
            return self._scan.get_int(field_name)
        else:
            raise RuntimeError(f"Field {field_name} not found")

    def get_string(self, field_name: str) -> str:
        if self.has_field(field_name):
            return self._scan.get_string(field_name)
        else:
            raise RuntimeError(f"Field {field_name} not found")

    def get_val(self, field_name: str) -> Constant:
        if self.has_field(field_name):
            return self._scan.get_val(field_name)
        else:
            raise RuntimeError(f"Field {field_name} not found")

    def has_field(self, field_name: str) -> bool:
        return field_name in self._field_list

    def close(self) -> None:
        self._scan.close()
