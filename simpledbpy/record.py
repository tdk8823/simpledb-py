from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from simpledbpy.file import BlockId, Page
from simpledbpy.query import RID, Constant, UpdateScan
from simpledbpy.tx.transaction import Transaction


class Types(Enum):
    INTEGER = 4
    VARCHAR = 12


@dataclass
class FieldInfo:
    type: Types
    length: int


class Schema:
    """The record schema of a table.
    A schema contains the name and type of each field of the table,
    as well as the length of each varchar field.
    """

    _fields: List[str]
    _info: Dict[str, FieldInfo]

    def __init__(self) -> None:
        self._fields = []
        self._info = {}

    def add_field(self, field_name: str, type: Types, length: int) -> None:
        """Add a field to the schema having a specified name, type, and length.
        If the field type is "integer", then the length value is irrelevant.

        Args:
            field_name (str): the name of the field
            type (Types): the type of field, according to the constants in simpledbpy.sql.types
            length (int): the conceptual length of a string field.
        """
        self._fields.append(field_name)
        self._info[field_name] = FieldInfo(type=type, length=length)

    def add_int_field(self, field_name: str) -> None:
        """Add integer field to the schema.

        Args:
            field_name (str): the name of the field
        """
        self.add_field(field_name, Types.INTEGER, 0)

    def add_string_field(self, field_name: str, length: int) -> None:
        """Add a string field to the schema. The length is the conceptual length of the field.
        For example, if the field is defined as varchar(8), then its length is 8.

        Args:
            field_name (str): the name of the field
            length (int): the length of chars in the varchar definition
        """
        self.add_field(field_name, Types.VARCHAR, length)

    def add(self, field_name: str, schema: "Schema") -> None:
        """Add a field to the schema having the same type and length as the corresponding field in another schema.

        Args:
            field_name (str): the name of the field
            schema (Schema): the other schema
        """
        type = schema.type(field_name)
        length = schema.length(field_name)
        self.add_field(field_name, type, length)

    def add_all(self, schema: "Schema") -> None:
        """Add all of the fields in the specified schema to the current schema.

        Args:
            schema (Schema): _description_
        """
        for field_name in schema.fields:
            self.add(field_name, schema)

    @property
    def fields(self) -> List[str]:
        """Return a collection containing the name of each field in the schema.

        Returns:
            List[str]: the collection of field names
        """
        return self._fields

    def has_field(self, field_name: str) -> bool:
        """Return True if the specified field is in the schema.

        Args:
            field_name (str): the name of the field

        Returns:
            bool: True if the field is in the schema
        """
        return field_name in self._fields

    def type(self, field_name: str) -> Types:
        """Return the type of the specified field, using the constants in Types.

        Args:
            field_name (str): the name of the field

        Returns:
            Types: the type of the field
        """
        field_info = self._info.get(field_name)
        assert field_info is not None
        return field_info.type

    def length(self, field_name: str) -> int:
        """Return the conceptual length of the specified field.
        If the field is not a string field, then the return value is undefined.
        Args:
            field_name (str): the name of the field

        Returns:
            int: the conceptual length of the field
        """
        field_info = self._info.get(field_name)
        assert field_info is not None
        return field_info.length


class Layout:
    """Description of the structure of a record.
    It contains the name, type, length and offset of each field of the table.
    """

    _schema: Schema
    _offsets: Dict[str, int]
    _slot_size: int

    def __init__(
        self, schema: Schema, offsets: Optional[Dict[str, int]] = None, slot_size: Optional[int] = None
    ) -> None:
        """This constructor creates a Layout object from a schema.
        If the offsets and slot_size retrieved from the catalog are provided, then they are used.
        Otherwise, the offsets and slot_size are calculated.

        Args:
            schema (Schema): the schema of the table's records
            offsets (Optional[int], optional): the already-calculated offsets of the fields within a record.
            slot_size (Optional[int], optional): the already-calculated length of each record.
        """
        self._schema = schema

        if offsets is not None and slot_size is not None:
            self._offsets = offsets
            self._slot_size = slot_size
        else:
            self._offsets = {}
            position = 4  # Integer.BYTES
            for field_name in schema.fields:
                self._offsets[field_name] = position
                position += self._length_in_bytes(field_name)
            self._slot_size = position

    @property
    def schema(self) -> Schema:
        """Return the schema of the table's records

        Returns:
            Schema: the table's recode schema
        """
        return self._schema

    def offset(self, field_name: str) -> int:
        """Return the offset of a specified field within a record

        Args:
            field_name (str): the name of the field

        Returns:
            int: the offset of that field within a record
        """
        offset = self._offsets.get(field_name)
        assert offset is not None
        return offset

    @property
    def slot_size(self) -> int:
        return self._slot_size

    def _length_in_bytes(self, field_name: str) -> int:
        """Return the length of the specified field in bytes.

        Args:
            field_name (str): the name of the field

        Returns:
            int: the length of the field in bytes
        """
        field_type = self._schema.type(field_name)
        if field_type == Types.INTEGER:
            return 4
        elif field_type == Types.VARCHAR:
            return Page.max_length(self._schema.length(field_name))
        else:
            raise ValueError("Unknown field type")


class RecordPage:
    """Store a record at a given location in a block."""

    EMPTY = 0
    USED = 1
    _tx: Transaction
    _block_id: BlockId
    _layout: Layout

    def __init__(self, tx: Transaction, block_id: BlockId, layout: Layout) -> None:
        self._tx = tx
        self._block_id = block_id
        self._layout = layout
        tx.pin(block_id)

    def get_int(self, slot: int, field_name: str) -> int:
        """Return the integer value stored for the specified field of a specified slot.

        Args:
            slot (int): the slot number
            field_name (str): the name of the field

        Returns:
            int: the integer stored in that field
        """

        field_position = self._offset(slot) + self._layout.offset(field_name)
        return self._tx.get_int(self._block_id, field_position)

    def get_string(self, slot: int, field_name: str) -> str:
        """Return the string value stored for the specified field of the specified slot.
        Args:
            slot (int): the slot number
            field_name (str): the name of the field

        Returns:
            str: the string stored in that field
        """
        field_position = self._offset(slot) + self._layout.offset(field_name)
        return self._tx.get_string(self._block_id, field_position)

    def set_int(self, slot: int, field_name: str, value: int) -> None:
        """Store an integer at the specified field of the specified slot.

        Args:
            slot (int): the slot number
            field_name (str): the name of the field
            value (int): the integer value stored in that field
        """

        field_position = self._offset(slot) + self._layout.offset(field_name)
        self._tx.set_int(self._block_id, field_position, value, True)

    def set_string(self, slot: int, field_name: str, value: str) -> None:
        """Store a string at the specified field of the specified slot.

        Args:
            slot (int): the slot number
            field_name (str): the name of the field
            value (str): the string value stored in that field
        """

        field_position = self._offset(slot) + self._layout.offset(field_name)
        self._tx.set_string(self._block_id, field_position, value, True)

    def delete(self, slot: int) -> None:
        self._set_flag(slot, self.EMPTY)

    def format(self) -> None:
        """Use the layout to format a new block of records.
        These values should not be logged because the old values are meaningless.
        """

        slot = 0
        while self._is_valid_slot(slot):
            self._tx.set_int(self._block_id, self._offset(slot), self.EMPTY, False)
            schema = self._layout.schema
            for field_name in schema.fields:
                field_position = self._offset(slot) + self._layout.offset(field_name)
                if schema.type(field_name) == Types.INTEGER:
                    self._tx.set_int(self._block_id, field_position, 0, False)
                else:
                    self._tx.set_string(self._block_id, field_position, "", False)
            slot += 1

    def next_after(self, slot: int) -> int:
        return self._search_after(slot, self.USED)

    def insert_after(self, slot: int) -> int:
        new_slot = self._search_after(slot, self.EMPTY)
        if new_slot >= 0:
            self._set_flag(new_slot, self.USED)
        return new_slot

    @property
    def block_id(self) -> BlockId:
        return self._block_id

    def _set_flag(self, slot: int, flag: int) -> None:
        """Set the record's empty/inuse flag.

        Args:
            slot (int): the slot number
            flag (int): the flag value
        """

        self._tx.set_int(self._block_id, self._offset(slot), flag, True)

    def _search_after(self, slot: int, flag: int) -> int:
        slot += 1
        while self._is_valid_slot(slot):
            if self._tx.get_int(self._block_id, self._offset(slot)) == flag:
                return slot
            slot += 1
        return -1

    def _is_valid_slot(self, slot: int) -> bool:
        return self._offset(slot + 1) <= self._tx.block_size

    def _offset(self, slot: int) -> int:
        return slot * self._layout.slot_size


class TableScan(UpdateScan):
    _tx: Transaction
    _layout: Layout
    _filename: str
    _current_slot: int
    _record_page: Optional[RecordPage]

    def __init__(self, tx: Transaction, table_name: str, layout: Layout) -> None:
        self._tx = tx
        self._layout = layout
        self._filename = table_name + ".tbl"
        self._record_page = None
        if tx.size(self._filename) == 0:
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
        block_id = BlockId(self._filename, rid.block_number)
        self._record_page = RecordPage(self._tx, block_id, self._layout)
        self._current_slot = rid.slot

    def get_rid(self) -> RID:
        assert self._record_page is not None
        return RID(self._record_page.block_id.block_number, self._current_slot)

    def _move_to_block(self, block_number: int) -> None:
        self.close()
        block_id = BlockId(self._filename, block_number)
        self._record_page = RecordPage(self._tx, block_id, self._layout)
        self._current_slot = -1

    def _move_to_new_block(self) -> None:
        self.close()
        block_id = self._tx.append(self._filename)
        self._record_page = RecordPage(self._tx, block_id, self._layout)
        self._record_page.format()
        self._current_slot = -1

    def _at_last_block(self) -> bool:
        assert self._record_page is not None
        return self._record_page.block_id.block_number == self._tx.size(self._filename) - 1
