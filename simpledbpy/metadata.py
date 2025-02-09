from threading import Lock
from typing import Dict

from simpledbpy.record import Layout, Schema, TableScan, Types
from simpledbpy.tx.transaction import Transaction


class TableManager:
    """The table manager. There are methods to create a table, save the metadata
    in the catalog, and obtain the metadata of a previously-created table.
    """

    MAX_NAME = 16  # The max characters a tablename or fieldname can have.
    _table_catalog_layout: Layout
    _field_catalog_layout: Layout

    def __init__(self, is_new: bool, tx: Transaction) -> None:
        """Create a new catalog manager for the database system.
        If the database is new, the two catalog tables are created.

        Args:
            is_new (bool): has the value True if the database is new, and False otherwise.
            tx (Transaction): the startup transaction.
        """

        table_catalog_schema = Schema()
        table_catalog_schema.add_string_field("tablename", self.MAX_NAME)
        table_catalog_schema.add_int_field("slotsize")
        self._table_catalog_layout = Layout(table_catalog_schema)

        field_catalog_schema = Schema()
        field_catalog_schema.add_string_field("tablename", self.MAX_NAME)
        field_catalog_schema.add_string_field("fieldname", self.MAX_NAME)
        field_catalog_schema.add_int_field("type")
        field_catalog_schema.add_int_field("length")
        field_catalog_schema.add_int_field("offset")
        self._field_catalog_layout = Layout(field_catalog_schema)

        if is_new:
            self.create_table("table_catalog", table_catalog_schema, tx)
            self.create_table("field_catalog", field_catalog_schema, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction) -> None:
        """Create a new table having the specified name and schema.

        Args:
            table_name (str): the name of the new table
            schema (Schema): the table's schema
            tx (Transaction): the transaction creating the table
        """

        layout = Layout(schema)
        # insert one record into the table catalog
        table_catalog = TableScan(tx, "table_catalog", self._table_catalog_layout)
        table_catalog.insert()
        table_catalog.set_string("tablename", table_name)
        table_catalog.set_int("slotsize", layout.slot_size)
        table_catalog.close()

        # insert a record into the field catalog for each field
        field_catalog = TableScan(tx, "field_catalog", self._field_catalog_layout)
        for fldname in schema.fields:
            field_catalog.insert()
            field_catalog.set_string("tablename", table_name)
            field_catalog.set_string("fieldname", fldname)
            field_catalog.set_int("type", schema.type(fldname).value)
            field_catalog.set_int("length", schema.length(fldname))
            field_catalog.set_int("offset", layout.offset(fldname))
        field_catalog.close()

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        """Retrieve the layout of the specified table from the catalog.

        Args:
            table_name (str): the name of the table
            tx (Transaction): the transaction

        Returns:
            Layout: the table's stored metadata
        """
        slot_size = -1
        table_catalog = TableScan(tx, "table_catalog", self._table_catalog_layout)
        while table_catalog.next():
            if table_catalog.get_string("tablename") == table_name:
                slot_size = table_catalog.get_int("slotsize")
                break
        table_catalog.close()

        schema = Schema()
        offsets = {}
        field_catalog = TableScan(tx, "field_catalog", self._field_catalog_layout)
        while field_catalog.next():
            if field_catalog.get_string("tablename") == table_name:
                field_name = field_catalog.get_string("fieldname")
                field_type = Types(field_catalog.get_int("type"))
                length = field_catalog.get_int("length")
                offset = field_catalog.get_int("offset")
                schema.add_field(field_name, field_type, length)
                offsets[field_name] = offset
        field_catalog.close()
        return Layout(schema, offsets, slot_size)


class StatInfo:
    _num_blocks: int
    _num_records: int

    def __init__(self, num_blocks: int, num_records: int) -> None:
        """Create a StatInfo object. Note that the number of distinct values is not passed into the constructor.
        The object fakes this value.

        Args:
            num_blocks (int): the number of blocks in the table
            num_records (int): the number of records in the table
        """
        self._num_blocks = num_blocks
        self._num_records = num_records

    @property
    def blocks_accessed(self) -> int:
        """Return the estimated number of blocks in the table.

        Returns:
            int: the estimated number of blocks in the table
        """
        return self._num_blocks

    @property
    def records_output(self) -> int:
        """Return the estimated number of records in the table.

        Returns:
            int: the estimated number of records in the table
        """
        return self._num_records

    def distinct_values(self, field_name: str) -> int:
        """Return the estimated number of distinct values for the specified field.
        This estimate is a complete guess, because doing something reasonable is beyond the scope of this system.

        Args:
            field_name (str): the name of the field

        Returns:
            int: a guess as to the number of distinct field values
        """
        return 1 + (self._num_records // 3)


class StatManager:
    """The statistics manager is responsible for keeping statistical information about each table.
    The manager does not store this information in the database.
    Instead, it calculates this information on system startup, and periodically refreshes it.
    """

    _table_manager: TableManager
    _table_stats: Dict[str, StatInfo]
    _num_calls: int
    _lock: Lock

    def __init__(self, table_manager: TableManager, tx: Transaction) -> None:
        """Create the statistics manager. The initial statistics are calculated by traversing the entire database.

        Args:
            table_manager (TableManager): the table manager
            tx (Transaction): the startup transaction
        """
        self._table_manager = table_manager
        self._table_stats = {}
        self._num_calls = 0
        self._lock = Lock()
        self._refresh_statistics(tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        """Return the statistical information about the specified table.

        Args:
            table_name (str): the name of the table
            layout (Layout): the table's layout
            tx (Transaction): the calling transaction

        Returns:
            StatInfo: the statistical information about the table
        """
        with self._lock:
            self._num_calls += 1
            if self._num_calls > 100:
                self._refresh_statistics(tx)
            stat_info = self._table_stats.get(table_name)
            if stat_info is None:
                stat_info = self._calc_table_stats(table_name, layout, tx)
                self._table_stats[table_name] = stat_info
            return stat_info

    def _refresh_statistics(self, tx: Transaction) -> None:
        with self._lock:
            table_stats = {}
            table_catalog_layout = self._table_manager.get_layout("table_catalog", tx)
            table_catalog = TableScan(tx, "table_catalog", table_catalog_layout)
            while table_catalog.next():
                table_name = table_catalog.get_string("tablename")
                layout = self._table_manager.get_layout(table_name, tx)
                statistic_info = self._calc_table_stats(table_name, layout, tx)
                table_stats[table_name] = statistic_info
            table_catalog.close()

    def _calc_table_stats(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        num_records = 0
        num_blocks = 0
        table_scan = TableScan(tx, table_name, layout)
        while table_scan.next():
            num_records += 1
            num_blocks = table_scan.get_rid().block_number + 1
        table_scan.close()
        return StatInfo(num_blocks, num_records)


class MetadataManager:
    _table_manager: TableManager
    _stat_manager: StatManager

    def __init__(self, is_new: bool, tx: Transaction) -> None:
        self._table_manager = TableManager(is_new, tx)
        self._stat_manager = StatManager(self._table_manager, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction) -> None:
        self._table_manager.create_table(table_name, schema, tx)

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        return self._table_manager.get_layout(table_name, tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        return self._stat_manager.get_stat_info(table_name, layout, tx)
