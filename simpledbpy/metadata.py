from threading import Lock
from typing import Dict, Optional

from simpledbpy.index import HashIndex, Index
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


class IndexInfo:
    """The information about an index.
    This information is used by the query planner in order to estimate the costs of using the index,
    and to obtain the layout of the index records. Its methods are essentially the same as those of Plan.
    """

    _index_name: str
    _field_name: str
    _tx: Transaction
    _table_schema: Schema
    _index_layout: Layout
    _stat_info: StatInfo

    def __init__(
        self, index_name: str, field_name: str, table_schema: Schema, tx: Transaction, stat_info: StatInfo
    ) -> None:
        """Create an IndexInfo object for the specified index.

        Args:
            index_name (str): the name of the index
            field_name (str): the name of the indexed field
            table_schema (Schema): the schema of the table
            tx (Transaction): the calling transaction
            stat_info (StatInfo): the statistical information about the table
        """
        self._index_name = index_name
        self._field_name = field_name
        self._tx = tx
        self._table_schema = table_schema
        self._index_layout = self._create_index_layout()
        self._stat_info = stat_info

    def open(self) -> Index:
        """Open the index described by this object.

        Returns:
            Index: the Index object associated with this information
        """
        return HashIndex(self._tx, self._index_name, self._index_layout)

    @property
    def blocks_accessed(self) -> int:
        """Estimate the number of block accesses required to find all index records having a particular search key.
        The method uses the table's metadata to estimate the size of the index file and the number of index records
        per block. It then passes this information to the traversalCost method of the appropriate index type,
        which provides the estimate.

        Returns:
            int: the number of block accesses rerquired to traverse the index
        """

        records_per_block = self._tx.block_size // self._index_layout.slot_size
        num_blocks = self._stat_info.records_output // records_per_block
        return HashIndex.search_cost(num_blocks, records_per_block)

    @property
    def records_output(self) -> int:
        """Return the estimated number of records having a search key. This value is the same as doing a select query;
        that is, it is the number of records in the table divided by the number of distinct values of the indexed field.

        Returns:
            int: the estimated number of records having a search key
        """
        return self._stat_info.records_output // self._stat_info.distinct_values(self._field_name)

    def distinct_values(self, field_name: str) -> int:
        """Return the distinct values for a specified field in the underlying table, or 1 for the indexed field.

        Args:
            field_name (str): the specified field

        Returns:
            int: the number of distinct values for the specified field
        """

        if self._field_name == field_name:
            return 1
        else:
            return self._stat_info.distinct_values(field_name)

    def _create_index_layout(self) -> Layout:
        """Return the layout of the index records. The schema consists of the dataRID (which is represented as two
        integers, the block number and the record ID) and the dataval (which is the indexed field).
        Schema information about the indexed field is obtained via the table's schema.

        Returns:
            Layout: _description_
        """
        schema = Schema()
        schema.add_int_field("block")
        schema.add_int_field("id")
        if self._table_schema.type(self._field_name) == Types.INTEGER:
            schema.add_int_field("dataval")
        else:
            field_len = self._table_schema.length(self._field_name)
            schema.add_string_field("dataval", field_len)
        return Layout(schema)


class IndexManager:
    """The index manager. The index manager has similar functionality to the table manager."""

    _layout: Layout
    _table_manager: TableManager
    _stat_manager: StatManager

    def __init__(self, is_new: bool, table_manager: TableManager, stat_manager: StatManager, tx: Transaction) -> None:
        if is_new:
            schema = Schema()
            schema.add_string_field("indexname", TableManager.MAX_NAME)
            schema.add_string_field("tablename", TableManager.MAX_NAME)
            schema.add_string_field("fieldname", TableManager.MAX_NAME)
            table_manager.create_table("idxcat", schema, tx)
        self._table_manager = table_manager
        self._stat_manager = stat_manager
        self._layout = table_manager.get_layout("idxcat", tx)

    def create_index(self, index_name: str, table_name: str, field_name: str, tx: Transaction) -> None:
        """Create an index of the specified type for the specified field. A unique ID is assigned to this index,
        and its information is stored in the idxcat table.

        Args:
            index_name (str): the name of the index
            table_name (str): the name of the indexed table
            field_name (str): the name of the indexed field
            tx (Transaction): the calling transaction
        """
        table_scan = TableScan(tx, "idxcat", self._layout)
        table_scan.insert()
        table_scan.set_string("indexname", index_name)
        table_scan.set_string("tablename", table_name)
        table_scan.set_string("fieldname", field_name)
        table_scan.close()

    def get_index_info(self, table_name: str, tx: Transaction) -> Dict[str, IndexInfo]:
        """Return a map containing the index info for all indexes on the specified table.

        Args:
            table_name (str): the name of the table
            tx (Transaction): the calling transaction

        Returns:
            Dict[str, IndexInfo]: a map of IndexInfo objects, keyed by their field names
        """
        result = {}
        table_scan = TableScan(tx, "idxcat", self._layout)
        while table_scan.next():
            if table_scan.get_string("tablename") == table_name:
                index_name = table_scan.get_string("indexname")
                field_name = table_scan.get_string("fieldname")
                table_layout = self._table_manager.get_layout(table_name, tx)
                table_stat_info = self._stat_manager.get_stat_info(table_name, table_layout, tx)
                index_info = IndexInfo(index_name, field_name, table_layout.schema, tx, table_stat_info)
                result[field_name] = index_info
        table_scan.close()
        return result


class ViewManager:
    MAX_VIEWDEF = 100

    _table_manager: TableManager

    def __init__(self, is_new: bool, table_manager: TableManager, tx: Transaction) -> None:
        self._table_manager = table_manager
        if is_new:
            schema = Schema()
            schema.add_string_field("viewname", TableManager.MAX_NAME)
            schema.add_string_field("viewdef", self.MAX_VIEWDEF)
            table_manager.create_table("viewcat", schema, tx)

    def create_view(self, view_name: str, view_def: str, tx: Transaction) -> None:
        layout = self._table_manager.get_layout("viewcat", tx)
        table_scan = TableScan(tx, "viewcat", layout)
        table_scan.insert()
        table_scan.set_string("viewname", view_name)
        table_scan.set_string("viewdef", view_def)
        table_scan.close()

    def get_view_def(self, view_name: str, tx: Transaction) -> Optional[str]:
        result = None
        layout = self._table_manager.get_layout("viewcat", tx)
        table_scan = TableScan(tx, "viewcat", layout)
        while table_scan.next():
            if table_scan.get_string("viewname") == view_name:
                result = table_scan.get_string("viewdef")
                break
        table_scan.close()
        return result


class MetadataManager:
    _table_manager: TableManager
    _view_manager: ViewManager
    _stat_manager: StatManager
    _index_manager: IndexManager

    def __init__(self, is_new: bool, tx: Transaction) -> None:
        self._table_manager = TableManager(is_new, tx)
        self._view_manager = ViewManager(is_new, self._table_manager, tx)
        self._stat_manager = StatManager(self._table_manager, tx)
        self._index_manager = IndexManager(is_new, self._table_manager, self._stat_manager, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction) -> None:
        self._table_manager.create_table(table_name, schema, tx)

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        return self._table_manager.get_layout(table_name, tx)

    def create_view(self, view_name: str, view_def: str, tx: Transaction) -> None:
        self._view_manager.create_view(view_name, view_def, tx)

    def get_view_def(self, view_name: str, tx: Transaction) -> Optional[str]:
        return self._view_manager.get_view_def(view_name, tx)

    def create_index(self, index_name: str, table_name: str, field_name: str, tx: Transaction) -> None:
        self._index_manager.create_index(index_name, table_name, field_name, tx)

    def get_index_info(self, table_name: str, tx: Transaction) -> Dict[str, IndexInfo]:
        return self._index_manager.get_index_info(table_name, tx)

    def get_stat_info(self, table_name: str, layout: Layout, tx: Transaction) -> StatInfo:
        return self._stat_manager.get_stat_info(table_name, layout, tx)
