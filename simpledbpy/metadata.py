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


class MetadataManager:
    _table_manager: TableManager

    def __init__(self, is_new: bool, tx: Transaction) -> None:
        self._table_manager = TableManager(is_new, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction) -> None:
        self._table_manager.create_table(table_name, schema, tx)

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        return self._table_manager.get_layout(table_name, tx)
