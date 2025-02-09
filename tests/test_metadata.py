import math
import random
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import FileManager
from simpledbpy.log import LogManager
from simpledbpy.metadata import MetadataManager
from simpledbpy.record import Schema, TableScan, Types
from simpledbpy.tx.transaction import Transaction


class TestMetadataManager(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")
        self.num_buffers = 8
        self.buffer_manager = BufferManager(self.file_manager, self.log_manager, self.num_buffers)

    def test_basic_usage(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        metadata_manager = MetadataManager(is_new=True, tx=tx)

        schema = Schema()
        schema.add_int_field("A")
        schema.add_string_field("B", 9)

        # test table metadata
        metadata_manager.create_table("MyTable", schema, tx)
        layout = metadata_manager.get_layout("MyTable", tx)
        schema_from_metadata_manager = layout.schema
        self.assertEqual(schema.fields, schema_from_metadata_manager.fields)
        self.assertEqual(
            layout.slot_size, 4 + 4 + 4 + 9
        )  # 4 bytes for flag, 4 bytes for int, 4 bytes for string length, 9 bytes for string
        for field in schema_from_metadata_manager.fields:
            if field == "A":
                self.assertEqual(Types.INTEGER, schema_from_metadata_manager.type(field))
            elif field == "B":
                self.assertEqual(Types.VARCHAR, schema_from_metadata_manager.type(field))
                self.assertEqual(9, schema_from_metadata_manager.length(field))
            else:
                self.fail("Unknown field")

        # test stat metadata
        num_records = 50
        table_scan = TableScan(tx, "MyTable", layout)
        for _ in range(num_records):
            table_scan.insert()
            int_value = round(random.random() * 50)
            string_value = f"rec{int_value}"
            table_scan.set_int("A", int_value)
            table_scan.set_string("B", string_value)
        stat_info = metadata_manager.get_stat_info("MyTable", layout, tx)
        records_per_block = self.block_size // layout.slot_size
        blocks_accessed = math.ceil(num_records / records_per_block)
        self.assertEqual(num_records, stat_info.records_output)
        self.assertEqual(blocks_accessed, stat_info.blocks_accessed)
        self.assertEqual(1 + num_records // 3, stat_info.distinct_values("A"))  # dummy value
        self.assertEqual(1 + num_records // 3, stat_info.distinct_values("B"))  # dummy value

        tx.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
