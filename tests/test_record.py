import random
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import FileManager
from simpledbpy.log import LogManager
from simpledbpy.record import Layout, RecordPage, Schema
from simpledbpy.tx.transaction import Transaction


class TestRecordPage(unittest.TestCase):
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

        schema = Schema()
        schema.add_int_field("A")
        schema.add_string_field("B", 9)
        layout = Layout(schema)
        for field_name in layout.schema.fields:
            offset = layout.offset(field_name)
            if field_name == "A":
                self.assertEqual(offset, 4)  # head of slot is 4 bytes
            elif field_name == "B":
                self.assertEqual(offset, 4 + 4)  # 4 bytes for int
            else:
                self.fail(f"unexpected field name {field_name}")

        block_id = tx.append("testfile")
        tx.pin(block_id)
        record_page = RecordPage(tx, block_id, layout)
        record_page.format()

        # test inserted records
        slot = record_page.insert_after(-1)
        inserted_recoreds = {}
        while slot >= 0:
            int_value = round(random.random() * 50)
            string_value = f"rec{int_value}"
            record_page.set_int(slot, "A", int_value)
            record_page.set_string(slot, "B", string_value)
            inserted_recoreds[slot] = (int_value, string_value)
            slot = record_page.insert_after(slot)
        slot = record_page.next_after(-1)
        while slot >= 0:
            int_value, string_value = inserted_recoreds[slot]
            self.assertEqual(record_page.get_int(slot, "A"), int_value)
            self.assertEqual(record_page.get_string(slot, "B"), string_value)
            slot = record_page.next_after(slot)

        # test delete records less than 25
        count = 0
        slot = record_page.next_after(-1)
        while slot >= 0:
            int_value = record_page.get_int(slot, "A")
            string_value = record_page.get_string(slot, "B")
            if int_value < 25:
                count += 1
                record_page.delete(slot)
            slot = record_page.next_after(slot)
        slot = record_page.next_after(-1)
        while slot >= 0:
            int_value, string_value = inserted_recoreds[slot]
            self.assertGreaterEqual(int_value, 25)
            slot = record_page.next_after(slot)

        tx.unpin(block_id)
        tx.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
