import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import BlockId, FileManager
from simpledbpy.log import LogManager
from simpledbpy.tx.transaction import Transaction


class TestTransaction(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")
        self.num_buffers = 8
        self.buffer_manager = BufferManager(self.file_manager, self.log_manager, self.num_buffers)

    def test_basic_usage(self) -> None:
        tx1 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        block_id = BlockId("testfile", 1)
        tx1.pin(block_id)

        # The block initially contains unknown bytes, so don't log those values here.
        tx1.set_int(block_id, 80, 1, False)
        tx1.set_string(block_id, 40, "one", False)
        tx1.commit()

        tx2 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx2.pin(block_id)
        int_val = tx2.get_int(block_id, 80)
        string_val = tx2.get_string(block_id, 40)
        self.assertEqual(int_val, 1)
        self.assertEqual(string_val, "one")

        new_int_val = int_val + 1
        new_string_val = string_val + "!"
        tx2.set_int(block_id, 80, new_int_val, True)
        tx2.set_string(block_id, 40, new_string_val, True)
        tx2.commit()

        tx3 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx3.pin(block_id)
        assert tx3.get_int(block_id, 80) == 2
        assert tx3.get_string(block_id, 40) == "one!"

        tx3.set_int(block_id, 80, 9999, True)
        assert tx3.get_int(block_id, 80) == 9999
        tx3.rollback()

        tx4 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx4.pin(block_id)
        assert tx4.get_int(block_id, 80) == 2
        tx4.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
