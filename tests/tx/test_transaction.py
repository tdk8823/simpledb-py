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
        """
        Expected Output:
            Transaction 1 committed
            Initial value at location 80 = 1
            Initial value at location 40 = one
            Transaction 2 committed
            New value at location 80 = 2
            New value at location 40 = one!
            Pre-rollback value at location 80 = 9999
            Transaction 3 rolled back
            Post-rollback value at location 80 = 2
            Transaction 4 committed
        """
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
        print(f"Initial value at location 80 = {int_val}")
        print(f"Initial value at location 40 = {string_val}")

        new_int_val = int_val + 1
        new_string_val = string_val + "!"
        tx2.set_int(block_id, 80, new_int_val, True)
        tx2.set_string(block_id, 40, new_string_val, True)
        tx2.commit()

        tx3 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx3.pin(block_id)
        print(f"New value at location 80 = {tx3.get_int(block_id, 80)}")
        print(f"New value at location 40 = {tx3.get_string(block_id, 40)}")

        tx3.set_int(block_id, 80, 9999, True)
        print(f"Pre-rollback value at location 80 = {tx3.get_int(block_id, 80)}")
        tx3.rollback()

        tx4 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx4.pin(block_id)
        print(f"Post-rollback value at location 80 = {tx4.get_int(block_id, 80)}")
        tx4.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
