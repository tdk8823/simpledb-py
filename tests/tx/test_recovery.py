import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import BlockId, FileManager, Page
from simpledbpy.log import LogManager
from simpledbpy.tx.transaction import Transaction


class TestRecoveryManager(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")
        self.num_buffers = 8
        self.buffer_manager = BufferManager(self.file_manager, self.log_manager, self.num_buffers)
        # self.block_id0 = BlockId("testfile", 0)
        # self.block_id1 = BlockId("testfile", 1)

    def test_basic_usage(self) -> None:
        """Expected Output:
        Transaction 1 committed
        Transaction 2 committed
        After Initialization:
            0 0 4 4 8 8 12 12 16 16 20 20 abc def
        After Modification:
            100 100 104 104 108 108 112 112 116 116 120 120 uvw xyz
        Transaction 3 rolled back
        After Rollback:
            0 100 4 104 8 108 12 112 16 116 20 120 abc xyz
        After Recovery:
            0 0 4 4 8 8 12 12 16 16 20 20 abc def
        """
        self._initiate()
        self._modify()
        self._recover()

    def _initiate(self) -> None:
        tx1 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx2 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        block_id0 = BlockId("testfile", 0)
        block_id1 = BlockId("testfile", 1)
        tx1.pin(block_id0)
        tx2.pin(block_id1)

        position = 0
        for _ in range(6):
            tx1.set_int(block_id0, position, position, False)
            tx2.set_int(block_id1, position, position, False)
            position += 4  # Integer.BYTES

        tx1.set_string(block_id0, 30, "abc", False)
        tx2.set_string(block_id1, 30, "def", False)
        tx1.commit()
        tx2.commit()
        self._print_values("After Initialization:")

    def _modify(self) -> None:
        tx3 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx4 = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        block_id0 = BlockId("testfile", 0)
        block_id1 = BlockId("testfile", 1)
        tx3.pin(block_id0)
        tx4.pin(block_id1)

        position = 0
        for _ in range(6):
            tx3.set_int(block_id0, position, position + 100, True)
            tx4.set_int(block_id1, position, position + 100, True)
            position += 4

        tx3.set_string(block_id0, 30, "uvw", True)
        tx4.set_string(block_id1, 30, "xyz", True)
        self.buffer_manager.flush_all(3)
        self.buffer_manager.flush_all(4)
        self._print_values("After Modification:")

        tx3.rollback()
        self._print_values("After Rollback:")

        # assume tx3 and tx4 are flushed to disk, but not committed.
        tx3._concurrency_manager.release()
        tx4._concurrency_manager.release()

    def _recover(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx.recover()
        self._print_values("After Recovery:")

    def _print_values(self, message: str) -> None:
        response = f"{message}\n    "
        page0 = Page(self.file_manager.block_size)
        page1 = Page(self.file_manager.block_size)
        block_id0 = BlockId("testfile", 0)
        block_id1 = BlockId("testfile", 1)
        self.file_manager.read(block_id0, page0)
        self.file_manager.read(block_id1, page1)
        position = 0
        for _ in range(6):
            response += f"{page0.get_int(position)} "
            response += f"{page1.get_int(position)} "
            position += 4  # Integer.BYTES

        response += f"{page0.get_string(30)} "
        response += f"{page1.get_string(30)} "
        print(response)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
