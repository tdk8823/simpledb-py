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

    def test_basic_usage(self) -> None:
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

        # After Initialization
        block_id0_contents = self._get_block_contents(block_id0)
        block_id1_contents = self._get_block_contents(block_id1)
        self.assertEqual(block_id0_contents, "0 4 8 12 16 20 abc")
        self.assertEqual(block_id1_contents, "0 4 8 12 16 20 def")

    def _get_block_contents(self, block_id: BlockId) -> str:
        page = Page(self.file_manager.block_size)
        self.file_manager.read(block_id, page)
        contents = ""
        position = 0
        for _ in range(6):
            contents += f"{page.get_int(position)} "
            position += 4
        contents += f"{page.get_string(30)}"
        return contents

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

        # After Modification
        block_id0_contents = self._get_block_contents(block_id0)
        block_id1_contents = self._get_block_contents(block_id1)
        self.assertEqual(block_id0_contents, "100 104 108 112 116 120 uvw")
        self.assertEqual(block_id1_contents, "100 104 108 112 116 120 xyz")

        tx3.rollback()

        # After Rollback
        block_id0_contents = self._get_block_contents(block_id0)
        block_id1_contents = self._get_block_contents(block_id1)
        self.assertEqual(block_id0_contents, "0 4 8 12 16 20 abc")
        self.assertEqual(block_id1_contents, "100 104 108 112 116 120 xyz")

        # assume tx3 and tx4 are flushed to disk, but not committed.
        tx3._concurrency_manager.release()
        tx4._concurrency_manager.release()

    def _recover(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        tx.recover()

        # After Recovery
        block_id0 = BlockId("testfile", 0)
        block_id1 = BlockId("testfile", 1)
        block_id0_contents = self._get_block_contents(block_id0)
        block_id1_contents = self._get_block_contents(block_id1)
        self.assertEqual(block_id0_contents, "0 4 8 12 16 20 abc")
        self.assertEqual(block_id1_contents, "0 4 8 12 16 20 def")

        tx.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
