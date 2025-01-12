import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from simpledbpy.buffer import Buffer, BufferAbortError, BufferManager
from simpledbpy.file import BlockId, FileManager
from simpledbpy.log import LogManager


class TestBuffer(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")
        self.buffer = Buffer(self.file_manager, self.log_manager)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_initial_pin_count(self) -> None:
        self.assertFalse(self.buffer.is_pinned())

    def test_pin_buffer(self) -> None:
        self.buffer.pin()
        self.assertTrue(self.buffer.is_pinned())
        self.assertEqual(self.buffer._pins, 1)

    def test_unpin_buffer(self) -> None:
        self.buffer.pin()
        self.buffer.unpin()
        self.assertFalse(self.buffer.is_pinned())
        self.assertEqual(self.buffer._pins, 0)

    def test_set_modified(self) -> None:
        self.buffer.set_modified(txnum=1, lsn=2)
        self.assertEqual(self.buffer._txnum, 1)
        self.assertEqual(self.buffer._lsn, 2)

    def test_assign_to_block(self) -> None:
        block_id = self.file_manager.append("testfile")
        self.buffer.assign_to_block(block_id)
        self.assertEqual(self.buffer._block_id, block_id)

    def test_flush(self) -> None:
        block_id = self.file_manager.append("testfile")
        self.buffer.assign_to_block(block_id)
        self.buffer.set_modified(txnum=1, lsn=2)
        self.buffer.flush()
        self.assertEqual(self.buffer._txnum, -1)


class TestBufferManager(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")
        self.num_buffers = 3
        self.buffer_manager = BufferManager(self.file_manager, self.log_manager, self.num_buffers)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_basic_usage(self) -> None:
        buffer1 = self.buffer_manager.pin(BlockId("testfile", 1))
        page = buffer1.contents
        n = page.get_int(80)
        page.set_int(80, n + 1)
        buffer1.set_modified(1, 0)
        self.buffer_manager.unpin(buffer1)

        # One of the following pins should replace the first buffer,
        # and the content of the buffer is written to the disk.
        buffer2 = self.buffer_manager.pin(BlockId("testfile", 2))
        self.buffer_manager.pin(BlockId("testfile", 3))
        self.buffer_manager.pin(BlockId("testfile", 4))

        self.buffer_manager.unpin(buffer2)
        buffer2 = self.buffer_manager.pin(BlockId("testfile", 1))
        page2 = buffer2.contents
        self.assertEqual(page2.get_int(80), n + 1)

    def test_initialization(self) -> None:
        self.assertEqual(len(self.buffer_manager._buffer_pool), self.num_buffers)

    def test_pin_and_unpin_buffer(self) -> None:
        block_id = BlockId("testfile", 1)
        buffer = self.buffer_manager.pin(block_id)
        self.assertIsNotNone(buffer)
        self.assertEqual(buffer._block_id, block_id)
        self.assertEqual(self.buffer_manager._num_available, self.num_buffers - 1)

        self.buffer_manager.unpin(buffer)
        self.assertFalse(buffer.is_pinned())
        self.assertEqual(self.buffer_manager._num_available, self.num_buffers)

    def test_flush_all_buffers(self) -> None:
        buffers = []
        for i in range(self.num_buffers):
            block_id = MagicMock(spec=BlockId)
            block_id.filename = "testfile"
            block_id.block_number = i
            buffers.append(self.buffer_manager.pin(block_id))
        for i, buffer in enumerate(buffers):
            buffer.set_modified(txnum=1, lsn=i)
        with patch.object(Buffer, "flush", autospec=True) as mock_flush:
            self.buffer_manager.flush_all(1)
            self.assertEqual(mock_flush.call_count, self.num_buffers)

    def test_pin_when_all_buffers_busy(self) -> None:
        blocks = [MagicMock(spec=BlockId) for _ in range(self.num_buffers)]
        for i, block in enumerate(blocks):
            block.filename = "testfile"
            block.block_number = i
            self.buffer_manager.pin(block)
        new_block = MagicMock(spec=BlockId)
        with self.assertRaises(BufferAbortError):
            self.buffer_manager.pin(new_block)


if __name__ == "__main__":
    unittest.main()
