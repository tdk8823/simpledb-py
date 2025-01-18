import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import BlockId, FileManager
from simpledbpy.log import LogManager
from simpledbpy.tx.transaction import Transaction


class TestConcurrency(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")
        self.num_buffers = 8
        self.buffer_manager = BufferManager(self.file_manager, self.log_manager, self.num_buffers)

    def test_basic_usage(self) -> None:

        class A(threading.Thread):
            def __init__(self, file_manager: FileManager, log_manager: LogManager, buffer_manager: BufferManager):
                super().__init__()
                self.file_manager = file_manager
                self.log_manager = log_manager
                self.buffer_manager = buffer_manager

            def run(self) -> None:
                try:
                    txA = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
                    blk1 = BlockId("testfile", 1)
                    blk2 = BlockId("testfile", 2)
                    txA.pin(blk1)
                    txA.pin(blk2)
                    print("Tx A: request slock 1")
                    txA.get_int(blk1, 0)
                    print("Tx A: receive slock 1")
                    time.sleep(1)
                    print("Tx A: request slock 2")
                    txA.get_int(blk2, 0)
                    print("Tx A: receive slock 2")
                    txA.commit()
                    print("Tx A: commit")
                except Exception as e:
                    print(f"Exception occurred: {e}")

        class B(threading.Thread):
            def __init__(self, file_manager: FileManager, log_manager: LogManager, buffer_manager: BufferManager):
                super().__init__()
                self.file_manager = file_manager
                self.log_manager = log_manager
                self.buffer_manager = buffer_manager

            def run(self) -> None:
                try:
                    txB = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
                    blk1 = BlockId("testfile", 1)
                    blk2 = BlockId("testfile", 2)
                    txB.pin(blk1)
                    txB.pin(blk2)
                    print("Tx B: request xlock 2")
                    txB.set_int(blk2, 0, 0, False)
                    print("Tx B: receive xlock 2")
                    time.sleep(1)
                    print("Tx B: request slock 1")
                    txB.get_int(blk1, 0)
                    print("Tx B: receive slock 1")
                    txB.commit()
                    print("Tx B: commit")

                except Exception as e:
                    print(f"Exception occurred: {e}")

        class C(threading.Thread):
            def __init__(self, file_manager: FileManager, log_manager: LogManager, buffer_manager: BufferManager):
                super().__init__()
                self.file_manager = file_manager
                self.log_manager = log_manager
                self.buffer_manager = buffer_manager

            def run(self) -> None:
                try:
                    txC = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
                    blk1 = BlockId("testfile", 1)
                    blk2 = BlockId("testfile", 2)
                    txC.pin(blk1)
                    txC.pin(blk2)
                    time.sleep(0.5)
                    print("Tx C: request xlock 1")
                    txC.set_int(blk1, 0, 0, False)
                    print("Tx C: receive xlock 1")
                    time.sleep(1)
                    print("Tx C: request slock 2")
                    txC.get_int(blk2, 0)
                    print("Tx C: receive slock 2")
                    txC.commit()
                    print("Tx C: commit")

                except Exception as e:
                    print(f"Exception occurred: {e}")

        a = A(self.file_manager, self.log_manager, self.buffer_manager)
        b = B(self.file_manager, self.log_manager, self.buffer_manager)
        c = C(self.file_manager, self.log_manager, self.buffer_manager)
        a.start()
        b.start()
        c.start()
        a.join()
        b.join()
        c.join()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
