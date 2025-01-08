import struct
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.file import BlockId, FileManager, Page
import os


class TestBlockId(unittest.TestCase):
    def test_blockid_str(self) -> None:
        block_id = BlockId("testfile", 1)
        self.assertEqual(str(block_id), "[file testfile, block 1]")


class TestPage(unittest.TestCase):
    def setUp(self) -> None:
        self.page = Page(100)

    def tearDown(self) -> None:
        self.assertEqual(len(self.page._byte_buffer), 100)

    def test_set_and_get_int(self) -> None:
        self.page.set_int(0, 12345678)
        result = self.page.get_int(0)
        self.assertEqual(result, 12345678)

    def test_set_and_get_bytes(self) -> None:
        test_bytes = b"\x01\x02\x03\x04"
        self.page.set_bytes(0, test_bytes)
        result = self.page.get_bytes(0)
        self.assertEqual(result, test_bytes)

    def test_set_and_get_string(self) -> None:
        test_string = "hello"
        self.page.set_string(0, test_string)
        result = self.page.get_string(0)
        self.assertEqual(result, test_string)

    def test_max_length(self) -> None:
        strlen = 10
        expected_length = 4 + strlen
        result = Page.max_length(strlen)
        self.assertEqual(result, expected_length)

    def test_contents(self) -> None:
        self.page.set_int(0, 65536)
        contents = self.page.contents()
        expected_contents = bytearray(100)
        expected_contents[0:4] = struct.pack(Page.FORMAT, 65536)
        self.assertEqual(contents, expected_contents)

    def test_set_int_out_of_bounds(self) -> None:
        with self.assertRaises(ValueError):
            self.page.set_int(97, 123)

    def test_get_int_out_of_bounds(self) -> None:
        with self.assertRaises(ValueError):
            self.page.get_int(100)

    def test_set_bytes_out_of_bounds(self) -> None:
        test_bytes = b"\x01\x02\x03\x04"
        with self.assertRaises(ValueError):
            self.page.set_bytes(97, test_bytes)

    def test_get_bytes_out_of_bounds(self) -> None:
        with self.assertRaises(ValueError):
            self.page.get_bytes(100)

    def test_set_string_out_of_bounds(self) -> None:
        test_string = "hello"
        with self.assertRaises(ValueError):
            self.page.set_string(97, test_string)

    def test_get_string_out_of_bounds(self) -> None:
        with self.assertRaises(ValueError):
            self.page.get_string(100)


class TestFileManager(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 1024
        self.file_manager = FileManager(self.db_directory, self.block_size)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_append_and_length(self) -> None:
        filename = "testfile"
        initial_length = self.file_manager.length(filename)
        self.assertEqual(initial_length, 0)

        self.file_manager.append(filename)
        length_after_append = self.file_manager.length(filename)
        self.assertEqual(length_after_append, 1)

    def test_write_and_read(self) -> None:
        filename = "testfile"
        block_id = self.file_manager.append(filename)

        page = Page(self.block_size)
        page.set_int(0, 12345678)
        page.set_string(4, "hello")
        page.set_bytes(9, b"\x01\x02\x03\x04")
        self.file_manager.write(block_id, page)

        read_page = Page(self.block_size)
        self.file_manager.read(block_id, read_page)

        self.assertEqual(page.contents(), read_page.contents())

    def test_concurrent_access(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        def write_and_read_concurrently(block_id: BlockId, data: bytearray) -> bytearray:
            page = Page(self.block_size)
            page.contents()[:] = data
            self.file_manager.write(block_id, page)
            read_page = Page(self.block_size)
            self.file_manager.read(block_id, read_page)
            return read_page.contents()

        block_id = self.file_manager.append("testfile")
        data = bytearray((i % 256 for i in range(self.block_size)))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(write_and_read_concurrently, block_id, data) for _ in range(10)]

        for future in futures:
            self.assertEqual(future.result(), data)


if __name__ == "__main__":
    unittest.main()
