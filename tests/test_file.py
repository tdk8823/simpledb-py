import struct
import unittest

from simpledbpy.file import BlockId, Page


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


if __name__ == "__main__":
    unittest.main()
