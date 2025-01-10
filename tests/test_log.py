import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.file import FileManager, Page
from simpledbpy.log import LogManager


class TestLog(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = TemporaryDirectory()
        self.db_directory = Path(self.tmp_dir.name)
        self.block_size = 400
        self.file_manager = FileManager(self.db_directory, self.block_size)
        self.log_manager = LogManager(self.file_manager, "simpledb.log")

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_basic_usage(self) -> None:
        number_of_records = 0
        for log_record in self.log_manager:
            number_of_records += 1
        self.assertEqual(number_of_records, 0)  # no records just after creation

        self._create_log_records(1, 35)
        self.assertEqual(self.log_manager._latest_lsn, 35)
        self.assertLessEqual(self.log_manager._lagest_saved_lsn, 35)  # not flushed yet
        self.log_manager.flush(35)
        self.assertEqual(self.log_manager._lagest_saved_lsn, 35)  # flushed

        self._create_log_records(36, 70)
        number_of_records = 0
        for log_record in self.log_manager:
            number_of_records += 1
        self.assertEqual(number_of_records, 70)  # flushued before iteration

    def _create_log_records(self, start: int, end: int) -> None:
        for i in range(start, end + 1):
            log_record = self._create_log_record(f"record{i}", i + 100)
            self.log_manager.append(log_record)

    def _create_log_record(self, s: str, n: int) -> bytes:
        """Create a log record having two values: a string and a number

        Args:
            s (str): The string value
            n (int): The number value

        Returns:
            bytes: The log record
        """
        string_position = 0
        number_position = string_position + Page.max_length(len(s))
        b = bytes(number_position + 4)
        page = Page(b)
        page.set_string(string_position, s)
        page.set_int(number_position, n)
        return bytes(page.contents())

    def test_append_log_record(self) -> None:
        # check initial boundary
        boundary = self.log_manager._log_page.get_int(0)
        self.assertEqual(boundary, self.block_size)

        log_record = b"a" * 5
        lsn = self.log_manager.append(log_record)
        self.assertEqual(lsn, 1)

        # check new boundary
        boundary = self.log_manager._log_page.get_int(0)
        self.assertEqual(boundary, self.block_size - 5 - 4)

        # check log record
        log_record_actual = self.log_manager._log_page.get_bytes(boundary)
        self.assertEqual(log_record, log_record_actual)

    def test_append_log_record_with_new_block(self) -> None:
        # initial block is almost full
        log_record = b"a" * 390
        lsn = self.log_manager.append(log_record)

        # new record should be written to a new block
        log_record = b"b" * 10
        lsn = self.log_manager.append(log_record)
        self.assertEqual(lsn, 2)
        self.assertEqual(self.log_manager._current_block.block_number, 1)

    def test_flush(self) -> None:
        log_record = b"testrecord"
        _ = self.log_manager.append(log_record)
        self.log_manager.flush(1)
        page = Page(self.block_size)
        self.file_manager.read(self.log_manager._current_block, page)
        boundary = page.get_int(0)
        log_record_actual = page.get_bytes(boundary)
        self.assertEqual(log_record, log_record_actual)

    def test_log_iterator(self) -> None:
        log_records = [self._create_log_record(f"record{i}", i + 100) for i in range(100)]
        for i, log_record in enumerate(log_records):
            self.log_manager.append(log_record)
        log_records.reverse()
        for log_record_acsuall, log_record_expected in zip(self.log_manager, log_records):
            self.assertEqual(log_record_acsuall, log_record_expected)


if __name__ == "__main__":
    unittest.main()
