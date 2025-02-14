import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import FileManager
from simpledbpy.log import LogManager

# from simpledbpy.query import ProductScan, TableScan
from simpledbpy.record import Layout, Schema
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

    # def test_basic_usage(self) -> None:
    #     tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)

    #     schema1 = Schema()
    #     schema1.add_int_field("A")
    #     schema1.add_string_field("B", 9)
    #     layout1 = Layout(schema1)
    #     table_scan1 = TableScan(tx, "T1", layout1)

    #     schema2 = Schema()
    #     schema2.add_int_field("C")
    #     schema2.add_string_field("D", 9)
    #     layout2 = Layout(schema2)
    #     table_scan2 = TableScan(tx, "T2", layout2)

    #     table_scan1.before_first()
    #     n = 200
    #     for i in range(n):
    #         table_scan1.insert()
    #         table_scan1.set_int("A", i)
    #         table_scan1.set_string("B", f"aaa{i}")
    #     table_scan1.close()

    #     table_scan2.before_first()
    #     for i in range(n):
    #         table_scan2.insert()
    #         table_scan2.set_int("C", n - i - 1)
    #         table_scan2.set_string("D", f"bbb{n-i-1}")
    #     table_scan2.close()

    #     scan1 = TableScan(tx, "T1", layout1)
    #     scan2 = TableScan(tx, "T2", layout2)
    #     scan3 = ProductScan(scan1, scan2)
    #     while scan3.next():
    #         print(scan3.get_string("B"))
    #         # self.assertEqual(scan3.get_int("A") + scan3.get_int("C"), n - 1)
    #         # self.assertEqual(
    #         #     scan3.get_string("B") + scan3.get_string("D"), f"aaa{scan3.get_int('A')}bbb{scan3.get_int('C')}"
    #         # )
    #     scan3.close()
    #     tx.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
