import random
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import FileManager
from simpledbpy.log import LogManager
from simpledbpy.query import Constant, Expression, Predicate, ProductScan, ProjectScan, SelectScan, TableScan, Term
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

    def test_product_scan(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)

        schema1 = Schema()
        schema1.add_int_field("A")
        schema1.add_string_field("B", 9)
        layout1 = Layout(schema1)
        table_scan1 = TableScan(tx, "T1", layout1)

        schema2 = Schema()
        schema2.add_int_field("C")
        schema2.add_string_field("D", 9)
        layout2 = Layout(schema2)
        table_scan2 = TableScan(tx, "T2", layout2)

        table_scan1.before_first()
        n = 200
        for i in range(n):
            table_scan1.insert()
            table_scan1.set_int("A", i)
            table_scan1.set_string("B", f"aaa{i}")
        table_scan1.close()

        table_scan2.before_first()
        for i in range(n):
            table_scan2.insert()
            table_scan2.set_int("C", n - i - 1)
            table_scan2.set_string("D", f"bbb{n-i-1}")
        table_scan2.close()

        scan1 = TableScan(tx, "T1", layout1)
        scan2 = TableScan(tx, "T2", layout2)
        scan3 = ProductScan(scan1, scan2)
        cnt1 = 0
        cnt2 = n - 1
        while scan3.next():
            self.assertEqual(scan3.get_int("A"), cnt1)
            self.assertEqual(scan3.get_int("C"), cnt2)
            self.assertEqual(scan3.get_string("B"), f"aaa{cnt1}")
            self.assertEqual(scan3.get_string("D"), f"bbb{cnt2}")
            cnt2 -= 1
            if cnt2 == -1:
                cnt2 = n - 1
                cnt1 += 1
        scan3.close()
        tx.commit()

    def test_scan(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)

        schema1 = Schema()
        schema1.add_int_field("A")
        schema1.add_string_field("B", 9)
        layout1 = Layout(schema1)
        scan1 = TableScan(tx, "T", layout1)

        scan1.before_first()
        n = 200
        for _ in range(n):
            scan1.insert()
            int_value = round(random.random() * 50)
            scan1.set_int("A", int_value)
            scan1.set_string("B", f"rec{int_value}")
        scan1.close()

        scan2 = TableScan(tx, "T", layout1)
        c = Constant(10)
        term = Term(Expression("A"), Expression(c))
        predication = Predicate(term)
        self.assertEqual(str(predication), "A = 10")
        scan3 = SelectScan(scan2, predication)
        fields = ["B"]
        scan4 = ProjectScan(scan3, fields)
        while scan4.next():
            self.assertEqual(scan4.get_string("B"), f"rec{10}")
        scan4.close()
        tx.commit()

    def test_scan2(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)

        schema1 = Schema()
        schema1.add_int_field("A")
        schema1.add_string_field("B", 9)
        layout1 = Layout(schema1)
        update_scan1 = TableScan(tx, "T1", layout1)
        update_scan1.before_first()
        n = 200
        for i in range(n):
            update_scan1.insert()
            update_scan1.set_int("A", i)
            update_scan1.set_string("B", f"rec{i}")
        update_scan1.close()

        schema2 = Schema()
        schema2.add_int_field("C")
        schema2.add_string_field("D", 9)
        layout2 = Layout(schema2)
        update_scan2 = TableScan(tx, "T2", layout2)
        update_scan2.before_first()
        for i in range(n):
            update_scan2.insert()
            update_scan2.set_int("C", n - i - 1)
            update_scan2.set_string("D", f"rec{n-i-1}")
        update_scan2.close()

        scan1 = TableScan(tx, "T1", layout1)
        scan2 = TableScan(tx, "T2", layout2)
        scan3 = ProductScan(scan1, scan2)
        term = Term(Expression("A"), Expression("C"))
        predication = Predicate(term)
        scan4 = SelectScan(scan3, predication)

        field_names = ["B", "D"]
        scan5 = ProjectScan(scan4, field_names)
        while scan5.next():
            self.assertEqual(scan5.get_string("B"), scan5.get_string("D"))
        scan5.close()
        tx.commit()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()
