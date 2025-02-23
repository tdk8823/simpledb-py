import random
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from simpledbpy.buffer import BufferManager
from simpledbpy.file import FileManager
from simpledbpy.log import LogManager
from simpledbpy.metadata import MetadataManager
from simpledbpy.plan import BasicQueryPlanner, BasicUpdatePlanner, Planner, ProjectPlan, SelectPlan, TablePlan
from simpledbpy.query import Constant, Expression, Predicate, TableScan, Term
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

        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        self.metadata_manager = MetadataManager(is_new=True, tx=tx)
        tx.commit()

        self._create_student_table()

    def test_planner(self) -> None:
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        query_planner = BasicQueryPlanner(self.metadata_manager)
        update_planner = BasicUpdatePlanner(self.metadata_manager)
        planner = Planner(query_planner, update_planner)
        query = "create table T1(A int, B varchar(9))"
        planner.execute_update(query, tx)

        n = 200
        for _ in range(n):
            print(_)
            int_value = round(random.random() * 50)
            string_value = f"rec{int_value}"
            query = f"insert into T1(A, B) values({int_value}, '{string_value}')"
            planner.execute_update(query, tx)

        query = "select B from T1 where A = 10"
        plan = planner.create_query_plan(query, tx)
        scan = plan.open()
        while scan.next():
            print(scan.get_string("B"))
            self.assertEqual(scan.get_string("B"), "rec10")
        scan.close()

        tx.commit()

    # def test_single_table_plan(self) -> None:
    #     tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)

    #     # the STUDENT node
    #     plan1 = TablePlan(tx, "student", self.metadata_manager)
    #     self.assertEqual(9, plan1.records_output())
    #     self.assertEqual(1, plan1.blocks_accessed())

    #     # the Select node for "major = 10"
    #     term = Term(Expression("MajorId"), Expression(Constant(10)))
    #     predication = Predicate(term)
    #     plan2 = SelectPlan(plan1, predication)

    #     # the Select node for "GradYear = 2022"
    #     term = Term(Expression("GradYear"), Expression(Constant(2022)))
    #     predication = Predicate(term)
    #     plan3 = SelectPlan(plan2, predication)

    #     # the Project node
    #     fields = ["SName", "MajorId", "GradYear"]
    #     plan4 = ProjectPlan(plan3, fields)
    #     self.assertEqual(plan3.blocks_accessed(), plan4.blocks_accessed())
    #     self.assertEqual(plan3.records_output(), plan4.records_output())

    #     scan2 = plan2.open()
    #     while scan2.next():
    #         self.assertEqual(10, scan2.get_int("MajorId"))

    #     scan4 = plan4.open()
    #     while scan4.next():
    #         self.assertRaises(RuntimeError, scan4.get_int, "SId")

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def _create_student_table(self) -> None:

        schema = Schema()
        schema.add_int_field("SId")
        schema.add_string_field("SName", 10)
        schema.add_int_field("MajorId")
        schema.add_int_field("GradYear")
        layout = Layout(schema)
        tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
        self.metadata_manager.create_table("student", schema, tx)
        table_scan = TableScan(tx, "student", layout)
        records = [
            (1, "joe", 10, 2021),
            (2, "amy", 20, 2020),
            (3, "max", 10, 2022),
            (4, "sue", 20, 2022),
            (5, "bob", 30, 2020),
            (6, "kim", 20, 2020),
            (7, "pat", 30, 2021),
            (8, "lee", 10, 2019),
            (9, "dan", 30, 2021),
        ]
        for record in records:
            table_scan.insert()
            table_scan.set_int("SId", record[0])
            table_scan.set_string("SName", record[1])
            table_scan.set_int("MajorId", record[2])
            table_scan.set_int("GradYear", record[3])
        table_scan.close()
        tx.commit()

    # def _create_dept_table(self) -> None:
    #     schema = Schema()
    #     schema.add_int_field("DId")
    #     schema.add_string_field("DName", 8)
    #     layout = Layout(schema)
    #     tx = Transaction(self.file_manager, self.log_manager, self.buffer_manager)
    #     self.metadata_manager.create_table("dept", schema, tx)
    #     table_scan = TableScan(tx, "dept", layout)
    #     records = [
    #         (10, "compsci"),
    #         (20, "math"),
    #         (30, "drama"),
    #     ]
    #     for record in records:
    #         table_scan.insert()
    #         table_scan.set_int("DId", record[0])
    #         table_scan.set_string("DName", record[1])
    #     table_scan.close()
    #     tx.commit()
