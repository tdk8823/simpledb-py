from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from simpledbpy.metadata import MetadataManager, StatInfo
from simpledbpy.parser import (
    CraeteViewData,
    CreateIndexData,
    CreateTableData,
    DeleteData,
    InsertData,
    ModifyData,
    Parser,
    ParserData,
    QueryData,
)
from simpledbpy.query import Predicate, ProductScan, ProjectScan, SelectScan, TableScan, UpdateScan
from simpledbpy.record import Layout, Schema
from simpledbpy.tx.transaction import Transaction

if TYPE_CHECKING:
    from simpledbpy.query import Scan


class Plan(ABC):
    """The interface implemented by each query plan.
    There is a Plan class for each relational algebra operator.
    """

    @abstractmethod
    def open(self) -> Scan:
        """Opens a scan corresponding to this plan. The scan will be positioned before its first record.

        Returns:
            Scan: a scan
        """

        raise NotImplementedError

    @abstractmethod
    def blocks_accessed(self) -> int:
        """Returns an estimate of the number of block accesses that will occur when the scan is read to completion.

        Returns:
            int: the estimated number of block accesses
        """

        raise NotImplementedError

    @abstractmethod
    def records_output(self) -> int:
        """Returns an estimate of the number of records in the query's output table.

        Returns:
            int: the estimated number of output records
        """

        raise NotImplementedError

    @abstractmethod
    def distinct_values(self, field_name: str) -> int:
        """Returns an estimate of the number of distinct values for the specified field in the query's output table.

        Args:
            field_name (str): the name of a field

        Returns:
            int: the estimated number of distinct field values in the output
        """

        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema of the query.

        Returns:
            Schema: the query's schema
        """

        raise NotImplementedError


class TablePlan(Plan):
    """The Plan class corresponding to a table."""

    _table_name: str
    _tx: Transaction
    _layout: Layout
    _stat_info: StatInfo

    def __init__(self, tx: Transaction, table_name: str, metadata_manager: MetadataManager) -> None:
        """Creates a leaf node in the query tree corresponding to the specified table.

        Args:
            tx (Transaction): the calling transaction
            table_name (str): the name of the table
            metadata_manager (MetadataManager): the metadata manager
        """

        self._table_name = table_name
        self._tx = tx
        self._layout = metadata_manager.get_layout(table_name, tx)
        self._stat_info = metadata_manager.get_stat_info(table_name, self._layout, tx)

    def open(self) -> Scan:
        """Creates a table scan for this query.

        Returns:
            Scan: a table scan
        """
        return TableScan(self._tx, self._table_name, self._layout)

    def blocks_accessed(self) -> int:
        """Estimates the number of block accesses for the table, which is obtainable from the statistics manager.

        Returns:
            int: the estimated number of block accesses
        """
        return self._stat_info.blocks_accessed

    def records_output(self) -> int:
        """Estimates the number of records in the table, which is obtainable from the statistics manager.

        Returns:
            int: the estimated number of records
        """
        return self._stat_info.records_output

    def distinct_values(self, field_name: str) -> int:
        """Estimates the number of distinct field values in the table, which is obtainable from the statistics manager.

        Args:
            field_name (str): the name of the field

        Returns:
            int: the estimated number of distinct field values
        """
        return self._stat_info.distinct_values(field_name)

    @property
    def schema(self) -> Schema:
        """Determines the schema of the table, which is obtainable from the catalog manager.

        Returns:
            Schema: the table's schema
        """
        return self._layout.schema


class SelectPlan(Plan):
    """The Plan class corresponding to the <i>select</i> relational algebra operator."""

    _plan: Plan
    _predication: Predicate

    def __init__(self, plan: Plan, predication: Predicate) -> None:
        """Creates a new select node in the query tree, having the specified subquery and predicate.

        Args:
            plan (Plan): the subquery
            predication (Predicate): the predicate
        """
        self._plan = plan
        self._predication = predication

    def open(self) -> Scan:
        """Creates a select scan for this query.

        Returns:
            Scan: a select scan
        """
        scan = self._plan.open()
        return SelectScan(scan, self._predication)

    def blocks_accessed(self) -> int:
        """Estimates the number of block accesses in the selection, which is the same as in the underlying query.

        Returns:
            int: the estimated number of block accesses
        """
        return self._plan.blocks_accessed()

    def records_output(self) -> int:
        """Estimates the number of output records in the selection,
        which is determined by the reduction factor of the predicate.

        Returns:
            int: the estimated number of output records
        """
        return self._plan.records_output() // self._predication.reduction_factor(self._plan)

    def distinct_values(self, field_name: str) -> int:
        """Estimates the number of distinct field values in the projection. If the predicate contains a term equating
        the specified  field to a constant, then this value will be 1. Otherwise, it will be the number of the distinct
        values in the underlying query  (but not more than the size of the output table).

        Args:
            field_name (str): the name of the field

        Returns:
            int: the estimated number of distinct field values
        """
        if self._predication.equal_with_constant(field_name) is not None:
            return 1
        else:
            field_name2 = self._predication.equal_with_field(field_name)
            if field_name2 is not None:
                return min(self._plan.distinct_values(field_name), self._plan.distinct_values(field_name2))
            else:
                return self._plan.distinct_values(field_name)

    @property
    def schema(self) -> Schema:
        """Returns the schema of the selection, which is the same as in the underlying query.

        Returns:
            Schema: the selection's schema
        """
        return self._plan.schema


class ProjectPlan(Plan):
    """The Plan class corresponding to the <i>project</i> relational algebra operator."""

    _plan: Plan
    _schema: Schema

    def __init__(self, plan: Plan, field_names: list[str]) -> None:
        """Creates a new project node in the query tree, having the specified subquery and field list.

        Args:
            plan (Plan): the subquery
            field_names (list[str]): the list of field names
        """
        self._plan = plan
        self._schema = Schema()
        for field_name in field_names:
            self._schema.add(field_name, plan.schema)

    def open(self) -> Scan:
        """Creates a project scan for this query.

        Returns:
            Scan: a project scan
        """
        scan = self._plan.open()
        return ProjectScan(scan, self._schema.fields)

    def blocks_accessed(self) -> int:
        """Estimates the number of block accesses in the projection, which is the same as in the underlying query.

        Returns:
            int: the estimated number of block accesses
        """
        return self._plan.blocks_accessed()

    def records_output(self) -> int:
        """Estimates the number of output records in the projection, which is the same as in the underlying query.

        Returns:
            int: the estimated number of output records
        """
        return self._plan.records_output()

    def distinct_values(self, field_name: str) -> int:
        """Estimates the number of distinct field values in the projection,
        which is the same as in the underlying query.

        Args:
            field_name (str): the name of the field

        Returns:
            int: the estimated number of distinct field values
        """
        return self._plan.distinct_values(field_name)

    @property
    def schema(self) -> Schema:
        """Returns the schema of the projection, which is taken from the field list.

        Returns:
            Schema: the projection's schema
        """
        return self._schema


class ProductPlan(Plan):
    """The Plan class corresponding to the <i>product</i> relational algebra operator."""

    _plan1: Plan
    _plan2: Plan
    _schema: Schema

    def __init__(self, plan1: Plan, plan2: Plan) -> None:
        """Creates a new product node in the query tree, having the two specified subqueries.

        Args:
            plan1 (Plan): the left-hand subquery
            plan2 (Plan): the right-hand subquery
        """
        self._plan1 = plan1
        self._plan2 = plan2
        self._schema = Schema()
        self._schema.add_all(plan1.schema)
        self._schema.add_all(plan2.schema)

    def open(self) -> Scan:
        """Creates a product scan for this query.

        Returns:
            Scan: a product scan
        """
        s1 = self._plan1.open()
        s2 = self._plan2.open()
        return ProductScan(s1, s2)

    def blocks_accessed(self) -> int:
        """Estimates the number of block accesses in the product.
        The formula is: <pre> B(product(p1,p2)) = B(p1) + R(p1)*B(p2) </pre>

        Returns:
            int: the estimated number of block accesses
        """
        return self._plan1.blocks_accessed() + self._plan1.records_output() * self._plan2.blocks_accessed()

    def records_output(self) -> int:
        """Estimates the number of output records in the product.
        The formula is: <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>

        Returns:
            int: the estimated number of output records
        """
        return self._plan1.records_output() * self._plan2.records_output()

    def distinct_values(self, field_name: str) -> int:
        """Estimates the distinct number of field values in the product. Since the product does not increase or
        decrease field values, the estimate is the same as in the appropriate underlying query.

        Args:
            field_name (str): the name of the field
        """
        if self._plan1.schema.has_field(field_name):
            return self._plan1.distinct_values(field_name)
        else:
            return self._plan2.distinct_values(field_name)

    @property
    def schema(self) -> Schema:
        """Returns the schema of the product, which is the union of the schemas of the underlying queries.

        Returns:
            Schema: the product's schema
        """
        return self._schema


class QueryPlanner(ABC):
    """The interface implemented by planners for the SQL select statement."""

    @abstractmethod
    def create_plan(self, query_data: QueryData, tx: Transaction) -> Plan:
        """Creates a plan for the parsed query.

        Args:
            query_data (QueryData): the parsed representation of the query
            tx (Transaction): the calling transaction

        Returns:
            Plan: a plan for that query
        """
        raise NotImplementedError


class BasicQueryPlanner(QueryPlanner):
    """The simplest, most naive query planner possible."""

    _metadata_manager: MetadataManager

    def __init__(self, metadata_manager: MetadataManager) -> None:
        self._metadata_manager = metadata_manager

    def create_plan(self, query_data: QueryData, tx: Transaction) -> Plan:
        """Creates a query plan as follows.
        - It first takes the product of all tables and views.
        - It then selects on the predicate.
        - Finally it projects on the field list.

        Args:
            query_data (QueryData): the parsed representation of the query
            tx (Transaction): the calling transaction

        Returns:
            Plan: a plan for that query
        """

        # Step 1: Create a plan for each mentioned table or view
        plans = []
        for table_name in query_data.table_names:
            view_def = self._metadata_manager.get_view_def(table_name, tx)
            if view_def is not None:  # Recursively plan the view
                parser = Parser(view_def)
                view_data = parser.query()
                plans.append(self.create_plan(view_data, tx))
            else:
                plans.append(TablePlan(tx, table_name, self._metadata_manager))

        # Step 2: Create the product of all table plans
        plan = plans.pop(0)
        for next_plan in plans:
            plan = ProductPlan(plan, next_plan)

        # Step 3: Add a selection plan for the predicate
        plan = SelectPlan(plan, query_data.predication)

        # Step 4: Project on the field names
        plan = ProjectPlan(plan, query_data.field_names)

        return plan


class UpdatePlanner(ABC):
    """The interface implemented by the planners for SQL insert, delete, and modify statements."""

    @abstractmethod
    def execute_insert(self, insert_data: InsertData, tx: Transaction) -> int:
        """Executes the specified insert statement, and returns the number of affected records.

        Args:
            insert_data (InsertData): the parsed representation of the insert statement
            tx (Transaction): the calling transaction

        Returns:
            int: the number of affected records
        """
        raise NotImplementedError

    @abstractmethod
    def execute_delete(self, delete_data: DeleteData, tx: Transaction) -> int:
        """Executes the specified delete statement, and returns the number of affected records.

        Args:
            delete_data (DeleteData): the parsed representation of the delete statement
            tx (Transaction): the calling transaction

        Returns:
            int: the number of affected records
        """
        raise NotImplementedError

    @abstractmethod
    def execute_modify(self, modify_data: ModifyData, tx: Transaction) -> int:
        """Executes the specified modify statement, and returns the number of affected records.

        Args:
            modify_data (ModifyData): the parsed representation of the modify statement
            tx (Transaction): the calling transaction

        Returns:
            int: the number of affected records
        """
        raise NotImplementedError

    @abstractmethod
    def execute_create_table(self, create_table_data: CreateTableData, tx: Transaction) -> int:
        """Executes the specified create table statement, and returns the number of affected records./

        Args:
            create_table_data (CreateTableData): the parsed representation of the create table statement
            tx (Transaction): the calling transaction

        Returns:
            int: the number of affected records
        """
        raise NotImplementedError

    @abstractmethod
    def execute_create_view(self, create_view_data: CraeteViewData, tx: Transaction) -> int:
        """Executes the specified create view statement, and returns the number of affected records.

        Args:
            create_view_data (CraeteViewData): the parsed representation of the create view statement
            tx (Transaction): the calling transaction

        Returns:
            int: the number of affected records
        """
        raise NotImplementedError

    @abstractmethod
    def execute_create_index(self, create_index_data: CreateIndexData, tx: Transaction) -> int:
        """Executes the specified create index statement, and returns the number of affected records.

        Args:
            create_index_data (CreateIndexData): the parsed representation of the create index statement
            tx (Transaction): the calling transaction

        Returns:
            int: the number of affected records
        """
        raise NotImplementedError


class BasicUpdatePlanner(UpdatePlanner):
    _metadata_manager: MetadataManager

    def __init__(self, metadata_manager: MetadataManager) -> None:
        self._metadata_manager = metadata_manager

    def execute_delete(self, delete_data: DeleteData, tx: Transaction) -> int:
        plan: Plan = TablePlan(tx, delete_data.table_name, self._metadata_manager)
        plan = SelectPlan(plan, delete_data.predication)
        update_scan = plan.open()
        assert isinstance(update_scan, UpdateScan)
        count = 0
        while update_scan.next():
            update_scan.delete()
            count += 1
        update_scan.close()
        return count

    def execute_modify(self, modify_data: ModifyData, tx: Transaction) -> int:
        plan: Plan = TablePlan(tx, modify_data.table_name, self._metadata_manager)
        plan = SelectPlan(plan, modify_data.predication)
        update_scan = plan.open()
        assert isinstance(update_scan, UpdateScan)
        count = 0
        while update_scan.next():
            value = modify_data.new_value.evaluate(update_scan)
            update_scan.set_value(modify_data.field_name, value)
            count += 1
        update_scan.close()
        return count

    def execute_insert(self, insert_data: InsertData, tx: Transaction) -> int:
        plan: Plan = TablePlan(tx, insert_data.table_name, self._metadata_manager)
        update_scan = plan.open()
        assert isinstance(update_scan, UpdateScan)
        update_scan.insert()
        for field_name, value in zip(insert_data.field_names, insert_data.values):
            update_scan.set_value(field_name, value)
        return 1

    def execute_create_table(self, create_table_data: CreateTableData, tx: Transaction) -> int:
        self._metadata_manager.create_table(create_table_data.table_name, create_table_data.schema, tx)
        return 0

    def execute_create_view(self, create_view_data: CraeteViewData, tx: Transaction) -> int:
        self._metadata_manager.create_view(create_view_data.view_name, create_view_data.view_name, tx)
        return 0

    def execute_create_index(self, create_index_data: CreateIndexData, tx: Transaction) -> int:
        self._metadata_manager.create_index(
            create_index_data.index_name, create_index_data.table_name, create_index_data.field_name, tx
        )
        return 0


class Planner:
    """The object that executes SQL statements."""

    _query_planner: QueryPlanner
    _update_planner: UpdatePlanner

    def __init__(self, query_planner: QueryPlanner, update_planner: UpdatePlanner) -> None:
        self._query_planner = query_planner
        self._update_planner = update_planner

    def create_query_plan(self, query: str, tx: Transaction) -> Plan:
        """Creates a plan for an SQL select statement, using the supplied planner.

        Args:
            query (str): the SQL query string
            tx (Transaction): the transaction

        Returns:
            Plan: the scan corresponding to the query plan
        """
        parser = Parser(query)
        query_data = parser.query()
        self._verify_query(query_data)
        return self._query_planner.create_plan(query_data, tx)

    def execute_update(self, update_command: str, tx: Transaction) -> int:
        """Executes an SQL insert, delete, modify, or create statement. The method dispatches to the appropriate method
        of the supplied update planner, depending on what the parser returns.

        Args:
            update_command (str): the SQL update string
            tx (Transaction): the transaction

        Returns:
            int: the integer donating the number of affected records
        """
        parser = Parser(update_command)
        update_data = parser.update_command()
        self._verify_update(update_data)
        if isinstance(update_data, InsertData):
            return self._update_planner.execute_insert(update_data, tx)
        elif isinstance(update_data, DeleteData):
            return self._update_planner.execute_delete(update_data, tx)
        elif isinstance(update_data, ModifyData):
            return self._update_planner.execute_modify(update_data, tx)
        elif isinstance(update_data, CreateTableData):
            return self._update_planner.execute_create_table(update_data, tx)
        elif isinstance(update_data, CraeteViewData):
            return self._update_planner.execute_create_view(update_data, tx)
        elif isinstance(update_data, CreateIndexData):
            return self._update_planner.execute_create_index(update_data, tx)
        else:
            return 0

    def _verify_query(self, query_data: QueryData) -> None:
        """SimpleDB does not verify queries, although it should.

        Args:
            query_data (QueryData): the parsed representation of the query
        """
        pass

    def _verify_update(self, update_data: ParserData) -> None:
        """SimpleDB does not verify updates, although it should.

        Args:
            update_data (ParserData): the parsed representation of the update
        """
        pass
