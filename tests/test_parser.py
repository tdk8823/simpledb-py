import unittest

from simpledbpy.parser import (
    BadSyntaxException,
    CraeteViewData,
    CreateIndexData,
    CreateTableData,
    DeleteData,
    InsertData,
    Lexer,
    ModifyData,
    Parser,
)
from simpledbpy.query import Constant, Expression, Schema


class TestLexer(unittest.TestCase):

    def test_keywords(self) -> None:
        sql: str = "SELECT id, name FROM test_table WHERE id = 10"
        lexer: Lexer = Lexer(sql)
        self.assertTrue(lexer.match_keyword("select"))
        lexer.eat_keyword("select")
        self.assertTrue(lexer.match_id())
        self.assertEqual(lexer.eat_id(), "id")
        self.assertTrue(lexer.match_delim(","))
        lexer.eat_delim(",")
        self.assertTrue(lexer.match_id())
        self.assertEqual(lexer.eat_id(), "name")
        self.assertTrue(lexer.match_keyword("from"))
        lexer.eat_keyword("from")
        self.assertTrue(lexer.match_id())
        self.assertEqual(lexer.eat_id(), "test_table")
        self.assertTrue(lexer.match_keyword("where"))
        lexer.eat_keyword("where")
        self.assertTrue(lexer.match_id())
        self.assertEqual(lexer.eat_id(), "id")
        self.assertTrue(lexer.match_delim("="))
        lexer.eat_delim("=")
        self.assertTrue(lexer.match_int_constant())
        self.assertEqual(lexer.eat_int_constant(), 10)

    def test_string_literals(self) -> None:
        sql: str = "INSERT INTO test_table VALUES ('test string', 12345)"
        lexer: Lexer = Lexer(sql)
        self.assertTrue(lexer.match_keyword("insert"))
        lexer.eat_keyword("insert")
        self.assertTrue(lexer.match_keyword("into"))
        lexer.eat_keyword("into")
        self.assertTrue(lexer.match_id())
        self.assertEqual(lexer.eat_id(), "test_table")
        self.assertTrue(lexer.match_keyword("values"))
        lexer.eat_keyword("values")
        self.assertTrue(lexer.match_delim("("))
        lexer.eat_delim("(")
        self.assertTrue(lexer.match_string_constant())
        self.assertEqual(lexer.eat_string_constant(), "test string")
        self.assertTrue(lexer.match_delim(","))
        lexer.eat_delim(",")
        self.assertTrue(lexer.match_int_constant())
        self.assertEqual(lexer.eat_int_constant(), 12345)
        self.assertTrue(lexer.match_delim(")"))
        lexer.eat_delim(")")

    def test_invalid_syntax(self) -> None:
        sql: str = "UPDATE test_table SET id = 'string' WHERE id = 123"
        lexer: Lexer = Lexer(sql)
        lexer.eat_keyword("update")
        lexer.eat_id()
        lexer.eat_keyword("set")
        lexer.eat_id()
        lexer.eat_delim("=")
        lexer.eat_string_constant()
        lexer.eat_keyword("where")
        lexer.eat_id()
        lexer.eat_delim("=")
        with self.assertRaises(BadSyntaxException):
            lexer._next_token()  # This should read the invalid character '!'
            lexer.eat_int_constant()

    def test_delimiters(self) -> None:
        sql: str = "CREATE TABLE test_table (id INT, name VARCHAR)"
        lexer: Lexer = Lexer(sql)
        lexer.eat_keyword("create")
        lexer.eat_keyword("table")
        lexer.eat_id()
        lexer.eat_delim("(")
        lexer.eat_id()
        lexer.eat_keyword("int")
        lexer.eat_delim(",")
        lexer.eat_id()
        lexer.eat_keyword("varchar")
        lexer.eat_delim(")")

    def test_unexpected_character(self) -> None:
        sql: str = "SELECT id FROM test_table$ WHERE id = 10"
        with self.assertRaises(BadSyntaxException):
            Lexer(sql)  # This should raise exception due to the unexpected character '$'


class TestParser(unittest.TestCase):

    def test_select_query(self) -> None:
        sql = "SELECT id, name FROM users WHERE age = 30"
        parser = Parser(sql)
        query_data = parser.query()

        self.assertEqual(str(query_data), "select id, name from users where age = 30")
        self.assertEqual(query_data.field_names, ["id", "name"])
        self.assertEqual(query_data.table_names, ["users"])
        self.assertEqual(len(query_data.predication._terms), 1)
        term = query_data.predication._terms[0]
        self.assertIsInstance(term._lhs, Expression)
        self.assertIsInstance(term._rhs, Expression)
        self.assertEqual(term._lhs.as_field_name(), "age")
        self.assertEqual(term._rhs.as_constant(), Constant(30))

    def test_insert_query(self) -> None:
        sql = "INSERT INTO users (id, name, age) VALUES (1, 'John Doe', 25)"
        parser = Parser(sql)
        insert_data = parser.update_command()
        assert isinstance(insert_data, InsertData)

        self.assertEqual(insert_data.table_name, "users")
        self.assertEqual(insert_data.field_names, ["id", "name", "age"])
        self.assertEqual(len(insert_data.values), 3)
        self.assertEqual(insert_data.values[0].as_int(), 1)
        self.assertEqual(insert_data.values[1].as_string(), "John Doe")
        self.assertEqual(insert_data.values[2].as_int(), 25)

    def test_update_query(self) -> None:
        sql = "UPDATE users SET age = 26 WHERE id = 1"
        parser = Parser(sql)
        modify_data = parser.update_command()
        assert isinstance(modify_data, ModifyData)

        self.assertEqual(modify_data.table_name, "users")
        self.assertEqual(modify_data.field_name, "age")
        self.assertEqual(modify_data.new_value.as_constant(), Constant(26))
        self.assertEqual(len(modify_data.predication._terms), 1)
        term = modify_data.predication._terms[0]
        self.assertIsInstance(term._lhs, Expression)
        self.assertIsInstance(term._rhs, Expression)
        self.assertEqual(term._lhs.as_field_name(), "id")
        self.assertEqual(term._rhs.as_constant(), Constant(1))

    def test_delete_query(self) -> None:
        sql = "DELETE FROM users WHERE id = 1"
        parser = Parser(sql)
        delete_data = parser.update_command()
        assert isinstance(delete_data, DeleteData)

        self.assertEqual(delete_data.table_name, "users")
        self.assertEqual(len(delete_data.predication._terms), 1)
        term = delete_data.predication._terms[0]
        self.assertIsInstance(term._lhs, Expression)
        self.assertIsInstance(term._rhs, Expression)
        self.assertEqual(term._lhs.as_field_name(), "id")
        self.assertEqual(term._rhs.as_constant(), Constant(1))

    def test_create_table_query(self) -> None:
        sql = "CREATE TABLE users (id INT, name VARCHAR(100))"
        parser = Parser(sql)
        create_table_data = parser.update_command()
        assert isinstance(create_table_data, CreateTableData)

        self.assertEqual(create_table_data.table_name, "users")
        expected_schema = Schema()
        expected_schema.add_int_field("id")
        expected_schema.add_string_field("name", 100)
        self.assertEqual(create_table_data.schema, expected_schema)

    def test_create_view_query(self) -> None:
        sql = "CREATE VIEW user_view AS SELECT id, name FROM users"
        parser = Parser(sql)
        create_view_data = parser.update_command()
        assert isinstance(create_view_data, CraeteViewData)

        self.assertEqual(create_view_data.view_name, "user_view")
        self.assertEqual(create_view_data.query_data.field_names, ["id", "name"])
        self.assertEqual(create_view_data.query_data.table_names, ["users"])

    def test_create_index_query(self) -> None:
        sql = "CREATE INDEX user_index ON users (id)"
        parser = Parser(sql)
        create_index_data = parser.update_command()
        assert isinstance(create_index_data, CreateIndexData)

        self.assertEqual(create_index_data.index_name, "user_index")
        self.assertEqual(create_index_data.table_name, "users")
        self.assertEqual(create_index_data.field_name, "id")
