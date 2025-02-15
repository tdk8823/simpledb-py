import unittest

from simpledbpy.parser import BadSyntaxException, Lexer


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
