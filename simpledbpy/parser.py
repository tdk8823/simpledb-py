import re
from dataclasses import dataclass
from typing import Collection, List, Optional, Tuple, Union

from simpledbpy.query import Constant, Expression, Predicate, Term
from simpledbpy.record import Schema


class BadSyntaxException(Exception):
    pass


class Lexer:
    """Creates a new lexical analyzer for SQL statement sql."""

    _keywords: Collection[str]
    _tokens: List[Tuple[str, Union[int, str]]]
    _current_token: Optional[Tuple[str, Union[int, str]]]

    def __init__(self, sql: str) -> None:
        self._init_keywords()
        self._tokens = self._tokenize(sql)
        self.current_token = None
        self._next_token()

    def match_delim(self, delim: str) -> bool:
        """Returns true if the current token is the specified delimiter character.

        Args:
            delim (str): a character denoting the delimiter

        Returns:
            bool: True if the delimiter is the current token
        """
        return self._current_token is not None and self._current_token[0] == "DELIM" and self._current_token[1] == delim

    def match_int_constant(self) -> bool:
        """Returns true if the current token is an integer.

        Returns:
            bool: True if the current token is an integer
        """
        return self._current_token is not None and self._current_token[0] == "NUMBER"

    def match_string_constant(self) -> bool:
        """Returns true if the current token is a string.

        Returns:
            bool: True if the current token is a string
        """
        return self._current_token is not None and self._current_token[0] == "STRING"

    def match_keyword(self, keyword: str) -> bool:
        """Returns true if the current token is the specified keyword.

        Args:
            keyword (str): the keyword string

        Returns:
            bool: True if the keyword is the current token
        """
        return self._current_token is not None and self._current_token[0] == keyword

    def match_id(self) -> bool:
        """Returns true if the current token is a legal identifier.

        Returns:
            bool: True if the current token is an identifier
        """
        return self._current_token is not None and self._current_token[0] == "ID"

    def eat_delim(self, delim: str) -> None:
        """Throws an exception if the current token is not the specified delimiter. Otherwise, moves to the next token.

        Args:
            delim (str): a character denoting the delimiter
        """
        if not self.match_delim(delim):
            raise BadSyntaxException(f"Expected delimiter {delim}")
        self._next_token()

    def eat_int_constant(self) -> int:
        """Throws an exception if the current token is not  an integer.
        Otherwise, returns that integer and moves to the next token.

        Returns:
            int: the integer value of the current token
        """
        if not self.match_int_constant():
            raise BadSyntaxException("Expected integer constant")
        value: int = self._current_token[1]  # type: ignore
        self._next_token()
        return value

    def eat_string_constant(self) -> str:
        """Throws an exception if the current token is not  a string.
        Otherwise, returns that string and moves to the next token.

        Returns:
            str: the string value of the current token
        """
        if not self.match_string_constant():
            raise BadSyntaxException("Expected string constant")
        value: str = self._current_token[1]  # type: ignore
        self._next_token()
        return value

    def eat_keyword(self, keyword: str) -> None:
        """Throws an exception if the current token is not the specified keyword. Otherwise, moves to the next token.

        Args:
            keyword (str): the keyword string
        """
        if not self.match_keyword(keyword):
            raise BadSyntaxException(f"Expected keyword {keyword}")
        self._next_token()

    def eat_id(self) -> str:
        """Throws an exception if the current token is not  an identifier.
        Otherwise, returns the identifier string  and moves to the next token.

        Returns:
            str: the string value of the current token
        """
        if not self.match_id():
            raise BadSyntaxException("Expected identifier")
        value: str = self._current_token[1]  # type: ignore
        self._next_token()
        return value

    def _tokenize(self, s: str) -> List[Tuple[str, Union[int, str]]]:
        token_specification: List[Tuple[str, str]] = [
            ("NUMBER", r"\d+"),  # Integer
            ("ID", r"[a-zA-Z_]\w*"),  # Identifiers
            ("STRING", r"\'[^\']*\'"),  # String literals
            ("DELIM", r"[=(),]"),  # Delimiters
            ("SKIP", r"[ \t]+"),  # Skip whitespace
            ("MISMATCH", r"."),  # Any other character
        ]
        tok_regex: str = "|".join("(?P<%s>%s)" % pair for pair in token_specification)
        get_token = re.compile(tok_regex).finditer
        tokens: List[Tuple[str, Union[int, str]]] = []
        for match in get_token(s):
            kind = match.lastgroup
            if kind is None:
                raise BadSyntaxException("Unexpected None value for token kind")
            value: str = match.group()
            if kind == "NUMBER":
                tokens.append((kind, int(value)))
            elif kind == "ID" and value.lower() in self._keywords:
                tokens.append((value.lower(), value))
            elif kind == "STRING":
                tokens.append((kind, value[1:-1]))  # Strip quotes
            elif kind == "SKIP":
                continue
            elif kind == "MISMATCH":
                raise BadSyntaxException(f"Unexpected character: {value}")
            else:
                tokens.append((kind, value))
        return tokens

    def _next_token(self) -> None:
        self._current_token = self._tokens.pop(0) if self._tokens else None

    def _init_keywords(self) -> None:
        self._keywords = {
            "select",
            "from",
            "where",
            "and",
            "insert",
            "into",
            "values",
            "delete",
            "update",
            "set",
            "create",
            "table",
            "int",
            "varchar",
            "view",
            "as",
            "index",
            "on",
        }


class ParserData:
    pass


@dataclass
class QueryData(ParserData):
    """Data for the SQL <i>select</i> statement."""

    field_names: List[str]
    table_names: List[str]
    predication: Predicate

    def __str__(self) -> str:
        result = "select "
        for field in self.field_names:
            result += field + ", "
        result = result[:-2]  # remove last comma
        result += " from "
        for table in self.table_names:
            result += table + ", "
        result = result[:-2]  # remove last comma
        predication_str = str(self.predication)
        if predication_str:
            result += " where " + predication_str
        return result


@dataclass
class InsertData(ParserData):
    table_name: str
    field_names: List[str]
    values: List[Constant]


@dataclass
class DeleteData(ParserData):
    table_name: str
    predication: Predicate


@dataclass
class ModifyData(ParserData):
    table_name: str
    field_name: str
    new_value: Expression
    predication: Predicate


@dataclass
class CreateTableData(ParserData):
    table_name: str
    schema: Schema


@dataclass
class CreateIndexData(ParserData):
    index_name: str
    table_name: str
    field_name: str


@dataclass
class CraeteViewData(ParserData):
    view_name: str
    query_data: QueryData


class Parser:
    """The SimpleDB parser"""

    _lexer: Lexer

    def __init__(self, sql: str) -> None:
        self._lexer = Lexer(sql)

    # Methods for parsing predicates, terms, expressions, constants, and fields

    def field(self) -> str:
        return self._lexer.eat_id()

    def constant(self) -> Constant:
        if self._lexer.match_string_constant():
            return Constant(self._lexer.eat_string_constant())
        else:
            return Constant(self._lexer.eat_int_constant())

    def expression(self) -> Expression:
        if self._lexer.match_id():
            return Expression(self.field())
        else:
            return Expression(self.constant())

    def term(self) -> Term:
        lhs = self.expression()
        self._lexer.eat_delim("=")
        rhs = self.expression()
        return Term(lhs, rhs)

    def predicate(self) -> Predicate:
        term = self.term()
        predication = Predicate(term)
        if self._lexer.match_keyword("and"):
            self._lexer.eat_keyword("and")
            predication.conjoin_with(self.predicate())
        return predication

    # Methods for parsing queries

    def query(self) -> QueryData:
        self._lexer.eat_keyword("select")
        fields = self._select_list()
        self._lexer.eat_keyword("from")
        tables = self._table_list()
        predication = Predicate()
        if self._lexer.match_keyword("where"):
            self._lexer.eat_keyword("where")
            predication = self.predicate()
        return QueryData(fields, tables, predication)

    def _select_list(self) -> List[str]:
        fields: List[str] = []
        fields.append(self.field())
        while self._lexer.match_delim(","):
            self._lexer.eat_delim(",")
            fields.append(self.field())
        return fields

    def _table_list(self) -> List[str]:
        tables: List[str] = []
        tables.append(self._lexer.eat_id())
        while self._lexer.match_delim(","):
            self._lexer.eat_delim(",")
            tables.append(self._lexer.eat_id())
        return tables

    # methods for parsing the various update commands

    def update_command(self) -> ParserData:
        if self._lexer.match_keyword("insert"):
            return self.insert()
        elif self._lexer.match_keyword("delete"):
            return self.delete()
        elif self._lexer.match_keyword("update"):
            return self.modify()
        else:
            return self._create()

    def _create(self) -> ParserData:
        self._lexer.eat_keyword("create")
        if self._lexer.match_keyword("table"):
            return self.create_table()
        elif self._lexer.match_keyword("view"):
            return self.create_view()
        else:
            return self.create_index()

    def delete(self) -> DeleteData:
        """Method for parsing delete commands"""
        self._lexer.eat_keyword("delete")
        self._lexer.eat_keyword("from")
        table_name = self._lexer.eat_id()
        predication = Predicate()
        if self._lexer.match_keyword("where"):
            self._lexer.eat_keyword("where")
            predication = self.predicate()
        return DeleteData(table_name, predication)

    # Methods for parsing insert commands.

    def insert(self) -> InsertData:
        self._lexer.eat_keyword("insert")
        self._lexer.eat_keyword("into")
        table_name = self._lexer.eat_id()
        self._lexer.eat_delim("(")
        field_names = self._field_list()
        self._lexer.eat_delim(")")
        self._lexer.eat_keyword("values")
        self._lexer.eat_delim("(")
        values = self._constant_list()
        self._lexer.eat_delim(")")
        return InsertData(table_name, field_names, values)

    def _field_list(self) -> List[str]:
        fields: List[str] = []
        fields.append(self.field())
        while self._lexer.match_delim(","):
            self._lexer.eat_delim(",")
            fields.append(self.field())
        return fields

    def _constant_list(self) -> List[Constant]:
        constants: List[Constant] = []
        constants.append(self.constant())
        while self._lexer.match_delim(","):
            self._lexer.eat_delim(",")
            constants.append(self.constant())
        return constants

    def modify(self) -> ModifyData:
        """Method for parsing modify commands."""
        self._lexer.eat_keyword("update")
        table_name = self._lexer.eat_id()
        self._lexer.eat_keyword("set")
        field_name = self.field()
        self._lexer.eat_delim("=")
        new_value = self.expression()
        predication = Predicate()
        if self._lexer.match_keyword("where"):
            self._lexer.eat_keyword("where")
            predication = self.predicate()
        return ModifyData(table_name, field_name, new_value, predication)

    # method for parsing create table commands

    def create_table(self) -> CreateTableData:
        self._lexer.eat_keyword("table")
        table_name = self._lexer.eat_id()
        self._lexer.eat_delim("(")
        schema = self._field_defs()
        self._lexer.eat_delim(")")
        return CreateTableData(table_name, schema)

    def _field_defs(self) -> Schema:
        schema = self._field_def()
        while self._lexer.match_delim(","):
            self._lexer.eat_delim(",")
            schema_to_add = self._field_defs()
            schema.add_all(schema_to_add)
        return schema

    def _field_def(self) -> Schema:
        field_name = self.field()
        return self._field_type(field_name)

    def _field_type(self, field_name: str) -> Schema:
        schema = Schema()
        if self._lexer.match_keyword("int"):
            self._lexer.eat_keyword("int")
            schema.add_int_field(field_name)
        else:
            self._lexer.eat_keyword("varchar")
            self._lexer.eat_delim("(")
            str_len = self._lexer.eat_int_constant()
            self._lexer.eat_delim(")")
            schema.add_string_field(field_name, str_len)
        return schema

    def create_view(self) -> CraeteViewData:
        """Method for parsing create view commands."""
        self._lexer.eat_keyword("view")
        view_name = self._lexer.eat_id()
        self._lexer.eat_keyword("as")
        query_data = self.query()
        return CraeteViewData(view_name, query_data)

    def create_index(self) -> CreateIndexData:
        """Method for parsing create index commands."""
        self._lexer.eat_keyword("index")
        index_name = self._lexer.eat_id()
        self._lexer.eat_keyword("on")
        table_name = self._lexer.eat_id()
        self._lexer.eat_delim("(")
        field_name = self.field()
        self._lexer.eat_delim(")")
        return CreateIndexData(index_name, table_name, field_name)
