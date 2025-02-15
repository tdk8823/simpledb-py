import re
from typing import Collection, List, Optional, Tuple, Union


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
