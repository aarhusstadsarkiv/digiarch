from collections.abc import Callable
from collections.abc import Generator
from re import compile as re_compile
from typing import Any
from typing import TypeVar

from acacore.database.table import Table
from click import argument
from click import BadParameter
from click import ClickException
from click import Context
from click import MissingParameter
from click import Parameter
from pydantic import BaseModel

M = TypeVar("M", bound=BaseModel)
FC = TypeVar("FC", bound=Callable[..., Any])
TQuery = list[tuple[str, str | bool | type[Ellipsis] | list[str] | None, str]]  # field name, value(s), operation

token_quotes = re_compile(r'(?<!\\)"((?:[^"]|(?<=\\)")*)"')
# noinspection RegExpUnnecessaryNonCapturingGroup
token_expr = re_compile(r"(?:\x00([^\x00]+)\x00|(?<!\\)\s+)")


def query_to_where(query: TQuery) -> tuple[str, list[str]]:
    query_fields: dict[str, list[tuple[str, bool]]] = {}
    where: list[str] = []
    parameters: list[str] = []

    for field, value, like in query:
        query_fields[field] = [*query_fields.get(field, []), (value, like)]

    for field, values in query_fields.items():
        where_field: list[str] = []

        for value, op in values:
            match (value, op):
                case None, "is":
                    where_field.append(f"{field} is null")
                case None, "is not":
                    where_field.append(f"{field} is not null")
                case True, "is":
                    where_field.append(f"{field} is true")
                case True, "is not":
                    where_field.append(f"{field} is false")
                case False, "is":
                    where_field.append(f"{field} is false")
                case False, "is not":
                    where_field.append(f"{field} is true")
                case _, "in" if isinstance(value, list):
                    where_field.append(f"{field} in ({','.join(['?'] * len(value))})")
                    parameters.extend(value)
                case _, "in" if isinstance(value, str):
                    where_field.append(f"instr({field}, ?) != 0")
                    parameters.append(value)
                case _, "=":
                    where_field.append(f"{field} = ?")
                    parameters.append(value)
                case _, "like":
                    where_field.append(f"{field} like ?")
                    parameters.append(value)

        where.append(f"({' or '.join(where_field)})")

    return " and ".join(where), parameters


def tokenize_query(query_string: str, default_field: str, allowed_fields: list[str]) -> TQuery:
    query_string = token_quotes.sub(r"\0\1\0", query_string)
    tokens: list[str] = [t for t in token_expr.split(query_string) if t]
    field: str = default_field
    like: bool = False
    from_file: bool = False

    query_tokens: TQuery = []

    for token in tokens:
        if token == "@null":
            query_tokens.append((field, None, "is"))
        elif token == "@notnull":
            query_tokens.append((field, None, "is not"))
        elif token == "@true":
            query_tokens.append((field, True, "is"))
        elif token == "@false":
            query_tokens.append((field, True, "is not"))
        elif token == "@like":
            like = True
        elif token == "@file":
            from_file = True
        elif token.startswith("@"):
            if (field := token.removeprefix("@")) not in allowed_fields:
                raise ValueError(f"Invalid field name {field}")
            like = False
            from_file = False
        elif from_file:
            with open(token) as fh:
                query_tokens.append((field, [line for l in fh.readlines() if (line := l.rstrip("\r\n"))], "in"))
        else:
            query_tokens.append((field, token, "like" if like else "="))

    return query_tokens


def argument_query(required: bool, default: str, allowed_fields: list[str] | None = None) -> Callable[[FC], FC]:
    def callback(ctx: Context, param: Parameter, value: str | None) -> TQuery:
        if not (value := value or "").strip() and required:
            raise MissingParameter(None, ctx, param)
        if not value:
            return []

        try:
            query = tokenize_query(value, default, allowed_fields or [])
            if not query and required:
                raise BadParameter("no values in query.", ctx, param)
            return query
        except ClickException:
            raise
        except FileNotFoundError as err:
            raise BadParameter(f"{err.filename} file not found", ctx, param)
        except ValueError as err:
            raise BadParameter(err.args[0], ctx, param)
        except Exception as err:
            raise BadParameter(repr(err), ctx, param)

    return argument("QUERY", nargs=1, required=required, callback=callback)


def query_table(
    table: Table[M],
    query: TQuery,
    order_by: list[tuple[str, str]] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> Generator[M, None, None]:
    where, parameters = query_to_where(query)
    yield from table.select(where, parameters, order_by, limit, offset)
