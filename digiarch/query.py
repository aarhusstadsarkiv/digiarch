from re import compile as re_compile
from typing import Any
from typing import Callable
from typing import Type
from typing import TypeVar

from click import argument
from click import BadParameter
from click import ClickException
from click import Context
from click import MissingParameter
from click import Parameter

FC = TypeVar("FC", bound=Callable[..., Any])
TQuery = list[tuple[str, str | bool | Type[Ellipsis] | None, bool]]

token_quotes = re_compile(r'(?<!\\)"((?:[^"]|(?<=\\)")*)"')
# noinspection RegExpUnnecessaryNonCapturingGroup
token_expr = re_compile(r"(?:\0([^\0]+)\0|(?<!\\)\s+)")


def query_to_where(query: TQuery) -> tuple[str, list[str]]:
    query_fields: dict[str, list[tuple[str, bool]]] = {}
    where: list[str] = []
    parameters: list[str] = []

    for field, value, like in query:
        query_fields[field] = [*query_fields.get(field, []), (value, like)]

    for field, values in query_fields.items():
        where_field: list[str] = []

        for value, like in values:
            if value is None:
                where_field.append(f"{field} is null")
            elif value is Ellipsis:
                where_field.append(f"{field} is not null")
            elif value is True:
                where_field.append(f"{field} is true")
            elif value is False:
                where_field.append(f"{field} is false")
            elif like:
                where_field.append(f"{field} like ?")
                parameters.append(value)
            else:
                where_field.append(f"{field} = ?")
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
            query_tokens.append((field, None, False))
        elif token == "@notnull":
            query_tokens.append((field, ..., True))
        elif token == "@true":
            query_tokens.append((field, True, False))
        elif token == "@false":
            query_tokens.append((field, False, False))
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
                query_tokens.extend([(field, line_, like) for line in fh.readlines() if (line_ := line.strip())])
        else:
            query_tokens.append((field, token, like))

    return query_tokens


def argument_query(required: bool, default: str, allowed_fields: list[str] | None = None) -> Callable[[FC], FC]:
    def callback(ctx: Context, param: Parameter, value: str | None) -> list[tuple[str, str, bool]]:
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
