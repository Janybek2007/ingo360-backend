from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.sql import Select

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def explain_analyze(
    session: AsyncSession,
    stmt: Select,
    *,
    buffers: bool = True,
    verbose: bool = True,
    settings: bool = False,
    format_json: bool = False,
) -> dict:
    try:
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
        sql_text = str(compiled)

        explain_options = ["ANALYZE"]

        if buffers:
            explain_options.append("BUFFERS")
        if verbose:
            explain_options.append("VERBOSE")
        if settings:
            explain_options.append("SETTINGS")
        if format_json:
            explain_options.append("FORMAT JSON")

        explain_clause = ", ".join(explain_options)
        explain_sql = f"EXPLAIN ({explain_clause})\n{sql_text}"

        result = await session.execute(text(explain_sql))

        if format_json:
            rows = result.scalar()
            return {
                "explain": True,
                "format": "json",
                "query_plan": rows,
                "full_sql": sql_text,
            }

        explain_rows = result.all()
        explain_text = "\n".join(str(row[0]) for row in explain_rows)

        return {
            "explain": True,
            "format": "text",
            "query_plan": explain_text,
            "full_sql": sql_text,
        }

    except Exception as e:
        return {
            "explain": True,
            "error": str(e),
            "full_sql": str(stmt),
        }
