import json
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.sql import Select

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ExplainArguments(BaseModel):
    buffers: bool = True
    verbose: bool = True
    settings: bool = False
    format_json: bool = False
    save_path: str | None = None


async def explain_analyze(
    session: "AsyncSession",
    stmt: Select,
    args: Annotated[ExplainArguments, ExplainArguments()],
) -> dict:
    try:
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
        sql_text = str(compiled)

        explain_options = ["ANALYZE"]

        if args.buffers:
            explain_options.append("BUFFERS")
        if args.verbose:
            explain_options.append("VERBOSE")
        if args.settings:
            explain_options.append("SETTINGS")
        if args.format_json:
            explain_options.append("FORMAT JSON")

        explain_clause = ", ".join(explain_options)
        explain_sql = f"EXPLAIN ({explain_clause})\n{sql_text}"

        result = await session.execute(text(explain_sql))

        if args.format_json:
            rows = result.scalar()
            if args.save_path:
                with open(args.save_path, "w", encoding="utf-8") as f:
                    json.dump(rows, f, ensure_ascii=False, indent=2)
            return {
                "explain": True,
                "format": "json",
                "query_plan": rows,
                "full_sql": sql_text,
            }

        explain_rows = result.all()
        explain_text = "\n".join(str(row[0]) for row in explain_rows)

        if args.save_path:
            with open(args.save_path, "w", encoding="utf-8") as f:
                f.write(explain_text)

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
