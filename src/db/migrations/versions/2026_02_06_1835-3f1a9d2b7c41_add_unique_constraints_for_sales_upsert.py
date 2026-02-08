"""add unique constraints for sales upsert

Revision ID: 3f1a9d2b7c41
Revises: 916decd3227d
Create Date: 2026-02-06 18:35:00.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3f1a9d2b7c41"
down_revision: Union[str, Sequence[str], None] = "916decd3227d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Удаляем дубликаты перед созданием UNIQUE.
    # Оставляем самую "свежую" запись в группе (updated_at DESC, id DESC).
    op.execute("""
        DELETE FROM primary_sales_and_stock p
        USING (
            SELECT ctid
            FROM (
                SELECT
                    ctid,
                    row_number() OVER (
                        PARTITION BY distributor_id, sku_id, month, year, indicator
                        ORDER BY updated_at DESC NULLS LAST, id DESC
                    ) AS rn
                FROM primary_sales_and_stock
            ) t
            WHERE t.rn > 1
        ) d
        WHERE p.ctid = d.ctid;
        """)

    op.execute("""
        DELETE FROM secondary_sales s
        USING (
            SELECT ctid
            FROM (
                SELECT
                    ctid,
                    row_number() OVER (
                        PARTITION BY pharmacy_id, sku_id, month, year, indicator
                        ORDER BY updated_at DESC NULLS LAST, id DESC
                    ) AS rn
                FROM secondary_sales
            ) t
            WHERE t.rn > 1
        ) d
        WHERE s.ctid = d.ctid;
        """)

    op.execute("""
        DELETE FROM tertiary_sales_and_stock tss
        USING (
            SELECT ctid
            FROM (
                SELECT
                    ctid,
                    row_number() OVER (
                        PARTITION BY pharmacy_id, sku_id, month, year, indicator
                        ORDER BY updated_at DESC NULLS LAST, id DESC
                    ) AS rn
                FROM tertiary_sales_and_stock
            ) t
            WHERE t.rn > 1
        ) d
        WHERE tss.ctid = d.ctid;
        """)

    op.create_unique_constraint(
        "uq_primary_sales_business_key",
        "primary_sales_and_stock",
        ["distributor_id", "sku_id", "month", "year", "indicator"],
    )
    op.create_unique_constraint(
        "uq_secondary_sales_business_key",
        "secondary_sales",
        ["pharmacy_id", "sku_id", "month", "year", "indicator"],
    )
    op.create_unique_constraint(
        "uq_tertiary_sales_business_key",
        "tertiary_sales_and_stock",
        ["pharmacy_id", "sku_id", "month", "year", "indicator"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "uq_tertiary_sales_business_key", "tertiary_sales_and_stock", type_="unique"
    )
    op.drop_constraint(
        "uq_secondary_sales_business_key", "secondary_sales", type_="unique"
    )
    op.drop_constraint(
        "uq_primary_sales_business_key", "primary_sales_and_stock", type_="unique"
    )
