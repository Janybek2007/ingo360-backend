"""add distributor_id to tertiary_sales_and_stock

Revision ID: c3b1f6a2d4e9
Revises: aa859b829641
Create Date: 2026-03-26 10:30:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3b1f6a2d4e9"
down_revision: Union[str, Sequence[str], None] = "aa859b829641"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "tertiary_sales_and_stock",
        sa.Column("distributor_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        op.f("fk_tertiary_sales_and_stock_distributor_id_distributors"),
        "tertiary_sales_and_stock",
        "distributors",
        ["distributor_id"],
        ["id"],
    )
    op.execute(
        """
        UPDATE tertiary_sales_and_stock t
        SET distributor_id = p.distributor_id
        FROM pharmacies p
        WHERE t.pharmacy_id = p.id
          AND t.distributor_id IS NULL
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        op.f("fk_tertiary_sales_and_stock_distributor_id_distributors"),
        "tertiary_sales_and_stock",
        type_="foreignkey",
    )
    op.drop_column("tertiary_sales_and_stock", "distributor_id")
