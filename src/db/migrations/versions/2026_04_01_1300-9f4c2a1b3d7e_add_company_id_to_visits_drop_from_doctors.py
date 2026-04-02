"""add company_id to visits and drop from doctors

Revision ID: 9f4c2a1b3d7e
Revises: 677e947c3a24
Create Date: 2026-04-01 13:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9f4c2a1b3d7e"
down_revision: Union[str, Sequence[str], None] = "677e947c3a24"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. add company_id to visits (nullable first)
    op.add_column("visits", sa.Column("company_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_visits_company", "visits", "companies", ["company_id"], ["id"]
    )

    # 2. backfill from employees
    op.execute(
        """
        UPDATE visits v
        SET company_id = e.company_id
        FROM employees e
        WHERE v.employee_id = e.id
        """
    )

    # 3. make NOT NULL after backfill
    op.alter_column("visits", "company_id", nullable=False)

    # 4. drop company_id from doctors
    op.execute("DROP INDEX IF EXISTS idx_doctor_company")
    op.execute(
        "ALTER TABLE doctors DROP CONSTRAINT IF EXISTS fk_doctors_company"
    )
    op.execute(
        "ALTER TABLE doctors DROP CONSTRAINT IF EXISTS fk_doctors_company_id_companies"
    )
    op.execute("ALTER TABLE doctors DROP COLUMN IF EXISTS company_id")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "doctors",
        sa.Column("company_id", sa.Integer(), nullable=False),
    )
    op.create_foreign_key(
        "fk_doctors_company", "doctors", "companies", ["company_id"], ["id"]
    )
    op.create_index("idx_doctor_company", "doctors", ["company_id"], unique=False)

    op.drop_constraint("fk_visits_company", "visits", type_="foreignkey")
    op.drop_column("visits", "company_id")
