"""restore doctor company_id

Revision ID: c7a1f3b6d9e4
Revises: 9f4c2a1b3d7e
Create Date: 2026-04-01 15:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = "c7a1f3b6d9e4"
down_revision: Union[str, Sequence[str], None] = "9f4c2a1b3d7e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    columns = [col["name"] for col in inspector.get_columns("doctors")]

    if "company_id" not in columns:
        op.add_column(
            "doctors",
            sa.Column("company_id", sa.Integer(), nullable=True),
        )

    op.create_foreign_key(
        "fk_doctors_company",
        "doctors",
        "companies",
        ["company_id"],
        ["id"],
    )

    op.execute(
        """
        UPDATE doctors d
        SET company_id = v.company_id
        FROM (
            SELECT doctor_id, MIN(company_id) AS company_id
            FROM visits
            WHERE doctor_id IS NOT NULL
            GROUP BY doctor_id
        ) v
        WHERE d.id = v.doctor_id
        """
    )

    op.execute(
        """
        UPDATE doctors d
        SET company_id = e.company_id
        FROM employees e
        WHERE d.company_id IS NULL
          AND d.responsible_employee_id = e.id
        """
    )

    # Fallback: set company_id from first available company for any remaining NULLs
    op.execute(
        """
        UPDATE doctors
        SET company_id = (SELECT id FROM companies LIMIT 1)
        WHERE company_id IS NULL
        """
    )

    op.alter_column("doctors", "company_id", nullable=False)

    op.create_index(
        "idx_doctor_company",
        "doctors",
        ["company_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "uq_doctor_global_doctor_company",
        "doctors",
        type_="unique",
    )
    op.drop_index("idx_doctor_company", table_name="doctors")
    op.drop_constraint("fk_doctors_company", "doctors", type_="foreignkey")
    op.drop_column("doctors", "company_id")
