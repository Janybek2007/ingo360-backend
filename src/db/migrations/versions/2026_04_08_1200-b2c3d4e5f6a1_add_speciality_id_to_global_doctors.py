"""add speciality_id to global_doctors

Revision ID: b2c3d4e5f6a1
Revises: a1b2c3d4e5f6
Create Date: 2026-04-08 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "b2c3d4e5f6a1"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "global_doctors",
        sa.Column("speciality_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_global_doctors_speciality_id",
        "global_doctors",
        "specialities",
        ["speciality_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_global_doctors_speciality_id",
        "global_doctors",
        ["speciality_id"],
    )



def downgrade() -> None:
    op.drop_index("idx_global_doctors_speciality_id", table_name="global_doctors")
    op.drop_constraint(
        "fk_global_doctors_speciality_id", "global_doctors", type_="foreignkey"
    )
    op.drop_column("global_doctors", "speciality_id")
