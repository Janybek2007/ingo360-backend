"""move import_log_id from doctors to global_doctors

Revision ID: a1b2c3d4e5f6
Revises: 7097f05a9582
Create Date: 2026-04-08 11:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "7097f05a9582"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавляем import_log_id в global_doctors
    op.add_column(
        "global_doctors",
        sa.Column("import_log_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_global_doctors_import_log_id",
        "global_doctors",
        "import_logs",
        ["import_log_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "idx_global_doctors_import_log_id",
        "global_doctors",
        ["import_log_id"],
    )

    # Удаляем import_log_id из doctors
    op.execute("""
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            SELECT tc.constraint_name INTO r
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = 'doctors'
              AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.column_name = 'import_log_id'
            LIMIT 1;

            IF FOUND THEN
                EXECUTE 'ALTER TABLE doctors DROP CONSTRAINT ' || quote_ident(r.constraint_name);
            END IF;
        END $$;
    """)
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = 'doctors' AND indexname = 'idx_doctor_import_log'
            ) THEN
                DROP INDEX idx_doctor_import_log;
            END IF;
        END $$;
    """)
    op.drop_column("doctors", "import_log_id")


def downgrade() -> None:
    # Возвращаем import_log_id в doctors
    op.add_column(
        "doctors",
        sa.Column("import_log_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "doctors_import_log_id_fkey",
        "doctors",
        "import_logs",
        ["import_log_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # Удаляем из global_doctors
    op.drop_index("idx_global_doctors_import_log_id", table_name="global_doctors")
    op.drop_constraint(
        "fk_global_doctors_import_log_id", "global_doctors", type_="foreignkey"
    )
    op.drop_column("global_doctors", "import_log_id")
