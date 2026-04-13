"""empty message

Revision ID: 708ef90a4527
Revises: 181b4677ed6b
Create Date: 2026-03-07 18:48:46.688679

"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = '708ef90a4527'
down_revision: Union[str, Sequence[str], None] = '181b4677ed6b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
