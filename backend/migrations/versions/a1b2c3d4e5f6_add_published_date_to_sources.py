"""add_published_date_to_sources

Revision ID: a1b2c3d4e5f6
Revises: d6ef8be4a6d4
Create Date: 2025-11-29 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'd6ef8be4a6d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add published_date column to sources table."""
    op.add_column(
        "sources",
        sa.Column("published_date", sa.String(), nullable=True),
    )


def downgrade() -> None:
    """Remove published_date column from sources table."""
    op.drop_column("sources", "published_date")

