"""add domain columns to companies

Revision ID: 973706d0aef6
Revises: a1b2c3d4e5f6
Create Date: 2025-12-01 14:30:31.759690

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '973706d0aef6'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add domain metadata columns to companies table."""
    op.add_column(
        "companies",
        sa.Column("domain_confidence", sa.Float(), nullable=True),
    )
    op.add_column(
        "companies",
        sa.Column("domain_source", sa.String(), nullable=True),
    )


def downgrade() -> None:
    """Remove domain metadata columns from companies table."""
    op.drop_column("companies", "domain_source")
    op.drop_column("companies", "domain_confidence")
