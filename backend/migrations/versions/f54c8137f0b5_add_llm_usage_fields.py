"""add_lm_cost_tracking_fields

Revision ID: f54c8137f0b5
Revises: d6ef8be4a6d4
Create Date: 2025-12-02 15:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f54c8137f0b5'
down_revision: Union[str, Sequence[str], None] = '973706d0aef6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'research_jobs',
        sa.Column('llm_usage', sa.JSON(), nullable=True)
    )
    op.add_column(
        'research_jobs',
        sa.Column('total_cost_usd', sa.Numeric(14, 6), nullable=True)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('research_jobs', 'total_cost_usd')
    op.drop_column('research_jobs', 'llm_usage')


