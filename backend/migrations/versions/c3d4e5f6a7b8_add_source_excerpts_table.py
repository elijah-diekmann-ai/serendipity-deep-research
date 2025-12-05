"""add source_excerpts table

Revision ID: c3d4e5f6a7b8
Revises: f54c8137f0b5
Create Date: 2025-12-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = 'b7c8d9e0f1a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'source_excerpts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source_id', sa.Integer(), nullable=False),
        sa.Column('plan_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('excerpt_text', sa.Text(), nullable=False),
        sa.Column('excerpt_type', sa.String(length=32), nullable=False),
        sa.Column('content_hash', sa.String(length=64), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['research_jobs.id'], ),
        sa.ForeignKeyConstraint(['plan_id'], ['research_qa_plans.id'], ),
        sa.ForeignKeyConstraint(['source_id'], ['sources.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source_id', 'content_hash', name='uq_source_excerpt_content')
    )
    op.create_index('ix_source_excerpts_job_id', 'source_excerpts', ['job_id'], unique=False)
    op.create_index(op.f('ix_source_excerpts_plan_id'), 'source_excerpts', ['plan_id'], unique=False)
    op.create_index(op.f('ix_source_excerpts_source_id'), 'source_excerpts', ['source_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_source_excerpts_source_id'), table_name='source_excerpts')
    op.drop_index(op.f('ix_source_excerpts_plan_id'), table_name='source_excerpts')
    op.drop_index('ix_source_excerpts_job_id', table_name='source_excerpts')
    op.drop_table('source_excerpts')

