"""Add research_qa_plans table for micro-research

Revision ID: b7c8d9e0f1a2
Revises: d6ef8be4a6d4
Create Date: 2025-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b7c8d9e0f1a2'
down_revision: Union[str, None] = '9925aba8d74d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'research_qa_plans',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('qa_id', sa.Integer(), nullable=True),
        sa.Column('question', sa.Text(), nullable=False),
        sa.Column('gap_statement', sa.Text(), nullable=False),
        sa.Column('intent', sa.String(64), nullable=True),
        sa.Column('plan_steps_json', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column('plan_markdown', sa.Text(), nullable=True),
        sa.Column('status', sa.String(32), nullable=False, server_default='PROPOSED'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('confirmed_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_source_ids', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('result_qa_id', sa.Integer(), nullable=True),
        sa.Column('estimated_cost_label', sa.String(32), nullable=True),
        sa.Column('llm_usage', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('total_cost_usd', sa.Numeric(14, 6), nullable=True),
        sa.ForeignKeyConstraint(['job_id'], ['research_jobs.id'], ),
        sa.ForeignKeyConstraint(['qa_id'], ['research_qa.id'], ),
        sa.ForeignKeyConstraint(['result_qa_id'], ['research_qa.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_research_qa_plans_job_id'), 'research_qa_plans', ['job_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_research_qa_plans_job_id'), table_name='research_qa_plans')
    op.drop_table('research_qa_plans')

