# SPDX-License-Identifier: Apache-2.0
"""
Add GIN trigram index on ultranormalize_name(projects.name)

Revision ID: 5b1c8e7f2a9d
Revises: c7e2a9d41f30
Create Date: 2026-10-28 10:30:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "5b1c8e7f2a9d"
down_revision = "c7e2a9d41f30"


def upgrade():
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction. Commit the
    # migration's transaction and issue the statement in an autocommit block.
    op.get_bind().commit()

    with op.get_context().autocommit_block():
        # env.py sets statement_timeout = 5000; a GIN trigram build over ~1M
        # rows, each first passed through ultranormalize_name(), needs more
        # headroom or it is cancelled, leaving an INVALID index.
        op.execute(sa.text("SET statement_timeout = 120000"))  # 120s
        op.execute(sa.text("SET lock_timeout = 5000"))  # 5s

        # Raw SQL: op.create_index() attaches an operator class by the
        # expression's key, and a text() expression has none, so the opclass
        # would silently be dropped.
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY projects_name_ultranormalized_trgm "
                "ON projects USING gin (ultranormalize_name(name) gin_trgm_ops)"
            )
        )


def downgrade():
    op.drop_index("projects_name_ultranormalized_trgm", table_name="projects")
