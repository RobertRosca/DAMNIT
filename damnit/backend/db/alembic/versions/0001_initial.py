"""initial DAMNIT schema

Revision ID: 0001
Revises:
Create Date: 2026-04-22

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


VALUE_KINDS = (
    "null", "int", "float", "str", "bool",
    "complex", "numpy", "png", "trendline", "timestamp", "error",
)
SUMMARY_TYPES = ("timestamp", "complex", "numpy", "trendline")


def upgrade() -> None:
    bind = op.get_bind()

    # ENUM types
    op.execute("CREATE TYPE value_kind AS ENUM (" +
               ", ".join(f"'{v}'" for v in VALUE_KINDS) + ")")
    op.execute("CREATE TYPE summary_type AS ENUM (" +
               ", ".join(f"'{v}'" for v in SUMMARY_TYPES) + ")")

    # --- db_info (install-wide) ---
    op.execute("""
        CREATE TABLE db_info (
            id SMALLINT PRIMARY KEY CHECK (id = 1),
            db_id TEXT NOT NULL
        )
    """)
    op.execute("INSERT INTO db_info (id, db_id) "
               "VALUES (1, encode(gen_random_bytes(20), 'hex')) "
               "ON CONFLICT (id) DO NOTHING")

    # --- proposal_info ---
    op.execute("""
        CREATE TABLE proposal_info (
            proposal INTEGER PRIMARY KEY,
            damnit_python TEXT,
            context_python TEXT,
            concurrent_jobs INTEGER NOT NULL DEFAULT 15,
            slurm_time TEXT,
            slurm_partition TEXT,
            slurm_reservation TEXT,
            noncluster_cpus TEXT,
            noncluster_mem TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    # --- run_info (partitioned by proposal) ---
    op.execute("""
        CREATE TABLE run_info (
            proposal INTEGER NOT NULL,
            run INTEGER NOT NULL,
            start_time DOUBLE PRECISION,
            added_at DOUBLE PRECISION,
            PRIMARY KEY (proposal, run)
        ) PARTITION BY LIST (proposal)
    """)

    # --- variables (partitioned by proposal) ---
    op.execute("""
        CREATE TABLE variables (
            proposal INTEGER NOT NULL,
            name TEXT NOT NULL,
            type TEXT,
            title TEXT,
            description TEXT,
            tags TEXT[] NOT NULL DEFAULT '{}',
            attributes JSONB,
            PRIMARY KEY (proposal, name)
        ) PARTITION BY LIST (proposal)
    """)
    op.execute("CREATE INDEX ix_variables_tags ON variables USING GIN (tags)")
    op.execute(
        "CREATE INDEX ix_variables_attributes "
        "ON variables USING GIN (attributes jsonb_path_ops)"
    )

    # --- run_variables (partitioned by proposal) ---
    op.execute("""
        CREATE TABLE run_variables (
            proposal INTEGER NOT NULL,
            run INTEGER NOT NULL,
            name TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            value_kind value_kind NOT NULL,
            value_num DOUBLE PRECISION,
            value_text TEXT,
            value_bool BOOLEAN,
            value_bytes BYTEA,
            timestamp DOUBLE PRECISION,
            max_diff DOUBLE PRECISION,
            provenance TEXT,
            summary_method TEXT,
            summary_type summary_type,
            attributes JSONB,
            PRIMARY KEY (proposal, run, name, version),
            CONSTRAINT value_columns_match_kind CHECK (
                (value_kind = 'null'
                    AND value_num IS NULL AND value_text IS NULL
                    AND value_bool IS NULL AND value_bytes IS NULL)
                OR (value_kind IN ('int','float','timestamp')
                    AND value_num IS NOT NULL
                    AND value_text IS NULL AND value_bool IS NULL
                    AND value_bytes IS NULL)
                OR (value_kind IN ('str','error')
                    AND value_text IS NOT NULL
                    AND value_num IS NULL AND value_bool IS NULL
                    AND value_bytes IS NULL)
                OR (value_kind = 'bool'
                    AND value_bool IS NOT NULL
                    AND value_num IS NULL AND value_text IS NULL
                    AND value_bytes IS NULL)
                OR (value_kind IN ('complex','numpy','png','trendline')
                    AND value_bytes IS NOT NULL
                    AND value_num IS NULL AND value_text IS NULL
                    AND value_bool IS NULL)
            )
        ) PARTITION BY LIST (proposal)
    """)
    op.execute(
        "CREATE UNIQUE INDEX ix_run_variables_current "
        "ON run_variables (proposal, run, name) WHERE is_current"
    )
    op.execute(
        "CREATE INDEX ix_run_variables_proposal_run "
        "ON run_variables (proposal, run)"
    )
    op.execute(
        "CREATE INDEX ix_run_variables_attributes "
        "ON run_variables USING GIN (attributes jsonb_path_ops)"
    )

    # --- time_comments (partitioned by proposal) ---
    op.execute("""
        CREATE TABLE time_comments (
            proposal INTEGER NOT NULL,
            id BIGSERIAL NOT NULL,
            timestamp DOUBLE PRECISION NOT NULL,
            comment TEXT,
            PRIMARY KEY (proposal, id)
        ) PARTITION BY LIST (proposal)
    """)

    # --- listener bookkeeping (not partitioned; tiny) ---
    op.execute("""
        CREATE TABLE listener_proposals (
            proposal INTEGER NOT NULL PRIMARY KEY,
            db_dir TEXT NOT NULL UNIQUE,
            official BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    op.execute("""
        CREATE TABLE listener_settings (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT
        )
    """)

    # --- NOTIFY triggers ---
    op.execute("""
        CREATE OR REPLACE FUNCTION damnit_notify_run_variables()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            chan TEXT;
            payload TEXT;
        BEGIN
            SELECT 'damnit_db_' || db_id INTO chan FROM db_info WHERE id = 1;
            payload := json_build_object(
                'msg_kind', 'run_values_updated',
                'data', json_build_object(
                    'proposal', NEW.proposal,
                    'run', NEW.run,
                    'values', json_build_object(NEW.name, NULL)
                )
            )::text;
            PERFORM pg_notify(chan, payload);
            RETURN NEW;
        END;
        $$;
    """)
    op.execute("""
        CREATE TRIGGER trg_notify_run_variables
        AFTER INSERT OR UPDATE ON run_variables
        FOR EACH ROW EXECUTE FUNCTION damnit_notify_run_variables()
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION damnit_notify_variables()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            chan TEXT;
            payload TEXT;
        BEGIN
            SELECT 'damnit_db_' || db_id INTO chan FROM db_info WHERE id = 1;
            payload := json_build_object(
                'msg_kind', 'variable_set',
                'data', json_build_object(
                    'name', NEW.name,
                    'title', NEW.title,
                    'type', NEW.type,
                    'attributes', NEW.attributes
                )
            )::text;
            PERFORM pg_notify(chan, payload);
            RETURN NEW;
        END;
        $$;
    """)
    op.execute("""
        CREATE TRIGGER trg_notify_variables
        AFTER INSERT OR UPDATE ON variables
        FOR EACH ROW EXECUTE FUNCTION damnit_notify_variables()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_notify_variables ON variables")
    op.execute("DROP TRIGGER IF EXISTS trg_notify_run_variables ON run_variables")
    op.execute("DROP FUNCTION IF EXISTS damnit_notify_variables()")
    op.execute("DROP FUNCTION IF EXISTS damnit_notify_run_variables()")
    op.execute("DROP TABLE IF EXISTS listener_settings")
    op.execute("DROP TABLE IF EXISTS listener_proposals")
    op.execute("DROP TABLE IF EXISTS time_comments")
    op.execute("DROP TABLE IF EXISTS run_variables")
    op.execute("DROP TABLE IF EXISTS variables")
    op.execute("DROP TABLE IF EXISTS run_info")
    op.execute("DROP TABLE IF EXISTS proposal_info")
    op.execute("DROP TABLE IF EXISTS db_info")
    op.execute("DROP TYPE IF EXISTS summary_type")
    op.execute("DROP TYPE IF EXISTS value_kind")
