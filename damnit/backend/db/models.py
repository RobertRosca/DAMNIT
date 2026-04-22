"""SQLAlchemy ORM models for DAMNIT."""
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (BigInteger, Boolean, CheckConstraint, DateTime, Float,
                        ForeignKeyConstraint, Index, Integer, SmallInteger,
                        String, Text, func)
from sqlalchemy.dialects.postgresql import ARRAY, BYTEA, ENUM, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


VALUE_KINDS = (
    "null", "int", "float", "str", "bool",
    "complex", "numpy", "png", "trendline", "timestamp", "error",
)

SUMMARY_TYPES = (
    "timestamp", "complex", "numpy", "trendline",
)

MSG_KINDS = (
    "variable_set", "run_values_updated",
    "processing_state_set", "processing_finished",
    "file_submission",
)

value_kind_enum = ENUM(*VALUE_KINDS, name="value_kind", create_type=False)
summary_type_enum = ENUM(*SUMMARY_TYPES, name="summary_type", create_type=False)


class Base(DeclarativeBase):
    pass


class DbInfo(Base):
    __tablename__ = "db_info"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True,
                                    default=1)
    db_id: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        CheckConstraint("id = 1", name="db_info_singleton"),
    )


class ProposalInfo(Base):
    __tablename__ = "proposal_info"

    proposal: Mapped[int] = mapped_column(Integer, primary_key=True)
    damnit_python: Mapped[Optional[str]] = mapped_column(Text)
    context_python: Mapped[Optional[str]] = mapped_column(Text)
    concurrent_jobs: Mapped[int] = mapped_column(Integer, nullable=False,
                                                 server_default="15")
    slurm_time: Mapped[Optional[str]] = mapped_column(Text)
    slurm_partition: Mapped[Optional[str]] = mapped_column(Text)
    slurm_reservation: Mapped[Optional[str]] = mapped_column(Text)
    noncluster_cpus: Mapped[Optional[str]] = mapped_column(Text)
    noncluster_mem: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class RunInfo(Base):
    __tablename__ = "run_info"

    proposal: Mapped[int] = mapped_column(Integer, primary_key=True)
    run: Mapped[int] = mapped_column(Integer, primary_key=True)
    start_time: Mapped[Optional[float]] = mapped_column(Float)
    added_at: Mapped[Optional[float]] = mapped_column(Float)

    __table_args__ = (
        {"postgresql_partition_by": "LIST (proposal)"},
    )


class Variable(Base):
    __tablename__ = "variables"

    proposal: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    type: Mapped[Optional[str]] = mapped_column(Text)
    title: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    tags: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False,
                                            server_default="{}")
    attributes: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_variables_tags", "tags", postgresql_using="gin"),
        Index("ix_variables_attributes", "attributes",
              postgresql_using="gin",
              postgresql_ops={"attributes": "jsonb_path_ops"}),
        {"postgresql_partition_by": "LIST (proposal)"},
    )


class RunVariable(Base):
    __tablename__ = "run_variables"

    proposal: Mapped[int] = mapped_column(Integer, primary_key=True)
    run: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True,
                                         server_default="1")
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False,
                                             server_default="true")
    value_kind: Mapped[str] = mapped_column(value_kind_enum, nullable=False)
    value_num: Mapped[Optional[float]] = mapped_column(Float)
    value_text: Mapped[Optional[str]] = mapped_column(Text)
    value_bool: Mapped[Optional[bool]] = mapped_column(Boolean)
    value_bytes: Mapped[Optional[bytes]] = mapped_column(BYTEA)
    timestamp: Mapped[Optional[float]] = mapped_column(Float)
    max_diff: Mapped[Optional[float]] = mapped_column(Float)
    provenance: Mapped[Optional[str]] = mapped_column(Text)
    summary_method: Mapped[Optional[str]] = mapped_column(Text)
    summary_type: Mapped[Optional[str]] = mapped_column(summary_type_enum)
    attributes: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)

    __table_args__ = (
        ForeignKeyConstraint(
            ["proposal", "name"],
            ["variables.proposal", "variables.name"],
            name="fk_run_variables_variable",
            deferrable=True,
            initially="DEFERRED",
        ),
        Index("ix_run_variables_current",
              "proposal", "run", "name",
              unique=True,
              postgresql_where="is_current"),
        Index("ix_run_variables_proposal_run", "proposal", "run"),
        Index("ix_run_variables_attributes", "attributes",
              postgresql_using="gin",
              postgresql_ops={"attributes": "jsonb_path_ops"}),
        {"postgresql_partition_by": "LIST (proposal)"},
    )


class TimeComment(Base):
    __tablename__ = "time_comments"

    proposal: Mapped[int] = mapped_column(Integer, primary_key=True)
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True,
                                    autoincrement=True)
    timestamp: Mapped[float] = mapped_column(Float, nullable=False)
    comment: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        {"postgresql_partition_by": "LIST (proposal)"},
    )


class ListenerProposal(Base):
    __tablename__ = "listener_proposals"

    proposal: Mapped[int] = mapped_column(Integer, primary_key=True)
    db_dir: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    official: Mapped[bool] = mapped_column(Boolean, nullable=False,
                                           server_default="false")


class ListenerSetting(Base):
    __tablename__ = "listener_settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(Text)


PARTITIONED_TABLES = ("run_info", "variables", "run_variables", "time_comments")
