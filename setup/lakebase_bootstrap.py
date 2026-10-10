#!/usr/bin/env python3
"""Provision and initialize Quilla's Lakebase Autoscaling LTAP store.

This script covers the Lakebase Autoscaling resources that are not yet
declarable by Databricks Asset Bundles. It is safe to run repeatedly.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from typing import Any

import psycopg
from psycopg import sql


PROJECT_SPEC = {
    "spec": {
        "display_name": "Quilla — LTAP Application State",
        "pg_version": 17,
        "history_retention_duration": "604800s",
        "default_endpoint_settings": {
            "autoscaling_limit_min_cu": 0.5,
            "autoscaling_limit_max_cu": 4,
            "suspend_timeout_duration": "300s",
        },
    }
}

DDL = r"""
CREATE SCHEMA IF NOT EXISTS quilla_app;

CREATE TABLE IF NOT EXISTS quilla_app.app_sessions (
  session_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  external_conversation_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  country_code CHAR(2) NOT NULL,
  locale TEXT NOT NULL CHECK (locale ~ '^(pt-BR|es-[A-Z]{2})$'),
  channel TEXT NOT NULL DEFAULT 'web',
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','closed','expired')),
  title TEXT,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at TIMESTAMPTZ
);
ALTER TABLE quilla_app.app_sessions
  ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'quilla-demo',
  ADD COLUMN IF NOT EXISTS external_conversation_id TEXT,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
UPDATE quilla_app.app_sessions
SET external_conversation_id = session_id::text
WHERE external_conversation_id IS NULL;
ALTER TABLE quilla_app.app_sessions
  ALTER COLUMN external_conversation_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS ix_sessions_actor_activity
  ON quilla_app.app_sessions (tenant_id, actor_id, last_activity_at DESC)
  WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_sessions_external_scope
  ON quilla_app.app_sessions (tenant_id, actor_id, external_conversation_id);

CREATE TABLE IF NOT EXISTS quilla_app.app_messages (
  message_id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES quilla_app.app_sessions(session_id) ON DELETE CASCADE,
  sequence_number BIGINT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('system','user','assistant','tool')),
  content TEXT NOT NULL,
  language_code TEXT NOT NULL CHECK (language_code IN ('pt','es')),
  model_endpoint TEXT,
  mlflow_trace_id TEXT,
  citations JSONB NOT NULL DEFAULT '[]'::jsonb,
  input_tokens INTEGER,
  output_tokens INTEGER,
  latency_ms INTEGER,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE quilla_app.app_messages
  ADD COLUMN IF NOT EXISTS sequence_number BIGINT,
  ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
WITH numbered AS (
  SELECT message_id,
         row_number() OVER (PARTITION BY session_id ORDER BY created_at, message_id) AS sequence_number
  FROM quilla_app.app_messages
  WHERE sequence_number IS NULL
)
UPDATE quilla_app.app_messages AS messages
SET sequence_number = numbered.sequence_number
FROM numbered
WHERE messages.message_id = numbered.message_id;
ALTER TABLE quilla_app.app_messages
  ALTER COLUMN sequence_number SET NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_session_time
  ON quilla_app.app_messages (session_id, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_messages_session_sequence
  ON quilla_app.app_messages (session_id, sequence_number);
CREATE INDEX IF NOT EXISTS ix_messages_trace
  ON quilla_app.app_messages (mlflow_trace_id) WHERE mlflow_trace_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS quilla_app.agent_runs (
  run_id UUID PRIMARY KEY,
  session_id UUID REFERENCES quilla_app.app_sessions(session_id) ON DELETE SET NULL,
  agent_name TEXT NOT NULL,
  mlflow_trace_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','review_required')),
  country_code CHAR(2),
  language_code TEXT CHECK (language_code IN ('pt','es')),
  input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_payload JSONB,
  risk_level TEXT CHECK (risk_level IS NULL OR risk_level IN ('low','medium','high','critical')),
  human_review_required BOOLEAN NOT NULL DEFAULT false,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_agent_runs_session_time
  ON quilla_app.agent_runs (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_agent_runs_trace
  ON quilla_app.agent_runs (mlflow_trace_id) WHERE mlflow_trace_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS quilla_app.decision_requests (
  request_id UUID PRIMARY KEY,
  party_id TEXT,
  account_id TEXT,
  country_code CHAR(2) NOT NULL,
  decision_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','evaluating','approved','declined','manual_review','cancelled')),
  requested_by TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  request_payload JSONB NOT NULL,
  model_scores JSONB NOT NULL DEFAULT '{}'::jsonb,
  policy_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  mlflow_trace_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_decisions_country_status_time
  ON quilla_app.decision_requests (country_code, status, created_at DESC);

CREATE TABLE IF NOT EXISTS quilla_app.decision_actions (
  action_id UUID PRIMARY KEY,
  request_id UUID NOT NULL REFERENCES quilla_app.decision_requests(request_id) ON DELETE CASCADE,
  action_type TEXT NOT NULL,
  outcome TEXT NOT NULL,
  reason_code TEXT,
  explanation TEXT,
  executed_by TEXT NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_decision_actions_request_time
  ON quilla_app.decision_actions (request_id, created_at);

CREATE TABLE IF NOT EXISTS quilla_app.cases (
  case_id UUID PRIMARY KEY,
  external_case_id TEXT UNIQUE,
  party_id TEXT,
  account_id TEXT,
  country_code CHAR(2) NOT NULL,
  case_type TEXT NOT NULL,
  priority TEXT NOT NULL CHECK (priority IN ('low','medium','high','critical')),
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open','triage','investigating','waiting','resolved','closed')),
  assigned_to TEXT,
  sla_due_at TIMESTAMPTZ,
  source_event_id TEXT,
  source_model TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_cases_queue
  ON quilla_app.cases (country_code, status, priority, sla_due_at);

CREATE TABLE IF NOT EXISTS quilla_app.work_items (
  work_item_id UUID PRIMARY KEY,
  case_id UUID NOT NULL REFERENCES quilla_app.cases(case_id) ON DELETE CASCADE,
  item_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','in_progress','blocked','completed','cancelled')),
  owner_id TEXT,
  due_at TIMESTAMPTZ,
  instructions TEXT,
  result JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_work_items_owner_status
  ON quilla_app.work_items (owner_id, status, due_at);

CREATE TABLE IF NOT EXISTS quilla_app.regulatory_documents (
  document_id TEXT PRIMARY KEY,
  country_code CHAR(2) NOT NULL,
  language_code TEXT NOT NULL CHECK (language_code IN ('pt','es')),
  document_type TEXT NOT NULL,
  topic TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  version TEXT,
  status TEXT,
  effective_date DATE,
  review_date DATE,
  is_official BOOLEAN NOT NULL DEFAULT false,
  synthetic_flag BOOLEAN NOT NULL DEFAULT true,
  disclaimer TEXT NOT NULL,
  source_system TEXT,
  source_updated_at TIMESTAMPTZ,
  search_vector TSVECTOR GENERATED ALWAYS AS (
    to_tsvector(
      CASE WHEN language_code = 'pt' THEN 'portuguese'::regconfig ELSE 'spanish'::regconfig END,
      coalesce(title, '') || ' ' || coalesce(topic, '') || ' ' || coalesce(body, '')
    )
  ) STORED,
  synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_regulatory_demo_safety CHECK (is_official = false AND synthetic_flag = true)
);
CREATE INDEX IF NOT EXISTS ix_regulatory_documents_fts
  ON quilla_app.regulatory_documents USING GIN (search_vector);
CREATE INDEX IF NOT EXISTS ix_regulatory_documents_filters
  ON quilla_app.regulatory_documents (country_code, language_code, topic, status);

CREATE TABLE IF NOT EXISTS quilla_app.regulatory_search_audit (
  search_id UUID PRIMARY KEY,
  session_id UUID REFERENCES quilla_app.app_sessions(session_id) ON DELETE SET NULL,
  actor_id TEXT,
  query_text TEXT NOT NULL,
  language_code TEXT NOT NULL CHECK (language_code IN ('pt','es')),
  country_code CHAR(2),
  retrieval_mode TEXT NOT NULL,
  vector_index TEXT,
  reranker_name TEXT,
  filters JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_document_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  result_scores JSONB NOT NULL DEFAULT '[]'::jsonb,
  latency_ms INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_search_audit_actor_time
  ON quilla_app.regulatory_search_audit (actor_id, created_at DESC);

CREATE TABLE IF NOT EXISTS quilla_app.regulatory_search_cache (
  cache_key TEXT PRIMARY KEY,
  query_hash TEXT NOT NULL,
  language_code TEXT NOT NULL,
  country_code CHAR(2),
  filters JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  hit_count BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_search_cache_expiry
  ON quilla_app.regulatory_search_cache (expires_at);

CREATE TABLE IF NOT EXISTS quilla_app.outbox_events (
  event_id UUID PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','publishing','published','failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_outbox_dispatch
  ON quilla_app.outbox_events (status, available_at) WHERE status IN ('pending','failed');

CREATE TABLE IF NOT EXISTS quilla_app.app_idempotency (
  idempotency_key TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('processing','completed','failed')),
  response_payload JSONB,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '7 days',
  PRIMARY KEY (tenant_id, actor_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_app_idempotency_expiry
  ON quilla_app.app_idempotency (expires_at);

CREATE TABLE IF NOT EXISTS quilla_app.app_feedback (
  feedback_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  session_id UUID NOT NULL REFERENCES quilla_app.app_sessions(session_id) ON DELETE CASCADE,
  message_id UUID REFERENCES quilla_app.app_messages(message_id) ON DELETE SET NULL,
  mlflow_trace_id TEXT NOT NULL,
  rating SMALLINT NOT NULL CHECK (rating IN (-1, 0, 1)),
  category TEXT,
  comment TEXT,
  idempotency_key TEXT NOT NULL,
  review_queue_name TEXT NOT NULL,
  review_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (review_status IN ('pending','submitted','reviewed','failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, actor_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_app_feedback_trace
  ON quilla_app.app_feedback (mlflow_trace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_app_feedback_review_queue
  ON quilla_app.app_feedback (review_status, created_at) WHERE review_status IN ('pending','failed');

CREATE TABLE IF NOT EXISTS quilla_app.external_tool_budget (
  provider TEXT PRIMARY KEY,
  daily_window DATE NOT NULL DEFAULT current_date,
  daily_calls INTEGER NOT NULL DEFAULT 0 CHECK (daily_calls >= 0),
  total_calls INTEGER NOT NULL DEFAULT 0 CHECK (total_calls >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quilla_app.external_tool_usage (
  usage_id UUID PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES quilla_app.external_tool_budget(provider),
  conversation_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('reserved','succeeded','failed')),
  latency_ms INTEGER,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_external_tool_usage_provider_time
  ON quilla_app.external_tool_usage (provider, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_external_tool_usage_conversation
  ON quilla_app.external_tool_usage (provider, conversation_id, created_at DESC);

COMMENT ON SCHEMA quilla_app IS
  'Quilla LTAP: estado operacional de baixa latência; histórico analítico permanece no Lakehouse.';
COMMENT ON TABLE quilla_app.regulatory_documents IS
  'Corpus demonstrativo sintético PT/ES. Não contém textos regulatórios oficiais.';
COMMENT ON TABLE quilla_app.external_tool_budget IS
  'Fail-closed persistent call budget for billable external MCP providers.';
"""


def cli(profile: str, *args: str, check: bool = True) -> Any:
    result = subprocess.run(
        ["databricks", *args, "--profile", profile, "--output", "json"],
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip()[:1000])
    if not result.stdout.strip():
        return {}
    return json.loads(result.stdout)


def connect(profile: str, endpoint: str, database: str) -> psycopg.Connection:
    parent = endpoint.rsplit("/endpoints/", 1)[0]
    endpoints = cli(profile, "postgres", "list-endpoints", parent)
    endpoint_info = next(item for item in endpoints if item["name"] == endpoint)
    credential = cli(profile, "postgres", "generate-database-credential", endpoint)
    user = cli(profile, "current-user", "me")["userName"]
    return psycopg.connect(
        host=endpoint_info["status"]["hosts"]["host"],
        port=5432,
        dbname=database,
        user=user,
        password=credential["token"],
        sslmode="require",
        connect_timeout=20,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--project", default="quilla-ltap")
    parser.add_argument("--database", default="quilla")
    parser.add_argument(
        "--app-principal",
        default="",
        help="PostgreSQL role created for the Databricks App service principal.",
    )
    args = parser.parse_args()

    project_name = f"projects/{args.project}"
    branch_name = f"{project_name}/branches/production"
    endpoint_name = f"{branch_name}/endpoints/primary"

    if cli(args.profile, "postgres", "get-project", project_name, check=False):
        print(f"= Lakebase project exists: {project_name}")
    else:
        cli(
            args.profile,
            "postgres",
            "create-project",
            args.project,
            "--json",
            json.dumps(PROJECT_SPEC, ensure_ascii=False),
        )
        print(f"+ Lakebase project created: {project_name}")

    for _ in range(60):
        endpoints = cli(args.profile, "postgres", "list-endpoints", branch_name)
        # IDLE is the expected healthy state after scale-to-zero; the first
        # PostgreSQL connection wakes the endpoint automatically.
        if endpoints and endpoints[0].get("status", {}).get("current_state") in {"ACTIVE", "IDLE"}:
            break
        time.sleep(5)
    else:
        raise TimeoutError(f"Lakebase endpoint did not become ACTIVE: {endpoint_name}")

    databases = cli(args.profile, "postgres", "list-databases", branch_name)
    database_resource = next(
        (
            item for item in databases
            if item.get("status", {}).get("postgres_database") == args.database
        ),
        None,
    )
    if database_resource is None:
        database_resource = cli(
            args.profile,
            "postgres",
            "create-database",
            branch_name,
            "--database-id",
            args.database,
            "--json",
            json.dumps({"postgres_database": args.database}),
        )
        print(f"+ database resource created: {args.database}")
    else:
        print(f"= database exists: {args.database}")

    with connect(args.profile, endpoint_name, args.database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DDL)
            if args.app_principal:
                role = sql.Identifier(args.app_principal)
                database = sql.Identifier(args.database)
                cursor.execute(
                    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, role)
                )
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA quilla_app TO {}").format(role)
                )
                cursor.execute(
                    sql.SQL(
                        "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA quilla_app TO {}"
                    ).format(role)
                )
                cursor.execute(
                    sql.SQL(
                        "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA quilla_app TO {}"
                    ).format(role)
                )
                cursor.execute(
                    sql.SQL(
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA quilla_app "
                        "GRANT SELECT, INSERT, UPDATE ON TABLES TO {}"
                    ).format(role)
                )
                cursor.execute(
                    sql.SQL(
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA quilla_app "
                        "GRANT USAGE, SELECT ON SEQUENCES TO {}"
                    ).format(role)
                )
                print(f"= least-privilege grants reconciled: {args.app_principal}")
            cursor.execute(
                """SELECT count(*) FROM information_schema.tables
                   WHERE table_schema = 'quilla_app'"""
            )
            table_count = cursor.fetchone()[0]
        connection.commit()

    print(json.dumps({
        "status": "ready",
        "project": project_name,
        "endpoint": endpoint_name,
        "database": args.database,
        "database_resource": database_resource.get("name"),
        "schema": "quilla_app",
        "table_count": table_count,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
