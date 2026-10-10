# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Lakebase Search
# MAGIC
# MAGIC Espelha o corpus governado no Lakebase e mantém um índice PostgreSQL GIN/tsvector
# MAGIC multilíngue para a experiência LTAP de baixa latência.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from psycopg.types.json import Jsonb

cfg = ai_config()
source = ai_table(cfg, "regulatory_documents")

rows = spark.table(source).orderBy("document_id").collect()

upsert = """
INSERT INTO quilla_app.regulatory_documents (
  document_id, country_code, language_code, document_type, topic, title, body,
  version, status, effective_date, review_date, is_official, synthetic_flag,
  disclaimer, source_system, source_updated_at, synced_at
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
)
ON CONFLICT (document_id) DO UPDATE SET
  country_code = EXCLUDED.country_code,
  language_code = EXCLUDED.language_code,
  document_type = EXCLUDED.document_type,
  topic = EXCLUDED.topic,
  title = EXCLUDED.title,
  body = EXCLUDED.body,
  version = EXCLUDED.version,
  status = EXCLUDED.status,
  effective_date = EXCLUDED.effective_date,
  review_date = EXCLUDED.review_date,
  is_official = EXCLUDED.is_official,
  synthetic_flag = EXCLUDED.synthetic_flag,
  disclaimer = EXCLUDED.disclaimer,
  source_system = EXCLUDED.source_system,
  source_updated_at = EXCLUDED.source_updated_at,
  synced_at = now()
"""

def values(row):
    return (
        row.document_id, row.country_code, row.language_code, row.document_type,
        row.topic, row.title, row.body, row.version, row.status,
        row.effective_date, row.review_date, row.is_official, row.synthetic_flag,
        row.disclaimer, row.source_system, row.source_updated_at,
    )

with lakebase_connection(cfg) as connection:
    with connection.cursor() as cursor:
        # A transação torna a reconciliação atômica: falhas preservam o espelho anterior.
        for offset in range(0, len(rows), 1000):
            cursor.executemany(upsert, [values(row) for row in rows[offset:offset + 1000]])

        source_ids = [row.document_id for row in rows]
        cursor.execute(
            "DELETE FROM quilla_app.regulatory_documents WHERE NOT (document_id = ANY(%s))",
            (source_ids,),
        )
        deleted = cursor.rowcount
        cursor.execute(
            """SELECT count(*), count(DISTINCT country_code),
                      count(DISTINCT language_code),
                      count(*) FILTER (WHERE is_official OR NOT synthetic_flag)
               FROM quilla_app.regulatory_documents"""
        )
        count, countries, languages, unsafe = cursor.fetchone()
        cursor.execute(
            """SELECT document_id, ts_rank(search_vector,
                      plainto_tsquery('portuguese', %s)) AS score
               FROM quilla_app.regulatory_documents
               WHERE country_code = 'BR'
                 AND search_vector @@ plainto_tsquery('portuguese', %s)
               ORDER BY score DESC LIMIT 3""",
            ("privacidade dados", "privacidade dados"),
        )
        pt_results = [row[0] for row in cursor.fetchall()]
        cursor.execute(
            """SELECT document_id, ts_rank(search_vector,
                      plainto_tsquery('spanish', %s)) AS score
               FROM quilla_app.regulatory_documents
               WHERE country_code = 'MX'
                 AND search_vector @@ plainto_tsquery('spanish', %s)
               ORDER BY score DESC LIMIT 3""",
            ("privacidad datos", "privacidad datos"),
        )
        es_results = [row[0] for row in cursor.fetchall()]
    connection.commit()

assert count == len(rows), f"Lakebase/source mismatch: {count} != {len(rows)}"
assert countries == 12
assert languages == 2
assert unsafe == 0

print(json.dumps({
    "status": "ready",
    "source_rows": len(rows),
    "lakebase_rows": count,
    "deleted_stale_rows": deleted,
    "countries": countries,
    "languages": languages,
    "full_text_pt_results": pt_results,
    "full_text_es_results": es_results,
}, ensure_ascii=False))

