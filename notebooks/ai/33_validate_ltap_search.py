# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Validação integrada LTAP + AI Search

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from databricks.ai_search.client import VectorSearchClient
from databricks.ai_search.reranker import DatabricksReranker
from pyspark.sql import Row

cfg = ai_config()
documents = ai_table(cfg, "regulatory_documents")
chunks = ai_table(cfg, "regulatory_chunks")
evidence_table = ai_table(cfg, "search_validation_runs")
index_name = ai_table(cfg, "regulatory_knowledge_hybrid_idx")

doc_count = spark.table(documents).count()
chunk_count = spark.table(chunks).count()
assert doc_count == 25_000, f"Expected 25,000 documents, got {doc_count}"
assert chunk_count == doc_count

with lakebase_connection(cfg) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM quilla_app.regulatory_documents")
        lakebase_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'quilla_app'"
        )
        ltap_tables = cursor.fetchone()[0]
assert lakebase_count == doc_count
assert ltap_tables >= 11

index = VectorSearchClient(disable_notice=True).get_index(cfg["vs_endpoint"], index_name)
response = index.similarity_search(
    query_text="¿Qué controles de seguridad y prevención de fraude debo revisar?",
    query_type="HYBRID",
    filters={"language_code": "es", "country_code": "CO"},
    columns=["document_id", "country_code", "language_code", "title", "chunk_text"],
    num_results=5,
    reranker=DatabricksReranker(columns_to_rerank=["chunk_text"]),
)
result_rows = response.get("result", {}).get("data_array", [])
assert result_rows, "Integrated hybrid search returned no results"

run_id = str(uuid.uuid4())
record = Row(
    validation_run_id=run_id,
    validated_at=datetime.now(timezone.utc),
    lakebase_project=cfg["lakebase_project"],
    lakebase_database=cfg["lakebase_database"],
    ltap_table_count=int(ltap_tables),
    document_count=int(doc_count),
    chunk_count=int(chunk_count),
    lakebase_search_count=int(lakebase_count),
    vector_search_endpoint=cfg["vs_endpoint"],
    vector_search_index=index_name,
    embedding_endpoint=cfg["embedding_endpoint"],
    retrieval_mode="HYBRID_RERANKED",
    result_count=len(result_rows),
    status="PASSED",
)
spark.createDataFrame([record]).write.mode("append").saveAsTable(evidence_table)

print(json.dumps(record.asDict(), ensure_ascii=False, default=str))
