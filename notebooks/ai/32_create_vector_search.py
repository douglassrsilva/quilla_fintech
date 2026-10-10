# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · AI Search híbrido e reranking
# MAGIC
# MAGIC Cria um índice Delta Sync HYBRID, gera embeddings PT/ES com Qwen3 e valida
# MAGIC o reranker gerenciado da Databricks com filtros por jurisdição.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import datetime
import time

from databricks.ai_search.client import VectorSearchClient
from databricks.ai_search.reranker import DatabricksReranker

cfg = ai_config()
source = ai_table(cfg, "regulatory_chunks")
index_name = ai_table(cfg, "regulatory_knowledge_hybrid_idx")
client = VectorSearchClient(disable_notice=True)

endpoints = client.list_endpoints().get("endpoints", [])
if cfg["vs_endpoint"] not in {endpoint.get("name") for endpoint in endpoints}:
    client.create_endpoint_and_wait(
        name=cfg["vs_endpoint"],
        endpoint_type="STANDARD",
        verbose=True,
        timeout=datetime.timedelta(minutes=90),
    )

existing = client.list_indexes(cfg["vs_endpoint"]).get("vector_indexes", [])
if index_name not in {index.get("name") for index in existing}:
    client.create_delta_sync_index_and_wait(
        endpoint_name=cfg["vs_endpoint"],
        index_name=index_name,
        source_table_name=source,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",
        embedding_source_column="chunk_text",
        embedding_model_endpoint_name=cfg["embedding_endpoint"],
        sync_computed_embeddings=True,
        index_subtype="HYBRID",
        verbose=True,
        timeout=datetime.timedelta(hours=3),
    )
else:
    index = client.get_index(cfg["vs_endpoint"], index_name)
    description = index.describe()
    if description.get("status", {}).get("ready"):
        index.sync()

index = client.get_index(cfg["vs_endpoint"], index_name)
for _ in range(180):
    description = index.describe()
    status = description.get("status", {})
    if status.get("ready") and "SYNC" not in status.get("detailed_state", "").upper():
        break
    time.sleep(20)
else:
    raise TimeoutError(f"Vector Search index not ready: {description.get('status')}")

reranker = DatabricksReranker(columns_to_rerank=["chunk_text"])
tests = [
    ("pt", "BR", "Como proteger dados pessoais e aplicar controles de privacidade?"),
    ("es", "MX", "¿Cómo proteger datos personales y aplicar controles de privacidad?"),
]
evidence = []
for language, country, query in tests:
    response = index.similarity_search(
        query_text=query,
        query_type="HYBRID",
        filters={"language_code": language, "country_code": country},
        columns=[
            "chunk_id", "document_id", "country_code", "language_code",
            "topic", "title", "chunk_text", "disclaimer", "is_official",
        ],
        num_results=5,
        reranker=reranker,
    )
    result = response.get("result", {})
    manifest = response.get("manifest", {}) or result.get("manifest", {})
    columns = [item["name"] for item in manifest.get("columns", [])]
    rows = [dict(zip(columns, row)) for row in result.get("data_array", [])]
    if not rows:
        raise AssertionError(f"No AI Search results for {language}/{country}")
    for row in rows:
        assert row["language_code"] == language
        assert row["country_code"] == country
        assert row["is_official"] is False
    evidence.append({
        "language": language,
        "country": country,
        "query": query,
        "result_ids": [row["document_id"] for row in rows],
        "result_count": len(rows),
    })

print(json.dumps({
    "status": "ready",
    "endpoint": cfg["vs_endpoint"],
    "index": index_name,
    "index_subtype": "HYBRID",
    "embedding_endpoint": cfg["embedding_endpoint"],
    "reranker": "DatabricksReranker",
    "tests": evidence,
}, ensure_ascii=False))
