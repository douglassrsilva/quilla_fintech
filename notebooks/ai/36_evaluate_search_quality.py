# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Avaliação de qualidade do AI Search
# MAGIC
# MAGIC Compara ANN, HYBRID, FULL_TEXT (Beta) e HYBRID + DatabricksReranker
# MAGIC sobre consultas naturais e por palavras-chave em todos os 12 países.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import hashlib
import math
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from databricks.ai_search.client import VectorSearchClient
from databricks.ai_search.reranker import DatabricksReranker
from pyspark.sql import types as T

cfg = ai_config()
source = ai_table(cfg, "regulatory_documents")
index_name = ai_table(cfg, "regulatory_knowledge_hybrid_idx")
query_table = ai_table(cfg, "search_quality_queries")
detail_table = ai_table(cfg, "search_quality_query_results")
metrics_table = ai_table(cfg, "search_quality_metrics")
validation_table = ai_table(cfg, "search_quality_validation_runs")
k = cfg["quality_results_per_query"]

country_names = {
    "AR": "Argentina",
    "BR": "Brasil",
    "CL": "Chile",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "DO": "República Dominicana",
    "EC": "Ecuador",
    "GT": "Guatemala",
    "MX": "México",
    "PA": "Panamá",
    "PE": "Perú",
    "UY": "Uruguay",
}
semantic_prompts = {
    "pt": {
        "Privacidade": "Como a Quilla deve proteger informações pessoais e respeitar o consentimento dos clientes no {market}?",
        "Crédito responsável": "Que cuidados a Quilla deve tomar para não oferecer empréstimos inadequados ao perfil do cliente no {market}?",
        "Prevenção à fraude": "Como a Quilla pode identificar transações suspeitas e impedir golpes no {market}?",
        "Residência de dados": "Onde as informações dos clientes da Quilla podem ser armazenadas e processadas no {market}?",
        "Atendimento": "Como a Quilla deve tratar reclamações e garantir respostas claras aos clientes no {market}?",
        "Segurança": "Que medidas protegem contas, acessos e sistemas da Quilla contra ataques no {market}?",
    },
    "es": {
        "Privacidad": "¿Cómo debe Quilla proteger información personal y respetar el consentimiento de sus clientes en {market}?",
        "Crédito responsable": "¿Qué controles debe aplicar Quilla para no ofrecer préstamos inadecuados al perfil del cliente en {market}?",
        "Prevención de fraude": "¿Cómo puede Quilla detectar transacciones sospechosas e impedir estafas en {market}?",
        "Residencia de datos": "¿Dónde puede Quilla almacenar y procesar la información de sus clientes en {market}?",
        "Atención": "¿Cómo debe Quilla atender reclamos y garantizar respuestas claras a sus clientes en {market}?",
        "Seguridad": "¿Qué medidas protegen las cuentas, accesos y sistemas de Quilla contra ataques en {market}?",
    },
}

segments = [
    row.asDict()
    for row in spark.table(source)
    .select("country_code", "language_code", "topic")
    .distinct()
    .orderBy("country_code", "topic")
    .collect()
]
assert len(segments) == 72

queries = []
for segment in segments:
    country = segment["country_code"]
    language = segment["language_code"]
    topic = segment["topic"]
    market = country_names[country]
    natural = semantic_prompts[language][topic].format(market=market)
    keyword = f"{topic} Quilla {country} controles"
    for query_kind, query_text in (("NATURAL_LANGUAGE", natural), ("KEYWORD", keyword)):
        digest = hashlib.sha256(
            f"{country}|{language}|{topic}|{query_kind}".encode("utf-8")
        ).hexdigest()[:20]
        queries.append({
            "query_id": f"Q-{digest}",
            "query_text": query_text,
            "query_kind": query_kind,
            "country_code": country,
            "language_code": language,
            "expected_topic": topic,
            "synthetic_flag": True,
        })

query_schema = T.StructType([
    T.StructField("query_id", T.StringType(), False),
    T.StructField("query_text", T.StringType(), False),
    T.StructField("query_kind", T.StringType(), False),
    T.StructField("country_code", T.StringType(), False),
    T.StructField("language_code", T.StringType(), False),
    T.StructField("expected_topic", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
(
    spark.createDataFrame(queries, query_schema)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(query_table)
)

index = VectorSearchClient(disable_notice=True).get_index(cfg["vs_endpoint"], index_name)
strategies = (
    ("ANN", None, False),
    ("HYBRID", "HYBRID", False),
    ("FULL_TEXT", "FULL_TEXT", False),
    ("HYBRID_RERANKED", "HYBRID", True),
)
columns = [
    "chunk_id",
    "document_id",
    "country_code",
    "language_code",
    "topic",
    "title",
    "chunk_text",
    "disclaimer",
    "is_official",
    "synthetic_flag",
]


def search_once(query, strategy_name, query_type, use_reranker):
    kwargs = {
        "query_text": query["query_text"],
        "filters": {
            "country_code": query["country_code"],
            "language_code": query["language_code"],
        },
        "columns": columns,
        "num_results": k,
    }
    if query_type:
        kwargs["query_type"] = query_type
    if use_reranker:
        kwargs["reranker"] = DatabricksReranker(columns_to_rerank=["chunk_text", "title"])

    last_error = None
    for attempt in range(3):
        started = time.perf_counter()
        try:
            response = index.similarity_search(**kwargs)
            latency_ms = (time.perf_counter() - started) * 1000.0
            result = response.get("result", {})
            manifest = response.get("manifest", {}) or result.get("manifest", {})
            names = [item["name"] for item in manifest.get("columns", [])]
            rows = [dict(zip(names, values)) for values in result.get("data_array", [])]
            return rows, latency_ms
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(
        f"Search failed after retries for {strategy_name}/{query['query_id']}: {last_error}"
    )


def evaluate_strategy(strategy):
    strategy_name, query_type, use_reranker = strategy
    evaluated = []
    for query in queries:
        rows, latency_ms = search_once(query, strategy_name, query_type, use_reranker)
        if not rows:
            raise AssertionError(f"No results for {strategy_name}/{query['query_id']}")
        for rank, row in enumerate(rows, start=1):
            if row.get("country_code") != query["country_code"]:
                raise AssertionError(f"Country filter violation: {query} / {row}")
            if row.get("language_code") != query["language_code"]:
                raise AssertionError(f"Language filter violation: {query} / {row}")
            if row.get("is_official") is not False or row.get("synthetic_flag") is not True:
                raise AssertionError(f"Unsafe retrieval row: {row}")
            relevance = 3 if row.get("topic") == query["expected_topic"] else 1
            evaluated.append({
                **query,
                "strategy": strategy_name,
                "uses_reranker": use_reranker,
                "rank": rank,
                "chunk_id": row.get("chunk_id"),
                "document_id": row.get("document_id"),
                "result_topic": row.get("topic"),
                "relevance_grade": relevance,
                "latency_ms": latency_ms,
            })
    return evaluated


evaluation_run_id = str(uuid.uuid4())
evaluated_at = datetime.now(timezone.utc)
details = []
with ThreadPoolExecutor(max_workers=len(strategies)) as executor:
    futures = {executor.submit(evaluate_strategy, strategy): strategy[0] for strategy in strategies}
    for future in as_completed(futures):
        details.extend(future.result())

detail_schema = T.StructType([
    T.StructField("evaluation_run_id", T.StringType(), False),
    T.StructField("evaluated_at", T.TimestampType(), False),
    T.StructField("query_id", T.StringType(), False),
    T.StructField("query_text", T.StringType(), False),
    T.StructField("query_kind", T.StringType(), False),
    T.StructField("country_code", T.StringType(), False),
    T.StructField("language_code", T.StringType(), False),
    T.StructField("expected_topic", T.StringType(), False),
    T.StructField("strategy", T.StringType(), False),
    T.StructField("uses_reranker", T.BooleanType(), False),
    T.StructField("rank", T.IntegerType(), False),
    T.StructField("chunk_id", T.StringType(), True),
    T.StructField("document_id", T.StringType(), True),
    T.StructField("result_topic", T.StringType(), True),
    T.StructField("relevance_grade", T.IntegerType(), False),
    T.StructField("latency_ms", T.DoubleType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
detail_records = [
    {
        **row,
        "evaluation_run_id": evaluation_run_id,
        "evaluated_at": evaluated_at,
    }
    for row in details
]
spark.createDataFrame(detail_records, detail_schema).write.mode("append").saveAsTable(detail_table)

by_query = {}
for row in details:
    key = (row["strategy"], row["query_kind"], row["language_code"], row["query_id"])
    by_query.setdefault(key, []).append(row)

per_query_metrics = []
ideal_dcg = sum(7.0 / math.log2(rank + 1) for rank in range(1, k + 1))
for (strategy, query_kind, language, query_id), rows in by_query.items():
    rows.sort(key=lambda item: item["rank"])
    relevance = [row["relevance_grade"] for row in rows]
    relevant_ranks = [rank for rank, grade in enumerate(relevance, start=1) if grade >= 2]
    dcg = sum((2 ** grade - 1) / math.log2(rank + 1) for rank, grade in enumerate(relevance, 1))
    per_query_metrics.append({
        "strategy": strategy,
        "query_kind": query_kind,
        "language_code": language,
        "query_id": query_id,
        "precision_at_k": len(relevant_ranks) / k,
        "topic_recall_at_k": 1.0 if relevant_ranks else 0.0,
        "mrr": 1.0 / relevant_ranks[0] if relevant_ranks else 0.0,
        "dcg_at_k": dcg,
        "ndcg_at_k": dcg / ideal_dcg,
        "average_relevance": sum(relevance) / k,
        "not_relevant_pct": sum(grade <= 1 for grade in relevance) / k,
        "latency_ms": rows[0]["latency_ms"],
    })

metric_groups = {}
for metric in per_query_metrics:
    key = (metric["strategy"], metric["query_kind"], metric["language_code"])
    metric_groups.setdefault(key, []).append(metric)

metric_records = []
for (strategy, query_kind, language), rows in metric_groups.items():
    count = len(rows)
    metric_records.append({
        "evaluation_run_id": evaluation_run_id,
        "evaluated_at": evaluated_at,
        "vector_search_endpoint": cfg["vs_endpoint"],
        "vector_search_index": index_name,
        "strategy": strategy,
        "query_kind": query_kind,
        "language_code": language,
        "query_count": count,
        "k": k,
        "precision_at_k": sum(r["precision_at_k"] for r in rows) / count,
        "topic_recall_at_k": sum(r["topic_recall_at_k"] for r in rows) / count,
        "mrr": sum(r["mrr"] for r in rows) / count,
        "dcg_at_k": sum(r["dcg_at_k"] for r in rows) / count,
        "ndcg_at_k": sum(r["ndcg_at_k"] for r in rows) / count,
        "average_relevance": sum(r["average_relevance"] for r in rows) / count,
        "not_relevant_pct": sum(r["not_relevant_pct"] for r in rows) / count,
        "average_latency_ms": sum(r["latency_ms"] for r in rows) / count,
        "synthetic_flag": True,
    })

metric_schema = T.StructType([
    T.StructField("evaluation_run_id", T.StringType(), False),
    T.StructField("evaluated_at", T.TimestampType(), False),
    T.StructField("vector_search_endpoint", T.StringType(), False),
    T.StructField("vector_search_index", T.StringType(), False),
    T.StructField("strategy", T.StringType(), False),
    T.StructField("query_kind", T.StringType(), False),
    T.StructField("language_code", T.StringType(), False),
    T.StructField("query_count", T.IntegerType(), False),
    T.StructField("k", T.IntegerType(), False),
    T.StructField("precision_at_k", T.DoubleType(), False),
    T.StructField("topic_recall_at_k", T.DoubleType(), False),
    T.StructField("mrr", T.DoubleType(), False),
    T.StructField("dcg_at_k", T.DoubleType(), False),
    T.StructField("ndcg_at_k", T.DoubleType(), False),
    T.StructField("average_relevance", T.DoubleType(), False),
    T.StructField("not_relevant_pct", T.DoubleType(), False),
    T.StructField("average_latency_ms", T.DoubleType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
spark.createDataFrame(metric_records, metric_schema).write.mode("append").saveAsTable(metrics_table)

expected_query_count = 144
expected_query_strategy_pairs = expected_query_count * len(strategies)
assert len(queries) == expected_query_count
assert len(by_query) == expected_query_strategy_pairs
assert {row["strategy"] for row in metric_records} == {item[0] for item in strategies}

validation_schema = T.StructType([
    T.StructField("evaluation_run_id", T.StringType(), False),
    T.StructField("validated_at", T.TimestampType(), False),
    T.StructField("query_count", T.IntegerType(), False),
    T.StructField("strategy_count", T.IntegerType(), False),
    T.StructField("country_count", T.IntegerType(), False),
    T.StructField("language_count", T.IntegerType(), False),
    T.StructField("result_count", T.IntegerType(), False),
    T.StructField("native_quality_evaluation_available", T.BooleanType(), False),
    T.StructField("native_trigger_mode", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
])
validation = {
    "evaluation_run_id": evaluation_run_id,
    "validated_at": evaluated_at,
    "query_count": len(queries),
    "strategy_count": len(strategies),
    "country_count": len({q["country_code"] for q in queries}),
    "language_count": len({q["language_code"] for q in queries}),
    "result_count": len(details),
    "native_quality_evaluation_available": True,
    "native_trigger_mode": "AI_SEARCH_INDEX_UI",
    "status": "PASSED",
}
spark.createDataFrame([validation], validation_schema).write.mode("append").saveAsTable(validation_table)

for table in (query_table, detail_table, metrics_table, validation_table):
    spark.sql(
        f"""ALTER TABLE {table} SET TBLPROPERTIES (
          'quilla.synthetic' = 'true',
          'quilla.layer' = 'ai_search_quality',
          'quilla.languages' = 'pt,es'
        )"""
    )

print(json.dumps({
    **validation,
    "vector_search_endpoint": cfg["vs_endpoint"],
    "vector_search_index": index_name,
    "strategies": [item[0] for item in strategies],
    "metrics_table": metrics_table,
}, ensure_ascii=False, default=str))
