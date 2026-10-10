# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · AI Enrich governado
# MAGIC
# MAGIC Enriquece uma amostra estratificada PT/ES com metadados operacionais e
# MAGIC justificativas, ancorando a resposta no índice AI Search governado.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

cfg = ai_config()
source = ai_table(cfg, "regulatory_documents")
output = ai_table(cfg, "regulatory_knowledge_enriched")
index_name = ai_table(cfg, "regulatory_knowledge_hybrid_idx")

window = Window.partitionBy("country_code", "language_code", "topic").orderBy("document_id")
sample = (
    spark.table(source)
    .withColumn("segment_rank", F.row_number().over(window))
    .filter(F.col("segment_rank") <= F.lit(cfg["enrich_docs_per_segment"]))
    .withColumn(
        "enrichment_content",
        F.concat_ws(
            "\n",
            F.concat(F.lit("Document ID: "), F.col("document_id")),
            F.concat(F.lit("Country: "), F.col("country_code")),
            F.concat(F.lit("Language: "), F.col("language_code")),
            F.concat(F.lit("Topic: "), F.col("topic")),
            F.concat(F.lit("Title: "), F.col("title")),
            F.col("body"),
            F.col("disclaimer"),
        ),
    )
    .select(
        "document_id",
        "country_code",
        "language_code",
        "document_type",
        "topic",
        "title",
        "is_official",
        "synthetic_flag",
        "disclaimer",
        "enrichment_content",
    )
)
sample.createOrReplaceTempView("_quilla_ai_enrich_source")

schema = json.dumps({
    "regulatory_domain": {
        "type": "string",
        "description": (
            "Categoria normalizada derivada do tópico: PRIVACY, RESPONSIBLE_CREDIT, "
            "FRAUD_PREVENTION, DATA_RESIDENCY, CUSTOMER_CARE ou SECURITY. Pode ser nula "
            "se a evidência sintética não sustentar uma classificação."
        ),
    },
    "operational_risk_level": {
        "type": "string",
        "description": "Um de LOW, MEDIUM ou HIGH, considerando somente o conteúdo sintético.",
    },
    "control_families": {
        "type": "array",
        "description": "Famílias de controles internos sugeridas pelo conteúdo.",
        "items": {"type": "string"},
    },
    "executive_summary": {
        "type": "string",
        "description": "Resumo no mesmo idioma do documento, sem apresentá-lo como orientação oficial.",
    },
    "review_priority": {
        "type": "integer",
        "description": "Prioridade demonstrativa de revisão entre 1 e 5.",
    },
    "applicable_market": {
        "type": "string",
        "description": "Código ISO alpha-2 do país informado no documento.",
    },
    "response_language": {
        "type": "string",
        "description": "pt ou es, igual ao idioma do documento.",
    },
}, ensure_ascii=False)

knowledge_sources = json.dumps([{
    "type": "vector_search",
    "description": "Base sintética e não oficial de políticas demonstrativas da Quilla.",
    "config": {
        "index_name": index_name,
        "text_col": "chunk_text",
        "doc_uri_col": "document_id",
        "filter_columns": ["country_code", "language_code", "topic"],
    },
}], ensure_ascii=False)

instructions = (
    "Enriqueça cada linha somente para fins demonstrativos. Preserve o país e o idioma "
    "informados, nunca trate o conteúdo como legislação ou orientação oficial e use a "
    "fonte AI Search governada como grounding. Use PT para documentos pt e ES para es."
)

def sql_literal(value: str) -> str:
    return value.replace("'", "''")

enriched = spark.sql(
    f"""
    SELECT *, ai_enrich(
      enrichment_content,
      '{sql_literal(schema)}',
      parse_json('{sql_literal(knowledge_sources)}'),
      options => map(
        'instructions', '{sql_literal(instructions)}',
        'enableRationale', 'true'
      )
    ) AS enrichment_result
    FROM _quilla_ai_enrich_source
    """
).select(
    "*",
    F.expr("enrichment_result:response:regulatory_domain:value::STRING").alias("regulatory_domain"),
    F.expr("enrichment_result:response:operational_risk_level:value::STRING").alias("operational_risk_level"),
    F.expr("enrichment_result:response:executive_summary:value::STRING").alias("executive_summary"),
    F.expr("enrichment_result:response:review_priority:value::INT").alias("review_priority"),
    F.expr("enrichment_result:response:applicable_market:value::STRING").alias("applicable_market"),
    F.expr("enrichment_result:response:response_language:value::STRING").alias("response_language"),
    F.expr("enrichment_result:metadata:sources::STRING").alias("grounding_sources"),
    F.expr("enrichment_result:response::STRING").alias("enrichment_response"),
    F.expr("enrichment_result:error_message::STRING").alias("error_message"),
    F.current_timestamp().alias("enriched_at"),
)

(
    enriched.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output)
)
spark.sql(
    f"""ALTER TABLE {output} SET TBLPROPERTIES (
      'quilla.synthetic' = 'true',
      'quilla.official' = 'false',
      'quilla.layer' = 'ai_enrichment',
      'quilla.languages' = 'pt,es',
      'quilla.preview_feature' = 'ai_enrich',
      'quilla.grounding_index' = '{index_name}'
    )"""
)

stats = spark.table(output).agg(
    F.count("*").alias("rows"),
    F.countDistinct("country_code").alias("countries"),
    F.countDistinct("language_code").alias("languages"),
    F.sum(F.when(F.col("error_message").isNotNull(), 1).otherwise(0)).alias("errors"),
    F.sum(F.when(F.col("is_official"), 1).otherwise(0)).alias("official_rows"),
    F.sum(F.when(F.col("enrichment_response").isNotNull(), 1).otherwise(0)).alias("enriched_rows"),
).first()

expected_rows = 72 * cfg["enrich_docs_per_segment"]
assert stats.rows == expected_rows, (stats.rows, expected_rows)
assert stats.countries == 12
assert stats.languages == 2
assert stats.errors == 0
assert stats.official_rows == 0
assert stats.enriched_rows == stats.rows

print(json.dumps({
    "status": "PASSED",
    "feature": "ai_enrich",
    "output_table": output,
    "grounding_index": index_name,
    "rows": stats.rows,
    "countries": stats.countries,
    "languages": ["pt", "es"],
    "errors": stats.errors,
}, ensure_ascii=False))
