# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · AI Prep Search
# MAGIC
# MAGIC Aplica `ai_prep_search` (Beta) a uma amostra estratificada do corpus PT/ES.
# MAGIC O corpus principal de 25 mil documentos permanece intacto; o tamanho desta
# MAGIC vitrine de preparação semântica é parametrizável para controlar custo.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

cfg = ai_config()
source = ai_table(cfg, "regulatory_documents")
prepared_table = ai_table(cfg, "regulatory_documents_ai_prepared")
prepared_chunks_table = ai_table(cfg, "regulatory_chunks_ai_prepared")

window = Window.partitionBy("country_code", "language_code", "topic").orderBy("document_id")
sample = (
    spark.table(source)
    .withColumn("segment_rank", F.row_number().over(window))
    .filter(F.col("segment_rank") <= F.lit(cfg["prep_docs_per_segment"]))
    .withColumn(
        "search_content",
        F.concat_ws(
            "\n",
            F.concat(F.lit("Título/Title: "), F.col("title")),
            F.concat(F.lit("Tema/Topic: "), F.col("topic")),
            F.concat(F.lit("Mercado/Market: "), F.col("country_code")),
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
        "search_content",
        "is_official",
        "synthetic_flag",
        "disclaimer",
    )
)
sample.createOrReplaceTempView("_quilla_ai_prep_source")

prepared = spark.sql(
    """
    SELECT *, ai_prep_search(search_content, map('version', '2.0')) AS prep_result
    FROM _quilla_ai_prep_source
    """
)
(
    prepared.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(prepared_table)
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {prepared_chunks_table} AS
    SELECT
      concat(p.document_id, '#prep-', lpad(c.value:chunk_position::STRING, 4, '0')) AS prep_chunk_id,
      c.value:chunk_id::STRING AS function_chunk_id,
      c.value:chunk_position::INT AS chunk_position,
      c.value:chunk_to_retrieve::STRING AS chunk_to_retrieve,
      c.value:chunk_to_embed::STRING AS chunk_to_embed,
      c.value:pages AS pages,
      p.document_id,
      p.country_code,
      p.language_code,
      p.document_type,
      p.topic,
      p.title,
      p.is_official,
      p.synthetic_flag,
      p.disclaimer,
      current_timestamp() AS prepared_at
    FROM {prepared_table} p,
    LATERAL variant_explode(p.prep_result:document.contents) c
    """
)

for table in (prepared_table, prepared_chunks_table):
    spark.sql(
        f"""ALTER TABLE {table} SET TBLPROPERTIES (
          'quilla.synthetic' = 'true',
          'quilla.official' = 'false',
          'quilla.layer' = 'ai_search_preparation',
          'quilla.languages' = 'pt,es',
          'quilla.preview_feature' = 'ai_prep_search'
        )"""
    )

stats = spark.table(prepared_chunks_table).agg(
    F.count("*").alias("chunks"),
    F.countDistinct("document_id").alias("documents"),
    F.countDistinct("country_code").alias("countries"),
    F.countDistinct("language_code").alias("languages"),
    F.sum(F.when(F.col("is_official"), 1).otherwise(0)).alias("official_rows"),
    F.sum(F.when(F.length("chunk_to_embed") > 0, 1).otherwise(0)).alias("embedding_ready"),
).first()

expected_documents = 72 * cfg["prep_docs_per_segment"]
assert stats.documents == expected_documents, (stats.documents, expected_documents)
assert stats.chunks >= stats.documents
assert stats.countries == 12
assert stats.languages == 2
assert stats.official_rows == 0
assert stats.embedding_ready == stats.chunks

print(json.dumps({
    "status": "PASSED",
    "feature": "ai_prep_search",
    "source_table": source,
    "prepared_table": prepared_table,
    "prepared_chunks_table": prepared_chunks_table,
    "documents": stats.documents,
    "chunks": stats.chunks,
    "countries": stats.countries,
    "languages": ["pt", "es"],
}, ensure_ascii=False))
