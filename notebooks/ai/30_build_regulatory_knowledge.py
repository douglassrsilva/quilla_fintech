# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Base de conhecimento regulatória demonstrativa PT/ES
# MAGIC
# MAGIC Cura os documentos aprovados pela Silver e publica documentos e chunks governados.
# MAGIC Todo o corpus permanece explicitamente sintético e não oficial.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

cfg = ai_config()
source = f"{cfg['catalog']}.{cfg['silver_schema']}.documents"
documents = ai_table(cfg, "regulatory_documents")
chunks = ai_table(cfg, "regulatory_chunks")

spark.sql(
    f"""CREATE SCHEMA IF NOT EXISTS {cfg['catalog']}.{cfg['ai_schema']}
        COMMENT 'Quilla AI — conhecimento PT/ES, busca híbrida e evidências de recuperação.'"""
)

source_df = spark.table(source)
safe_df = source_df.filter(
    (F.col("synthetic_flag") == F.lit(True))
    & (F.col("is_official") == F.lit(False))
    & F.col("language_code").isin("pt", "es")
)

if safe_df.count() != source_df.count():
    raise ValueError("Unsafe or unsupported documents found in the Silver source")

curated_df = safe_df.select(
    "document_id",
    "country_code",
    "language_code",
    "document_type",
    "topic",
    "title",
    "body",
    "version",
    "status",
    "effective_date",
    "review_date",
    "is_official",
    "synthetic_flag",
    "disclaimer",
    "source_system",
    F.lit("synthetic_demo_only").alias("knowledge_scope"),
    F.col("ingested_at").alias("source_updated_at"),
)

(
    curated_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(documents)
)

chunk_df = curated_df.select(
    F.concat(F.col("document_id"), F.lit("#000")).alias("chunk_id"),
    "document_id",
    "country_code",
    "language_code",
    "document_type",
    "topic",
    "title",
    F.concat_ws(
        "\n",
        F.concat(F.lit("Título: "), F.col("title")),
        F.concat(F.lit("Tema: "), F.col("topic")),
        F.concat(F.lit("País: "), F.col("country_code")),
        F.col("body"),
        F.col("disclaimer"),
    ).alias("chunk_text"),
    "version",
    "status",
    "effective_date",
    "review_date",
    "is_official",
    "synthetic_flag",
    "disclaimer",
    "knowledge_scope",
    "source_updated_at",
)

(
    chunk_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(chunks)
)

for table in (documents, chunks):
    spark.sql(
        f"""ALTER TABLE {table} SET TBLPROPERTIES (
          delta.enableChangeDataFeed = true,
          'quilla.synthetic' = 'true',
          'quilla.official' = 'false',
          'quilla.layer' = 'ai_knowledge',
          'quilla.languages' = 'pt,es'
        )"""
    )

# Triggered Delta Sync indexes must not go longer than the source table's
# deleted-file retention window. Thirty days gives the on-demand demo ample
# recovery headroom without retaining files indefinitely.
spark.sql(
    f"""ALTER TABLE {chunks} SET TBLPROPERTIES (
      'delta.deletedFileRetentionDuration' = 'interval 30 days'
    )"""
)

stats = spark.table(chunks).agg(
    F.count("*").alias("chunks"),
    F.countDistinct("document_id").alias("documents"),
    F.countDistinct("country_code").alias("countries"),
    F.countDistinct("language_code").alias("languages"),
    F.sum(F.when(F.col("is_official"), 1).otherwise(0)).alias("official_rows"),
).first()

assert stats.documents == source_df.count()
assert stats.chunks == stats.documents
assert stats.countries == 12
assert stats.languages == 2
assert stats.official_rows == 0

print(json.dumps({
    "status": "ready",
    "source": source,
    "documents_table": documents,
    "chunks_table": chunks,
    "documents": stats.documents,
    "chunks": stats.chunks,
    "countries": stats.countries,
    "languages": ["pt", "es"],
    "official_rows": stats.official_rows,
}, ensure_ascii=False))
