# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import dbldatagen as dg
from mimesis import Generic
from pyspark.sql import types as T

cfg = load_config({"party_rows": 144_000})
rows = cfg["party_rows"]
pool_size = min(512, max(50, rows // len(COUNTRIES)))

# A small locale-aware Mimesis pool is expanded with Spark. This keeps driver memory
# bounded while retaining native PT/ES names and deterministic generation.
name_rows = []
for index, country in enumerate(COUNTRIES):
    mimesis_locale = "pt-br" if country["code"] == "BR" else ("es-mx" if country["code"] == "MX" else "es")
    generator = Generic(locale=mimesis_locale, seed=cfg["seed"] + index)
    for pool_index in range(pool_size):
        name_rows.append((country["code"], pool_index, generator.person.first_name(), generator.person.last_name()))

name_pool = spark.createDataFrame(
    name_rows,
    "country_code string, pool_index int, generated_first_name string, generated_last_name string",
)

# dbldatagen supplies the scalable row backbone; localized attributes are joined from
# the bounded Mimesis pool and all other values use deterministic Spark expressions.
base = (
    dg.DataGenerator(
        sparkSession=spark,
        name="quilla_parties",
        rows=rows,
        partitions=max(4, rows // 100_000),
        randomSeed=cfg["seed"],
    )
    .withIdOutput()
    .build()
    .withColumnRenamed("id", "row_id")
    .withColumn("country_index", F.pmod(F.col("row_id"), F.lit(12)).cast("int"))
    .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
    .withColumn("pool_index", F.pmod(stable_hash("row_id", seed=cfg["seed"]), F.lit(pool_size)).cast("int"))
    .join(F.broadcast(name_pool), ["country_code", "pool_index"], "inner")
)

h = stable_hash("row_id", seed=cfg["seed"])
party_id = F.format_string("QPTY-%012d", F.col("row_id") + 1)
is_company = F.pmod(h, F.lit(100)) < 22
language = country_value(F.col("country_code"), "language")
company_prefix = F.when(language == "pt", F.lit("Grupo")).otherwise(F.lit("Grupo"))
company_suffix = F.when(language == "pt", F.lit("Tecnologia Ltda.")).otherwise(F.lit("Tecnología S.A."))

parties_df = (
    base
    .withColumn("party_id", party_id)
    .withColumn("party_type", F.when(is_company, "company").otherwise("person"))
    .withColumn("locale_code", country_value(F.col("country_code"), "locale"))
    .withColumn("preferred_language", language)
    .withColumn("first_name", F.when(~is_company, F.col("generated_first_name")))
    .withColumn("last_name", F.when(~is_company, F.col("generated_last_name")))
    .withColumn(
        "legal_name",
        F.when(is_company, F.concat_ws(" ", company_prefix, F.col("generated_last_name"), company_suffix))
        .otherwise(F.concat_ws(" ", F.col("generated_first_name"), F.col("generated_last_name"))),
    )
    .withColumn("display_name", F.col("legal_name"))
    .withColumn("email", F.concat(F.lower(F.regexp_replace("party_id", "-", "")), F.lit("@example.test")))
    .withColumn("phone_masked", F.format_string("+%02d *** *** %04d", F.pmod(h, F.lit(90)) + 10, F.pmod(h, F.lit(10000))))
    .withColumn("city", country_value(F.col("country_code"), "city"))
    .withColumn(
        "customer_segment",
        F.when(is_company & (F.pmod(h, F.lit(10)) < 2), "enterprise")
        .when(is_company, "smb")
        .when(F.pmod(h, F.lit(10)) < 2, "affluent")
        .otherwise("mass_market"),
    )
    .withColumn("kyc_status", F.when(F.pmod(h, F.lit(100)) < 91, "verified").when(F.pmod(h, F.lit(100)) < 97, "pending").otherwise("enhanced_review"))
    .withColumn("risk_band", F.when(F.pmod(h, F.lit(100)) < 68, "low").when(F.pmod(h, F.lit(100)) < 93, "medium").otherwise("high"))
    .withColumn("onboarding_ts", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2019-01-01 00:00:00")) + F.pmod(h, F.lit(241_747_199)))))
    .withColumn("source_system", F.concat(F.lit("QUILLA_CRM_"), F.col("country_code")))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select(
        "party_id", "party_type", "country_code", "locale_code", "preferred_language",
        "legal_name", "first_name", "last_name", "display_name", "email", "phone_masked",
        "city", "customer_segment", "kyc_status", "risk_band", "onboarding_ts",
        "source_system", "synthetic_flag", "ingested_at",
    )
)

write_parquet(parties_df, cfg, "parties", ["country_code"])
