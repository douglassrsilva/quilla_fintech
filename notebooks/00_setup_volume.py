# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = load_config()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg['catalog']}`.`{cfg['schema']}`")
spark.sql(
    f"COMMENT ON SCHEMA `{cfg['catalog']}`.`{cfg['schema']}` IS "
    "'Quilla synthetic raw landing data. Demonstration use only.'"
)
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{cfg['catalog']}`.`{cfg['schema']}`.`{cfg['volume']}` "
    "COMMENT 'Versioned Parquet landing zone for Quilla synthetic data'"
)

dbutils.fs.mkdirs(cfg["root"])
print(json.dumps({
    "catalog": cfg["catalog"],
    "schema": cfg["schema"],
    "volume": cfg["volume"],
    "snapshot": cfg["snapshot"],
    "root": cfg["root"],
    "seed": cfg["seed"],
}))

