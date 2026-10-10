# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from datetime import date

from pyspark.sql import types as T

cfg = load_config()

# COMMAND ----------

country_rows = [
    (
        c["code"], c["name_pt"], c["name_es"], c["locale"], c["language"],
        c["currency"], c["timezone"], c["tier"],
        "avaliar_por_pais / evaluar_por_pais", True, date(2024, 9, 1), None, True,
    )
    for c in COUNTRIES
]
country_schema = T.StructType([
    T.StructField("country_code", T.StringType(), False),
    T.StructField("country_name_pt", T.StringType(), False),
    T.StructField("country_name_es", T.StringType(), False),
    T.StructField("locale_code", T.StringType(), False),
    T.StructField("language_code", T.StringType(), False),
    T.StructField("currency_code", T.StringType(), False),
    T.StructField("timezone", T.StringType(), False),
    T.StructField("market_tier", T.StringType(), False),
    T.StructField("data_residency_policy", T.StringType(), False),
    T.StructField("is_active", T.BooleanType(), False),
    T.StructField("effective_from", T.DateType(), False),
    T.StructField("effective_to", T.DateType(), True),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
countries_df = spark.createDataFrame(country_rows, country_schema)
write_parquet(countries_df, cfg, "countries")

# COMMAND ----------

product_defs = [
    (1, "CONTA_DIGITAL", "banking", "Conta digital", "Cuenta digital", "Conta multimoeda para o dia a dia", "Cuenta multidivisa para el día a día", "individual", "regional", "low"),
    (2, "CONTA_PREMIUM", "banking", "Conta premium", "Cuenta premium", "Serviços e benefícios avançados", "Servicios y beneficios avanzados", "affluent", "regional", "medium"),
    (3, "CONTA_PJ", "business_banking", "Conta empresarial", "Cuenta empresarial", "Gestão financeira para empresas", "Gestión financiera para empresas", "smb", "regional", "medium"),
    (4, "CARTEIRA_DIGITAL", "payments", "Carteira digital", "Billetera digital", "Pagamentos e transferências instantâneas", "Pagos y transferencias instantáneas", "individual", "regional", "low"),
    (5, "CARTAO_DEBITO", "cards", "Cartão de débito", "Tarjeta de débito", "Compras vinculadas ao saldo", "Compras vinculadas al saldo", "individual", "regional", "medium"),
    (6, "CARTAO_CREDITO", "cards", "Cartão de crédito", "Tarjeta de crédito", "Crédito rotativo e parcelado", "Crédito rotativo y en cuotas", "individual", "country_adapted", "high"),
    (7, "CARTAO_CORPORATIVO", "cards", "Cartão corporativo", "Tarjeta corporativa", "Controle de despesas empresariais", "Control de gastos empresariales", "enterprise", "regional", "high"),
    (8, "PIX_TRANSFER", "payments", "Transferência instantânea", "Transferencia instantánea", "Movimentação local em tempo real", "Movimiento local en tiempo real", "all", "country_specific", "medium"),
    (9, "P2P", "payments", "Transferência entre pessoas", "Transferencia entre personas", "Envio de dinheiro entre clientes", "Envío de dinero entre clientes", "individual", "regional", "medium"),
    (10, "CROSS_BORDER", "payments", "Pagamentos internacionais", "Pagos internacionales", "Movimentação de dinheiro entre países", "Movimiento de dinero entre países", "all", "regional", "high"),
    (11, "ADQUIRENCIA", "merchant_services", "Adquirência", "Adquirencia", "Aceitação de pagamentos para lojistas", "Aceptación de pagos para comercios", "merchant", "country_adapted", "high"),
    (12, "PAYMENT_LINK", "merchant_services", "Link de pagamento", "Enlace de pago", "Cobrança digital compartilhável", "Cobro digital compartible", "merchant", "regional", "medium"),
    (13, "CREDITO_PESSOAL", "lending", "Crédito pessoal", "Crédito personal", "Crédito orientado por capacidade de pagamento", "Crédito según capacidad de pago", "individual", "country_adapted", "high"),
    (14, "CAPITAL_GIRO", "lending", "Capital de giro", "Capital de trabajo", "Financiamento para pequenas empresas", "Financiamiento para pequeñas empresas", "smb", "country_adapted", "high"),
    (15, "ANTECIPACAO", "lending", "Antecipação de recebíveis", "Anticipo de cuentas por cobrar", "Liquidez baseada em faturas", "Liquidez basada en facturas", "smb", "country_adapted", "high"),
    (16, "BNPL", "lending", "Compre agora, pague depois", "Compra ahora, paga después", "Parcelamento no ponto de venda", "Cuotas en el punto de venta", "individual", "country_adapted", "high"),
    (17, "INVEST_CASH", "investments", "Reserva inteligente", "Reserva inteligente", "Aplicação automática de liquidez", "Inversión automática de liquidez", "all", "country_adapted", "high"),
    (18, "FX_HEDGE", "treasury", "Proteção cambial", "Cobertura cambiaria", "Gestão de exposição entre moedas", "Gestión de exposición entre monedas", "business", "country_adapted", "high"),
    (19, "SEGURO_CELULAR", "insurance", "Seguro celular", "Seguro de celular", "Proteção para dispositivos", "Protección para dispositivos", "individual", "country_adapted", "medium"),
    (20, "SEGURO_VIDA", "insurance", "Seguro de vida", "Seguro de vida", "Cobertura de proteção pessoal", "Cobertura de protección personal", "individual", "country_adapted", "high"),
    (21, "SEGURO_PJ", "insurance", "Seguro empresarial", "Seguro empresarial", "Coberturas modulares para empresas", "Coberturas modulares para empresas", "smb", "country_adapted", "high"),
    (22, "OPEN_FINANCE", "data_services", "Conexão financeira", "Conexión financiera", "Consentimento e agregação de contas", "Consentimiento y agregación de cuentas", "all", "country_specific", "high"),
    (23, "FOLHA_PAGAMENTO", "business_banking", "Folha de pagamento", "Nómina", "Pagamentos a colaboradores", "Pagos a colaboradores", "business", "country_adapted", "high"),
    (24, "COBRANCA", "business_banking", "Cobrança inteligente", "Cobranza inteligente", "Automação de contas a receber", "Automatización de cuentas por cobrar", "business", "regional", "medium"),
    (25, "QUILLA_IQ", "intelligence", "Quilla IQ", "Quilla IQ", "Inteligência financeira explicável", "Inteligencia financiera explicable", "all", "regional", "medium"),
]
products_df = spark.createDataFrame(
    product_defs,
    "product_id int, product_code string, business_line string, product_name_pt string, product_name_es string, description_pt string, description_es string, customer_segment string, availability_scope string, risk_class string",
).withColumns({
    "is_active": F.lit(True),
    "effective_from": F.to_date(F.lit(EVENT_START)),
    "synthetic_flag": F.lit(True),
})
write_parquet(products_df, cfg, "products")

# COMMAND ----------

rates = {
    "BR": 5.40, "MX": 19.20, "CO": 4100.0, "AR": 1050.0,
    "CL": 940.0, "PE": 3.75, "EC": 1.0, "GT": 7.70,
    "DO": 60.0, "CR": 515.0, "PA": 1.0, "UY": 42.0,
}
country_rate_df = spark.createDataFrame(
    [(c["code"], c["currency"], float(rates[c["code"]])) for c in COUNTRIES],
    "country_code string, base_currency string, baseline_rate double",
)
dates_df = spark.sql(
    f"SELECT explode(sequence(to_date('{EVENT_START}'), date_add(to_date('{EVENT_END}'), 1), interval 1 day)) AS rate_date"
)
fx_df = (
    dates_df.crossJoin(F.broadcast(country_rate_df))
    .withColumn("day_index", F.datediff("rate_date", F.to_date(F.lit(EVENT_START))))
    .withColumn("noise", (F.pmod(F.xxhash64("rate_date", "country_code", F.lit(cfg["seed"])), F.lit(2001)) - 1000) / F.lit(100000.0))
    .withColumn("trend", F.sin(F.col("day_index") / F.lit(37.0)) * F.lit(0.012))
    .withColumn("mid_rate", F.round(F.col("baseline_rate") * (F.lit(1.0) + F.col("noise") + F.col("trend")), 6))
    .withColumn("buy_rate", F.round(F.col("mid_rate") * F.lit(0.9975), 6))
    .withColumn("sell_rate", F.round(F.col("mid_rate") * F.lit(1.0025), 6))
    .withColumn("quote_currency", F.lit("USD"))
    .withColumn("rate_source", F.lit("QUILLA_SYNTHETIC_FX"))
    .withColumn("observed_at", F.to_timestamp(F.concat_ws(" ", F.col("rate_date"), F.lit("21:00:00"))))
    .withColumn("rate_month", F.date_format("rate_date", "yyyy-MM"))
    .withColumn("synthetic_flag", F.lit(True))
    .select("rate_date", "rate_month", "country_code", "base_currency", "quote_currency", "mid_rate", "buy_rate", "sell_rate", "rate_source", "observed_at", "synthetic_flag")
)
write_parquet(fx_df, cfg, "fx_rates", ["rate_month"])

