# Databricks notebook source
# MAGIC %md #Equity Defense Program
# MAGIC - which assets are most at risk of redemption?

# COMMAND ----------

# MAGIC %md ### Funds
# MAGIC - GFA, EUPAC, WMIF, FI, ICA

# COMMAND ----------

# MAGIC %md ### Firms
# MAGIC - Edward Jones, LPL, Raymond James, Advisor Group, Merrill, Morgan Stanley, UBS, Wells Fargo

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md # Load Data

# COMMAND ----------

# underlying transactions
financial_transaction = spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/financial_transaction/")

# dimension table for fund attributes (fund_acrnm_cd)
investment_vehicle = spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/investment_vehicle/")

# COMMAND ----------


