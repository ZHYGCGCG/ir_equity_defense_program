# Databricks notebook source
jdbcHostname = "lasr-sqldwdb-eastus2-prd.database.windows.net"
jdbcDatabase = "lasr-sqldwdb-prd"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
  "user" : dbutils.secrets.get(scope = "nad_sqldw", key = "username"),
  "password" : dbutils.secrets.get(scope = "nad_sqldw", key = "password"),
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

tenant_id = dbutils.secrets.get(scope = "nadmodelmanagement-sp", key = "tenant_id")
spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("fs.adl.oauth2.client.id", dbutils.secrets.get(scope = "nadmodelmanagement-sp", key = "client_id"))
spark.conf.set("fs.adl.oauth2.credential", dbutils.secrets.get(scope = "nadmodelmanagement-sp", key = "client_secret"))
spark.conf.set("fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))


#### Spark Configurations

print('Original Shuffle Paritions: ', sqlContext.getConf('spark.sql.shuffle.partitions'))

sqlContext.setConf('spark.sql.shuffle.partitions', '1000')
spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")

# COMMAND ----------

def ReadLASR(table_name):
  df = spark.read.jdbc(url=jdbcUrl, table = table_name, lowerBound = 100000, upperBound = 400000, properties=connectionProperties, numPartitions=10)
  return df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.ml.feature import *
from re import *
from functools import *
import pandas as pd
