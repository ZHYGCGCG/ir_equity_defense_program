# Databricks notebook source
adlsPath = 'adl://adlseastus2lasr.azuredatalakestore.net/'

#config 
tenant_id = dbutils.secrets.get(scope = "nadmodelmanagement-sp", key = "tenant_id")
spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("fs.adl.oauth2.client.id", dbutils.secrets.get(scope = "nadmodelmanagement-sp", key = "client_id"))
spark.conf.set("fs.adl.oauth2.credential", dbutils.secrets.get(scope = "nadmodelmanagement-sp", key = "client_secret"))
spark.conf.set("fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))

#LASR connection config
username=dbutils.secrets.get(scope = "nad_sqldw", key = "username")
password=dbutils.secrets.get(scope = "nad_sqldw", key = "password")
jdbcHostname = "lasr-sqldwdb-eastus2-prd.database.windows.net"
jdbcDatabase = "lasr-sqldwdb-prd"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


def query_lasr(sql):
  query_partitions = 10
  df = spark.read.jdbc(url=jdbcUrl, table=sql, numPartitions=query_partitions, properties=connectionProperties)
  return df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.ml.feature import *
import pandas as pd
