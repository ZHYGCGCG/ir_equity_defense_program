# Databricks notebook source
# MAGIC %md # Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
import re
from databricks import koalas as ks
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import date
from pyspark.sql.types import DateType, StringType, IntegerType
from functools import reduce
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, Binarizer, OneHotEncoder, StandardScaler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
import pytz

# COMMAND ----------

start_time = datetime.now(pytz.timezone('America/Los_Angeles'))
print('Starting Run: ' + str(start_time))

# COMMAND ----------

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

data_list = []

def quick_dict(name, data, join_cols):
  return {'Name': name, 'Data': data, 'JoinColumns': join_cols}

# COMMAND ----------

def ReadLASR(table_name):
  df = spark.read.jdbc(url=jdbcUrl, table = table_name, lowerBound = 100000, upperBound = 400000, properties=connectionProperties, numPartitions=10)
  return df

# COMMAND ----------

# MAGIC %md # Load Base Data

# COMMAND ----------

spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/sales_mthly_agg").createOrReplaceTempView('BaseSales')
spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/asset_mthly_agg").createOrReplaceTempView('BaseAssets')

# COMMAND ----------

# MAGIC %md # Assets

# COMMAND ----------

# DBTITLE 1,Advisor
# MAGIC %sql
# MAGIC -- We use these funds because they are the highest selling RP funds, these 30 funds represent 99% of all RP Sales
# MAGIC 
# MAGIC DROP VIEW IF EXISTS AdvisorAssets;
# MAGIC CREATE TEMPORARY VIEW AdvisorAssets AS
# MAGIC 
# MAGIC SELECT
# MAGIC    add_months(to_date(CONCAT(BaseAssets.MO_NUM, '01'), 'yyyyMMdd'), 1) as MonthDate
# MAGIC ,  COALESCE(BaseAssets.FINCL_ITRMY_PRSON_ID, 0) as FinancialIntermediaryPersonID
# MAGIC ,  SUM(CASE WHEN BusinessLine.BusinessLine1 = 'IR' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as Assets_IR
# MAGIC ,  SUM(CASE WHEN BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as Assets_DCIO
# MAGIC ,  SUM(CASE WHEN BusinessLine3 in ('RKD', 'PlanPremier') THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as Assets_DCProp
# MAGIC ,  SUM(CASE WHEN BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as Assets_DCSmallRPSolution
# MAGIC ,  SUM(CASE WHEN BusinessLine2 not in ('DCIO', 'Small RP Solutions') THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as Assets_OtherIR
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsTD_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsEUPAC_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAMBAL_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsGFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsNPF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsWMIF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsBFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsFI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsNWF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsWGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAMCAP_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAMF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsGVT_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsSCWF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsICA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsIFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsCIB_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsIBFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAHIT_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsNEF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsSBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsIGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsPSG_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsWBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsPSGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsSTBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsILBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsPSMGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsGBAL_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAFMF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsMMF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsTD_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsEUPAC_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAMBAL_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsGFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsNPF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsWMIF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsBFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsFI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsNWF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsWGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAMCAP_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAMF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsGVT_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsSCWF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsICA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsIFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsCIB_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsIBFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAHIT_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsNEF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsSBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsIGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsPSG_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsWBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsPSGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsSTBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsILBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsPSMGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsGBAL_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsAFMF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as FundAssetsMMF_SmallRP
# MAGIC 
# MAGIC FROM
# MAGIC     BaseAssets BaseAssets
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     alpha.InvestmentVehicle  InvestmentVehicleDim
# MAGIC     ON
# MAGIC         BaseAssets.INV_VEH_ID = InvestmentVehicleDim.InvestmentVehicleID
# MAGIC 
# MAGIC LEFT JOIN alpha.TEP_MSSBI_BusinessLine BusinessLine 
# MAGIC ON 
# MAGIC     (   BaseAssets.INV_VEH_ID = BusinessLine.INV_VEH_ID
# MAGIC     and BaseAssets.RK_SOLTN_CD = BusinessLine.RK_SOLTN_CD
# MAGIC     and BaseAssets.FUND_SHARE_CLASS_ID = BusinessLine.FUND_SHARE_CLASS_ID
# MAGIC     and BaseAssets.INVSR_ACCT_TYP_CD = BusinessLine.INVSR_ACCT_TYP_CD
# MAGIC     )
# MAGIC 
# MAGIC WHERE
# MAGIC         add_months(to_date(CONCAT(BaseAssets.MO_NUM, '01'), 'yyyyMMdd'), 1) >= '2017-01-01'
# MAGIC     and	BaseAssets.TEAM_IND = 'N'
# MAGIC     and BaseAssets.FINCL_ITRMY_PRSON_ID <> 0
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleType in ('American Funds', 'AFIS', 'SMA')
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleID <> 161 ---MMF
# MAGIC     and BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT <> 0
# MAGIC 
# MAGIC GROUP BY
# MAGIC     add_months(to_date(CONCAT(BaseAssets.MO_NUM, '01'), 'yyyyMMdd'), 1)
# MAGIC ,	BaseAssets.FINCL_ITRMY_PRSON_ID
# MAGIC ;

# COMMAND ----------

data_list.append(quick_dict('AdvisorAssets', spark.sql('select * from AdvisorAssets'), ['MonthDate', 'FinancialIntermediaryPersonID']))

# COMMAND ----------

# DBTITLE 1,Office
# MAGIC %sql
# MAGIC -- We use these funds because they are the highest selling RP funds, these 30 funds represent 99% of all RP Sales
# MAGIC 
# MAGIC DROP VIEW IF EXISTS OfficeAssets;
# MAGIC CREATE TEMPORARY VIEW OfficeAssets AS
# MAGIC 
# MAGIC SELECT
# MAGIC    add_months(to_date(CONCAT(BaseAssets.MO_NUM, '01'), 'yyyyMMdd'), 1) as MonthDate
# MAGIC ,  COALESCE(BaseAssets.CRDTD_TO_OFFC_ID, 0) as OfficeID
# MAGIC ,  SUM(CASE WHEN BusinessLine.BusinessLine1 = 'IR' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeAssets_IR
# MAGIC ,  SUM(CASE WHEN BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeAssets_DCIO
# MAGIC ,  SUM(CASE WHEN BusinessLine3 in ('RKD', 'PlanPremier') THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeAssets_DCProp
# MAGIC ,  SUM(CASE WHEN BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeAssets_DCSmallRPSolution
# MAGIC ,  SUM(CASE WHEN BusinessLine2 not in ('DCIO', 'Small RP Solutions') THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeAssets_OtherIR
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsTD_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsEUPAC_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAMBAL_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsGFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsNPF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsWMIF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsBFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsFI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsNWF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsWGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAMCAP_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAMF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsGVT_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsSCWF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsICA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsIFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsCIB_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsIBFA_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAHIT_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsNEF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsSBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsIGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsPSG_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsWBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsPSGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsSTBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsILBF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsPSMGI_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsGBAL_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAFMF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsMMF_DCIO
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsTD_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsEUPAC_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAMBAL_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsGFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsNPF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsWMIF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsBFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsFI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsNWF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsWGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAMCAP_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAMF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsGVT_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsSCWF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsICA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsIFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsCIB_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsIBFA_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAHIT_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsNEF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsSBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsIGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsPSG_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsWBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsPSGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsSTBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsILBF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsPSMGI_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsGBAL_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsAFMF_SmallRP
# MAGIC ,  SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT ELSE 0 END) as OfficeFundAssetsMMF_SmallRP
# MAGIC 
# MAGIC FROM
# MAGIC     BaseAssets BaseAssets
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     alpha.InvestmentVehicle  InvestmentVehicleDim
# MAGIC     ON
# MAGIC         BaseAssets.INV_VEH_ID = InvestmentVehicleDim.InvestmentVehicleID
# MAGIC 
# MAGIC LEFT JOIN alpha.TEP_MSSBI_BusinessLine BusinessLine 
# MAGIC ON 
# MAGIC     (   BaseAssets.INV_VEH_ID = BusinessLine.INV_VEH_ID
# MAGIC     and BaseAssets.RK_SOLTN_CD = BusinessLine.RK_SOLTN_CD
# MAGIC     and BaseAssets.FUND_SHARE_CLASS_ID = BusinessLine.FUND_SHARE_CLASS_ID
# MAGIC     and BaseAssets.INVSR_ACCT_TYP_CD = BusinessLine.INVSR_ACCT_TYP_CD
# MAGIC     )
# MAGIC 
# MAGIC WHERE
# MAGIC         add_months(to_date(CONCAT(BaseAssets.MO_NUM, '01'), 'yyyyMMdd'), 1) >= '2017-01-01'
# MAGIC     and	BaseAssets.TEAM_IND = 'N'
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleType in ('American Funds', 'AFIS', 'SMA')
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleID <> 161 ---MMF
# MAGIC     and BaseAssets.ASSET_VAL_AMT + BaseAssets.TEAM_ALLOC_ASSET_VAL_AMT <> 0
# MAGIC 
# MAGIC GROUP BY
# MAGIC     add_months(to_date(CONCAT(BaseAssets.MO_NUM, '01'), 'yyyyMMdd'), 1)
# MAGIC ,	BaseAssets.CRDTD_TO_OFFC_ID
# MAGIC ;

# COMMAND ----------

data_list.append(quick_dict('OfficeAssets', spark.sql('select * from OfficeAssets'), ['MonthDate', 'OfficeID']))

# COMMAND ----------

# MAGIC %md # Sales and Redemptions

# COMMAND ----------

# DBTITLE 1,Advisor
# MAGIC %sql
# MAGIC -- We use these funds because they are the highest selling RP funds, these 30 funds represent 99% of all RP Sales
# MAGIC 
# MAGIC DROP VIEW IF EXISTS AdvisorSales;
# MAGIC CREATE TEMPORARY VIEW AdvisorSales AS
# MAGIC 
# MAGIC SELECT
# MAGIC    add_months(to_date(CONCAT(Base.MO_NUM, '01'), 'yyyyMMdd'), 1) as MonthDate
# MAGIC ,  COALESCE(Base.FINCL_ITRMY_PRSON_ID, 0) as FinancialIntermediaryPersonID
# MAGIC ,  ABS(SUM(Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT)) as Sales_IR
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as Sales_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine3 in ('RKD', 'PlanPremier') THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as Sales_DCProp
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as Sales_DCSmallRPSolution
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 not in ('DCIO', 'Small RP Solutions') THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as Sales_OtherIR
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesTD_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesEUPAC_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAMBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesGFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesNPF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesWMIF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesFI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesNWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesWGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAMCAP_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesGVT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesSCWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesICA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesIFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesCIB_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesIBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAHIT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesNEF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesSBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesIGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesPSG_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesWBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesPSGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesSTBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesILBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesPSMGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesGBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAFMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesMMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesTD_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesEUPAC_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAMBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesGFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesNPF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesWMIF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesFI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesNWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesWGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAMCAP_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesGVT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesSCWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesICA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesIFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesCIB_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesIBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAHIT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesNEF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesSBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesIGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesPSG_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesWBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesPSGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesSTBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesILBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesPSMGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesGBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesAFMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as FundSalesMMF_SmallRP
# MAGIC 
# MAGIC ,  ABS(SUM(Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT)) as Redemptions_IR
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as Redemptions_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine3 in ('RKD', 'PlanPremier') THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as Redemptions_DCProp
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as Redemptions_DCSmallRPSolution
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 not in ('DCIO', 'Small RP Solutions') THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as Redemptions_OtherIR
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsTD_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsEUPAC_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAMBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsGFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsNPF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsWMIF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsFI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsNWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsWGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAMCAP_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsGVT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsSCWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsICA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsIFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsCIB_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsIBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAHIT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsNEF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsSBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsIGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsPSG_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsWBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsPSGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsSTBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsILBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsPSMGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsGBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAFMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsMMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsTD_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsEUPAC_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAMBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsGFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsNPF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsWMIF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsFI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsNWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsWGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAMCAP_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsGVT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsSCWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsICA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsIFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsCIB_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsIBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAHIT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsNEF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsSBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsIGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsPSG_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsWBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsPSGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsSTBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsILBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsPSMGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsGBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsAFMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as FundRedemptionsMMF_SmallRP
# MAGIC FROM
# MAGIC     BaseSales Base
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     alpha.InvestmentVehicle  InvestmentVehicleDim
# MAGIC     ON
# MAGIC         Base.INV_VEH_ID = InvestmentVehicleDim.InvestmentVehicleID
# MAGIC 
# MAGIC LEFT JOIN alpha.TEP_MSSBI_BusinessLine BusinessLine 
# MAGIC ON 
# MAGIC     (   Base.INV_VEH_ID = BusinessLine.INV_VEH_ID
# MAGIC     and Base.RK_SOLTN_CD = BusinessLine.RK_SOLTN_CD
# MAGIC     and Base.FUND_SHARE_CLASS_ID = BusinessLine.FUND_SHARE_CLASS_ID
# MAGIC     and Base.INVSR_ACCT_TYP_CD = BusinessLine.INVSR_ACCT_TYP_CD
# MAGIC     )
# MAGIC 
# MAGIC WHERE
# MAGIC         add_months(to_date(CONCAT(Base.MO_NUM, '01'), 'yyyyMMdd'), 1) >= '2017-01-01'
# MAGIC     and	Base.TEAM_IND = 'N'
# MAGIC     and Base.FINCL_ITRMY_PRSON_ID <> 0
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleType in ('American Funds', 'AFIS', 'SMA')
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleID <> 161 ---MMF
# MAGIC     and Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT + Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT <> 0
# MAGIC 
# MAGIC GROUP BY
# MAGIC     add_months(to_date(CONCAT(Base.MO_NUM, '01'), 'yyyyMMdd'), 1)
# MAGIC ,	Base.FINCL_ITRMY_PRSON_ID
# MAGIC ;

# COMMAND ----------

data_list.append(quick_dict('AdvisorSales', spark.sql('select * from AdvisorSales'), ['MonthDate', 'FinancialIntermediaryPersonID']))

# COMMAND ----------

# DBTITLE 1,Office
# MAGIC %sql
# MAGIC -- We use these funds because they are the highest selling RP funds, these 30 funds represent 99% of all RP Sales
# MAGIC 
# MAGIC DROP VIEW IF EXISTS OfficeSales;
# MAGIC CREATE TEMPORARY VIEW OfficeSales AS
# MAGIC 
# MAGIC SELECT
# MAGIC    add_months(to_date(CONCAT(Base.MO_NUM, '01'), 'yyyyMMdd'), 1) as MonthDate
# MAGIC ,  ABS(COALESCE(Base.CRDTD_TO_OFFC_ID, 0)) as OfficeID
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine1 = 'IR' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeSales_IR
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeSales_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine3 in ('RKD', 'PlanPremier') THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeSales_DCProp
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeSales_DCSmallRPSolution
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 not in ('DCIO', 'Small RP Solutions') THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeSales_OtherIR
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesTD_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesEUPAC_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAMBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesGFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesNPF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesWMIF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesFI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesNWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesWGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAMCAP_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesGVT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesSCWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesICA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesIFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesCIB_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesIBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAHIT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesNEF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesSBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesIGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesPSG_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesWBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesPSGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesSTBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesILBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesPSMGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesGBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAFMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesMMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesTD_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesEUPAC_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAMBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesGFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesNPF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesWMIF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesFI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesNWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesWGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAMCAP_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesGVT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesSCWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesICA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesIFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesCIB_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesIBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAHIT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesNEF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesSBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesIGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesPSG_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesWBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesPSGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesSTBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesILBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesPSMGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesGBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesAFMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT ELSE 0 END)) as OfficeFundSalesMMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine1 = 'IR' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeRedemptions_IR
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeRedemptions_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine3 in ('RKD', 'PlanPremier') THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeRedemptions_DCProp
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeRedemptions_DCSmallRPSolution
# MAGIC ,  ABS(SUM(CASE WHEN BusinessLine2 not in ('DCIO', 'Small RP Solutions') THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeRedemptions_OtherIR
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsTD_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsEUPAC_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAMBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsGFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsNPF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsWMIF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsFI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsNWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsWGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAMCAP_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsGVT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsSCWF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsICA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsIFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsCIB_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsIBFA_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAHIT_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsNEF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsSBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsIGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsPSG_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsWBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsPSGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsSTBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsILBF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsPSMGI_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsGBAL_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAFMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'DCIO' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsMMF_DCIO
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode LIKE 'AFTD%' OR InvestmentVehicleDim.ParentFundAcronymCode LIKE 'TCGTD%') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsTD_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'EUPAC') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsEUPAC_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAMBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsGFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NPF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsNPF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WMIF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsWMIF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'BFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'FI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsFI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsNWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsWGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMCAP') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAMCAP_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GVT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsGVT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SCWF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsSCWF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ICA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsICA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsIFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'CIB') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsCIB_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IBFA') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsIBFA_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AHIT') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAHIT_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'NEF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsNEF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'SBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsSBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'IGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsIGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSG') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsPSG_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'WBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsWBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsPSGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'STBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsSTBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'ILBF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsILBF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'PSMGI') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsPSMGI_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'GBAL') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsGBAL_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'AFMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsAFMF_SmallRP
# MAGIC ,  ABS(SUM(CASE WHEN (InvestmentVehicleDim.ParentFundAcronymCode = 'MMF') AND BusinessLine.BusinessLine2 = 'Small RP Solutions' THEN Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT ELSE 0 END)) as OfficeFundRedemptionsMMF_SmallRP
# MAGIC 
# MAGIC FROM
# MAGIC     BaseSales Base
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     alpha.InvestmentVehicle  InvestmentVehicleDim
# MAGIC     ON
# MAGIC         Base.INV_VEH_ID = InvestmentVehicleDim.InvestmentVehicleID
# MAGIC 
# MAGIC LEFT JOIN alpha.TEP_MSSBI_BusinessLine BusinessLine 
# MAGIC ON 
# MAGIC     (   Base.INV_VEH_ID = BusinessLine.INV_VEH_ID
# MAGIC     and Base.RK_SOLTN_CD = BusinessLine.RK_SOLTN_CD
# MAGIC     and Base.FUND_SHARE_CLASS_ID = BusinessLine.FUND_SHARE_CLASS_ID
# MAGIC     and Base.INVSR_ACCT_TYP_CD = BusinessLine.INVSR_ACCT_TYP_CD
# MAGIC     )
# MAGIC 
# MAGIC WHERE
# MAGIC         add_months(to_date(CONCAT(Base.MO_NUM, '01'), 'yyyyMMdd'), 1) >= '2017-01-01'
# MAGIC     and	Base.TEAM_IND = 'N'
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleType in ('American Funds', 'AFIS', 'SMA')
# MAGIC     --and InvestmentVehicleDim.InvestmentVehicleID <> 161 ---MMF
# MAGIC     and Base.SLS_AMT + Base.TEAM_ALLOC_SLS_AMT + Base.RDMPT_AMT + Base.TEAM_ALLOC_RDMPT_AMT <> 0
# MAGIC 
# MAGIC GROUP BY
# MAGIC     add_months(to_date(CONCAT(Base.MO_NUM, '01'), 'yyyyMMdd'), 1)
# MAGIC ,	Base.CRDTD_TO_OFFC_ID
# MAGIC ;

# COMMAND ----------

data_list.append(quick_dict('OfficeSales', spark.sql('select * from OfficeSales'), ['MonthDate', 'OfficeID']))

# COMMAND ----------

# MAGIC %md # Opportunities

# COMMAND ----------

# MAGIC %r
# MAGIC library(sparklyr)
# MAGIC library(SparkR)
# MAGIC library(dplyr)
# MAGIC library(lubridate)
# MAGIC 
# MAGIC sc = sparklyr::spark_connect(method = 'databricks')
# MAGIC 
# MAGIC opp_data = SparkR::sql('SELECT ID, Sequence_N__C, StageName, Opp_Amt, Close_DT, Created_DT FROM alpha.TEP_SFDC_OPP_04 WHERE CREATED_DT < CLOSE_DT') %>%
# MAGIC   as.data.frame()
# MAGIC 
# MAGIC opp_data = opp_data %>%
# MAGIC   transmute(ID = ID,
# MAGIC             FinancialIntermediaryPersonUID = Sequence_N__C,
# MAGIC             Stage = StageName,
# MAGIC             Amount = Opp_Amt,
# MAGIC             CloseDate = ymd(Close_DT),
# MAGIC             CreatedDate = ymd(Created_DT))
# MAGIC opp_data[is.na(opp_data$Amount), 'Amount'] = 0
# MAGIC 
# MAGIC opp_data = opp_data %>%
# MAGIC   mutate(#CloseDate = if_else(CloseDate < CreatedDate, CreatedDate + days(1), CloseDate),
# MAGIC          CreatedMonth = floor_date(CreatedDate, unit = 'months'),
# MAGIC          CloseMonth = floor_date(CloseDate, unit = 'months'),
# MAGIC          Stage = case_when(
# MAGIC            Stage %in% c('Won', 'Commitment', 'Closed Won') ~ 'Won',
# MAGIC            Stage %in% c('Lost', 'Declined/Disqualified') ~ 'Lost',
# MAGIC            TRUE ~ 'Open'
# MAGIC          ))
# MAGIC 
# MAGIC # OF opportunities won during this month
# MAGIC # of opportunities lost during this month
# MAGIC # of opportunities still open during this month
# MAGIC 
# MAGIC opp_sum = data.frame()
# MAGIC 
# MAGIC GetOppData = function(rel_month){
# MAGIC   library(dplyr)
# MAGIC   opp_data %>%
# MAGIC     filter(CreatedMonth <= rel_month) %>%
# MAGIC     group_by(FinancialIntermediaryPersonUID) %>%
# MAGIC     summarise(OpportunitiesWon = sum(Stage %in% c('Closed Won', 'Commitment', 'Won') & CloseMonth==rel_month),
# MAGIC               OpportunitiesLost = sum(Stage=='Lost' & CloseMonth==rel_month),
# MAGIC               OpportunitiesCreated = sum(CreatedMonth==rel_month),
# MAGIC               OpportunitiesOpen = sum(CloseMonth > rel_month & CreatedMonth < rel_month)) %>%
# MAGIC     ungroup() %>%
# MAGIC     mutate(MonthDate = rel_month)
# MAGIC }
# MAGIC 
# MAGIC rel_months = seq.Date(from = ymd('2016-01-01'), to = floor_date(Sys.Date(), 'months'), by = 'month')
# MAGIC opp_list = spark.lapply(rel_months, GetOppData)
# MAGIC 
# MAGIC opp_sum = bind_rows(opp_list)
# MAGIC 
# MAGIC opp_out = opp_sum %>%
# MAGIC   filter(OpportunitiesWon + OpportunitiesLost + OpportunitiesCreated + OpportunitiesOpen > 0) %>%
# MAGIC   createDataFrame()
# MAGIC 
# MAGIC SparkR::createOrReplaceTempView(viewName='ProcessedOpportunities', x = opp_out)

# COMMAND ----------

data_list.append(quick_dict('IROpportunities', spark.sql('select * from ProcessedOpportunities'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Visits
# MAGIC 
# MAGIC Check out weekly tables to see how `alpha.Visits` is built

# COMMAND ----------

data_list.append(quick_dict('Visits', spark.sql('select * from alpha.Visits'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Calls

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP VIEW IF EXISTS Calls;
# MAGIC CREATE TEMPORARY VIEW Calls AS
# MAGIC 
# MAGIC SELECT
# MAGIC     trunc(ActivityDate, 'month') as MonthDate
# MAGIC ,	FinancialIntermediaryPersonUID
# MAGIC 
# MAGIC ,   SUM(CASE WHEN TaskType = 'Call' AND SFMType in ('ASR', 'Internal Wealth Specialist') AND ActivityTypeOriginal LIKE '%Inbound%' THEN 1 ELSE 0 END) as IWSInboundCalls
# MAGIC ,   SUM(CASE WHEN TaskType = 'Call' AND SFMType in ('ASR', 'Internal Wealth Specialist') AND ActivityTypeOriginal LIKE '%Outbound%' THEN 1 ELSE 0 END) as IWSOutboundCalls
# MAGIC ,   SUM(CASE WHEN TaskType = 'Call' AND SFMType in ('ASR', 'Internal Wealth Specialist') AND ActivityTypeOriginal LIKE '%Scheduled Call%' THEN 1 ELSE 0 END) as IWSScheduledCalls
# MAGIC ,   SUM(CASE WHEN TaskType = 'Call' AND SFMType in ('ASR', 'Internal Wealth Specialist') AND ActivityTypeOriginal LIKE '%Conference Call%' THEN 1 ELSE 0 END) as IWSConferenceCalls
# MAGIC 
# MAGIC ,   SUM(CASE WHEN TaskType = 'Call' AND SFMType = 'RPSC' AND ActivityTypeOriginal LIKE '%Inbound%' THEN 1 ELSE 0 END) as RPSCInboundCalls
# MAGIC ,   SUM(CASE WHEN TaskType = 'Call' AND SFMType = 'RPSC' AND ActivityTypeOriginal LIKE '%Outbound%' THEN 1 ELSE 0 END) as RPSCOutboundCalls
# MAGIC 
# MAGIC --,   SUM(CASE WHEN BusinessSegmentDiscussed like '%not meet%' THEN 1 ELSE 0 END) as MeetingDeclines
# MAGIC 	
# MAGIC FROM
# MAGIC 	alpha.Calls
# MAGIC 
# MAGIC WHERE
# MAGIC   ActivityDate >= '2017-01-01'
# MAGIC 
# MAGIC GROUP BY
# MAGIC 		trunc(ActivityDate, 'month')
# MAGIC 	,	FinancialIntermediaryPersonUID

# COMMAND ----------

data_list.append(quick_dict('Calls', spark.sql('select * from Calls'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # ACES
# MAGIC 
# MAGIC As far as I'm aware, there is no ACES in ADLS, so for now the query runs in LASR and I simply port that table, checkout Weekly Tables for that port

# COMMAND ----------

data_list.append(quick_dict('ACES', spark.sql('select * from alpha.ACES_Monthly'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Demographics

# COMMAND ----------

data_list.append(quick_dict('Demographics', spark.sql('select * from alpha.Demographics'), ['FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Marketing Emails

# COMMAND ----------

data_list.append(quick_dict('Emails', spark.sql('select * from alpha.Emails'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Literature

# COMMAND ----------

data_list.append(quick_dict('Literature', spark.sql('select * from alpha.Literature'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Global Activity

# COMMAND ----------

office_opp = spark.sql('select * from alpha.OfficeGlobalOpportunity')
org_opp = spark.sql('select * from alpha.OrganizationGlobalOpportunity')

### We dont need advisory and brokerage divide, less applicable in IR
office_opp.select(*[col for col in office_opp.columns if ('Advisory' not in col) and ('Brokerage' not in col)]).createOrReplaceTempView('OfficeGlobalOpportunity')
org_opp.select(*[col for col in org_opp.columns if ('Advisory' not in col) and ('Brokerage' not in col)]).createOrReplaceTempView('OrganizationGlobalOpportunity')

data_list.append(quick_dict('OfficeGlobal', office_opp, ['MonthDate', 'OfficeID']))
data_list.append(quick_dict('OrganizationGlobal', org_opp, ['MonthDate', 'OrganizationID']))

# COMMAND ----------

# MAGIC %md # IR Information
# MAGIC 
# MAGIC - See WeeklyTables for the copying of TEP Tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP VIEW IF EXISTS IRInfo;
# MAGIC CREATE TEMPORARY VIEW IRInfo AS
# MAGIC 
# MAGIC SELECT
# MAGIC   AE_SEQ_NUM AS FinancialIntermediaryPersonUID
# MAGIC , FINAL_RP_NUM AS RPPlans
# MAGIC , FINAL_RP_AMT AS RPAssets
# MAGIC , AF_PROP_PLAN_NUM AS AFRPPlans
# MAGIC , AF_PROP_ASSET_AMT AS AFRPAssets
# MAGIC , TOTAL_DC_NUM AS DCPlans
# MAGIC , TOTAL_DC_AMT AS DCAssets
# MAGIC , 1 AS IsRPAdvisor
# MAGIC 
# MAGIC FROM 
# MAGIC 	alpha.TEP_RP_FA_INFO_AGG06

# COMMAND ----------

data_list.append(quick_dict('IRInfo', spark.sql('select * from IRInfo'), ['FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Web Activity

# COMMAND ----------

data_list.append(quick_dict('WebActivity', spark.sql('select * from alpha.WebActivity'), ['MonthDate', 'FinancialIntermediaryPersonUID']))

# COMMAND ----------

# MAGIC %md # Assemble Data

# COMMAND ----------

# MAGIC %md ## Base Table

# COMMAND ----------



# COMMAND ----------

person_dim = spark.sql('SELECT FINCL_ITRMY_PRSON_ID as FinancialIntermediaryPersonID, FINCL_ITRMY_PRSON_UID as FinancialIntermediaryPersonUID, rpc_terr_id, fincl_itrmy_org_num, fincl_itrmy_offc_id FROM alpha.Person')
uids = spark.sql('SELECT AE_SEQ_NUM AS FinancialIntermediaryPersonUID FROM alpha.TEP_RP_FA_INFO_AGG06').select('FinancialIntermediaryPersonUID').withColumn('FinancialIntermediaryPersonUID', f.col('FinancialIntermediaryPersonUID').cast(IntegerType())).distinct()
all_months = spark.createDataFrame(pd.DataFrame({'MonthDate': pd.date_range('2017-01-01', date.today(), freq='MS')}))
all_combos =  f.broadcast(uids).crossJoin(f.broadcast(all_months)).join(person_dim.select('FinancialIntermediaryPersonID', 'FinancialIntermediaryPersonUID'), how = 'inner', on = 'FinancialIntermediaryPersonUID')

# COMMAND ----------

all_combos.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS SalesActivity;
# MAGIC CREATE TEMPORARY VIEW SalesActivity AS
# MAGIC 
# MAGIC SELECT
# MAGIC     to_date(CONCAT(Sales.Mo_Num, '01'), 'yyyyMMdd') as MonthDate
# MAGIC ,   Sales.FINCL_ITRMY_PRSON_ID as FinancialIntermediaryPersonID
# MAGIC ,   Sales.CRDTD_TO_ORG_ID as OrganizationID
# MAGIC ,   Sales.CRDTD_TO_OFFC_ID as OfficeID
# MAGIC ,   Sales.RPC_TERR_ID as RPCTerritoryID
# MAGIC ,   SUM(Sales.SLS_AMT + Sales.TEAM_ALLOC_SLS_AMT + Sales.RDMPT_AMT + Sales.TEAM_ALLOC_RDMPT_AMT) as Activity
# MAGIC 
# MAGIC FROM
# MAGIC     BaseSales		Sales
# MAGIC                 
# MAGIC WHERE
# MAGIC     Sales.Mo_Num >= 201701
# MAGIC and	Sales.FINCL_ITRMY_PRSON_ID <> 0
# MAGIC and	SALES.TEAM_IND = 'N'
# MAGIC 
# MAGIC 
# MAGIC GROUP BY
# MAGIC     to_date(CONCAT(Sales.Mo_Num, '01'), 'yyyyMMdd')			
# MAGIC ,	Sales.FINCL_ITRMY_PRSON_ID
# MAGIC ,	Sales.CRDTD_TO_ORG_ID
# MAGIC ,	Sales.CRDTD_TO_OFFC_ID
# MAGIC ,   Sales.RPC_TERR_ID
# MAGIC 
# MAGIC ;
# MAGIC 
# MAGIC DROP VIEW IF EXISTS VisitsActivity;
# MAGIC CREATE TEMPORARY VIEW VisitsActivity AS
# MAGIC 
# MAGIC SELECT
# MAGIC     trunc(StartDateTime, 'month') as MonthDate
# MAGIC ,   Visits.FinancialIntermediaryPersonUniqueID as FinancialIntermediaryPersonUID
# MAGIC ,   Office.OrganizationIDOffice as OrganizationID
# MAGIC ,   Visits.OfficeID as OfficeID
# MAGIC ,   Visits.AtVisitTerritoryID as RPCTerritoryID
# MAGIC ,   COUNT(*) as Activity
# MAGIC 
# MAGIC FROM
# MAGIC     jarvis.CleanVisits		Visits
# MAGIC     
# MAGIC LEFT JOIN jarvis.Office Office
# MAGIC   on Visits.OfficeID = Office.OfficeID
# MAGIC                 
# MAGIC WHERE
# MAGIC         Visits.MoNum >= 201701
# MAGIC     and	Visits.FinancialIntermediaryPersonUniqueID <> 0
# MAGIC 
# MAGIC GROUP BY
# MAGIC    trunc(StartDateTime, 'month')			
# MAGIC ,  Visits.FinancialIntermediaryPersonUniqueID
# MAGIC ,  Office.OrganizationIDOffice
# MAGIC ,  Visits.OfficeID
# MAGIC ,  Visits.AtVisitTerritoryID

# COMMAND ----------

sales_activity = spark.sql('SELECT * FROM SalesActivity')
sales_activity = sales_activity.withColumn('ActivityRank', f.row_number().over(Window.partitionBy('FinancialIntermediaryPersonID', 'MonthDate').orderBy(f.desc('Activity'))))
sales_map = sales_activity.where(f.col('ActivityRank') == 1).drop(*['Activity', 'ActivityRank'])

visits_activity = spark.sql('SELECT * FROM VisitsActivity')
visits_activity = visits_activity.withColumn('ActivityRank', f.row_number().over(Window.partitionBy('FinancialIntermediaryPersonUID', 'MonthDate').orderBy(f.desc('Activity'))))
visits_map = visits_activity.where(f.col('ActivityRank') == 1).drop(*['Activity', 'ActivityRank'])

sales_map.createOrReplaceTempView('SalesMap')
visits_map.createOrReplaceTempView('VisitsMap')
all_combos.createOrReplaceTempView('Base')
person_dim.createOrReplaceTempView('PersonDim')

max_date = all_months.agg({'MonthDate': 'max'}).first()[0]

advisor_map = spark.sql(f"""

SELECT
  Base.FinancialIntermediaryPersonUID
, Base.MonthDate
, CASE
    WHEN Base.MonthDate = '{max_date}' THEN Person.fincl_itrmy_org_num
    ELSE COALESCE(Sales.OrganizationID, Visits.OrganizationID)
  END as OrganizationID
, CASE
    WHEN Base.MonthDate = '{max_date}' THEN Person.fincl_itrmy_offc_id
    ELSE COALESCE(Sales.OfficeID, Visits.OfficeID)
  END as OfficeID
, CASE
    WHEN Base.MonthDate = '{max_date}' THEN Person.rpc_terr_id
    ELSE COALESCE(Sales.RPCTerritoryID, Visits.RPCTerritoryID)
  END as RPCTerritoryID

FROM Base Base

LEFT JOIN PersonDim Person
ON Base.FinancialIntermediaryPersonUID = Person.FinancialIntermediaryPersonUID

LEFT JOIN SalesMap Sales
ON  Sales.FinancialIntermediaryPersonID = Person.FinancialIntermediaryPersonID
and Sales.MonthDate = Base.MonthDate

LEFT JOIN VisitsMap Visits
ON  Visits.FinancialIntermediaryPersonUID = Base.FinancialIntermediaryPersonUID
and Visits.MonthDate = Base.MonthDate

""")

# COMMAND ----------

map_count = advisor_map.count()
combo_count = all_combos.count()

assert map_count == combo_count

# COMMAND ----------

base = all_combos.join(advisor_map, how = 'left', on = ['FinancialIntermediaryPersonUID', 'MonthDate'])
base_count = base.count()
assert base_count == map_count

# COMMAND ----------

for col in ['OfficeID', 'OrganizationID', 'RPCTerritoryID']:
  #Forward Fill
  window = Window.partitionBy('FinancialIntermediaryPersonUID').orderBy('MonthDate').rowsBetween(-500, 0) #500 is arbitrary
  filled_col = f.last(base[col], ignorenulls = True).over(window)
  base = base.withColumn(col, filled_col)
  #Back Fill
  window = Window.partitionBy('FinancialIntermediaryPersonUID').orderBy('MonthDate').rowsBetween(0, 500)
  filled_col = f.first(base[col], ignorenulls = True).over(window)
  base = base.withColumn(col, filled_col)
  
org_names = spark.sql('SELECT OrganizationID, ParentOrganizationName FROM jarvis.Organization')
base = base.join(org_names, how = 'left', on = 'OrganizationID')

# COMMAND ----------

# MAGIC %md ## Join Together

# COMMAND ----------

for i, element in enumerate(data_list):
  print(f"""Joining {element['Name']}, using {str(element['JoinColumns'])} as join columns.""")
  
  if i == 0:
    master = base.join(element['Data'], how = 'left', on = element['JoinColumns'])
  else:
    master = master.join(element['Data'], how = 'left', on = element['JoinColumns'])
    
master = master.fillna(0).fillna('Unknown')

# COMMAND ----------

master.createOrReplaceTempView('Master')

master = spark.sql("""
SELECT
  *
, CASE
     WHEN ParentOrganizationName = 'Edward Jones' THEN 'EJ'
     WHEN ParentOrganizationName = 'Wells Fargo Network' THEN 'WF'
     WHEN ParentOrganizationName = 'LPL Group' THEN 'LPL'
     WHEN ParentOrganizationName = 'Raymond James Group' THEN 'RJ'
     WHEN ParentOrganizationName = 'Morgan Stanley Wealth Management' THEN 'MS'
     WHEN ParentOrganizationName = 'Merrill' THEN 'ML'
     WHEN ParentOrganizationName = 'UBS' THEN 'UBS'
     ELSE 'Other'
  END AS FirmGroup
  
FROM Master
""")

# COMMAND ----------

# Unique on UID+Month
row_count = master.count()
assert row_count == base_count
assert master.select(['FinancialIntermediaryPersonUID', 'MonthDate']).distinct().count() == row_count, 'Not unique on UID and Month'

# COMMAND ----------

from pyspark.sql.types import NumericType
for c in [x for x in master.schema.fields if isinstance(x.dataType, NumericType)]:
  master = master.withColumn(c.name, f.when(f.col(c.name) < 0, 0).otherwise(f.col(c.name)).cast(c.dataType))

# COMMAND ----------

master \
.write.format('delta').mode('overwrite').option("overwriteSchema", "true") \
.partitionBy('MonthDate') \
.saveAsTable("edp.AlphaIR")
