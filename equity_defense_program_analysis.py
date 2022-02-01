# Databricks notebook source
# MAGIC %md #Equity Defense Program
# MAGIC - which assets are most at risk of redemption?

# COMMAND ----------

# MAGIC %md ##To Do:
# MAGIC #### Data
# MAGIC - add returns data
# MAGIC - add trailing window averages (by month)
# MAGIC - lag of macroeconomic variables
# MAGIC 
# MAGIC ####Analysis
# MAGIC - periods of high redemptions (above trailing averages)
# MAGIC - correlation of macroeconomic with periods of high redemptions
# MAGIC - money in motion, exchange in/out by asset category (where are equity assets going?)

# COMMAND ----------

# MAGIC %md ### Questions:
# MAGIC ####New Fields that need to be explored (do these give us more coverage?)
# MAGIC  - 'dervd_invsr_acct_num',
# MAGIC  - 'dervd_clrd_thru_org_num',
# MAGIC  - 'dervd_plan_ptcpt_id',
# MAGIC  - 'dervd_brkr_idfct_num',
# MAGIC  - 'dervd_rtrmt_plan_id',
# MAGIC  
# MAGIC ####Do we count exchange out as a redemption?
# MAGIC ####Do we need to break this out by share class? (e.g. R2 can be high redemption while R6 is stable)

# COMMAND ----------

# MAGIC %md ### Funds
# MAGIC - GFA, EUPAC, WMIF, FI, ICA

# COMMAND ----------

fund_list = ['GFA', 'EUPAC', 'WMIF', 'FI', 'ICA']

# COMMAND ----------

# MAGIC %md ### Firms
# MAGIC - Edward Jones, LPL, Raymond James, Advisor Group, Merrill, Morgan Stanley, UBS, Wells Fargo

# COMMAND ----------

firm_list = ['Edward Jones', 'LPL', 'Raymond James', 'Advisor Group', 'Merrill', 'Morgan Stanley', 'UBS', 'Wells Fargo']

# COMMAND ----------

# MAGIC %md # Load Data

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md #Primary Visualizations

# COMMAND ----------

fund_list

# COMMAND ----------

# DBTITLE 1,Month Counter
# MAGIC %sql
# MAGIC Select 
# MAGIC   mo_num, 
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg 
# MAGIC GROUP by mo_num
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,Funds redemptions per month : 201701 - 202106
# MAGIC %sql
# MAGIC Select 
# MAGIC   mo_num, 
# MAGIC   fund_acrnm_cd,
# MAGIC   ABS(SUM(rdmpt_amt)) as abs_tot_rdmpt_amt,
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg 
# MAGIC GROUP by mo_num, fund_acrnm_cd
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,GFA redemptions by orgs
# MAGIC %sql
# MAGIC Select 
# MAGIC   *,
# MAGIC   ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg
# MAGIC where fund_acrnm_cd = "GFA"
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,EUPAC redemptions by orgs
# MAGIC %sql
# MAGIC Select 
# MAGIC   *,
# MAGIC   ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg
# MAGIC where fund_acrnm_cd = "EUPAC"
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,WMIF redemptions by orgs
# MAGIC %sql
# MAGIC Select 
# MAGIC   *,
# MAGIC   ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg
# MAGIC where fund_acrnm_cd = "WMIF"
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,FI redemptions by orgs
# MAGIC %sql
# MAGIC Select 
# MAGIC   *,
# MAGIC   ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg
# MAGIC where fund_acrnm_cd = "FI"
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,ICA redemptions by orgs
# MAGIC %sql
# MAGIC Select 
# MAGIC   *,
# MAGIC   ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC   DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC from edp.base_data_agg
# MAGIC where fund_acrnm_cd = "ICA"
# MAGIC order by mo_num

# COMMAND ----------

# DBTITLE 1,Funds Comparison with moving average Redemption
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   avg(abs_tot_rdmpt_amt) OVER(partition by parent_fund_acronym_group ORDER BY month_counter
# MAGIC      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )
# MAGIC      as moving_average_3,
# MAGIC   avg(abs_tot_rdmpt_amt) OVER(partition by parent_fund_acronym_group ORDER BY month_counter
# MAGIC      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )
# MAGIC      as moving_average_6,
# MAGIC   avg(abs_tot_rdmpt_amt) OVER(partition by parent_fund_acronym_group ORDER BY month_counter
# MAGIC      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW )
# MAGIC      as moving_average_12
# MAGIC From
# MAGIC   (Select 
# MAGIC     month_number, 
# MAGIC     parent_fund_acronym_group,
# MAGIC     ABS(SUM(redemption_amount)) as abs_tot_rdmpt_amt,
# MAGIC     DENSE_RANK () OVER (ORDER BY month_number) month_counter
# MAGIC   from edp.base_data
# MAGIC   GROUP by month_number, parent_fund_acronym_group
# MAGIC   order by month_number)

# COMMAND ----------

# DBTITLE 1,Funds Comparison with moving average NCF
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   avg(net_cash_flow) OVER(partition by parent_fund_acronym_group ORDER BY month_counter
# MAGIC      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )
# MAGIC      as moving_average_3,
# MAGIC   avg(net_cash_flow) OVER(partition by parent_fund_acronym_group ORDER BY month_counter
# MAGIC      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )
# MAGIC      as moving_average_6,
# MAGIC   avg(net_cash_flow) OVER(partition by parent_fund_acronym_group ORDER BY month_counter
# MAGIC      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW )
# MAGIC      as moving_average_12
# MAGIC From
# MAGIC   (Select 
# MAGIC     month_number, 
# MAGIC     parent_fund_acronym_group,
# MAGIC     sum(redemption_amount) + SUM(sale_amount) as net_cash_flow,
# MAGIC --     ABS(SUM(rdmpt_amt)) as abs_tot_rdmpt_amt,
# MAGIC     DENSE_RANK () OVER (ORDER BY month_number) month_counter
# MAGIC   from edp.base_data
# MAGIC --   where fund_acrnm_cd = "GFA"
# MAGIC   GROUP by month_number, parent_fund_acronym_group
# MAGIC   order by month_number)

# COMMAND ----------

# DBTITLE 1,GFA Comparison with moving average
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CASE 
# MAGIC     WHEN abs_tot_rdmpt_amt > moving_average_3 THEN "Strong Anomaly"
# MAGIC     WHEN abs_tot_rdmpt_amt > moving_average_6 THEN "Medium Anomaly"
# MAGIC     WHEN abs_tot_rdmpt_amt > moving_average_12 THEN "Weak Anomaly"
# MAGIC   ELSE "Normal" END as period_anomaly_type
# MAGIC FROM
# MAGIC   (SELECT 
# MAGIC     *,
# MAGIC     avg(abs_tot_rdmpt_amt) OVER(ORDER BY month_counter
# MAGIC        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )
# MAGIC        as moving_average_3,
# MAGIC     avg(abs_tot_rdmpt_amt) OVER(ORDER BY month_counter
# MAGIC        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )
# MAGIC        as moving_average_6,
# MAGIC     avg(abs_tot_rdmpt_amt) OVER(ORDER BY month_counter
# MAGIC        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW )
# MAGIC        as moving_average_12
# MAGIC   From
# MAGIC     (Select 
# MAGIC       mo_num, 
# MAGIC       fund_acrnm_cd,
# MAGIC       ABS(SUM(rdmpt_amt)) as abs_tot_rdmpt_amt,
# MAGIC       DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC     from edp.base_data_agg
# MAGIC     where fund_acrnm_cd = "GFA"
# MAGIC     GROUP by mo_num, fund_acrnm_cd
# MAGIC     order by mo_num))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Macroeconomic Info Back fill 

# COMMAND ----------

macroeconomic_file_location = 'adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/equity_defense_program/'

for file in dbutils.fs.ls(macroeconomic_file_location):
  
  file_standard = sub('.csv','',
                  sub(' ', '_',
                  sub('-', '_', 
                  file.name))).lower()
  
  globals()[file_standard] = spark.read.option("header",True).csv(macroeconomic_file_location + file.name)

# cannot reduce with join function so applying manually
# df.reduce((a,b) => a.join(b, Seq("id", "uid1")))
macroeconomic_variables = \
consumersentimentindex \
.join(gdp, how = 'left', on = 'AsOfDate') \
.join(goldprice, how = 'left', on = 'AsOfDate') \
.join(house_pricing_index, how = 'left', on = 'AsOfDate') \
.join(market_volatility_vix, how = 'left', on = 'AsOfDate') \
.join(oilprice, how = 'left', on = 'AsOfDate') \
.join(tdsp_fodsp, how = 'left', on = 'AsOfDate') \
.join(ted_spread, how = 'left', on = 'AsOfDate') \
.join(treasury_yield, how = 'left', on = 'AsOfDate') \
.withColumn('date',to_date(col("AsOfDate"),"MM-dd-yyyy")) \
.withColumn('mo_num', year(col('date'))*lit(100)+month(col('date'))) \
.drop('AsOfDate','date')

# remove spaces from column names
macroeconomic_variables = macroeconomic_variables.select([col(column).alias(column.replace(' ', '_')) for column in macroeconomic_variables.columns])

# COMMAND ----------

macroeconomic_variables.display()

# COMMAND ----------

# DBTITLE 1,All Time Series for GFA
# MAGIC %sql
# MAGIC SELECT
# MAGIC   mo_num,
# MAGIC   month_counter,
# MAGIC   fund_acrnm_cd,
# MAGIC   SUM(abs_rdmpt_amt) as _abs_rdmpt_amt,
# MAGIC   SUM(rdmpt_amt) as _rdmpt_amt,
# MAGIC   SUM(sls_amt) as sls_amt,
# MAGIC   SUM(exchg_in_amt) as exchg_in_amt,
# MAGIC   SUM(exchg_out_amt) as exchg_out_amt,
# MAGIC   SUM(rdmpt_amt+exchg_out_amt) as out_amt,
# MAGIC   SUM(rdmpt_amt+sls_amt) as net_flow_amt,
# MAGIC   SUM(exchg_in_amt+exchg_out_amt) as exchg_net_flow_amt,
# MAGIC   SUM(rdmpt_amt+sls_amt+exchg_in_amt+exchg_out_amt) as tot_net_flow_amt
# MAGIC FROM
# MAGIC   (Select *,
# MAGIC     ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC     DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC   from edp.base_data
# MAGIC   where fund_acrnm_cd = 'GFA') 
# MAGIC group by fund_acrnm_cd,month_counter,mo_num
# MAGIC order by month_counter

# COMMAND ----------

# DBTITLE 1,All Time Series for EUPAC
# MAGIC %sql
# MAGIC SELECT
# MAGIC   mo_num,
# MAGIC   month_counter,
# MAGIC   fund_acrnm_cd,
# MAGIC   SUM(abs_rdmpt_amt) as _abs_rdmpt_amt,
# MAGIC   SUM(rdmpt_amt) as _rdmpt_amt,
# MAGIC   SUM(sls_amt) as sls_amt,
# MAGIC   SUM(exchg_in_amt) as exchg_in_amt,
# MAGIC   SUM(exchg_out_amt) as exchg_out_amt,
# MAGIC   SUM(rdmpt_amt+exchg_out_amt) as out_amt,
# MAGIC   SUM(rdmpt_amt+sls_amt) as net_flow_amt,
# MAGIC   SUM(exchg_in_amt+exchg_out_amt) as exchg_net_flow_amt,
# MAGIC   SUM(rdmpt_amt+sls_amt+exchg_in_amt+exchg_out_amt) as tot_net_flow_amt
# MAGIC FROM
# MAGIC   (Select *,
# MAGIC     ABS(rdmpt_amt) as abs_rdmpt_amt,
# MAGIC     DENSE_RANK () OVER (ORDER BY mo_num) month_counter
# MAGIC   from edp.base_data
# MAGIC   where fund_acrnm_cd = 'EUPAC') 
# MAGIC group by fund_acrnm_cd,month_counter,mo_num
# MAGIC order by month_counter

# COMMAND ----------



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

spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/sales_mthly_agg").createOrReplaceTempView('BaseSales')
spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/asset_mthly_agg").createOrReplaceTempView('BaseAssets')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC From BaseSales

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC From BaseAssets

# COMMAND ----------

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

# MAGIC %sql
# MAGIC SELECT *
# MAGIC From AdvisorAssets

# COMMAND ----------

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

# MAGIC %sql
# MAGIC SELECT *
# MAGIC From AdvisorSales
