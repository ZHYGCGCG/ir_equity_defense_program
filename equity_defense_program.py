# Databricks notebook source
# MAGIC %md #Equity Defense Program
# MAGIC - which assets are most at risk of redemption?

# COMMAND ----------

# MAGIC %md ### Questions:
# MAGIC New Fields that need to be explored (do these give us more coverage?)
# MAGIC  - 'dervd_invsr_acct_num',
# MAGIC  - 'dervd_clrd_thru_org_num',
# MAGIC  - 'dervd_plan_ptcpt_id',
# MAGIC  - 'dervd_brkr_idfct_num',
# MAGIC  - 'dervd_rtrmt_plan_id',

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

# MAGIC %md ### Financial Transaction Data

# COMMAND ----------

table_list = ['financial_transaction', 'investment_vehicle', 'fincl_itrmy_org']

for table in table_list:
  globals()[table] = spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/" + table + "/")

# COMMAND ----------

# MAGIC %md ### Macroeconomic Data

# COMMAND ----------

macroeconomic_file_location = 'adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/equity_defense_program/'

for file in dbutils.fs.ls(macroeconomic_file_location):
  
  file_standard = sub('.csv','',
                  sub(' ', '_',
                  sub('-', '_', 
                  file.name))).lower()
  
  globals()[file_standard] = spark.read.option("header",True).csv(macroeconomic_file_location + file.name)

# cannot reduce with join function so applying manually
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

# MAGIC %md ### Target equity funds

# COMMAND ----------

# funds can be under multiple investment ids (child : parent relationship)

target_equity_funds = \
investment_vehicle \
.select('inv_veh_id', 'fund_acrnm_cd','inv_veh_nm','fund_fmly_nm') \
.filter(col('fund_fmly_nm')=='American Funds') \
.filter(col('fund_acrnm_cd').isin(fund_list) | col('fund_acrnm_cd').isin(['AF-' + fund for fund in fund_list])) \
.withColumn('fund_acrnm_cd', regexp_replace(col('fund_acrnm_cd'), 'AF-', ''))

target_equity_funds.display()

# COMMAND ----------

# MAGIC %md ### Target firms

# COMMAND ----------

# this is a wider net but we arn't sure which ids are used in financial transaction table
# using parent org name rather than org name to include more

firm_list_standardized = []

for firm in firm_list:
  firm_standard = sub(' ', '_', firm).lower()
  
  firm_list_standardized.append(firm_standard + '_org_ids')
  
  globals()[firm_standard + '_org_ids'] = \
  fincl_itrmy_org \
  .filter(lower(col('parnt_org_nm')).contains(lower(lit(firm)))) \
  .withColumn('org_nm_standard', lit(firm_standard)) \
  .select('fincl_itrmy_org_id', 'org_nm_standard') \
  .distinct()
  
target_org_ids = reduce(DataFrame.unionAll, [globals()[firm_org_ids] for firm_org_ids in firm_list_standardized])

target_org_ids.groupBy(col('org_nm_standard')).count().orderBy(col('count').desc()).display()

# COMMAND ----------

# MAGIC %md ### Filter Transaction Table
# MAGIC - only return records for our target fund / orgs

# COMMAND ----------

base_data = \
financial_transaction \
.filter(col('mo_num') > 201700) \
.filter((col('rpo_ind') == 'Y') | (col('instn_ind') == 'Y')) \
.join(
  target_equity_funds \
  .select('inv_veh_id', 'fund_acrnm_cd'),
  on = 'inv_veh_id',
  how = 'inner'
     ) \
.join(
  target_org_ids \
  .withColumnRenamed('fincl_itrmy_org_id', 'crdtd_to_org_id'),
  on = 'crdtd_to_org_id',
  how = 'inner'
     ) \
.join(
  macroeconomic_variables,
  on = 'mo_num',
  how = 'inner'
     ) \
.select(
  'fincl_txn_id',
  'mo_num',
  'rpo_ind',
  'instn_ind',
  'fund_acrnm_cd',
  'org_nm_standard',
  'fincl_itrmy_prson_id',
  'rpc_terr_id',
  'irm_terr_id',
  'rtrmt_plan_id',
  'rdmpt_amt',
  'sls_amt',
  'exchg_in_amt',
  'exchg_out_amt',
  'SentimentIndex',
  'GDP',
  'GoldPrice',
  'HPIIndex',
  'VIX_Open',
  'VIX_High',
  'VIX_Low',
  'VIX_Close',
  'OilPrice',
  'TDSP',
  'FODSP',
  'TEDRate',
  '3_Months_Yield',
  '6_Months_Yield',
  '1_Year_Yield',
  '3_Year_Yield',
  '5_Year_Yield',
  '10_Year_Yield',
  '20_Year_Yield'
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS edp;

# COMMAND ----------

base_data \
.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
.saveAsTable("edp.base_data")

# COMMAND ----------

base_data_agg = \
base_data \
.groupBy(
  'fund_acrnm_cd',
  'org_nm_standard',
  'mo_num',
) \
.agg(
  sum('rdmpt_amt').alias('rdmpt_amt')
)

# COMMAND ----------

base_data_agg \
.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
.saveAsTable("edp.base_data_agg")
