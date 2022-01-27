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

# MAGIC %md ### Fund Returns
# MAGIC - starting with all funds R6 returns because investment vehicle id does not match transaction table

# COMMAND ----------

# https://www.capitalgroup.com/individual/investments/fund/rgagx
# Suffix list
# R1 AX
# R2 BX
# R3 CX
# R4 EX
# R5 FX
# R6 GX

trading_symbols_data = [
  ('GFA', 'RGAGX'),
  ('EUPAC', 'RERGX'),
  ('WMIF', 'RWMGX'),
  ('FI', 'RFNGX'),
  ('ICA', 'RICGX')
]

cols = ["fund_acrnm_cd","TradingSymbol"]
trading_symbols = spark.createDataFrame(data=trading_symbols_data, schema = cols)

# COMMAND ----------

Operation_TradingExchange_TradingExchangeList = spark.read.format('parquet').options(header='true').load('adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/imc/data_inputs/Operation_TradingExchange_TradingExchangeList.parquet') \
.select('InvestmentVehicleId','TradingSymbol') \
.distinct()

# COMMAND ----------

target_trading_symbols = \
trading_symbols \
.join(Operation_TradingExchange_TradingExchangeList, on = 'TradingSymbol', how = 'inner')

# COMMAND ----------

fund_returns = spark.read.format('parquet').options(header='true').load('adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/imc/data_inputs/morningstar_fund_returns.parquet') \
.join(target_trading_symbols, on = 'InvestmentVehicleId', how = 'inner') \
.filter(col('Return').isNotNull())

fund_returns_pd = \
fund_returns \
.drop('InvestmentVehicleID','TradingSymbol') \
.toPandas()

fund_returns_pd['Date'] = pd.to_datetime(
fund_returns_pd['Date'],
format='%Y-%m-%d')

fund_returns_pd.set_index('Date')

fund_returns_pd['Return'] = pd.to_numeric(
fund_returns_pd['Return'],
downcast='float')

# COMMAND ----------

# resample data to monthly

returns_monthly = \
fund_returns_pd \
.set_index('Date') \
.groupby('fund_acrnm_cd') \
.resample('M') \
.agg(lambda x: (x + 1).prod() - 1) 

# COMMAND ----------

annual_return_timeframes = [1, 3, 5, 10]

for period in annual_return_timeframes:
  returns_monthly['return_' + str(period) + 'y'] = returns_monthly.reset_index().set_index('Date').groupby(['fund_acrnm_cd'])['Return'].rolling(period * 12).agg(lambda x: (x + 1).prod() - 1) 
  
returns_monthly = returns_monthly.reset_index()

returns_monthly['mo_num'] = pd.DatetimeIndex(returns_monthly['Date']).year * 100 + pd.DatetimeIndex(returns_monthly['Date']).month

returns_monthly = returns_monthly.drop(['Return','Date'], axis=1)

fund_returns_rolling = spark.createDataFrame(returns_monthly)

# COMMAND ----------

# MAGIC %md ### Output Base Data
# MAGIC - only return records for our target fund / orgs

# COMMAND ----------

base_data = \
financial_transaction \
.select(
  'trd_dt',
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
)\
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
.join(
  fund_returns_rolling,
  on = ['mo_num','fund_acrnm_cd'],
  how = 'inner'
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

# MAGIC %md ### Trailing Redemptions
# MAGIC - 3, 6, 12 mo averages

# COMMAND ----------



# COMMAND ----------

base_data_agg \
.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
.saveAsTable("edp.base_data_agg")

# COMMAND ----------


