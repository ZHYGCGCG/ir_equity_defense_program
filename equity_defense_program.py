# Databricks notebook source
# MAGIC %md #Equity Defense Program
# MAGIC - which assets are most at risk of redemption?

# COMMAND ----------

# MAGIC %md 
# MAGIC ## To Do:
# MAGIC 
# MAGIC ### Treat all input/output as flows?
# MAGIC - all negatives summed
# MAGIC - all positive summed
# MAGIC 
# MAGIC ### Rank returns to peers rather than flat values
# MAGIC - returns for each share class (we have sales at that level)
# MAGIC 
# MAGIC ### Lag of macroeconomic variables
# MAGIC 
# MAGIC ### Other fund measures (and lags)
# MAGIC - sharpe, IV, vs benchmark, etc
# MAGIC 
# MAGIC ### Do we have holding data to do % redemptions of total holdings?
# MAGIC 
# MAGIC ### Plan sizing data?
# MAGIC 
# MAGIC ### How do we tie advisor to Plan?
# MAGIC 
# MAGIC ### Backfill/forward fill data?

# COMMAND ----------

# MAGIC %md ###Thoughts:
# MAGIC #### Windows of time for activity for sales/redemptions because of long sales cycle
# MAGIC - trailing 6 mo redemption/sales
# MAGIC - leading 6 mo

# COMMAND ----------

# MAGIC %md ### Funds
# MAGIC - GFA, EUPAC, WMIF, FI, ICA

# COMMAND ----------

fund_list = ['GFA', 'EUPAC', 'WMIF', 'FI', 'ICA']

# COMMAND ----------

# MAGIC %md ### Firms
# MAGIC - Edward Jones, LPL, Raymond James, Advisor Group, Merrill, Morgan Stanley, UBS, Wells Fargo

# COMMAND ----------

# UBS is too common for regex find
# advisor group is a consortium of firms

firm_list = \
[
('edward_jones',   ['Edward Jones']),
('lpl',            ['LPL']),
('raymond_james',  ['Raymond James']),
('advisor_group',  ['FSC Securities Corporation', 'Royal Alliance Associates', 'SagePoint Financial', 'Woodbury Financial Services']),
('merrill',        ['Merrill Lynch']),
('morgan_stanley', ['Morgan Stanley']),
('ubs',            ['UBS Financial', 'UBS Securities']),
('wells_fargo',    ['Wells Fargo'])
]

# COMMAND ----------

# MAGIC %md # Load Data

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

refresh_data = True

month_number_start = 201700

# COMMAND ----------

# MAGIC %md ### Macroeconomic Data

# COMMAND ----------

# can get goldprice to work if we explicitly read as double

# schema = StructType() \
#       .add("AsOfDate",StringType(),True) \
#       .add("GDP",DoubleType(),True) 
      
# df_with_schema = spark.read.format("csv") \
#       .option("header", True) \
#       .schema(schema) \
#       .load("adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/equity_defense_program/GDP.csv")

# COMMAND ----------

macroeconomic_file_location = 'adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/equity_defense_program/'

macro_lookups = []

for file in dbutils.fs.ls(macroeconomic_file_location):

  file_standard = sub('.csv','',
                  sub(' ', '_',
                  sub('-', '_', 
                  file.name))).lower()

  macro_lookups.append(file_standard)

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
.withColumn('month_number', year(col('date'))*lit(100)+month(col('date'))) \
.drop('AsOfDate','date')

# macroeconomic_variables = reduce(lambda a, b: a.join(b,how = 'left', on = 'AsOfDate'), [globals()[file_standard] for file_standard in macro_lookups])

# remove spaces from column names
macroeconomic_variables = macroeconomic_variables.select([col(column).alias(column.replace(' ', '_')).cast(DoubleType()) for column in macroeconomic_variables.columns])

# COMMAND ----------

if refresh_data:

  macroeconomic_file_location = 'adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/equity_defense_program/'

  macro_lookups = []

  for file in dbutils.fs.ls(macroeconomic_file_location):

    file_standard = sub('.csv','',
                    sub(' ', '_',
                    sub('-', '_', 
                    file.name))).lower()

    macro_lookups.append(file_standard)

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
  .withColumn('month_number', year(col('date'))*lit(100)+month(col('date'))) \
  .drop('AsOfDate','date')

  # macroeconomic_variables = reduce(lambda a, b: a.join(b,how = 'left', on = 'AsOfDate'), [globals()[file_standard] for file_standard in macro_lookups])

  # remove spaces from column names
  macroeconomic_variables = macroeconomic_variables.select([col(column).alias(column.replace(' ', '_')).cast(DoubleType()) for column in macroeconomic_variables.columns])
  
  macroeconomic_variables \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.macroeconomic_variables")

macroeconomic_variables = spark.table('edp.macroeconomic_variables')

# COMMAND ----------

# MAGIC %md ### Target firms

# COMMAND ----------

if refresh_data:

  standard_redemptions = spark.table("edp.base_data")
  
  firm_lookups = []

  for standard_firm in firm_list:
    for lookup_name in standard_firm[1]:
      firm_lookups.append(lookup_name)
      globals()[lookup_name] = \
      standard_redemptions \
      .filter(lower(col('organization_name')).contains(lower(lit(lookup_name)))) \
      .withColumn('organization_name_standard', lit(standard_firm[0])) \
      .select('organization_name', 'organization_name_standard') \
      .distinct()

  target_firms = reduce(DataFrame.unionAll, [globals()[lookup_name] for lookup_name in firm_lookups])
  
  target_firms \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.target_firms")

target_firms = spark.table('edp.target_firms')

# COMMAND ----------

# MAGIC %md ### Target Funds

# COMMAND ----------

if refresh_data:
  # https://www.capitalgroup.com/individual/investments/fund/rgagx
  # Suffix list
  # R1 AX
  # R2 BX
  # R3 CX
  # R4 EX
  # R5 FX
  # R6 GX

  fund_suffix_list = ['AX','BX','CX','EX','FX','GX']

  def add_suffix(input):
    return([input + suffix for suffix in fund_suffix_list])

  trading_symbols_data = [
    ('GFA',   add_suffix('RGA')),
    ('EUPAC', add_suffix('RER')),
    ('WMIF',  add_suffix('RWM')),
    ('FI',    add_suffix('RFN')),
    ('ICA',   add_suffix('RIC'))
  ]

  symbol_lookups = []

  Operation_TradingExchange_TradingExchangeList = spark.read.format('parquet').options(header='true').load('adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/imc/data_inputs/Operation_TradingExchange_TradingExchangeList.parquet') \
  .withColumnRenamed('TradingSymbol', 'ticker_symbol') \
  .withColumnRenamed('InvestmentVehicleId', 'investment_vehicle_id') \
  .select('investment_vehicle_id','ticker_symbol') \
  .distinct()

  for standard_symbol in trading_symbols_data:
    for trading_symbol in standard_symbol[1]:
      symbol_lookups.append(trading_symbol)
      globals()[trading_symbol] = \
      Operation_TradingExchange_TradingExchangeList \
      .filter(col('ticker_symbol') == lit(trading_symbol)) \
      .withColumn('parent_fund_acronym_group', lit(standard_symbol[0])) \
      .withColumn('parent_share_class_name', 
                  when(col('ticker_symbol').endswith('AX'),'R-1') \
                 .when(col('ticker_symbol').endswith('BX'),'R-2') \
                 .when(col('ticker_symbol').endswith('CX'),'R-3') \
                 .when(col('ticker_symbol').endswith('EX'),'R-4') \
                 .when(col('ticker_symbol').endswith('FX'),'R-5') \
                 .when(col('ticker_symbol').endswith('GX'),'R-6') \
                 .otherwise(None)
                 ) \
      .select('ticker_symbol', 'parent_fund_acronym_group', 'investment_vehicle_id','parent_share_class_name') \
      .distinct()

  target_funds = reduce(DataFrame.unionAll, [globals()[trading_symbol] for trading_symbol in symbol_lookups])

  target_funds \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.target_funds")
  
target_funds = spark.table('edp.target_funds')

# COMMAND ----------

# MAGIC %md ### Output Base Data

# COMMAND ----------

if refresh_data:
  
  inst = spark.read.parquet("adl://adlseastus2lasr.azuredatalakestore.net/lasr/data/prepared/mss_pz_shared_ns/institutionaldatasales")
  
  standard_inst = \
  inst \
  .withColumnRenamed('monthnumber','month_number') \
  .withColumnRenamed('retirementplanid','retirement_plan_id') \
  .withColumnRenamed('organizationname','organization_name') \
  .withColumnRenamed('businessline1','business_line_1') \
  .withColumnRenamed('parentfundacronymgroup','parent_fund_acronym_group') \
  .withColumnRenamed('parentshareclassname','parent_share_class_name') \
  .filter((col('retirement_plan_id') != '#') & (col('retirement_plan_id') != 0)) \
  .filter(col('month_number') > month_number_start) \
  .join(
  target_firms,
  on = 'organization_name',
  how = 'inner'
  ) \
  .join(
    target_funds,
    on = ['parent_fund_acronym_group','parent_share_class_name'],
    how = 'inner'
  ) \
  .groupBy(
    'month_number',
    'retirement_plan_id',
    'organization_name_standard',
    'business_line_1',
    'parent_fund_acronym_group',
    'parent_share_class_name',
    'ticker_symbol',
    'investment_vehicle_id'
  ) \
  .agg(
    sum('saleamount').alias('sale_amount'),
    sum('redemptionamount').alias('redemption_amount'),
    sum('exchangeinamount').alias('exchange_in_amount'),
    sum('exchangeoutamount').alias('exchange_out_amount')
  ) \
  .withColumn('data_source',lit('inst'))
  
  standard_inst \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.standard_inst")
  
  
  iriis = ReadLASR("MSS_M_SALESTRANSPARENCY_S.vwFinancialTransactionClassified")

  standard_iriis = \
  iriis \
  .filter(col('BusinessLine1')=='IR') \
  .withColumnRenamed('MonthNumber','month_number') \
  .withColumnRenamed('RetirementPlanId','retirement_plan_id') \
  .withColumnRenamed('OrganizationName','organization_name') \
  .withColumnRenamed('BusinessLine1','business_line_1') \
  .withColumnRenamed('ParentFundAcronymGroup','parent_fund_acronym_group') \
  .withColumnRenamed('ParentShareClassName','parent_share_class_name') \
  .filter((col('retirement_plan_id') != '#') & (col('retirement_plan_id') != 0)) \
  .filter(col('month_number') > month_number_start) \
  .join(
  target_firms,
  on = 'organization_name',
  how = 'inner'
  ) \
  .join(
    target_funds,
    on = ['parent_fund_acronym_group','parent_share_class_name'],
    how = 'inner'
  ) \
  .groupBy(
    'month_number',
    'retirement_plan_id',
    'organization_name_standard',
    'business_line_1',
    'parent_fund_acronym_group',
    'parent_share_class_name',
    'ticker_symbol',
    'investment_vehicle_id'
  ) \
  .agg(
    sum('SaleAmount').alias('sale_amount'),
    sum('RedemptionAmount').alias('redemption_amount'),
    sum('ExchangeInAmount').alias('exchange_in_amount'),
    sum('ExchangeOutAmount').alias('exchange_out_amount')
  ) \
  .withColumn('data_source',lit('iriis'))
  
  standard_iriis \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.standard_iriis")
  
standard_inst = spark.table('edp.standard_inst')
standard_iriis = spark.table('edp.standard_iriis')

standard_redemptions = \
standard_inst \
.union(standard_iriis)

# COMMAND ----------

# MAGIC %md ### Fund Returns
# MAGIC - computing seperately <a href="https://adb-3737625127627863.3.azuredatabricks.net/?o=3737625127627863#notebook/2096184987765418/command/2096184987765427">here</a>

# COMMAND ----------

if refresh_data:
  fund_returns = \
  spark.read.format('parquet').options(header='true').load('adl://adlseastus2lasr.azuredatalakestore.net/lasr/sandbox/nad/zhyg/imc/data_inputs/fund_returns_monthly.parquet')
  
  fund_returns \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.fund_returns")

fund_returns = spark.table('edp.fund_returns')

# COMMAND ----------

base_data = \
standard_redemptions \
.join(
  macroeconomic_variables,
  on = 'month_number',
  how = 'inner'
     ) \
.join(
  fund_returns,
  on = ['month_number','ticker_symbol'],
  how = 'inner'
) \
.drop('GoldPrice','VIX_Open','VIX_High','VIX_Low')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS edp;

# COMMAND ----------

# MAGIC %md ### Forward fill null values
# MAGIC - macroeconomic data can be quarterly

# COMMAND ----------

keys = ['parent_fund_acronym_group','retirement_plan_id','organization_name_standard','business_line_1','parent_share_class_name','morningstar_category_grouped','data_source','investment_vehicle_id']

window = Window.partitionBy(keys)\
               .orderBy('month_number')\
               .rowsBetween(-sys.maxsize, 0)

for column in (set(base_data.columns) - set(keys)):
  # forward fill
  base_data = \
  base_data \
  .withColumn(column, last(col(column), ignorenulls=True).over(window))
  
  # back fill (do both to fill in null records that occur first)
  base_data = \
  base_data \
  .withColumn(column, first(col(column), ignorenulls=True).over(window))

# COMMAND ----------

# MAGIC %md ### Trailing Redemptions
# MAGIC - 3, 6, 12 mo averages

# COMMAND ----------

trailing_periods = [3, 6, 12]

for period in trailing_periods:
  trailing_window = (Window.partitionBy('parent_fund_acronym_group','retirement_plan_id','organization_name','business_line_1','parent_share_class_name').orderBy('month_number').rangeBetween(-(period + 1), -1))
  base_data = base_data.withColumn('avg_redemption_amount_lag_' + str(period) + '_months', avg("redemption_amount").over(trailing_window))

# COMMAND ----------

if refresh_data:
  base_data \
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("edp.base_data")
