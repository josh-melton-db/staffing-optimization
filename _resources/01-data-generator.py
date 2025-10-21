# Databricks notebook source
dbutils.widgets.dropdown('reset_all_data', 'false', ['true', 'false'], 'Reset all data')
dbutils.widgets.text('catalog', 'main', 'Catalog Name')
dbutils.widgets.text('schema', 'staffing_optimization', 'Schema Name')

# COMMAND ----------

print("Starting ./_resources/01-data-generator")

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
reset_all_data = dbutils.widgets.get('reset_all_data') == 'true'

# COMMAND ----------

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Full namespace: {catalog}.{schema}")
print(f"Reset all data: {reset_all_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Packages

# COMMAND ----------

import pandas as pd
import numpy as np
import datetime

from dateutil.relativedelta import relativedelta
from dateutil import rrule

import os
import string
import random

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window

import statsmodels.api as sm
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate package volume time series data for distribution centers

# COMMAND ----------

# MAGIC %md
# MAGIC Parameters

# COMMAND ----------

ts_length_in_weeks = 104 # Length of a time series in weeks
n_distribution_centers = 12 # Number of distribution centers
n_worker_types = 5 # Number of different worker roles

# COMMAND ----------

# MAGIC %md
# MAGIC Create Worker Types Table

# COMMAND ----------

WorkerTypeSchema = StructType([       
    StructField('worker_type', StringType(), True),
    StructField('hourly_wage', FloatType(), True),
    StructField('packages_per_hour', FloatType(), True),
    StructField('training_cost', FloatType(), True)
])

# Worker types: Package Handlers, Sorters, Drivers, Loaders, Supervisors
worker_data = [
    ("Package_Handler", 18.5, 85.0, 500.0),
    ("Sorter", 19.0, 120.0, 600.0),
    ("Loader", 18.0, 95.0, 450.0),
    ("Driver", 24.0, 45.0, 1200.0),
    ("Supervisor", 32.0, 0.0, 800.0)  # Supervisors don't handle packages directly
]

worker_types_table = spark.createDataFrame(data=worker_data, schema=WorkerTypeSchema)

display(worker_types_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Create Distribution Centers Table

# COMMAND ----------

distribution_centers_table = spark.createDataFrame(
  list(range(1,(n_distribution_centers+1))),
  StringType()).toDF("dc_number")

distribution_centers_table = distribution_centers_table.select(
    f.concat_ws('_',f.lit("DC"), f.col("dc_number")).alias("distribution_center")
)

display(distribution_centers_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Distribution Center Details with Regions

# COMMAND ----------

# Assign regions and sizes to distribution centers
regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
sizes = ["Small", "Medium", "Large"]

dc_details_data = []
for i in range(1, n_distribution_centers + 1):
    dc_name = f"DC_{i}"
    region = regions[(i-1) % len(regions)]
    size = sizes[(i-1) % len(sizes)]
    # Size affects base package volume
    size_multiplier = {"Small": 0.7, "Medium": 1.0, "Large": 1.4}[size]
    dc_details_data.append((dc_name, region, size, size_multiplier))

DCDetailsSchema = StructType([
    StructField('distribution_center', StringType(), True),
    StructField('region', StringType(), True),
    StructField('size', StringType(), True),
    StructField('size_multiplier', FloatType(), True)
])

dc_details_table = spark.createDataFrame(data=dc_details_data, schema=DCDetailsSchema)

display(dc_details_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Generate Date Series

# COMMAND ----------

# End Date: Monday of the current week
end_date = datetime.datetime.now().replace(hour=0, minute=0, second= 0, microsecond=0) 
end_date = end_date + datetime.timedelta(-end_date.weekday()) #Make sure to get the monday before

# Start date: Is a monday, since we will go back integer number of weeks
start_date = end_date + relativedelta(weeks= (- ts_length_in_weeks))

# Make a sequence 
date_range = list(rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=end_date))

#Create a pandas data frame
date_range = pd.DataFrame(date_range, columns =['date'])

display(date_range)

# COMMAND ----------

# MAGIC %md
# MAGIC Simulate parameters for ARMA series for package volumes

# COMMAND ----------

# Define schema for new columns
arma_schema = StructType(
  [
    StructField("Variance_RN", FloatType(), True),
    StructField("Offset_RN", FloatType(), True),
    StructField("AR_Pars_RN", ArrayType(FloatType()), True),
    StructField("MA_Pars_RN", ArrayType(FloatType()), True)
  ]
)

# Join DC details with DC table for time series generation
dc_with_details = distribution_centers_table.join(dc_details_table, ["distribution_center"], "inner")

# Generate random numbers for the ARMA process
np.random.seed(123)
n_ = dc_with_details.count()

# Package volumes are much higher than product demand
# Base volume around 50,000 packages per week, with variation
variance_random_number = list(abs(np.random.normal(5000, 1000, n_)))
offset_random_number = list(np.maximum(abs(np.random.normal(50000, 20000, n_)), 20000))
ar_length_random_number = np.random.choice(list(range(1,4)), n_)
ar_parameters_random_number = [np.random.uniform(low=0.1, high=0.3, size=x) for x in ar_length_random_number] 
ma_length_random_number = np.random.choice(list(range(1,4)), n_)
ma_parameters_random_number = [np.random.uniform(low=0.1, high=0.3, size=x) for x in ma_length_random_number] 

# Collect in a dataframe
pdf_helper = (pd.DataFrame(variance_random_number, columns =['Variance_RN']). 
              assign(Offset_RN = offset_random_number).
              assign(AR_Pars_RN = ar_parameters_random_number).
              assign(MA_Pars_RN = ma_parameters_random_number) 
             )

spark_df_helper = spark.createDataFrame(pdf_helper, schema=arma_schema)
spark_df_helper = (spark_df_helper.
  withColumn("row_id", f.monotonically_increasing_id()).
  withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
  drop(f.col("row_id"))
                  )

dc_with_details = (dc_with_details.
                   withColumn("row_id", f.monotonically_increasing_id()).
                   withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
                   drop(f.col("row_id"))
                  )

dc_with_details = dc_with_details.join(spark_df_helper, ("row_num")).drop(f.col("row_num"))
display(dc_with_details)

# COMMAND ----------

# MAGIC %md 
# MAGIC Generate package volume time series for each distribution center

# COMMAND ----------

# To maximize parallelism, we can allocate each distribution center its own Spark task
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = dc_with_details.select("distribution_center").distinct().count()

# function to generate an ARMA process
def generate_arma(arparams, maparams, var, offset, number_of_points, plot):
  np.random.seed(123)
  ar = np.r_[1, arparams] 
  ma = np.r_[1, maparams] 
  y = sm.tsa.arma_generate_sample(ar, ma, number_of_points, scale=var, burnin= 1) + offset
  y = np.round(y).astype(int)
  y = np.absolute(y)
  
  if plot:
    x = np.arange(1, len(y) +1)
    plt.plot(x, y, color ="red")
    plt.show()
    
  return(y)

#Schema for output dataframe
schema = StructType(  
                    [
                      StructField("distribution_center", StringType(), True),
                      StructField("date", DateType(), True),
                      StructField("package_volume", FloatType(), True),
                      StructField("row_number", FloatType(), True)
                    ])

# Generate package volume time series using ARMA
def time_series_generator_pandas_udf(pdf):
  out_df = date_range.assign(
   package_volume = generate_arma(arparams = pdf.AR_Pars_RN.iloc[0], 
                        maparams= pdf.MA_Pars_RN.iloc[0], 
                        var = pdf.Variance_RN.iloc[0], 
                        offset = pdf.Offset_RN.iloc[0] * pdf.size_multiplier.iloc[0],  # Apply size multiplier
                        number_of_points = date_range.shape[0], 
                        plot = False),
  distribution_center = pdf["distribution_center"].iloc[0]
  )
  
  out_df["row_number"] = range(0,len(out_df))
  
  out_df = out_df[["distribution_center", "date", "package_volume", "row_number"]]

  return(out_df)

# Apply the Pandas UDF and clean up
package_volume_df = ( 
  dc_with_details.
   groupby("distribution_center"). 
   applyInPandas(time_series_generator_pandas_udf, schema).
   select("distribution_center", "date", "package_volume")
)

display(package_volume_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Save package volume as a Delta table (Unity Catalog Managed Table)

# COMMAND ----------

# Write as managed table in Unity Catalog
package_volume_df.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.package_volume")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.package_volume"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) as row_count FROM {catalog}.{schema}.package_volume"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Worker Types Table (Unity Catalog Managed Table)

# COMMAND ----------

# Write as managed table in Unity Catalog
worker_types_table.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.worker_types")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.worker_types"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Distribution Center Details Table (Unity Catalog Managed Table)

# COMMAND ----------

# Write as managed table in Unity Catalog
dc_details_table.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.dc_details")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.dc_details"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate current staffing levels for each DC and worker type

# COMMAND ----------

# Cross join DCs with worker types to get all combinations
current_staffing = distribution_centers_table.crossJoin(worker_types_table.select("worker_type"))

# Generate current headcount based on average package volumes and worker productivity
# This creates a realistic baseline staffing scenario
np.random.seed(456)

def generate_current_staffing(pdf: pd.DataFrame) -> pd.DataFrame:
    # Add some variation to staffing levels
    # Supervisors: 1 per 15-20 workers
    # Other workers: based on workload with some inefficiency
    pdf_return = pdf.copy()
    
    headcounts = []
    for idx, row in pdf.iterrows():
        if row['worker_type'] == 'Supervisor':
            # 1 supervisor per 15-20 other workers (will adjust after)
            headcount = np.random.randint(2, 6)
        else:
            # Base staffing on typical volume
            # Assume 40 hours per week, but only 70-90% utilization
            utilization = np.random.uniform(0.7, 0.9)
            base_headcount = np.random.randint(20, 80)
            headcount = int(base_headcount * utilization)
        headcounts.append(headcount)
    
    pdf_return['current_headcount'] = headcounts
    return pdf_return[['distribution_center', 'worker_type', 'current_headcount']]

staffing_schema = StructType([
    StructField('distribution_center', StringType(), True),
    StructField('worker_type', StringType(), True),
    StructField('current_headcount', IntegerType(), True)
])

current_staffing_table = (
    current_staffing
    .groupBy("distribution_center")
    .applyInPandas(generate_current_staffing, schema=staffing_schema)
)

display(current_staffing_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save current staffing as Delta table (Unity Catalog Managed Table)

# COMMAND ----------

# Write as managed table in Unity Catalog
current_staffing_table.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.current_staffing")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.current_staffing"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate labor costs table (wages, overtime, hiring costs)

# COMMAND ----------

# Labor cost parameters by worker type
labor_costs_data = []
for worker_type, base_wage, _, training_cost in worker_data:
    overtime_multiplier = 1.5  # Time and a half for overtime
    overtime_wage = base_wage * overtime_multiplier
    # Hiring costs include recruitment, onboarding, and training
    total_hiring_cost = training_cost + np.random.uniform(300, 800)
    # Termination/severance costs
    termination_cost = base_wage * 40  # Approximate 1 week severance
    
    labor_costs_data.append((
        worker_type,
        base_wage,
        overtime_wage,
        total_hiring_cost,
        termination_cost
    ))

LaborCostSchema = StructType([
    StructField('worker_type', StringType(), True),
    StructField('regular_hourly_wage', FloatType(), True),
    StructField('overtime_hourly_wage', FloatType(), True),
    StructField('hiring_cost', FloatType(), True),
    StructField('termination_cost', FloatType(), True)
])

labor_costs_table = spark.createDataFrame(data=labor_costs_data, schema=LaborCostSchema)

display(labor_costs_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save labor costs as Delta table (Unity Catalog Managed Table)

# COMMAND ----------

# Write as managed table in Unity Catalog
labor_costs_table.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.labor_costs")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.labor_costs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate labor productivity/requirements table

# COMMAND ----------

# Define how many packages each worker type can handle per hour
# and the mix of workers needed
productivity_data = []

# For each distribution center, define the worker mix ratios
for dc_row in dc_details_table.collect():
    dc_name = dc_row['distribution_center']
    
    # Package Handlers - primary workers for sorting and processing
    productivity_data.append((dc_name, "Package_Handler", 85.0, 0.40))  # 40% of workforce
    
    # Sorters - specialized in sorting operations
    productivity_data.append((dc_name, "Sorter", 120.0, 0.25))  # 25% of workforce
    
    # Loaders - loading and unloading trucks
    productivity_data.append((dc_name, "Loader", 95.0, 0.25))  # 25% of workforce
    
    # Drivers - delivering packages
    productivity_data.append((dc_name, "Driver", 45.0, 0.08))  # 8% of workforce (packages delivered per hour)
    
    # Supervisors - manage teams (1 per 15-20 workers)
    productivity_data.append((dc_name, "Supervisor", 0.0, 0.02))  # 2% of workforce

ProductivitySchema = StructType([
    StructField('distribution_center', StringType(), True),
    StructField('worker_type', StringType(), True),
    StructField('packages_per_hour', FloatType(), True),
    StructField('workforce_mix_ratio', FloatType(), True)
])

labor_productivity_table = spark.createDataFrame(data=productivity_data, schema=ProductivitySchema)

display(labor_productivity_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save labor productivity as Delta table (Unity Catalog Managed Table)

# COMMAND ----------

# Write as managed table in Unity Catalog
labor_productivity_table.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.labor_productivity")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.labor_productivity"))

# COMMAND ----------

print("Ending ./_resources/01-data-generator")
