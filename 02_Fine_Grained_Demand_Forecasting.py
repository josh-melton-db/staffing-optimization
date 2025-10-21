# Databricks notebook source
# MAGIC %md
# MAGIC # Staffing Optimization for Logistics Operations
# MAGIC
# MAGIC This solution demonstrates how to optimize staffing and labor allocation across distribution centers for a logistics company.

# COMMAND ----------

# MAGIC %md
# MAGIC # Package Volume Forecasting

# COMMAND ----------

# MAGIC %md
# MAGIC *Prerequisite: Make sure to run 01_Introduction_And_Setup before running this notebook.*
# MAGIC
# MAGIC In this notebook we execute one-week-ahead forecast to estimate next week's package volume for each distribution center. This forecast will be used to determine labor requirements and optimize staffing levels.
# MAGIC
# MAGIC Key highlights for this notebook:
# MAGIC - Use Databricks' collaborative and interactive notebook environment to develop time series forecasting models
# MAGIC - Use Pandas UDFs (user-defined functions) to take your single-node data science code and distribute it across multiple nodes
# MAGIC - Forecast package volumes for each distribution center to support workforce planning

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Full namespace: {catalog}.{schema}")

# COMMAND ----------

import os
import datetime as dt
import numpy as np
import pandas as pd

from statsmodels.tsa.api import ExponentialSmoothing

import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

package_volume_df = spark.read.table(f"{catalog}.{schema}.package_volume")
package_volume_df = package_volume_df.cache() # just for this example notebook

# COMMAND ----------

display(package_volume_df)

# COMMAND ----------

# This is just to create one example for development and testing
#example_dc = package_volume_df.select("distribution_center").orderBy("distribution_center").limit(1).collect()[0].distribution_center
#pdf = package_volume_df.filter(f.col("distribution_center") == example_dc).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## One-step ahead forecast via Holt's Winters Seasonal Method
# MAGIC Holt-Winters' method is based on triple exponential smoothing and is able to account for both trend and seasonality.

# COMMAND ----------

def one_step_ahead_forecast(pdf: pd.DataFrame) -> pd.DataFrame:

    #Prepare series for forecast
    series_df = pd.Series(pdf['package_volume'].values, index=pdf['date'])
    series_df = series_df.asfreq(freq='W-MON')

    # One-step ahead forecast
    fit1 = ExponentialSmoothing(
        series_df,
        trend="add",
        seasonal="add",
        use_boxcox=False,
        initialization_method="estimated",
    ).fit(method="ls")
    fcast1 = fit1.forecast(1).rename("Additive trend and additive seasonal")

    # Collect Result
    df = pd.DataFrame(data = 
                      {
                         'distribution_center': pdf['distribution_center'].iloc[0], 
                         'date' : pd.to_datetime(series_df.index.values[-1]) + dt.timedelta(days=7), 
                         'forecasted_volume' : int(abs(fcast1.iloc[-1]))
                      }, 
                          index = [0]
                     )
    return df

# COMMAND ----------

fc_schema = StructType(
  [
    StructField('distribution_center', StringType()),
    StructField('date', DateType()),
    StructField('forecasted_volume', IntegerType())
  ]
)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = package_volume_df.select("distribution_center").distinct().count()

forecast_df = (
  package_volume_df
  .repartition(n_tasks, "distribution_center")
  .groupBy("distribution_center")
  .applyInPandas(one_step_ahead_forecast, schema=fc_schema)
)

display(forecast_df)

# COMMAND ----------

assert package_volume_df.select('distribution_center').distinct().count() == forecast_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate labor requirements based on forecasted volume

# COMMAND ----------

# Load labor productivity data
labor_productivity = spark.read.table(f"{catalog}.{schema}.labor_productivity")

# COMMAND ----------

display(labor_productivity)

# COMMAND ----------

# Calculate required worker-hours per DC and worker type
# Assumptions: 
# - 40 hours per worker per week (standard full-time)
# - Workers operate at stated productivity rates

# Join forecast with productivity data
labor_requirements = (
    forecast_df
    .join(labor_productivity, ["distribution_center"], "inner")
    .withColumn(
        "required_worker_hours",
        f.when(
            f.col("packages_per_hour") > 0,
            f.col("forecasted_volume") / f.col("packages_per_hour")
        ).otherwise(
            # For supervisors and non-package-handling roles, use workforce mix ratio
            f.col("forecasted_volume") * f.col("workforce_mix_ratio") / 5000  # Baseline calculation
        )
    )
    .withColumn(
        "required_headcount",
        f.ceil(f.col("required_worker_hours") / 40.0)  # Convert hours to FTE (40 hrs/week)
    )
)

display(labor_requirements)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save forecasts and labor requirements to Unity Catalog

# COMMAND ----------

# Write the forecast data as managed table
forecast_df.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.volume_forecast")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.volume_forecast"))

# COMMAND ----------

# Write the labor requirements data as managed table
labor_requirements.write \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{schema}.labor_requirements")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.labor_requirements"))

# COMMAND ----------

# MAGIC %md 
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |

# COMMAND ----------


