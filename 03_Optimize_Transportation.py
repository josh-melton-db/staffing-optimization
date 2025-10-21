# Databricks notebook source
# MAGIC %md
# MAGIC # Staffing Optimization for Logistics Operations
# MAGIC
# MAGIC This solution demonstrates how to optimize staffing and labor allocation across distribution centers for a logistics company.

# COMMAND ----------

# MAGIC %md
# MAGIC # Staffing Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC *Prerequisite: Make sure to run 02_Fine_Grained_Demand_Forecasting before running this notebook.*
# MAGIC
# MAGIC In this notebook we solve a Linear Programming (LP) problem to optimize staffing levels across distribution centers and worker types. The goal is to minimize total labor costs while meeting operational requirements based on forecasted package volumes.
# MAGIC
# MAGIC Key highlights for this notebook:
# MAGIC - Use Databricks' collaborative and interactive notebook environment to develop optimization procedures
# MAGIC - Pandas UDFs (user-defined functions) can take your single-node data science code and distribute it across multiple nodes
# MAGIC - Scale optimization to hundreds of distribution centers
# MAGIC
# MAGIC More precisely we solve the following optimization problem for each distribution center.
# MAGIC
# MAGIC *Mathematical goal:*
# MAGIC Minimize total labor costs across all worker types, where total cost includes: <br/>
# MAGIC - Regular time wages: workers × hours × regular_hourly_wage <br/>
# MAGIC - Overtime wages: overtime_hours × overtime_hourly_wage <br/>
# MAGIC - Hiring costs: new_hires × hiring_cost <br/>
# MAGIC - Termination costs: layoffs × termination_cost
# MAGIC
# MAGIC *Mathematical constraints:*
# MAGIC - Workforce must be non-negative integers (can't have fractional workers)
# MAGIC - Total labor capacity must meet or exceed required worker-hours based on forecasted volumes
# MAGIC - Workforce mix ratios should be approximately maintained (e.g., 40% Package Handlers, 25% Sorters, etc.)
# MAGIC - Overtime should not exceed 20% of regular hours per worker
# MAGIC - New hires and layoffs are minimized (penalty in objective function)

# COMMAND ----------

# The pulp library is used for solving the LP problem
%pip install pulp

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

print(cloud_storage_path)
print(dbName)

# COMMAND ----------

import os
import datetime as dt
import re
import numpy as np
import pandas as pd

import pulp

import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and prepare data for optimization

# COMMAND ----------

# Load labor requirements (from forecasting notebook)
labor_requirements = spark.read.table(f"{dbName}.labor_requirements")
display(labor_requirements)

# COMMAND ----------

# Load current staffing levels
current_staffing = spark.read.table(f"{dbName}.current_staffing")
display(current_staffing)

# COMMAND ----------

# Load labor costs
labor_costs = spark.read.table(f"{dbName}.labor_costs")
display(labor_costs)

# COMMAND ----------

# Join all information needed for optimization
# One row per distribution center and worker type
staffing_optimization_input = (
    labor_requirements
    .join(current_staffing, ["distribution_center", "worker_type"], "inner")
    .join(labor_costs, ["worker_type"], "inner")
    .select(
        "distribution_center",
        "worker_type",
        "required_worker_hours",
        "required_headcount",
        "current_headcount",
        "regular_hourly_wage",
        "overtime_hourly_wage",
        "hiring_cost",
        "termination_cost",
        "workforce_mix_ratio"
    )
)

display(staffing_optimization_input)

# COMMAND ----------

# Define output schema of final result table
res_schema = StructType(
  [
    StructField('distribution_center', StringType()),
    StructField('worker_type', StringType()),
    StructField('current_headcount', IntegerType()),
    StructField('optimal_headcount', IntegerType()),
    StructField('new_hires', IntegerType()),
    StructField('layoffs', IntegerType()),
    StructField('regular_hours', FloatType()),
    StructField('overtime_hours', FloatType()),
    StructField('total_cost', FloatType()),
    StructField('optimization_status', StringType())
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define staffing optimization function

# COMMAND ----------

# Define a function that solves the staffing optimization LP for one distribution center
def staffing_optimization(pdf: pd.DataFrame) -> pd.DataFrame:
    
    dc_name = pdf['distribution_center'].iloc[0]
    
    # Get list of worker types
    worker_types = sorted(pdf['worker_type'].unique().tolist())
    
    # Create decision variables
    # Number of workers for each type (integer)
    workers = pulp.LpVariable.dicts("Workers", worker_types, lowBound=0, cat='Integer')
    
    # Overtime hours per worker type (continuous, non-negative)
    overtime = pulp.LpVariable.dicts("Overtime", worker_types, lowBound=0, cat='Continuous')
    
    # New hires per worker type (integer, non-negative)
    new_hires = pulp.LpVariable.dicts("NewHires", worker_types, lowBound=0, cat='Integer')
    
    # Layoffs per worker type (integer, non-negative)
    layoffs = pulp.LpVariable.dicts("Layoffs", worker_types, lowBound=0, cat='Integer')
    
    # Create dictionaries for parameters
    current_staff = {}
    required_hours = {}
    regular_wage = {}
    ot_wage = {}
    hire_cost = {}
    term_cost = {}
    
    for idx, row in pdf.iterrows():
        wt = row['worker_type']
        current_staff[wt] = row['current_headcount']
        required_hours[wt] = row['required_worker_hours']
        regular_wage[wt] = row['regular_hourly_wage']
        ot_wage[wt] = row['overtime_hourly_wage']
        hire_cost[wt] = row['hiring_cost']
        term_cost[wt] = row['termination_cost']
    
    # Create the LP problem
    prob = pulp.LpProblem(f"Staffing_Optimization_{dc_name}", pulp.LpMinimize)
    
    # Objective function: Minimize total cost
    # Cost = regular wages + overtime wages + hiring costs + termination costs
    prob += (
        pulp.lpSum([workers[wt] * 40 * regular_wage[wt] for wt in worker_types]) +  # Regular time
        pulp.lpSum([overtime[wt] * ot_wage[wt] for wt in worker_types]) +  # Overtime
        pulp.lpSum([new_hires[wt] * hire_cost[wt] for wt in worker_types]) +  # Hiring
        pulp.lpSum([layoffs[wt] * term_cost[wt] for wt in worker_types]),  # Terminations
        "Total_Labor_Cost"
    )
    
    # Constraints
    
    # 1. Meet labor requirements: regular hours + overtime >= required hours
    for wt in worker_types:
        prob += (
            workers[wt] * 40 + overtime[wt] >= required_hours[wt],
            f"Labor_Requirement_{wt}"
        )
    
    # 2. Overtime cannot exceed 20% of regular hours (8 hours per worker per week max)
    for wt in worker_types:
        prob += (
            overtime[wt] <= workers[wt] * 8,
            f"Overtime_Limit_{wt}"
        )
    
    # 3. Workforce change equation: new_workers = current + hires - layoffs
    for wt in worker_types:
        prob += (
            workers[wt] == current_staff[wt] + new_hires[wt] - layoffs[wt],
            f"Workforce_Change_{wt}"
        )
    
    # 4. Supervisors constraint: at least 1 supervisor per 20 total workers
    total_non_supervisor_workers = pulp.lpSum([workers[wt] for wt in worker_types if wt != 'Supervisor'])
    prob += (
        workers.get('Supervisor', 0) * 20 >= total_non_supervisor_workers,
        "Supervisor_Ratio"
    )
    
    # Solve the problem
    prob.solve(pulp.PULP_CBC_CMD(msg=0))  # msg=0 suppresses solver output
    
    # Extract results
    results = []
    
    if pulp.LpStatus[prob.status] == "Optimal":
        for wt in worker_types:
            optimal_workers = int(workers[wt].varValue) if workers[wt].varValue is not None else 0
            ot_hours = overtime[wt].varValue if overtime[wt].varValue is not None else 0.0
            hires = int(new_hires[wt].varValue) if new_hires[wt].varValue is not None else 0
            terms = int(layoffs[wt].varValue) if layoffs[wt].varValue is not None else 0
            
            # Calculate total cost for this worker type
            cost = (optimal_workers * 40 * regular_wage[wt] + 
                   ot_hours * ot_wage[wt] + 
                   hires * hire_cost[wt] + 
                   terms * term_cost[wt])
            
            results.append({
                'distribution_center': dc_name,
                'worker_type': wt,
                'current_headcount': current_staff[wt],
                'optimal_headcount': optimal_workers,
                'new_hires': hires,
                'layoffs': terms,
                'regular_hours': optimal_workers * 40.0,
                'overtime_hours': ot_hours,
                'total_cost': cost,
                'optimization_status': 'Optimal'
            })
    else:
        # If optimization failed, return current state
        for wt in worker_types:
            results.append({
                'distribution_center': dc_name,
                'worker_type': wt,
                'current_headcount': current_staff[wt],
                'optimal_headcount': current_staff[wt],
                'new_hires': 0,
                'layoffs': 0,
                'regular_hours': current_staff[wt] * 40.0,
                'overtime_hours': 0.0,
                'total_cost': current_staff[wt] * 40 * regular_wage[wt],
                'optimization_status': pulp.LpStatus[prob.status]
            })
    
    return pd.DataFrame(results)

# COMMAND ----------

# Test the function on one DC
# test_dc = "DC_1"
# pdf = staffing_optimization_input.filter(f.col("distribution_center") == test_dc).toPandas()
# staffing_optimization(pdf)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = staffing_optimization_input.select("distribution_center").distinct().count()

optimal_staffing_df = (
  staffing_optimization_input
  .repartition(n_tasks, "distribution_center")
  .groupBy("distribution_center")
  .applyInPandas(staffing_optimization, schema=res_schema)
)

display(optimal_staffing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze optimization results

# COMMAND ----------

# Summary statistics
summary_stats = optimal_staffing_df.groupBy("worker_type").agg(
    f.sum("new_hires").alias("total_new_hires"),
    f.sum("layoffs").alias("total_layoffs"),
    f.sum("optimal_headcount").alias("total_optimal_headcount"),
    f.sum("current_headcount").alias("total_current_headcount"),
    f.sum("overtime_hours").alias("total_overtime_hours"),
    f.sum("total_cost").alias("total_weekly_cost")
)

display(summary_stats)

# COMMAND ----------

# Cost breakdown by DC
dc_costs = optimal_staffing_df.groupBy("distribution_center").agg(
    f.sum("total_cost").alias("total_weekly_cost"),
    f.sum("optimal_headcount").alias("total_workers"),
    f.sum("new_hires").alias("total_new_hires"),
    f.sum("layoffs").alias("total_layoffs")
).orderBy(f.desc("total_weekly_cost"))

display(dc_costs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save optimization results to delta

# COMMAND ----------

staffing_recommendations_delta_path = os.path.join(cloud_storage_path, 'staffing_recommendations')

# COMMAND ----------

# Write the data 
optimal_staffing_df.write \
.mode("overwrite") \
.format("delta") \
.save(staffing_recommendations_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.staffing_recommendations")
spark.sql(f"CREATE TABLE {dbName}.staffing_recommendations USING DELTA LOCATION '{staffing_recommendations_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.staffing_recommendations"))

# COMMAND ----------

# MAGIC %md 
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |

# COMMAND ----------


