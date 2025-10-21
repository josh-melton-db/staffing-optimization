# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

import os
import re 
import mlflow

# COMMAND ----------

# Unity Catalog configuration
# Get current user and create user-specific schema
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

# Unity Catalog three-level namespace
catalog = "main"  # Using 'main' catalog - change if you have a different catalog
schema = f"staffing_optimization_{current_user_no_at}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

# Create or reset schema
if reset_all:
  spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Full namespace: {catalog}.{schema}")

# COMMAND ----------

reset_all = dbutils.widgets.get('reset_all_data')
reset_all_bool = (reset_all == 'true')

# COMMAND ----------

# Generate data
dirname = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
filename = "01-data-generator"
if (os.path.basename(dirname) != '_resources'):
  dirname = os.path.join(dirname,'_resources')
generate_data_notebook_path = os.path.join(dirname,filename)

def generate_data():
  dbutils.notebook.run(generate_data_notebook_path, 600, {"reset_all_data": reset_all, "catalog": catalog, "schema": schema})

if reset_all_bool:
  generate_data()
else:
  # Check if tables exist
  try:
    spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.package_volume").collect()
  except: 
    generate_data()

# COMMAND ----------

mlflow.set_experiment(f'/Users/{current_user}/staffing_optimization_logistics')

# COMMAND ----------


