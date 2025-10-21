# Databricks notebook source
# MAGIC %md
# MAGIC # Staffing Optimization for Logistics Operations
# MAGIC
# MAGIC This solution demonstrates how to optimize staffing and labor allocation across distribution centers for a logistics company.
# MAGIC
# MAGIC # Introduction
# MAGIC
# MAGIC
# MAGIC **Situation**:
# MAGIC We model a logistics company operating 12 distribution centers across 5 regions (Northeast, Southeast, Midwest, Southwest, and West). Each distribution center handles package sorting, loading, and delivery operations with varying volumes based on their size and region. The company employs 5 different types of workers: Package Handlers, Sorters, Loaders, Drivers, and Supervisors.
# MAGIC
# MAGIC
# MAGIC **The following are given**:
# MAGIC - Historical package volume time series for each distribution center (2 years of weekly data)
# MAGIC - Distribution center details including region, size, and capacity multipliers
# MAGIC - Worker type definitions with hourly wages, productivity rates (packages per hour), and training costs
# MAGIC - Current staffing levels at each distribution center by worker type
# MAGIC - Labor costs including regular wages, overtime rates, hiring costs, and termination costs
# MAGIC - Labor productivity metrics defining how many packages each worker type can process per hour
# MAGIC - Workforce mix ratios indicating optimal proportions of each worker type
# MAGIC
# MAGIC
# MAGIC **We proceed in 2 steps**:
# MAGIC - *Package Volume Forecasting*: Forecast package volumes one week ahead for each distribution center
# MAGIC   - Generate time series forecasts for weekly package volumes at each distribution center
# MAGIC   - Account for seasonality, trends, and regional variations
# MAGIC - *Staffing Optimization*: Optimize workforce allocation and hiring decisions
# MAGIC   - Calculate labor requirements based on forecasted volumes and productivity rates
# MAGIC   - Minimize total labor costs (regular time + overtime + hiring/termination) while meeting service levels
# MAGIC   - Determine optimal staffing levels and hiring/layoff recommendations for each DC and worker type

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=true
