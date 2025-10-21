![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Staffing Optimization for Logistics Operations

This solution accelerator demonstrates how to optimize workforce allocation and staffing decisions for logistics companies operating distribution centers, similar to FedEx, UPS, or DHL.

## Problem Overview

**Situation**:
A logistics company operates 12 distribution centers across 5 regions (Northeast, Southeast, Midwest, Southwest, and West). Each distribution center processes packages for sorting, loading, and delivery with varying volumes based on size, region, and seasonality. The company employs 5 different types of workers: Package Handlers, Sorters, Loaders, Drivers, and Supervisors.

The company is experiencing growth with current staffing at 60-75% of optimal capacity. This creates a hiring and expansion scenario where the optimization determines optimal staffing levels, hiring recommendations, and cost-effective workforce allocation strategies.

**What's given**:
- Historical package volume time series for each distribution center (2 years of weekly data)
- Distribution center details including region, size, and capacity multipliers
- Worker type definitions with hourly wages, productivity rates (packages per hour), and training costs
- Current staffing levels at each distribution center by worker type
- Labor costs including regular wages, overtime rates, hiring costs, and termination costs
- Labor productivity metrics defining how many packages each worker type can process per hour
- Workforce mix ratios indicating optimal proportions of each worker type

**We proceed in 2 steps**:
- *Package Volume Forecasting*: Forecast package volumes one week ahead for each distribution center
  - Generate time series forecasts for weekly package volumes at each distribution center
  - Account for seasonality, trends, and regional variations
  - Calculate labor requirements based on forecasted volumes and productivity rates
- *Staffing Optimization*: Optimize workforce allocation and hiring decisions
  - Minimize total labor costs (regular time + overtime + hiring/termination) while meeting service levels
  - Determine optimal staffing levels and hiring/layoff recommendations for each DC and worker type
  - Balance workforce mix ratios and supervisor requirements

## Key Business Outcomes

- **Cost Reduction**: Minimize labor costs while maintaining service levels
- **Improved Planning**: Data-driven workforce planning based on forecasted demand
- **Scalability**: Solution scales to hundreds of distribution centers using distributed processing
- **Operational Efficiency**: Optimal balance of regular time, overtime, and workforce changes
- **Enterprise-Ready**: Uses Unity Catalog for secure, governed data management with three-tier namespace

## Technical Highlights

- **Unity Catalog Integration**: All tables are managed in Unity Catalog with `catalog.schema.table` namespace
- **Managed Tables**: No need to manage cloud storage paths - tables are stored as managed Delta tables
- **Time Series Forecasting**: Holt-Winters seasonal method for package volume predictions
- **Linear Programming**: PuLP library for staffing optimization with multiple constraints
- **Distributed Processing**: Pandas UDFs scale optimization across all distribution centers
- **Delta Lake**: All data stored in Delta format for ACID transactions and time travel

___

Modified from original Supply Chain Optimization accelerator by Max Köhler  
Adapted for Staffing Optimization by Josh Melton

___

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |

## Getting started

Although specific solutions can be downloaded as .dbc archives from our websites, we recommend cloning these repositories onto your databricks environment. Not only will you get access to latest code, but you will be part of a community of experts driving industry best practices and re-usable solutions, influencing our respective industries. 

<img width="500" alt="add_repo" src="https://user-images.githubusercontent.com/4445837/177207338-65135b10-8ccc-4d17-be21-09416c861a76.png">

To start using a solution accelerator in Databricks simply follow these steps: 

1. Clone solution accelerator repository in Databricks using [Databricks Repos](https://www.databricks.com/product/repos)
2. Attach the `RUNME` notebook to any cluster and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. The job configuration is written in the RUNME notebook in json format. 
3. Execute the multi-step-job to see how the pipeline runs. 
4. You might want to modify the samples in the solution accelerator to your need, collaborate with other users and run the code samples against your own data. To do so start by changing the Git remote of your repository  to your organization’s repository vs using our samples repository (learn more). You can now commit and push code, collaborate with other user’s via Git and follow your organization’s processes for code development.

The cost associated with running the accelerator is the user's responsibility.


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
