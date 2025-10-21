# Transformation Summary: Supply Chain → Staffing Optimization

## Overview
This codebase has been transformed from a **Supply Chain Optimization** solution (optimizing product distribution from plants to stores) to a **Staffing Optimization for Logistics Operations** solution (optimizing workforce allocation across distribution centers for a logistics company like FedEx).

## Key Changes

### 1. Problem Domain Shift
- **From**: Manufacturing plants → Distribution Centers → Stores (product flow)
- **To**: Distribution Centers → Worker Types → Package Volumes (labor optimization)

### 2. Data Model Changes

#### Original Tables
1. `part_level_demand` - Product demand at stores
2. `distribution_center_to_store_mapping_table` - DC to store assignments
3. `transport_cost_table` - Shipping costs from plants to DCs
4. `supply_table` - Maximum supply from each plant

#### New Tables
1. `package_volume` - Package volume time series per distribution center
2. `dc_details` - Distribution center information (region, size, multiplier)
3. `worker_types` - Worker definitions (Package Handlers, Sorters, Loaders, Drivers, Supervisors)
4. `current_staffing` - Current headcount by DC and worker type
5. `labor_costs` - Wages, overtime rates, hiring/termination costs
6. `labor_productivity` - Productivity rates and workforce mix ratios
7. `volume_forecast` - Forecasted package volumes
8. `labor_requirements` - Calculated labor needs based on forecasts
9. `staffing_recommendations` - Optimized staffing levels and hiring/layoff recommendations

### 3. File-by-File Changes

#### `_resources/01-data-generator.py`
**Original Purpose**: Generate synthetic product demand data across stores and DCs

**New Purpose**: Generate synthetic package volume data and workforce/labor data

**Major Changes**:
- Removed: Product categories, stores, plants
- Added: 12 Distribution Centers with regions and sizes
- Added: 5 Worker types with wages, productivity rates, training costs
- Changed time series: From product demand → Package volumes (50k avg per week)
- Added: Current staffing levels, labor costs, productivity metrics
- Parameters: `n_distribution_centers=12`, `n_worker_types=5`

#### `_resources/00-setup.py`
**Changes**:
- Updated database prefix: `supply_chain_optimization` → `staffing_optimization_logistics`
- Updated MLflow experiment name to match

#### `01_Introduction_And_Setup.py`
**Changes**:
- Updated introduction and problem description
- Changed from 3 plants, 5 DCs, 30 products, stores → 12 DCs, 5 regions, 5 worker types
- Updated solution overview to describe volume forecasting and staffing optimization

#### `02_Fine_Grained_Demand_Forecasting.py`
**Original Purpose**: Forecast product demand at store level, aggregate to DC level

**New Purpose**: Forecast package volumes at DC level, calculate labor requirements

**Major Changes**:
- Input changed: `part_level_demand` → `package_volume`
- Forecasting unit changed: (product, store) → (distribution_center)
- Output field changed: `demand` → `forecasted_volume`
- Added labor requirements calculation:
  - Join with `labor_productivity` table
  - Calculate required worker-hours based on forecasted volume
  - Convert to required headcount (40 hrs/week FTE)
- New outputs: `volume_forecast`, `labor_requirements` tables

#### `03_Optimize_Transportation.py` → Staffing Optimization
**Original Purpose**: Minimize transportation costs from plants to DCs using Linear Programming

**New Purpose**: Minimize total labor costs while meeting operational requirements

**Major Changes**:

**Problem Formulation**:
- **Old Objective**: Minimize shipping costs (plant → DC)
- **New Objective**: Minimize labor costs (wages + overtime + hiring + terminations)

**Decision Variables**:
- **Old**: Quantity to ship from each plant to each DC
- **New**: 
  - Number of workers per type
  - Overtime hours per type
  - New hires per type
  - Layoffs per type

**Constraints**:
- **Old**:
  - Supply limits per plant
  - Demand requirements per DC
  - Non-negative integer quantities
  
- **New**:
  - Labor capacity must meet requirements
  - Overtime ≤ 20% of regular hours (8 hrs/worker/week)
  - Workforce change equation: new = current + hires - layoffs
  - Supervisor ratio: 1 per 20 workers
  - Non-negative integer headcounts

**Optimization Function**:
- Completely rewritten `staffing_optimization()` function
- Uses PuLP to solve LP per distribution center
- Returns optimal staffing levels, hiring/layoff recommendations, cost breakdown

**Output Analysis**:
- Summary by worker type (total hires, layoffs, costs)
- Cost breakdown by distribution center
- Saves to `staffing_recommendations` table

#### `README.md`
**Changes**:
- Updated title and description
- Changed problem overview from supply chain to staffing
- Updated business outcomes section
- Added key outcomes: cost reduction, workforce planning, scalability

### 4. Key Parameters

#### Distribution Centers
- **Count**: 12 DCs
- **Regions**: Northeast, Southeast, Midwest, Southwest, West
- **Sizes**: Small (0.7x), Medium (1.0x), Large (1.4x) capacity multipliers

#### Worker Types
| Worker Type | Hourly Wage | Packages/Hour | Training Cost | Mix Ratio |
|-------------|-------------|---------------|---------------|-----------|
| Package Handler | $18.50 | 85 | $500 | 40% |
| Sorter | $19.00 | 120 | $600 | 25% |
| Loader | $18.00 | 95 | $450 | 25% |
| Driver | $24.00 | 45 | $1,200 | 8% |
| Supervisor | $32.00 | N/A | $800 | 2% |

#### Time Series
- **Length**: 104 weeks (2 years)
- **Frequency**: Weekly
- **Volume Range**: 20,000 - 80,000 packages/week per DC

### 5. Business Logic

#### Volume to Labor Conversion
```
Required Hours = Package Volume / Packages per Hour
Required FTE = Required Hours / 40 hours per week
```

#### Cost Components
1. **Regular Time**: Workers × 40 hrs × Regular Wage
2. **Overtime**: OT Hours × Overtime Wage (1.5x)
3. **Hiring**: New Hires × (Training Cost + Recruiting)
4. **Termination**: Layoffs × Termination Cost (~1 week severance)

#### Optimization Constraints
- Meet forecasted labor requirements
- Limit overtime to 20% of regular time
- Maintain workforce mix ratios
- Ensure adequate supervision (1:20 ratio)

### 6. Workflow Execution

The workflow remains a 3-step process:

1. **Setup** (`01_Introduction_And_Setup.py`)
   - Creates database and tables
   - Generates synthetic data

2. **Forecasting** (`02_Fine_Grained_Demand_Forecasting.py`)
   - Forecasts package volumes per DC
   - Calculates labor requirements

3. **Optimization** (`03_Optimize_Transportation.py`)
   - Optimizes staffing levels
   - Generates hiring/layoff recommendations
   - Analyzes cost breakdown

## Files Modified
- ✅ `_resources/01-data-generator.py` (major rewrite)
- ✅ `_resources/00-setup.py` (database name, experiment)
- ✅ `01_Introduction_And_Setup.py` (documentation)
- ✅ `02_Fine_Grained_Demand_Forecasting.py` (major rewrite)
- ✅ `03_Optimize_Transportation.py` (complete rewrite)
- ✅ `README.md` (documentation)
- ⚠️ `RUNME.py` (no changes needed - orchestration only)

## Testing Recommendations

1. **Data Generation**: Run `01_Introduction_And_Setup.py` with `reset_all_data=true`
2. **Verify Tables**: Check that all 9 tables are created successfully
3. **Forecasting**: Run `02_Fine_Grained_Demand_Forecasting.py` and verify forecasts are reasonable
4. **Optimization**: Run `03_Optimize_Transportation.py` and verify:
   - Optimization status = "Optimal"
   - Costs are calculated correctly
   - Hiring/layoff recommendations are sensible
   - Constraints are satisfied

## Next Steps for Productionization

1. **Add Validation**: Data quality checks, forecast accuracy metrics
2. **Tune Parameters**: Adjust productivity rates, cost parameters based on real data
3. **Add Seasonality**: Enhance forecasting to handle peak seasons (holidays, etc.)
4. **Multi-week Planning**: Extend optimization to multi-week horizons
5. **Shift Scheduling**: Add shift-level optimization (morning/evening/night)
6. **Skills Matrix**: Add worker skill requirements and cross-training
7. **Visualization**: Create dashboards for workforce planning
8. **What-if Analysis**: Add scenario planning capabilities

## Database Schema Summary

```
staffing_optimization_logistics
├── package_volume (dc, date, volume)
├── dc_details (dc, region, size, multiplier)
├── worker_types (type, wage, productivity, training_cost)
├── current_staffing (dc, type, headcount)
├── labor_costs (type, regular_wage, ot_wage, hire_cost, term_cost)
├── labor_productivity (dc, type, packages_per_hour, mix_ratio)
├── volume_forecast (dc, date, forecasted_volume)
├── labor_requirements (dc, type, required_hours, required_headcount)
└── staffing_recommendations (dc, type, current, optimal, hires, layoffs, costs)
```

---

**Original Author**: Max Köhler (Supply Chain Optimization)  
**Adapted By**: Josh Melton (Staffing Optimization)  
**Date**: October 2025

