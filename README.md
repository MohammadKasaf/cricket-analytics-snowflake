# ODI Cricket Analytics using Snowflake

## Project Overview

This project analyzes One Day International (ODI) cricket match data using Snowflake. It demonstrates how raw cricket data can be loaded, transformed, cleaned, and organized into analytical tables for reporting and performance analysis.

## Tech Stack

- **Cloud Data Warehouse:** Snowflake
- **Query Language:** SQL
- **Version Control:** Git and GitHub
- **Data Format:** JSON

## Project Architecture

The project follows a layered data architecture:

1. **LAND** — Initial data loading and staging.
2. **RAW** — Stores raw cricket match data.
3. **CLEAN** — Contains cleaned and transformed data.
4. **CONSUMPTION** — Contains analytical fact tables, dimension tables, and reporting views.

```text
Cricket JSON Data
       |
       v
     LAND
       |
       v
      RAW
       |
       v
     CLEAN
       |
       v
  CONSUMPTION
       |
       v
 Analytics & Reports
```

## Key Features

- JSON cricket data ingestion
- Data cleaning and transformation using SQL
- Fact and dimension table design
- Match-level analysis
- Team performance analysis
- Batter performance analysis
- Bowler performance analysis
- Analytical views for reporting

## Database Structure

Database: `CRICKET`

Schemas:

- `LAND`
- `RAW`
- `CLEAN`
- `CONSUMPTION`

## Example Analytics

The project is designed to support analysis such as:

- Match scores and results
- Team performance
- Batter runs and performance
- Bowler wickets and bowling performance
- Match and venue statistics

## Getting Started

1. Create the `CRICKET` database in Snowflake.
2. Create the required schemas and tables.
3. Load the source JSON match data.
4. Run the SQL transformations in the correct order.
5. Query the consumption tables and analytical views.

## Current Project Status

This is a work in progress. The current dataset includes a match between Bangladesh and New Zealand from the 2023 ODI Cricket World Cup. Further matches and additional data quality checks can be added as the project develops.

## Future Improvements

- Load multiple ODI matches.
- Automate the data pipeline.
- Add data quality checks and audit logging.
- Build a dashboard for cricket analytics.
- Expand batter, bowler, and team performance metrics.

## Author

Mohammad Kasaf

---

**Note:** This project is intended for educational purposes and demonstrates SQL-based data engineering and analytics using Snowflake.
