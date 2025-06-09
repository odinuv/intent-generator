# Sample Data for Intent Analysis Script

This directory contains sample CSV files that can be used to test the intent analysis script without requiring a live Snowflake connection.

## Files Included

1. **kbc_component_configuration_version.csv** - Configuration changes and versions
2. **kbc_component_configuration_row_version.csv** - Configuration row changes  
3. **kbc_job.csv** - Job executions with success/failure status
4. **kbc_table_event.csv** - Table-related events (creation, preview, export, errors)

## Sample Data Story

The sample data represents a user session where:

1. **09:15-09:59** - User sets up MySQL data extraction for customers, orders, and products tables
2. **10:30-10:48** - User configures Snowflake writer and successfully loads data
3. **11:00-11:37** - User creates a SQL transformation with initial error, then fixes it successfully
4. **14:15-15:05** - User attempts MongoDB extraction but repeatedly fails due to version incompatibility

This creates a realistic scenario with:
- **Partial Success** - MySQL and Snowflake components work, MongoDB fails
- **Troubleshooting** - SQL error that gets fixed, multiple MongoDB retry attempts
- **Mixed Intent Fulfillment** - Some goals achieved, others not

## How to Use

To test with this data, you would need to modify the SnowflakeClient to read from these CSV files instead of querying Snowflake. The data structure matches exactly what the real Snowflake queries would return.

## Session Parameters

- **Token ID**: `155991_kbc-eu-central-1`
- **Project ID**: `12345_kbc-eu-central-1`
- **Date Range**: December 2, 2024 (09:15 - 15:05)

## Expected Analysis Results

The script should identify this as a single session that includes:
- Primary Goal: "ETL/ELT pipeline setup/Data export/sharing"
- Development Stage: "Creating new use cases"
- Intent Tags: ["data-extraction", "database-source", "etl", "snowflake"]
- Fulfillment: "Partial Success"
- Summary: "I want to extract data from MySQL and MongoDB databases and load it into Snowflake for analysis." 