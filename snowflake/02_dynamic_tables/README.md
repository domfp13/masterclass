# Snowflake Dynamic Tables Tutorial

A simplified guide to building declarative data pipelines using Snowflake Dynamic Tables.

## Overview

This tutorial demonstrates how to build a multi-layered lakehouse architecture (Bronze → Silver → Gold) using Snowflake's native Dynamic Tables feature. Unlike more complex approaches involving Iceberg tables and external catalogs, this tutorial focuses purely on Snowflake's built-in capabilities.

## What You'll Learn

- Create and configure Snowflake databases for a layered architecture
- Implement Dynamic Tables with automated refresh using TARGET_LAG
- Configure incremental refresh modes for efficient data processing
- Build declarative data pipelines that automatically maintain themselves
- Monitor Dynamic Table health and refresh status

## Architecture

```
Bronze Layer (Raw Data)
    ├── de_orders
    ├── de_order_items
    ├── de_products
    └── de_customers
           ↓
Silver Layer (Cleaned & Enriched)
    ├── de_orders_cleaned
    └── de_order_items_enriched
           ↓
Gold Layer (Aggregated Analytics)
    ├── order_summary
    └── sales_summary_trends
```

## Prerequisites

- Snowflake account with permissions to create:
  - Databases and schemas
  - Warehouses
  - Dynamic Tables
- Basic knowledge of SQL
- Familiarity with data warehouse concepts

## Getting Started

### Option 1: Run the Jupyter Notebook

1. Open `dynamic_tables_tutorial.ipynb` in your Jupyter environment
2. Execute cells sequentially from top to bottom
3. Each cell is self-contained and includes explanations

### Option 2: Copy SQL to Snowflake Worksheet

You can copy the SQL code from the notebook cells directly into a Snowflake worksheet and execute them in order.

## Tutorial Structure

1. **Environment Setup**: Create databases, schemas, and warehouses
2. **Bronze Layer**: Create raw data tables and load initial data
3. **Silver Layer**: Build Dynamic Tables for data transformation
4. **Gold Layer**: Create aggregated analytics tables
5. **Testing**: Insert new data and observe automatic propagation
6. **Monitoring**: Check refresh status and data freshness

## Key Features

### Dynamic Tables

Dynamic Tables are declarative - you define the query, and Snowflake automatically:
- Schedules refreshes based on TARGET_LAG
- Processes only new/changed data (when using INCREMENTAL mode)
- Manages dependencies between tables
- Handles refresh orchestration

### TARGET_LAG Configuration

```sql
TARGET_LAG = '1 minute'  -- Near real-time
TARGET_LAG = '1 hour'    -- Standard batch
TARGET_LAG = '1 day'     -- Daily batch
TARGET_LAG = 'DOWNSTREAM' -- Refresh when downstream tables need it
```

### Refresh Modes

- **INCREMENTAL**: Processes only new or changed data (most efficient)
- **FULL**: Reprocesses all data (use for complex aggregations)

## Benefits vs Iceberg Approach

This simplified approach offers several advantages:

| Feature | This Tutorial | Iceberg Version |
|---------|--------------|-----------------|
| Setup Complexity | Simple | Complex |
| External Dependencies | None | AWS Glue, S3, IAM |
| Learning Curve | Gentle | Steep |
| Time to Value | Minutes | Hours |
| Maintenance | Low | Higher |

## Sample Queries

### Check Layer Counts
```sql
SELECT 'BRONZE - de_orders' AS layer_table, COUNT(*) FROM BRONZE_ANALYTICS_DB.PUBLIC.de_orders
UNION ALL
SELECT 'SILVER - de_orders_cleaned', COUNT(*) FROM SILVER_ANALYTICS_DB.PUBLIC.de_orders_cleaned
UNION ALL
SELECT 'GOLD - order_summary', COUNT(*) FROM GOLD_ANALYTICS_DB.PUBLIC.order_summary;
```

### Monitor Dynamic Tables
```sql
SHOW DYNAMIC TABLES;

SELECT name, database_name, target_lag, refresh_mode, scheduling_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name IN ('SILVER_ANALYTICS_DB', 'GOLD_ANALYTICS_DB');
```

## Troubleshooting

### Dynamic Tables Not Refreshing?

1. Check warehouse status: `SHOW WAREHOUSES LIKE 'ANALYTICS_WH';`
2. Verify scheduling state: `SHOW DYNAMIC TABLES;`
3. Check for errors in refresh history:
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
   WHERE state = 'FAILED';
   ```

### Manual Refresh

If needed, you can manually trigger a refresh:
```sql
ALTER DYNAMIC TABLE SILVER_ANALYTICS_DB.PUBLIC.de_orders_cleaned REFRESH;
```

## Best Practices

1. **Start with appropriate TARGET_LAG**: Balance freshness with costs
2. **Use INCREMENTAL mode**: Most efficient for large datasets
3. **Layer your data**: Separate raw, transformed, and aggregated data
4. **Monitor regularly**: Check refresh history and data freshness
5. **Test incremental updates**: Verify data flows correctly through layers

## Resources

- [Snowflake Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Dynamic Tables Best Practices](https://docs.snowflake.com/en/user-guide/dynamic-tables-best-practices)
- [Monitoring Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-tasks-manage)

## Support

For questions or issues:
1. Check the Snowflake documentation linked above
2. Review the troubleshooting section
3. Consult your Snowflake administrator

## License

This tutorial is provided as-is for educational purposes.

---

**Happy Learning!** 🎓

