# Airflow Daily Sales Revenue Pipeline

## Summary
Built an automated data pipeline using Apache Airflow to process and analyze daily sales revenue from a PostgreSQL database.

## Key Features
- Automated ETL workflow using Airflow DAG
- Data extraction from PostgreSQL using SQL
- Revenue calculation using Python (pandas)
- Time series visualization using matplotlib
- Daily scheduled execution

## Pipeline Flow
Extract → Transform → Visualize

## Implementation
- Extract: Query sales data from PostgreSQL
- Transform: Compute daily revenue (quantity × unit_price) and aggregate by date
- Load/Output: Save results to CSV and generate a plot

## Tools & Technologies
- Apache Airflow
- PostgreSQL
- Python (pandas, matplotlib)

## Output
- Daily revenue dataset (CSV)
- Time series revenue plot


## Next Steps
- Store results directly in a database instead of CSV
- Add logging and monitoring
- Integrate with a dashboard (e.g., Power BI)
