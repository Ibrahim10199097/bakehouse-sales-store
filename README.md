# bakehouse-sales-store
This is a small end-to-end data pipeline I built on Databricks to practice the medallion architecture (Bronze → Silver → Gold → Analytics). It takes a raw sales dataset, cleans it up, and turns it into tables that are actually ready for reporting in Power BI.

## What it does

The data starts as a messy multi-sheet Excel file with sales transactions, customers, products, and regions. I run it through four stages:

**Bronze** – just dumps the raw Excel sheets into Delta tables, no changes. This is the "as-is" copy of the source data.

**Silver** – here's where the cleanup happens. I checked for duplicate rows (found and removed a bunch in the sales table), checked for nulls, and joined everything — sales, customers, products, regions — into one big enriched table. I also added some extra logic here, like customer segments (Champions, At Risk, Lost Customers, etc.) and flags for top-performing vs slow-moving products.

**Gold** – aggregates the enriched data into business-friendly tables: customer summary, product performance, monthly sales trends, and regional performance.

**Analytics** – built 5 SQL views on top of the Gold tables specifically for Power BI, with things like revenue % contribution and rankings already calculated so the dashboard doesn't have to do the heavy lifting.

## Tech used
- Databricks + Unity Catalog
- PySpark / Spark SQL

## Notebooks
- `Bronze_Layer_-_Data_Model_Ingestion`
- `Silver_Layer_-_Data_Transformation`
- `Gold_Layer_-_Business_Aggregates`
- `Analytics_Layer_-_BI_Views`

---
Built by Ibrahim Maher
