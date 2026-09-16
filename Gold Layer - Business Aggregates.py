# Databricks notebook source
# DBTITLE 1,Configuration
# Gold Layer Configuration
# Creates business-ready aggregated tables and views

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define catalogs
silver_catalog = "workspace"
silver_schema = "silver"
gold_catalog = "workspace"
gold_schema = "gold"

# Create gold schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.{gold_schema}")

print(f"Gold Layer Configuration")
print(f"Source: {silver_catalog}.{silver_schema}")
print(f"Target: {gold_catalog}.{gold_schema}")

# COMMAND ----------

# DBTITLE 1,Gold Table 1: Customer Summary
print("\n" + "="*80)
print("CREATING GOLD TABLE: CUSTOMER SUMMARY")
print("="*80 + "\n")

# Load enriched silver table
df = spark.table(f"{silver_catalog}.{silver_schema}.fact_sales_enriched")

# Aggregate customer-level metrics
customer_summary = df.groupBy(
    "customer_key",
    "customer_name",
    "customer_gender",
    "customer_marital_status",
    "customer_segment"
).agg(
    F.count("order_number").alias("total_orders"),
    F.sum("total_sales").alias("lifetime_revenue"),
    F.sum("gross_profit").alias("lifetime_profit"),
    F.avg("total_sales").alias("avg_order_value"),
    F.avg("profit_margin_pct").alias("avg_profit_margin"),
    F.sum("order_quantity").alias("total_items_purchased"),
    F.max("order_date").alias("last_purchase_date"),
    F.min("order_date").alias("first_purchase_date"),
    F.countDistinct("product_category").alias("product_categories_purchased"),
    F.sum(F.when(F.col("high_value_transaction"), 1).otherwise(0)).alias("high_value_orders")
).withColumn(
    "customer_tenure_days",
    F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
)

# Save to Gold
table_name = f"{gold_catalog}.{gold_schema}.customer_summary"
customer_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✓ Created {table_name}")
print(f"✓ Total customers: {customer_summary.count():,}")
display(customer_summary.orderBy(F.desc("lifetime_revenue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Table 2: Product Performance Summary
print("\n" + "="*80)
print("CREATING GOLD TABLE: PRODUCT PERFORMANCE SUMMARY")
print("="*80 + "\n")

# Aggregate product-level metrics
product_summary = df.groupBy(
    "product_key",
    "product_name",
    "product_category",
    "product_subcategory",
    "product_color",
    "abc_classification",
    "performance_status"
).agg(
    F.sum("total_sales").alias("total_revenue"),
    F.sum("gross_profit").alias("total_profit"),
    F.avg("profit_margin_pct").alias("avg_profit_margin"),
    F.sum("order_quantity").alias("units_sold"),
    F.count("order_number").alias("number_of_orders"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.avg("list_price").alias("avg_selling_price"),
    F.max("order_date").alias("last_sale_date")
)

# Save to Gold
table_name = f"{gold_catalog}.{gold_schema}.product_performance_summary"
product_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✓ Created {table_name}")
print(f"✓ Total products: {product_summary.count():,}")
display(product_summary.orderBy(F.desc("total_revenue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Table 3: Monthly Sales Summary
print("\n" + "="*80)
print("CREATING GOLD TABLE: MONTHLY SALES SUMMARY")
print("="*80 + "\n")

# Aggregate monthly sales metrics
monthly_summary = df.groupBy(
    "order_year",
    "order_month",
    "month_num",
    "quarter",
    "season",
    "product_category"
).agg(
    F.sum("total_sales").alias("monthly_revenue"),
    F.sum("gross_profit").alias("monthly_profit"),
    F.avg("profit_margin_pct").alias("avg_profit_margin"),
    F.sum("order_quantity").alias("units_sold"),
    F.count("order_number").alias("total_orders"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.avg("total_sales").alias("avg_order_value")
)

# Calculate month-over-month growth
window_spec = Window.partitionBy("product_category").orderBy("order_year", "month_num")

monthly_summary = monthly_summary.withColumn(
    "previous_month_revenue",
    F.lag("monthly_revenue", 1).over(window_spec)
).withColumn(
    "mom_growth_pct",
    F.round(
        F.when(F.col("previous_month_revenue").isNotNull(),
               ((F.col("monthly_revenue") - F.col("previous_month_revenue")) / F.col("previous_month_revenue")) * 100
        ).otherwise(None),
        2
    )
)

# Save to Gold
table_name = f"{gold_catalog}.{gold_schema}.monthly_sales_summary"
monthly_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✓ Created {table_name}")
print(f"✓ Total months: {monthly_summary.count():,}")
display(monthly_summary.orderBy(F.desc("order_year"), F.desc("month_num")).limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Table 4: Regional Performance Summary
print("\n" + "="*80)
print("CREATING GOLD TABLE: REGIONAL PERFORMANCE SUMMARY")
print("="*80 + "\n")

# Aggregate regional metrics
regional_summary = df.groupBy(
    "country",
    "region",
    "city",
    "product_category"
).agg(
    F.sum("total_sales").alias("regional_revenue"),
    F.sum("gross_profit").alias("regional_profit"),
    F.avg("profit_margin_pct").alias("avg_profit_margin"),
    F.sum("order_quantity").alias("units_sold"),
    F.count("order_number").alias("total_orders"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.avg("total_sales").alias("avg_order_value")
)

# Save to Gold
table_name = f"{gold_catalog}.{gold_schema}.regional_performance_summary"
regional_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✓ Created {table_name}")
print(f"✓ Total regions: {regional_summary.count():,}")
display(regional_summary.orderBy(F.desc("regional_revenue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Layer Summary
print("\n" + "="*80)
print("GOLD LAYER CREATION COMPLETE!")
print("="*80 + "\n")

print("Business-Ready Tables Created:")
print("-" * 80)

tables = [
    f"{gold_catalog}.{gold_schema}.customer_summary",
    f"{gold_catalog}.{gold_schema}.product_performance_summary",
    f"{gold_catalog}.{gold_schema}.monthly_sales_summary",
    f"{gold_catalog}.{gold_schema}.regional_performance_summary"
]

for table in tables:
    row_count = spark.table(table).count()
    print(f"✓ {table}: {row_count:,} rows")

print("\n" + "="*80)
print("Gold layer tables are ready for BI dashboards and reporting!")
print("="*80)

# COMMAND ----------

