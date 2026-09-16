# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Analytics Layer - BI Views
# MAGIC %md
# MAGIC # Analytics Layer - Business Intelligence Views
# MAGIC
# MAGIC This notebook creates 5 analytical views on top of the existing Gold Layer for Power BI dashboards and KPI reporting. Each view is optimized for BI consumption with pre-calculated KPIs, NULL handling, and division-by-zero protection.

# COMMAND ----------

# DBTITLE 1,Create Analytics Schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.analytics;
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# DBTITLE 1,View 1: Product Performance
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- View: vw_product_performance
# MAGIC -- Source: workspace.gold.product_performance_summary
# MAGIC -- Grain: One row per product
# MAGIC -- KPIs: Total Revenue, Total Profit, Profit Margin %, Units Sold,
# MAGIC --       Number of Orders, Unique Customers, Avg Selling Price,
# MAGIC --       Avg Order Value, Revenue Contribution %, Product Rank
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.analytics.vw_product_performance AS
# MAGIC SELECT
# MAGIC     product_key,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_subcategory,
# MAGIC     product_color,
# MAGIC     abc_classification,
# MAGIC     performance_status,
# MAGIC     COALESCE(total_revenue, 0)       AS total_revenue,
# MAGIC     COALESCE(total_profit, 0)        AS total_profit,
# MAGIC     COALESCE(avg_profit_margin, 0)  AS avg_profit_margin_pct,
# MAGIC     COALESCE(units_sold, 0)         AS total_quantity_sold,
# MAGIC     COALESCE(number_of_orders, 0)   AS number_of_orders,
# MAGIC     COALESCE(unique_customers, 0)   AS unique_customers,
# MAGIC     COALESCE(avg_selling_price, 0)  AS avg_selling_price,
# MAGIC     ROUND(
# MAGIC         total_revenue / NULLIF(number_of_orders, 0), 2
# MAGIC     )                               AS avg_order_value,
# MAGIC     ROUND(
# MAGIC         total_revenue
# MAGIC         / NULLIF(SUM(total_revenue) OVER (), 0)
# MAGIC         * 100, 2
# MAGIC     )                               AS revenue_contribution_pct,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY total_revenue DESC
# MAGIC     )                               AS product_rank_by_revenue,
# MAGIC     last_sale_date
# MAGIC FROM workspace.gold.product_performance_summary;

# COMMAND ----------

# DBTITLE 1,View 2: Customer Behavior
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- View: vw_customer_behavior
# MAGIC -- Source: workspace.gold.customer_summary
# MAGIC --         workspace.gold.fact_sales_enriched (for region lookup)
# MAGIC -- Grain: One row per customer
# MAGIC -- KPIs: Total Orders, Total Revenue, Total Profit, Avg Order Value,
# MAGIC --       Avg Profit Margin, Total Quantity, First/Last Purchase Date,
# MAGIC --       Purchase Frequency, Revenue Contribution %, Customer Rank,
# MAGIC --       Repeat Customer Flag, High Value Orders, Categories Purchased
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.analytics.vw_customer_behavior AS
# MAGIC WITH customer_region AS (
# MAGIC     SELECT
# MAGIC         customer_key,
# MAGIC         MAX(country) AS country,
# MAGIC         MAX(region)  AS region,
# MAGIC         MAX(city)    AS city
# MAGIC     FROM workspace.gold.fact_sales_enriched
# MAGIC     GROUP BY customer_key
# MAGIC )
# MAGIC SELECT
# MAGIC     c.customer_key,
# MAGIC     c.customer_name,
# MAGIC     c.customer_gender,
# MAGIC     c.customer_marital_status,
# MAGIC     c.customer_segment,
# MAGIC     r.country,
# MAGIC     r.region,
# MAGIC     r.city,
# MAGIC     COALESCE(c.total_orders, 0)              AS total_orders,
# MAGIC     COALESCE(c.lifetime_revenue, 0)         AS total_revenue,
# MAGIC     COALESCE(c.lifetime_profit, 0)          AS total_profit,
# MAGIC     COALESCE(c.avg_order_value, 0)          AS avg_order_value,
# MAGIC     COALESCE(c.avg_profit_margin, 0)        AS avg_profit_margin_pct,
# MAGIC     COALESCE(c.total_items_purchased, 0)    AS total_quantity_purchased,
# MAGIC     COALESCE(c.product_categories_purchased, 0)
# MAGIC                                             AS product_categories_purchased,
# MAGIC     COALESCE(c.high_value_orders, 0)        AS high_value_orders,
# MAGIC     c.first_purchase_date,
# MAGIC     c.last_purchase_date,
# MAGIC     COALESCE(c.customer_tenure_days, 0)     AS customer_tenure_days,
# MAGIC     CASE WHEN c.total_orders > 1 THEN TRUE ELSE FALSE END
# MAGIC                                             AS is_repeat_customer,
# MAGIC     ROUND(
# MAGIC         c.total_orders
# MAGIC         / NULLIF(c.customer_tenure_days, 0)
# MAGIC         * 30, 2
# MAGIC     )                                       AS purchase_frequency_per_month,
# MAGIC     ROUND(
# MAGIC         c.lifetime_revenue
# MAGIC         / NULLIF(SUM(c.lifetime_revenue) OVER (), 0)
# MAGIC         * 100, 2
# MAGIC     )                                       AS customer_revenue_contribution_pct,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY c.lifetime_revenue DESC
# MAGIC     )                                       AS customer_rank_by_revenue
# MAGIC FROM workspace.gold.customer_summary c
# MAGIC LEFT JOIN customer_region r
# MAGIC     ON c.customer_key = r.customer_key;

# COMMAND ----------

# DBTITLE 1,View 3: Daily Analytics
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- View: vw_daily_analytics
# MAGIC -- Source: workspace.gold.fact_sales_enriched
# MAGIC -- Grain: One row per day
# MAGIC -- KPIs: Daily Revenue, Daily Profit, Profit Margin %, Number of Orders,
# MAGIC --       Number of Customers, Quantity Sold, Avg Order Value,
# MAGIC --       Avg Items per Order, Previous Day Revenue, Revenue Growth %
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.analytics.vw_daily_analytics AS
# MAGIC WITH daily_base AS (
# MAGIC     SELECT
# MAGIC         CAST(order_date AS DATE)        AS order_date,
# MAGIC         MAX(order_year)                  AS order_year,
# MAGIC         MAX(order_month)                 AS order_month,
# MAGIC         MAX(quarter)                     AS quarter,
# MAGIC         MAX(month_num)                   AS month_num,
# MAGIC         MAX(day_of_week)                 AS day_of_week,
# MAGIC         MAX(is_weekend)                  AS is_weekend,
# MAGIC         MAX(season)                      AS season,
# MAGIC         SUM(COALESCE(total_sales, 0))    AS daily_revenue,
# MAGIC         SUM(COALESCE(gross_profit, 0))   AS daily_profit,
# MAGIC         SUM(COALESCE(order_quantity, 0)) AS quantity_sold,
# MAGIC         COUNT(DISTINCT order_number)     AS number_of_orders,
# MAGIC         COUNT(DISTINCT customer_key)     AS number_of_customers
# MAGIC     FROM workspace.gold.fact_sales_enriched
# MAGIC     GROUP BY CAST(order_date AS DATE)
# MAGIC ),
# MAGIC daily_with_lag AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         LAG(daily_revenue) OVER (ORDER BY order_date)
# MAGIC             AS previous_day_revenue
# MAGIC     FROM daily_base
# MAGIC )
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     DATE_FORMAT(order_date, 'EEEE')    AS day_name,
# MAGIC     day_of_week,
# MAGIC     is_weekend,
# MAGIC     order_month,
# MAGIC     month_num,
# MAGIC     quarter,
# MAGIC     order_year,
# MAGIC     season,
# MAGIC     daily_revenue,
# MAGIC     daily_profit,
# MAGIC     ROUND(
# MAGIC         daily_profit / NULLIF(daily_revenue, 0) * 100, 2
# MAGIC     )                                  AS profit_margin_pct,
# MAGIC     number_of_orders,
# MAGIC     number_of_customers,
# MAGIC     quantity_sold,
# MAGIC     ROUND(
# MAGIC         daily_revenue / NULLIF(number_of_orders, 0), 2
# MAGIC     )                                  AS avg_order_value,
# MAGIC     ROUND(
# MAGIC         quantity_sold / NULLIF(number_of_orders, 0), 2
# MAGIC     )                                  AS avg_items_per_order,
# MAGIC     previous_day_revenue,
# MAGIC     ROUND(
# MAGIC         (daily_revenue - previous_day_revenue)
# MAGIC         / NULLIF(previous_day_revenue, 0) * 100, 2
# MAGIC     )                                  AS revenue_growth_pct
# MAGIC FROM daily_with_lag
# MAGIC ORDER BY order_date;

# COMMAND ----------

# DBTITLE 1,View 4: Region Analytics
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- View: vw_region_analytics
# MAGIC -- Source: workspace.gold.fact_sales_enriched
# MAGIC -- Grain: One row per country + region + city
# MAGIC -- KPIs: Total Revenue, Total Profit, Profit Margin %, Total Orders,
# MAGIC --       Total Customers, Quantity Sold, Avg Order Value,
# MAGIC --       Revenue per Customer, Revenue Contribution %, Regional Rank
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.analytics.vw_region_analytics AS
# MAGIC WITH region_base AS (
# MAGIC     SELECT
# MAGIC         COALESCE(country, 'Unknown')       AS country,
# MAGIC         COALESCE(region, 'Unknown')        AS region,
# MAGIC         COALESCE(city, 'Unknown')          AS city,
# MAGIC         SUM(COALESCE(total_sales, 0))       AS total_revenue,
# MAGIC         SUM(COALESCE(gross_profit, 0))     AS total_profit,
# MAGIC         COUNT(DISTINCT order_number)       AS total_orders,
# MAGIC         COUNT(DISTINCT customer_key)       AS total_customers,
# MAGIC         SUM(COALESCE(order_quantity, 0))   AS quantity_sold
# MAGIC     FROM workspace.gold.fact_sales_enriched
# MAGIC     GROUP BY country, region, city
# MAGIC )
# MAGIC SELECT
# MAGIC     country,
# MAGIC     region,
# MAGIC     city,
# MAGIC     total_revenue,
# MAGIC     total_profit,
# MAGIC     ROUND(
# MAGIC         total_profit / NULLIF(total_revenue, 0) * 100, 2
# MAGIC     )                                  AS profit_margin_pct,
# MAGIC     total_orders,
# MAGIC     total_customers,
# MAGIC     quantity_sold,
# MAGIC     ROUND(
# MAGIC         total_revenue / NULLIF(total_orders, 0), 2
# MAGIC     )                                  AS avg_order_value,
# MAGIC     ROUND(
# MAGIC         total_revenue / NULLIF(total_customers, 0), 2
# MAGIC     )                                  AS revenue_per_customer,
# MAGIC     ROUND(
# MAGIC         total_revenue
# MAGIC         / NULLIF(SUM(total_revenue) OVER (), 0) * 100, 2
# MAGIC     )                                  AS revenue_contribution_pct,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY total_revenue DESC
# MAGIC     )                                  AS regional_rank_by_revenue
# MAGIC FROM region_base;

# COMMAND ----------

# DBTITLE 1,View 5: Customer Product Breakdown
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- View: vw_customer_product_breakdown
# MAGIC -- Source: workspace.gold.fact_sales_enriched
# MAGIC -- Grain: One row per customer + product combination
# MAGIC -- KPIs: Total Revenue, Total Quantity, Number of Orders, Avg Order Value,
# MAGIC --       Avg Quantity per Order, Customer-Product Revenue Contribution %,
# MAGIC --       Product Revenue Contribution %, Product Rank for Customer,
# MAGIC --       Customer Rank for Product
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.analytics.vw_customer_product_breakdown AS
# MAGIC WITH customer_product_base AS (
# MAGIC     SELECT
# MAGIC         customer_key,
# MAGIC         MAX(customer_name)        AS customer_name,
# MAGIC         MAX(customer_segment)     AS customer_segment,
# MAGIC         MAX(region)               AS region,
# MAGIC         MAX(country)              AS country,
# MAGIC         product_key,
# MAGIC         MAX(product_name)         AS product_name,
# MAGIC         MAX(product_category)      AS product_category,
# MAGIC         MAX(product_subcategory)   AS product_subcategory,
# MAGIC         SUM(COALESCE(total_sales, 0))   AS total_revenue,
# MAGIC         SUM(COALESCE(order_quantity, 0)) AS total_quantity_purchased,
# MAGIC         SUM(COALESCE(gross_profit, 0))  AS total_profit,
# MAGIC         COUNT(DISTINCT order_number)    AS number_of_orders
# MAGIC     FROM workspace.gold.fact_sales_enriched
# MAGIC     GROUP BY customer_key, product_key
# MAGIC )
# MAGIC SELECT
# MAGIC     customer_key,
# MAGIC     customer_name,
# MAGIC     customer_segment,
# MAGIC     region,
# MAGIC     country,
# MAGIC     product_key,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_subcategory,
# MAGIC     total_revenue,
# MAGIC     total_quantity_purchased,
# MAGIC     total_profit,
# MAGIC     number_of_orders,
# MAGIC     ROUND(
# MAGIC         total_revenue / NULLIF(number_of_orders, 0), 2
# MAGIC     )  AS avg_order_value,
# MAGIC     ROUND(
# MAGIC         total_quantity_purchased / NULLIF(number_of_orders, 0), 2
# MAGIC     )  AS avg_quantity_per_order,
# MAGIC     ROUND(
# MAGIC         total_revenue
# MAGIC         / NULLIF(SUM(total_revenue) OVER (PARTITION BY customer_key), 0)
# MAGIC         * 100, 2
# MAGIC     )  AS customer_product_revenue_contribution_pct,
# MAGIC     ROUND(
# MAGIC         total_revenue
# MAGIC         / NULLIF(SUM(total_revenue) OVER (PARTITION BY product_key), 0)
# MAGIC         * 100, 2
# MAGIC     )  AS product_revenue_contribution_pct,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY customer_key ORDER BY total_revenue DESC
# MAGIC     )  AS product_rank_for_customer,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY product_key ORDER BY total_revenue DESC
# MAGIC     )  AS customer_rank_for_product
# MAGIC FROM customer_product_base;

# COMMAND ----------

# DBTITLE 1,Verify All Analytics Views
# MAGIC %sql --name view_verification
# MAGIC SHOW VIEWS IN workspace.analytics;

# COMMAND ----------

# DBTITLE 1,Row Counts per View
# MAGIC %sql --name view_row_counts
# MAGIC SELECT 'vw_product_performance'        AS view_name, COUNT(*) AS row_count FROM workspace.analytics.vw_product_performance
# MAGIC UNION ALL
# MAGIC SELECT 'vw_customer_behavior',         COUNT(*) FROM workspace.analytics.vw_customer_behavior
# MAGIC UNION ALL
# MAGIC SELECT 'vw_daily_analytics',           COUNT(*) FROM workspace.analytics.vw_daily_analytics
# MAGIC UNION ALL
# MAGIC SELECT 'vw_region_analytics',         COUNT(*) FROM workspace.analytics.vw_region_analytics
# MAGIC UNION ALL
# MAGIC SELECT 'vw_customer_product_breakdown', COUNT(*) FROM workspace.analytics.vw_customer_product_breakdown;