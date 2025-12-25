# Bronze Layer Initialization Notebook (Unity Catalog)
# ============================================================
# Purpose:
#   - Create all Bronze (raw) tables for the data warehouse
#   - Replace legacy SQL Server DDL with modern PySpark
#   - Use managed Delta tables (no DBFS paths)
# ============================================================

#
# Platform:
#   - Databricks Lakehouse
#   - Unity Catalog enabled
#   - PySpark + Delta Lake
#
# Architecture:
#   - Medallion (Bronze layer)
#
# ============================================================

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DateType, TimestampType
)

# ------------------------------------------------------------
# Set Catalog & Schema (change if needed)
# ------------------------------------------------------------
CATALOG_NAME = "main"     # default Unity Catalog
SCHEMA_NAME  = "bronze"   # medallion layer

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# ------------------------------------------------------------
# Helper Function: Create Managed Bronze Delta Table
# ------------------------------------------------------------
# - Drops table if it exists
# - Creates empty DataFrame with explicit schema
# - Saves as managed Delta table
# ------------------------------------------------------------

def create_bronze_table(table_name: str, schema: StructType):
    # Drop existing table
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    # Create empty DataFrame
    empty_df = spark.createDataFrame([], schema)

    # Write managed Delta table
    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

# ============================================================
# Define Schemas & Create Bronze Tables
# ============================================================

# -----------------------------
# CRM - Customer Information
# -----------------------------
crm_cust_schema = StructType([
    StructField("cst_id", IntegerType(), True),
    StructField("cst_key", StringType(), True),
    StructField("cst_firstname", StringType(), True),
    StructField("cst_lastname", StringType(), True),
    StructField("cst_marital_status", StringType(), True),
    StructField("cst_gndr", StringType(), True),
    StructField("cst_create_date", DateType(), True)
])

create_bronze_table(
    table_name="bronze.crm_cust_info",
    schema=crm_cust_schema
)

# -----------------------------
# CRM - Product Information
# -----------------------------
crm_prd_schema = StructType([
    StructField("prd_id", IntegerType(), True),
    StructField("prd_key", StringType(), True),
    StructField("prd_nm", StringType(), True),
    StructField("prd_cost", IntegerType(), True),
    StructField("prd_line", StringType(), True),
    StructField("prd_start_dt", TimestampType(), True),
    StructField("prd_end_dt", TimestampType(), True)
])

create_bronze_table(
    table_name="bronze.crm_prd_info",
    schema=crm_prd_schema
)

# -----------------------------
# CRM - Sales Details
# -----------------------------
crm_sales_schema = StructType([
    StructField("sls_ord_num", StringType(), True),
    StructField("sls_prd_key", StringType(), True),
    StructField("sls_cust_id", IntegerType(), True),
    StructField("sls_order_dt", IntegerType(), True),
    StructField("sls_ship_dt", IntegerType(), True),
    StructField("sls_due_dt", IntegerType(), True),
    StructField("sls_sales", IntegerType(), True),
    StructField("sls_quantity", IntegerType(), True),
    StructField("sls_price", IntegerType(), True)
])

create_bronze_table(
    table_name="bronze.crm_sales_details",
    schema=crm_sales_schema
)

# -----------------------------
# ERP - Location Data
# -----------------------------
erp_loc_schema = StructType([
    StructField("cid", StringType(), True),
    StructField("cntry", StringType(), True)
])

create_bronze_table(
    table_name="bronze.erp_loc_a101",
    schema=erp_loc_schema
)

# -----------------------------
# ERP - Customer Demographics
# -----------------------------
erp_cust_schema = StructType([
    StructField("cid", StringType(), True),
    StructField("bdate", DateType(), True),
    StructField("gen", StringType(), True)
])

create_bronze_table(
    table_name="bronze.erp_cust_az12",
    schema=erp_cust_schema
)

# -----------------------------
# ERP - Product Category Mapping
# -----------------------------
erp_px_schema = StructType([
    StructField("id", StringType(), True),
    StructField("cat", StringType(), True),
    StructField("subcat", StringType(), True),
    StructField("maintenance", StringType(), True)
])

create_bronze_table(
    table_name="bronze.erp_px_cat_g1v2",
    schema=erp_px_schema
)

# ============================================================
# Completion
# ============================================================

print("✅ Bronze layer tables created successfully using PySpark and Unity Catalog.")







