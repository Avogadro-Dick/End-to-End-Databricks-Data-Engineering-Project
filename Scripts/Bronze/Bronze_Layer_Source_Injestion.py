# ============================================================
# Bronze Layer – Source Ingestion
# GitHub → Requests → Pandas → Spark → Delta
# ============================================================

import requests
import pandas as pd
from io import StringIO
from pyspark.sql.types import *
import time

# ------------------------------------------------------------
# Set database context
# ------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS DataWarehouse")
spark.sql("USE DataWarehouse")

# ------------------------------------------------------------
# Helper Function
# ------------------------------------------------------------
def load_github_csv_to_delta(url: str, table_name: str, schema: StructType):
    """
    Downloads CSV from GitHub into Pandas,
    converts to Spark DataFrame,
    writes as managed Delta table (overwrite + schema)
    """
    print(f">> Loading table: {table_name}")
    start_time = time.time()

    response = requests.get(url)
    response.raise_for_status()

    csv_data = StringIO(response.text)
    pdf = pd.read_csv(csv_data)

    sdf = spark.createDataFrame(pdf, schema=schema)

    sdf.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    end_time = time.time()
    print(f">> Completed in {end_time - start_time:.2f} seconds\n")

# ============================================================
# Schemas (Bronze – raw, minimal typing)
# ============================================================

crm_cust_schema = StructType([
    StructField("cst_id", IntegerType(), True),
    StructField("cst_key", StringType(), True),
    StructField("cst_firstname", StringType(), True),
    StructField("cst_lastname", StringType(), True),
    StructField("cst_marital_status", StringType(), True),
    StructField("cst_gndr", StringType(), True),
    StructField("cst_create_date", StringType(), True)
])

crm_prd_schema = StructType([
    StructField("prd_id", IntegerType(), True),
    StructField("prd_key", StringType(), True),
    StructField("prd_nm", StringType(), True),
    StructField("prd_cost", IntegerType(), True),
    StructField("prd_line", StringType(), True),
    StructField("prd_start_dt", StringType(), True),
    StructField("prd_end_dt", StringType(), True)
])

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

erp_cust_schema = StructType([
    StructField("cid", StringType(), True),
    StructField("bdate", StringType(), True),
    StructField("gen", StringType(), True)
])

erp_loc_schema = StructType([
    StructField("cid", StringType(), True),
    StructField("cntry", StringType(), True)
])

erp_px_schema = StructType([
    StructField("id", StringType(), True),
    StructField("cat", StringType(), True),
    StructField("subcat", StringType(), True),
    StructField("maintenance", StringType(), True)
])

# ============================================================
# GitHub Source URLs (YOUR LINKS)
# ============================================================

crm_cust_csv = "https://raw.githubusercontent.com/Avogadro-Dick/End-to-End-Databricks-Data-Engineering-Project/main/Datasets/sources_crm/cust_info.csv"
crm_prd_csv = "https://raw.githubusercontent.com/Avogadro-Dick/End-to-End-Databricks-Data-Engineering-Project/main/Datasets/sources_crm/prd_info.csv"
crm_sales_csv = "https://raw.githubusercontent.com/Avogadro-Dick/End-to-End-Databricks-Data-Engineering-Project/main/Datasets/sources_crm/sales_details.csv"

erp_cust_csv = "https://raw.githubusercontent.com/Avogadro-Dick/End-to-End-Databricks-Data-Engineering-Project/main/Datasets/sources_erp/CUST_AZ12.csv"
erp_loc_csv = "https://raw.githubusercontent.com/Avogadro-Dick/End-to-End-Databricks-Data-Engineering-Project/main/Datasets/sources_erp/LOC_A101.csv"
erp_px_csv = "https://raw.githubusercontent.com/Avogadro-Dick/End-to-End-Databricks-Data-Engineering-Project/main/Datasets/sources_erp/PX_CAT_G1V2.csv"

# ============================================================
# Load CRM Sources → Bronze
# ============================================================

load_github_csv_to_delta(crm_cust_csv, "bronze.crm_cust_info", crm_cust_schema)
load_github_csv_to_delta(crm_prd_csv, "bronze.crm_prd_info", crm_prd_schema)
load_github_csv_to_delta(crm_sales_csv, "bronze.crm_sales_details", crm_sales_schema)

# ============================================================
# Load ERP Sources → Bronze
# ============================================================

load_github_csv_to_delta(erp_cust_csv, "bronze.erp_cust_az12", erp_cust_schema)
load_github_csv_to_delta(erp_loc_csv, "bronze.erp_loc_a101", erp_loc_schema)
load_github_csv_to_delta(erp_px_csv, "bronze.erp_px_cat_g1v2", erp_px_schema)

# ============================================================
# Verification
# ============================================================

print("✅ Bronze source ingestion complete")

display(spark.table("bronze.crm_cust_info"))
display(spark.table("bronze.crm_prd_info"))
display(spark.table("bronze.crm_sales_details"))

