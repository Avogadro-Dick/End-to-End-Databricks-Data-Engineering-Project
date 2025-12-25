# ============================================================
# Databricks Notebook: Silver Layer ETL (Bronze → Silver)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# ============================================================
# Helper function: Convert date columns safely
# ============================================================
def safe_to_date(df, col_name, fmt="yyyyMMdd"):
    """
    Converts a column to date using try_to_date to handle invalid values.
    """
    return df.withColumn(col_name, F.to_date(F.expr(f"try_to_date({col_name}, '{fmt}')")))

# ============================================================
# Load and Transform CRM Customer Table
# ============================================================
start_time = time.time()
print(">> Loading silver.crm_cust_info")

crm_cust = spark.table("bronze.crm_cust_info")

# Add row_number window to get latest record per customer
window_cust = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())

silver_crm_cust = (
    crm_cust
    .withColumn("flag_last", F.row_number().over(window_cust))
    .filter(F.col("flag_last") == 1)
    .select(
        "cst_id",
        "cst_key",
        F.trim(F.col("cst_firstname")).alias("cst_firstname"),
        F.trim(F.col("cst_lastname")).alias("cst_lastname"),
        F.when(F.upper(F.trim(F.col("cst_marital_status"))) == "S", "Single")
         .when(F.upper(F.trim(F.col("cst_marital_status"))) == "M", "Married")
         .otherwise("n/a").alias("cst_marital_status"),
        F.when(F.upper(F.trim(F.col("cst_gndr"))) == "F", "Female")
         .when(F.upper(F.trim(F.col("cst_gndr"))) == "M", "Male")
         .otherwise("n/a").alias("cst_gndr"),
        F.to_date("cst_create_date", "yyyy-MM-dd").alias("cst_create_date"),
        F.current_timestamp().alias("dwh_create_date")
    )
)

# Write to Silver Delta table
silver_crm_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.crm_cust_info")

print(f">> silver.crm_cust_info loaded in {time.time() - start_time:.2f} seconds")

# ============================================================
# Load and Transform CRM Product Table
# ============================================================
start_time = time.time()
print(">> Loading silver.crm_prd_info")

crm_prd = spark.table("bronze.crm_prd_info")

silver_crm_prd = (
    crm_prd
    .withColumn("cat_id", F.expr("substring(prd_key, 1, 5)"))
    .withColumn("prd_key", F.expr("substring(prd_key, 7, length(prd_key))"))
    .withColumn("prd_line", F.when(F.upper(F.trim(F.col("prd_line"))) == "M", "Mountain")
                            .when(F.upper(F.trim(F.col("prd_line"))) == "R", "Road")
                            .when(F.upper(F.trim(F.col("prd_line"))) == "S", "Other Sales")
                            .when(F.upper(F.trim(F.col("prd_line"))) == "T", "Touring")
                            .otherwise("n/a"))
    .withColumn("prd_start_dt", F.to_date("prd_start_dt", "yyyy-MM-dd"))
    .withColumn("prd_end_dt", F.to_date("prd_end_dt", "yyyy-MM-dd"))
    .withColumn("dwh_create_date", F.current_timestamp())
    .select(
        "prd_id", "cat_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt", "dwh_create_date"
    )
)

silver_crm_prd.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.crm_prd_info")

print(f">> silver.crm_prd_info loaded in {time.time() - start_time:.2f} seconds")

# ============================================================
# Load and Transform CRM Sales Table
# ============================================================
start_time = time.time()
print(">> Loading silver.crm_sales_details")

crm_sales = spark.table("bronze.crm_sales_details")

silver_crm_sales = (
    crm_sales
    .withColumn("sls_order_dt", F.expr("try_to_date(sls_order_dt, 'yyyyMMdd')"))
    .withColumn("sls_ship_dt", F.expr("try_to_date(sls_ship_dt, 'yyyyMMdd')"))
    .withColumn("sls_due_dt", F.expr("try_to_date(sls_due_dt, 'yyyyMMdd')"))
    .withColumn("sls_sales", F.when((F.col("sls_sales").isNull()) | (F.col("sls_sales") <= 0),
                                    F.col("sls_quantity") * F.abs(F.col("sls_price")))
                              .otherwise(F.col("sls_sales")))
    .withColumn("sls_price", F.when((F.col("sls_price").isNull()) | (F.col("sls_price") <= 0),
                                    F.col("sls_sales") / F.nullif(F.col("sls_quantity"), F.lit(0)))
                              .otherwise(F.col("sls_price")))
    .withColumn("dwh_create_date", F.current_timestamp())
)

silver_crm_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.crm_sales_details")

print(f">> silver.crm_sales_details loaded in {time.time() - start_time:.2f} seconds")

# ============================================================
# Load and Transform ERP Customer Table
# ============================================================
start_time = time.time()
print(">> Loading silver.erp_cust_az12")

erp_cust = spark.table("bronze.erp_cust_az12")

silver_erp_cust = (
    erp_cust
    .withColumn("cid", F.when(F.col("cid").startswith("NAS"), F.expr("substring(cid, 4, length(cid))"))
                          .otherwise(F.col("cid")))
    .withColumn("bdate", F.when(F.col("bdate") > F.current_date(), None)
                          .otherwise(F.col("bdate")))
    .withColumn("gen", F.when(F.upper(F.trim(F.col("gen"))).isin("F", "FEMALE"), "Female")
                        .when(F.upper(F.trim(F.col("gen"))).isin("M", "MALE"), "Male")
                        .otherwise("n/a"))
    .withColumn("dwh_create_date", F.current_timestamp())
)

silver_erp_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.erp_cust_az12")

print(f">> silver.erp_cust_az12 loaded in {time.time() - start_time:.2f} seconds")

# ============================================================
# Load and Transform ERP Location Table
# ============================================================
start_time = time.time()
print(">> Loading silver.erp_loc_a101")

erp_loc = spark.table("bronze.erp_loc_a101")

silver_erp_loc = (
    erp_loc
    .withColumn("cid", F.regexp_replace("cid", "-", ""))
    .withColumn("cntry", F.when(F.trim(F.col("cntry")) == "DE", "Germany")
                         .when(F.trim(F.col("cntry")).isin("US", "USA"), "United States")
                         .when((F.col("cntry") == "") | (F.col("cntry").isNull()), "n/a")
                         .otherwise(F.trim(F.col("cntry"))))
    .withColumn("dwh_create_date", F.current_timestamp())
)

silver_erp_loc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.erp_loc_a101")

print(f">> silver.erp_loc_a101 loaded in {time.time() - start_time:.2f} seconds")

# ============================================================
# Load and Transform ERP Product Category Table
# ============================================================
start_time = time.time()
print(">> Loading silver.erp_px_cat_g1v2")

erp_px = spark.table("bronze.erp_px_cat_g1v2")

silver_erp_px = (
    erp_px
    .withColumn("dwh_create_date", F.current_timestamp())
)

silver_erp_px.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.erp_px_cat_g1v2")

print(f">> silver.erp_px_cat_g1v2 loaded in {time.time() - start_time:.2f} seconds")

# ============================================================
# Display sample data
# ============================================================
display(spark.table("silver.crm_cust_info"))
display(spark.table("silver.crm_prd_info"))
display(spark.table("silver.crm_sales_details"))

