# ============================================================
# Silver Layer Quality Checks - PySpark
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import Window

# -------------------------
# Load Silver tables
# -------------------------
df_cust    = spark.table("silver.crm_cust_info")
df_prd     = spark.table("silver.crm_prd_info")
df_sales   = spark.table("silver.crm_sales_details")
df_erp_cust= spark.table("silver.erp_cust_az12")
df_loc     = spark.table("silver.erp_loc_a101")
df_px      = spark.table("silver.erp_px_cat_g1v2")

# ============================================================
# 1. CRM Customers Checks
# ============================================================

print("=== crm_cust_info: Duplicate or NULL Primary Keys ===")
display(
    df_cust.groupBy("cst_id")
           .count()
           .filter((F.col("count") > 1) | F.col("cst_id").isNull())
)

print("=== crm_cust_info: Unwanted spaces in cst_key ===")
display(
    df_cust.filter(F.col("cst_key") != F.trim(F.col("cst_key")))
)

print("=== crm_cust_info: Distinct marital_status values ===")
display(df_cust.select("cst_marital_status").distinct())

# ============================================================
# 2. CRM Products Checks
# ============================================================

print("=== crm_prd_info: Duplicate or NULL Primary Keys ===")
display(
    df_prd.groupBy("prd_id")
          .count()
          .filter((F.col("count") > 1) | F.col("prd_id").isNull())
)

print("=== crm_prd_info: Unwanted spaces in prd_nm ===")
display(df_prd.filter(F.col("prd_nm") != F.trim(F.col("prd_nm"))))

print("=== crm_prd_info: Invalid or NULL prd_cost ===")
display(df_prd.filter((F.col("prd_cost").isNull()) | (F.col("prd_cost") < 0)))

print("=== crm_prd_info: Distinct prd_line values ===")
display(df_prd.select("prd_line").distinct())

print("=== crm_prd_info: Invalid date orders (prd_end_dt < prd_start_dt) ===")
display(df_prd.filter(F.col("prd_end_dt") < F.col("prd_start_dt")))

# ============================================================
# 3. CRM Sales Details Checks
# ============================================================

print("=== crm_sales_details: Null dates ===")
display(
    df_sales.filter(
        F.col("sls_order_dt").isNull() |
        F.col("sls_ship_dt").isNull() |
        F.col("sls_due_dt").isNull()
    )
)

print("=== crm_sales_details: Invalid date order (order > ship/due) ===")
display(
    df_sales.filter(
        (F.col("sls_order_dt") > F.col("sls_ship_dt")) |
        (F.col("sls_order_dt") > F.col("sls_due_dt"))
    )
)

print("=== crm_sales_details: Sales consistency check (sales != qty * price) ===")
display(
    df_sales.filter(
        (F.col("sls_sales") != F.col("sls_quantity") * F.col("sls_price")) |
        F.col("sls_sales").isNull() |
        F.col("sls_quantity").isNull() |
        F.col("sls_price").isNull() |
        (F.col("sls_sales") <= 0) |
        (F.col("sls_quantity") <= 0) |
        (F.col("sls_price") <= 0)
    )
)

# ============================================================
# 4. ERP Customer Checks
# ============================================================

print("=== erp_cust_az12: Birthdate out of range ===")
display(
    df_erp_cust.filter((F.col("bdate") < F.lit("1924-01-01")) | (F.col("bdate") > F.current_date()))
)

print("=== erp_cust_az12: Distinct gender values ===")
display(df_erp_cust.select("gen").distinct())

# ============================================================
# 5. ERP Location Checks
# ============================================================

print("=== erp_loc_a101: Distinct country codes ===")
display(df_loc.select("cntry").distinct().orderBy("cntry"))

# ============================================================
# 6. ERP Product Categories Checks
# ============================================================

print("=== erp_px_cat_g1v2: Unwanted spaces ===")
display(
    df_px.filter(
        (F.col("cat") != F.trim(F.col("cat"))) |
        (F.col("subcat") != F.trim(F.col("subcat"))) |
        (F.col("maintenance") != F.trim(F.col("maintenance")))
    )
)

print("=== erp_px_cat_g1v2: Distinct maintenance values ===")
display(df_px.select("maintenance").distinct())

