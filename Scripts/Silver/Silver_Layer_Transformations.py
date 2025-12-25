# ============================================================
# Silver Layer Transformation (Bronze → Silver)
# Handles invalid dates using try_to_date
# Ensures the 'silver' schema exists before writing tables
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 0️⃣ Create Silver Schema if not exists
# ============================================================
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("USE silver")

# ============================================================
# 1️⃣ Silver CRM Customer Info
# ============================================================
silver_crm_cust_info = (
    spark.table("bronze.crm_cust_info")
    .select(
        F.col("cst_id").cast("int"),
        F.col("cst_key"),
        F.col("cst_firstname"),
        F.col("cst_lastname"),
        F.col("cst_marital_status"),
        F.col("cst_gndr"),
        F.expr("try_to_date(cst_create_date, 'yyyy-MM-dd')").alias("cst_create_date"),
        F.current_timestamp().alias("dwh_create_date")
    )
    .dropDuplicates(["cst_id"])
)

silver_crm_cust_info.write.format("delta").mode("overwrite").saveAsTable("silver.crm_cust_info")
display(silver_crm_cust_info)


# ============================================================
# 2️⃣ Silver CRM Product Info
# ============================================================
silver_crm_prd_info = (
    spark.table("bronze.crm_prd_info")
    .select(
        F.col("prd_id").cast("int"),
        F.split("prd_key", "-").getItem(0).alias("cat_id"),
        F.col("prd_key"),
        F.col("prd_nm"),
        F.col("prd_cost").cast("int"),
        F.col("prd_line"),
        F.expr("try_to_date(prd_start_dt, 'yyyy-MM-dd')").alias("prd_start_dt"),
        F.expr("try_to_date(prd_end_dt, 'yyyy-MM-dd')").alias("prd_end_dt"),
        F.current_timestamp().alias("dwh_create_date")
    )
    .dropDuplicates(["prd_id"])
)

silver_crm_prd_info.write.format("delta").mode("overwrite").saveAsTable("silver.crm_prd_info")
display(silver_crm_prd_info)


# ============================================================
# 3️⃣ Silver CRM Sales Details
# ============================================================
silver_crm_sales_details = (
    spark.table("bronze.crm_sales_details")
    .select(
        F.col("sls_ord_num"),
        F.col("sls_prd_key"),
        F.col("sls_cust_id").cast("int"),
        F.expr("try_to_date(cast(sls_order_dt as string), 'yyyyMMdd')").alias("sls_order_dt"),
        F.expr("try_to_date(cast(sls_ship_dt as string), 'yyyyMMdd')").alias("sls_ship_dt"),
        F.expr("try_to_date(cast(sls_due_dt as string), 'yyyyMMdd')").alias("sls_due_dt"),
        F.col("sls_sales").cast("int"),
        F.col("sls_quantity").cast("int"),
        F.col("sls_price").cast("int"),
        F.current_timestamp().alias("dwh_create_date")
    )
)

silver_crm_sales_details.write.format("delta").mode("overwrite").saveAsTable("silver.crm_sales_details")
display(silver_crm_sales_details)


# ============================================================
# 4️⃣ Silver ERP Location
# ============================================================
silver_erp_loc = (
    spark.table("bronze.erp_loc_a101")
    .select(
        F.col("cid"),
        F.col("cntry"),
        F.current_timestamp().alias("dwh_create_date")
    )
)

silver_erp_loc.write.format("delta").mode("overwrite").saveAsTable("silver.erp_loc_a101")
display(silver_erp_loc)


# ============================================================
# 5️⃣ Silver ERP Customer AZ12
# ============================================================
silver_erp_cust = (
    spark.table("bronze.erp_cust_az12")
    .select(
        F.col("cid"),
        F.expr("try_to_date(bdate, 'yyyy-MM-dd')").alias("bdate"),
        F.col("gen"),
        F.current_timestamp().alias("dwh_create_date")
    )
)

silver_erp_cust.write.format("delta").mode("overwrite").saveAsTable("silver.erp_cust_az12")
display(silver_erp_cust)


# ============================================================
# 6️⃣ Silver ERP PX Category
# ============================================================
silver_erp_px = (
    spark.table("bronze.erp_px_cat_g1v2")
    .select(
        F.col("id"),
        F.col("cat"),
        F.col("subcat"),
        F.col("maintenance"),
        F.current_timestamp().alias("dwh_create_date")
    )
)

silver_erp_px.write.format("delta").mode("overwrite").saveAsTable("silver.erp_px_cat_g1v2")
display(silver_erp_px)


print("✅ Silver layer tables created successfully: crm_cust_info, crm_prd_info, crm_sales_details, erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2")

