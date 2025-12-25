# ============================================================
# GOLD LAYER: Transform Silver Tables into Business-Ready Data
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------
# Step 1: Ensure Gold schema exists
# ------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# ------------------------------------------------------------
# Step 2: Load Silver tables
# ------------------------------------------------------------
crm_cust_info = spark.table("silver.crm_cust_info")
erp_cust_az12 = spark.table("silver.erp_cust_az12")
erp_loc_a101 = spark.table("silver.erp_loc_a101")
crm_prd_info = spark.table("silver.crm_prd_info")
erp_px_cat_g1v2 = spark.table("silver.erp_px_cat_g1v2")
crm_sales_details = spark.table("silver.crm_sales_details")

# ------------------------------------------------------------
# Step 3: Create Dimension: dim_customers
# ------------------------------------------------------------
# Join CRM and ERP customer & location data
dim_customers = crm_cust_info.alias("ci") \
    .join(erp_cust_az12.alias("ca"), F.col("ci.cst_key") == F.col("ca.cid"), "left") \
    .join(erp_loc_a101.alias("la"), F.col("ci.cst_key") == F.col("la.cid"), "left") \
    .select(
        "ci.cst_id",
        "ci.cst_key",
        "ci.cst_firstname",
        "ci.cst_lastname",
        "la.cntry",
        "ci.cst_marital_status",
        F.when(F.col("ci.cst_gndr") != "n/a", F.col("ci.cst_gndr"))
         .otherwise(F.coalesce(F.col("ca.gen"), F.lit("n/a"))).alias("gender"),
        F.col("ca.bdate").alias("birthdate"),
        F.col("ci.cst_create_date").alias("create_date")
    )

# Add surrogate key
window_cust = Window.orderBy("cst_id")
dim_customers = dim_customers.withColumn("customer_key", F.row_number().over(window_cust))

# Save as Delta table
dim_customers.write.format("delta").mode("overwrite").saveAsTable("gold.dim_customers")

# ------------------------------------------------------------
# Step 4: Create Dimension: dim_products
# ------------------------------------------------------------
dim_products = crm_prd_info.alias("pn") \
    .join(erp_px_cat_g1v2.alias("pc"), F.col("pn.cat_id") == F.col("pc.id"), "left") \
    .filter(F.col("pn.prd_end_dt").isNull()) \
    .select(
        "pn.prd_id",
        "pn.prd_key",
        "pn.prd_nm",
        "pn.cat_id",
        "pc.cat",
        "pc.subcat",
        "pc.maintenance",
        "pn.prd_cost",
        "pn.prd_line",
        "pn.prd_start_dt"
    )

# Add surrogate key
window_prod = Window.orderBy("prd_start_dt", "prd_key")
dim_products = dim_products.withColumn("product_key", F.row_number().over(window_prod))

# Save as Delta table
dim_products.write.format("delta").mode("overwrite").saveAsTable("gold.dim_products")

# ------------------------------------------------------------
# Step 5: Create Fact Table: fact_sales
# ------------------------------------------------------------
fact_sales = crm_sales_details.alias("sd") \
    .join(dim_products.alias("pr"), F.col("sd.sls_prd_key") == F.col("pr.prd_key"), "left") \
    .join(dim_customers.alias("cu"), F.col("sd.sls_cust_id") == F.col("cu.cst_id"), "left") \
    .select(
        F.col("sd.sls_ord_num").alias("order_number"),
        F.col("pr.product_key"),
        F.col("cu.customer_key"),
        F.col("sd.sls_order_dt").alias("order_date"),
        F.col("sd.sls_ship_dt").alias("shipping_date"),
        F.col("sd.sls_due_dt").alias("due_date"),
        F.col("sd.sls_sales").alias("sales_amount"),
        F.col("sd.sls_quantity").alias("quantity"),
        F.col("sd.sls_price").alias("price")
    )

# Save as Delta table
fact_sales.write.format("delta").mode("overwrite").saveAsTable("gold.fact_sales")

# ------------------------------------------------------------
# Step 6: Display tables to verify
# ------------------------------------------------------------
display(spark.table("gold.dim_customers"))
display(spark.table("gold.dim_products"))
display(spark.table("gold.fact_sales"))

print("✅ Gold Layer tables created successfully!")

