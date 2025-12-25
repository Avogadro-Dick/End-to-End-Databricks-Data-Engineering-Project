# ============================================================
# Gold Layer Quality Checks - PySpark
# ============================================================

from pyspark.sql import functions as F

# -------------------------
# Load Gold tables
# -------------------------
df_customers = spark.table("gold.dim_customers")
df_products  = spark.table("gold.dim_products")
df_sales     = spark.table("gold.fact_sales")

# ============================================================
# 1. dim_customers: Uniqueness Check
# ============================================================

print("=== dim_customers: Duplicate customer_key ===")
display(
    df_customers.groupBy("customer_key")
                .count()
                .filter(F.col("count") > 1)
)

# ============================================================
# 2. dim_products: Uniqueness Check
# ============================================================

print("=== dim_products: Duplicate product_key ===")
display(
    df_products.groupBy("product_key")
               .count()
               .filter(F.col("count") > 1)
)

# ============================================================
# 3. fact_sales: Referential Integrity with Dimensions
# ============================================================

print("=== fact_sales: Missing references in dimensions ===")
df_sales_check = df_sales.alias("f") \
    .join(df_customers.alias("c"), F.col("f.customer_key") == F.col("c.customer_key"), "left") \
    .join(df_products.alias("p"), F.col("f.product_key") == F.col("p.product_key"), "left") \
    .filter(F.col("c.customer_key").isNull() | F.col("p.product_key").isNull())

display(df_sales_check)

# ============================================================
# 4. fact_sales: Sales > 0 and Quantity > 0
# ============================================================

print("=== fact_sales: Check for invalid sales or quantity ===")
display(
    df_sales.filter(
        (F.col("sales_amount") <= 0) |
        (F.col("quantity") <= 0)
    )
)

# ============================================================
# 5. Optional: Summary of Gold Data
# ============================================================

print("=== Summary Counts ===")
print("dim_customers count:", df_customers.count())
print("dim_products count:", df_products.count())
print("fact_sales count:", df_sales.count())

