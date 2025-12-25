# ============================================================
# Databricks Notebook: Create Database and Schemas (Initialization)
# ============================================================

database_name = "DataWarehouse"

# Drop database if it exists (CAUTION: deletes all tables)
spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")

# Create database
spark.sql(f"CREATE DATABASE {database_name}")

# Switch to the database
spark.sql(f"USE {database_name}")

# Logical schema placeholders: bronze, silver, gold
# In Databricks, these are usually just prefixes in table names.
# Optionally, you can create empty tables to indicate schema, but not required.

print(f"Database '{database_name}' created. Use prefixes for schemas: bronze.*, silver.*, gold.*")

