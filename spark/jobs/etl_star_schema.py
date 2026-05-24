#!/usr/bin/env python3
"""
ETL Job 1: Raw data (mock_data) -> Star Schema in PostgreSQL

Star Schema:
  Dimensions: dim_date, dim_customer, dim_seller,
              dim_product, dim_store, dim_supplier
  Fact:       fact_sales
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Connection settings
# ---------------------------------------------------------------------------
PG_URL = "jdbc:postgresql://bigdata_postgres:5432/bigdata"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ETL: Raw -> Star Schema")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def write_pg(df, table: str) -> None:
    """Overwrite a PostgreSQL table with the given DataFrame."""
    count = df.count()
    df.write.mode("overwrite").jdbc(PG_URL, table, properties=PG_PROPS)
    print(f"  -> {table}: {count} rows written")


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def build_dim_date(raw):
    return (
        raw.select(
            F.to_date(F.col("sale_date"), "M/d/yyyy").alias("full_date")
        )
        .distinct()
        .filter(F.col("full_date").isNotNull())
        .withColumn("date_key",    F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year",        F.year("full_date"))
        .withColumn("quarter",     F.quarter("full_date"))
        .withColumn("month",       F.month("full_date"))
        .withColumn("day",         F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("month_name",  F.date_format("full_date", "MMMM"))
        .withColumn("day_name",    F.date_format("full_date", "EEEE"))
    )


def build_dim_customer(raw):
    w = Window.orderBy("customer_id")
    return (
        raw.select(
            F.col("sale_customer_id").alias("customer_id"),
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").cast("int").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("postal_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
        )
        .dropDuplicates(["customer_id"])
        .withColumn("customer_key", F.row_number().over(w))
    )


def build_dim_seller(raw):
    w = Window.orderBy("seller_id")
    return (
        raw.select(
            F.col("sale_seller_id").alias("seller_id"),
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("postal_code"),
        )
        .dropDuplicates(["seller_id"])
        .withColumn("seller_key", F.row_number().over(w))
    )


def build_dim_product(raw):
    w = Window.orderBy("product_id")
    return (
        raw.select(
            F.col("sale_product_id").alias("product_id"),
            F.col("product_name").alias("name"),
            F.col("product_category").alias("category"),
            F.col("product_price").cast("double").alias("price"),
            F.col("product_weight").cast("double").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            F.col("product_rating").cast("double").alias("rating"),
            F.col("product_reviews").cast("int").alias("reviews"),
            F.to_date(F.col("product_release_date"), "M/d/yyyy").alias("release_date"),
            F.to_date(F.col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
            F.col("pet_category"),
        )
        .dropDuplicates(["product_id"])
        .withColumn("product_key", F.row_number().over(w))
    )


def build_dim_store(raw):
    w = Window.orderBy("store_name", F.col("city"))
    return (
        raw.select(
            F.col("store_name"),
            F.col("store_location").alias("location"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_country").alias("country"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
        .dropDuplicates(["store_name", "city", "country"])
        .withColumn("store_id", F.row_number().over(w))
    )


def build_dim_supplier(raw):
    w = Window.orderBy("supplier_name")
    return (
        raw.select(
            F.col("supplier_name"),
            F.col("supplier_contact").alias("contact"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
        )
        .dropDuplicates(["supplier_name", "city", "country"])
        .withColumn("supplier_id", F.row_number().over(w))
    )


# ---------------------------------------------------------------------------
# Fact builder
# ---------------------------------------------------------------------------

def build_fact_sales(raw, dim_date, dim_customer, dim_seller,
                     dim_product, dim_store, dim_supplier):
    """
    Join raw rows back to dimension surrogate keys and
    produce the fact_sales table.
    """
    # Parsed sale date
    raw_d = raw.withColumn(
        "parsed_date", F.to_date(F.col("sale_date"), "M/d/yyyy")
    )

    # Slim key-only views with unambiguous aliases
    c_keys  = dim_customer.select("customer_id",  "customer_key")
    s_keys  = dim_seller.select("seller_id",   "seller_key")
    p_keys  = dim_product.select("product_id",  "product_key")

    # Rename potentially-conflicting columns before joining
    st_keys = dim_store.select(
        "store_id",
        F.col("store_name").alias("_st_name"),
        F.col("city").alias("_st_city"),
        F.col("country").alias("_st_country"),
    )
    su_keys = dim_supplier.select(
        "supplier_id",
        F.col("supplier_name").alias("_su_name"),
        F.col("city").alias("_su_city"),
    )
    d_keys  = dim_date.select(
        "date_key",
        F.col("full_date").alias("_full_date"),
    )

    return (
        raw_d
        # customers
        .join(c_keys,  raw_d["sale_customer_id"] == c_keys["customer_id"],  "left")
        # sellers
        .join(s_keys,  raw_d["sale_seller_id"]   == s_keys["seller_id"],    "left")
        # products
        .join(p_keys,  raw_d["sale_product_id"]  == p_keys["product_id"],   "left")
        # stores
        .join(
            st_keys,
            (raw_d["store_name"]    == st_keys["_st_name"])
            & (raw_d["store_city"]  == st_keys["_st_city"])
            & (raw_d["store_country"] == st_keys["_st_country"]),
            "left",
        )
        # suppliers
        .join(
            su_keys,
            (raw_d["supplier_name"] == su_keys["_su_name"])
            & (raw_d["supplier_city"] == su_keys["_su_city"]),
            "left",
        )
        # dates
        .join(d_keys, raw_d["parsed_date"] == d_keys["_full_date"], "left")
        # select only the fact columns
        .select(
            F.col("customer_key"),
            F.col("seller_key"),
            F.col("product_key"),
            F.col("store_id").alias("store_key"),
            F.col("supplier_id").alias("supplier_key"),
            F.col("date_key"),
            F.col("sale_quantity").cast("int"),
            F.col("sale_total_price").cast("double"),
            F.col("product_price").cast("double").alias("unit_price"),
        )
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading raw data from PostgreSQL (mock_data)...")
    raw = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)
    raw.cache()
    total = raw.count()
    print(f"  Loaded {total} rows")

    # ------------------------------------------------------------------
    print("\nBuilding dimension tables...")
    # ------------------------------------------------------------------
    dim_date     = build_dim_date(raw)
    dim_customer = build_dim_customer(raw)
    dim_seller   = build_dim_seller(raw)
    dim_product  = build_dim_product(raw)
    dim_store    = build_dim_store(raw)
    dim_supplier = build_dim_supplier(raw)

    # Cache so we can reuse for fact building without re-computing
    for df in (dim_date, dim_customer, dim_seller,
               dim_product, dim_store, dim_supplier):
        df.cache()

    # ------------------------------------------------------------------
    print("\nWriting dimension tables to PostgreSQL...")
    # ------------------------------------------------------------------
    write_pg(dim_date,     "dim_date")
    write_pg(dim_customer, "dim_customer")
    write_pg(dim_seller,   "dim_seller")
    write_pg(dim_product,  "dim_product")
    write_pg(dim_store,    "dim_store")
    write_pg(dim_supplier, "dim_supplier")

    # ------------------------------------------------------------------
    print("\nBuilding fact_sales...")
    # ------------------------------------------------------------------
    fact = build_fact_sales(
        raw, dim_date, dim_customer, dim_seller,
        dim_product, dim_store, dim_supplier
    )
    write_pg(fact, "fact_sales")

    raw.unpersist()
    print("\n=== Star Schema ETL complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
