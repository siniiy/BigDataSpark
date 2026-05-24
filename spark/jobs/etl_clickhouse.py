#!/usr/bin/env python3
"""
ETL Job 2: Star Schema (PostgreSQL) -> 6 Report Mart Tables (ClickHouse)

Marts:
  1. mart_product_sales   – product sales analytics
  2. mart_customer_sales  – customer behaviour analytics
  3. mart_time_sales      – time / seasonality analytics
  4. mart_store_sales     – store performance analytics
  5. mart_supplier_sales  – supplier performance analytics
  6. mart_product_quality – product quality analytics
"""

import urllib.request
import urllib.error
from pyspark.sql import SparkSession, DataFrame
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

CH_URL = "jdbc:clickhouse://bigdata_clickhouse:8123/bigdata"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "",
    "socket_timeout": "300000",
}
CH_HTTP = "http://bigdata_clickhouse:8123/"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ETL: Star Schema -> ClickHouse Marts")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def ch_execute(sql: str) -> None:
    """Run a DDL / DML statement on ClickHouse via HTTP API."""
    data = sql.encode("utf-8")
    req = urllib.request.Request(CH_HTTP + "?user=default&password=", data=data)
    try:
        urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse error executing [{sql[:80]}...]: {msg}") from exc


def write_ch(df: DataFrame, table: str) -> None:
    """Truncate the ClickHouse table, then append the DataFrame via JDBC."""
    ch_execute(f"TRUNCATE TABLE IF EXISTS bigdata.{table}")
    count = df.count()
    (
        df.repartition(1)           # single partition -> single INSERT batch
        .write
        .mode("append")
        .option("numPartitions", "1")
        .option("batchsize", "5000")
        .jdbc(CH_URL, f"bigdata.{table}", properties=CH_PROPS)
    )
    print(f"  -> bigdata.{table}: {count} rows written")


def read_pg(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)


# ---------------------------------------------------------------------------
# Mart builders
# ---------------------------------------------------------------------------

def build_mart_product_sales(fact, dim_product) -> DataFrame:
    """
    Витрина продаж по продуктам.
    Columns preserved from product dim: rating, reviews (static attributes).
    """
    w_qty = Window.orderBy(F.desc("total_quantity"))
    w_rev = Window.orderBy(F.desc("total_revenue"))

    dp = dim_product.select(
        "product_key",
        "product_id",
        F.col("name").alias("product_name"),
        "category",
        F.col("rating").cast("double").alias("avg_rating"),
        F.col("reviews").cast("long").alias("review_count"),
    )

    return (
        fact.join(dp, "product_key", "inner")
        .groupBy("product_id", "product_name", "category", "avg_rating", "review_count")
        .agg(
            F.sum("sale_quantity").cast("long").alias("total_quantity"),
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
        )
        .withColumn("quantity_rank", F.rank().over(w_qty).cast("int"))
        .withColumn("revenue_rank",  F.rank().over(w_rev).cast("int"))
        .select(
            F.col("product_id").cast("long"),
            "product_name",
            "category",
            "total_quantity",
            "total_revenue",
            "avg_rating",
            "review_count",
            "quantity_rank",
            "revenue_rank",
        )
        .fillna({"avg_rating": 0.0, "review_count": 0})
    )


def build_mart_customer_sales(fact, dim_customer) -> DataFrame:
    """
    Витрина продаж по клиентам.
    """
    w_pur = Window.orderBy(F.desc("total_purchases"))

    dc = dim_customer.select(
        "customer_key",
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
        "country",
    )

    return (
        fact.join(dc, "customer_key", "inner")
        .groupBy("customer_id", "customer_name", "country")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_purchases"),
            F.count("*").cast("long").alias("order_count"),
            F.avg("sale_total_price").cast("double").alias("avg_check"),
        )
        .withColumn("purchase_rank", F.rank().over(w_pur).cast("int"))
        .select(
            F.col("customer_id").cast("long"),
            "customer_name",
            "country",
            "total_purchases",
            "order_count",
            "avg_check",
            "purchase_rank",
        )
        .fillna({"country": "Unknown", "customer_name": "Unknown"})
    )


def build_mart_time_sales(fact, dim_date) -> DataFrame:
    """
    Витрина продаж по времени.
    """
    dd = dim_date.select("date_key", "year", "month", "month_name")

    return (
        fact.join(dd, "date_key", "inner")
        .groupBy("year", "month", "month_name")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.count("*").cast("long").alias("order_count"),
            F.avg("sale_total_price").cast("double").alias("avg_order_size"),
        )
        .select(
            F.col("year").cast("int"),
            F.col("month").cast("int"),
            "month_name",
            "total_revenue",
            "order_count",
            "avg_order_size",
        )
        .fillna({"month_name": "Unknown"})
        .orderBy("year", "month")
    )


def build_mart_store_sales(fact, dim_store) -> DataFrame:
    """
    Витрина продаж по магазинам.
    fact.store_key == dim_store.store_id
    """
    w_rev = Window.orderBy(F.desc("total_revenue"))

    ds = dim_store.select("store_id", "store_name", "city", "country")

    return (
        fact.join(ds, fact["store_key"] == ds["store_id"], "inner")
        .groupBy("store_id", "store_name", "city", "country")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.count("*").cast("long").alias("order_count"),
            F.avg("sale_total_price").cast("double").alias("avg_check"),
        )
        .withColumn("revenue_rank", F.rank().over(w_rev).cast("int"))
        .select(
            F.col("store_id").cast("long"),
            "store_name",
            "city",
            "country",
            "total_revenue",
            "order_count",
            "avg_check",
            "revenue_rank",
        )
        .fillna({"city": "Unknown", "country": "Unknown", "store_name": "Unknown"})
    )


def build_mart_supplier_sales(fact, dim_supplier) -> DataFrame:
    """
    Витрина продаж по поставщикам.
    fact.supplier_key == dim_supplier.supplier_id
    """
    w_rev = Window.orderBy(F.desc("total_revenue"))

    dsu = dim_supplier.select("supplier_id", "supplier_name", "country")

    return (
        fact.join(dsu, fact["supplier_key"] == dsu["supplier_id"], "inner")
        .groupBy("supplier_id", "supplier_name", "country")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.avg("unit_price").cast("double").alias("avg_product_price"),
            F.count("*").cast("long").alias("order_count"),
        )
        .withColumn("revenue_rank", F.rank().over(w_rev).cast("int"))
        .select(
            F.col("supplier_id").cast("long"),
            "supplier_name",
            "country",
            "total_revenue",
            "avg_product_price",
            "order_count",
            "revenue_rank",
        )
        .fillna({"country": "Unknown", "supplier_name": "Unknown", "avg_product_price": 0.0})
    )


def build_mart_product_quality(fact, dim_product) -> DataFrame:
    """
    Витрина качества продукции.
    Shows rating, reviews, and sales volume together to enable correlation analysis.
    """
    w_rating = Window.orderBy(F.desc("avg_rating"))
    w_review = Window.orderBy(F.desc("total_reviews"))
    w_sales  = Window.orderBy(F.desc("total_sales_quantity"))

    dp = dim_product.select(
        "product_key",
        "product_id",
        F.col("name").alias("product_name"),
        "category",
        F.col("rating").cast("double").alias("avg_rating"),
        F.col("reviews").cast("long").alias("total_reviews"),
    )

    return (
        fact.join(dp, "product_key", "inner")
        .groupBy("product_id", "product_name", "category", "avg_rating", "total_reviews")
        .agg(
            F.sum("sale_quantity").cast("long").alias("total_sales_quantity"),
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
        )
        .withColumn("rating_rank", F.rank().over(w_rating).cast("int"))
        .withColumn("review_rank", F.rank().over(w_review).cast("int"))
        .withColumn("sales_rank",  F.rank().over(w_sales).cast("int"))
        .select(
            F.col("product_id").cast("long"),
            "product_name",
            "category",
            "avg_rating",
            "total_reviews",
            "total_sales_quantity",
            "total_revenue",
            "rating_rank",
            "review_rank",
            "sales_rank",
        )
        .fillna({"avg_rating": 0.0, "total_reviews": 0})
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    print("Reading star schema tables from PostgreSQL...")
    # ------------------------------------------------------------------
    fact         = read_pg(spark, "fact_sales")
    dim_customer = read_pg(spark, "dim_customer")
    dim_product  = read_pg(spark, "dim_product")
    dim_store    = read_pg(spark, "dim_store")
    dim_supplier = read_pg(spark, "dim_supplier")
    dim_date     = read_pg(spark, "dim_date")

    # Cache the smaller dimension tables
    for df in (dim_customer, dim_product, dim_store, dim_supplier, dim_date):
        df.cache()

    fact_rows = fact.count()
    print(f"  fact_sales: {fact_rows} rows")

    # ------------------------------------------------------------------
    print("\nBuilding and writing mart tables to ClickHouse...")
    # ------------------------------------------------------------------

    print("\n[1/6] mart_product_sales")
    write_ch(build_mart_product_sales(fact, dim_product),   "mart_product_sales")

    print("\n[2/6] mart_customer_sales")
    write_ch(build_mart_customer_sales(fact, dim_customer), "mart_customer_sales")

    print("\n[3/6] mart_time_sales")
    write_ch(build_mart_time_sales(fact, dim_date),         "mart_time_sales")

    print("\n[4/6] mart_store_sales")
    write_ch(build_mart_store_sales(fact, dim_store),       "mart_store_sales")

    print("\n[5/6] mart_supplier_sales")
    write_ch(build_mart_supplier_sales(fact, dim_supplier), "mart_supplier_sales")

    print("\n[6/6] mart_product_quality")
    write_ch(build_mart_product_quality(fact, dim_product), "mart_product_quality")

    print("\n=== ClickHouse Mart ETL complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
