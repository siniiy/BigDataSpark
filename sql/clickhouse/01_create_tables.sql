-- ClickHouse init: create the bigdata database and all 6 report mart tables.
-- Executed automatically on first container start.

CREATE DATABASE IF NOT EXISTS bigdata;

-- ============================================================
-- 1. Витрина продаж по продуктам (Product Sales Mart)
--    • Top-10 most sold products  (order by quantity_rank)
--    • Total revenue per category (group by category in queries)
--    • Avg rating & review count per product
-- ============================================================
CREATE TABLE IF NOT EXISTS bigdata.mart_product_sales
(
    product_id      Int64,
    product_name    String,
    category        String,
    total_quantity  Int64,
    total_revenue   Float64,
    avg_rating      Float64,
    review_count    Int64,
    quantity_rank   Int32,
    revenue_rank    Int32
)
ENGINE = MergeTree()
ORDER BY product_id;

-- ============================================================
-- 2. Витрина продаж по клиентам (Customer Sales Mart)
--    • Top-10 customers by total purchases (order by purchase_rank)
--    • Distribution by country
--    • Avg check per customer
-- ============================================================
CREATE TABLE IF NOT EXISTS bigdata.mart_customer_sales
(
    customer_id     Int64,
    customer_name   String,
    country         String,
    total_purchases Float64,
    order_count     Int64,
    avg_check       Float64,
    purchase_rank   Int32
)
ENGINE = MergeTree()
ORDER BY customer_id;

-- ============================================================
-- 3. Витрина продаж по времени (Time Sales Mart)
--    • Monthly & annual sales trends
--    • Revenue comparison across periods
--    • Avg order size by month
-- ============================================================
CREATE TABLE IF NOT EXISTS bigdata.mart_time_sales
(
    year           Int32,
    month          Int32,
    month_name     String,
    total_revenue  Float64,
    order_count    Int64,
    avg_order_size Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

-- ============================================================
-- 4. Витрина продаж по магазинам (Store Sales Mart)
--    • Top-5 stores by revenue  (order by revenue_rank)
--    • Distribution by city / country
--    • Avg check per store
-- ============================================================
CREATE TABLE IF NOT EXISTS bigdata.mart_store_sales
(
    store_id      Int64,
    store_name    String,
    city          String,
    country       String,
    total_revenue Float64,
    order_count   Int64,
    avg_check     Float64,
    revenue_rank  Int32
)
ENGINE = MergeTree()
ORDER BY store_id;

-- ============================================================
-- 5. Витрина продаж по поставщикам (Supplier Sales Mart)
--    • Top-5 suppliers by revenue  (order by revenue_rank)
--    • Avg price of supplier goods
--    • Distribution by supplier country
-- ============================================================
CREATE TABLE IF NOT EXISTS bigdata.mart_supplier_sales
(
    supplier_id       Int64,
    supplier_name     String,
    country           String,
    total_revenue     Float64,
    avg_product_price Float64,
    order_count       Int64,
    revenue_rank      Int32
)
ENGINE = MergeTree()
ORDER BY supplier_id;

-- ============================================================
-- 6. Витрина качества продукции (Product Quality Mart)
--    • Products with highest / lowest rating  (order by rating_rank)
--    • Correlation rating ↔ sales volume (rating + total_sales)
--    • Products with most reviews  (order by review_rank)
-- ============================================================
CREATE TABLE IF NOT EXISTS bigdata.mart_product_quality
(
    product_id            Int64,
    product_name          String,
    category              String,
    avg_rating            Float64,
    total_reviews         Int64,
    total_sales_quantity  Int64,
    total_revenue         Float64,
    rating_rank           Int32,
    review_rank           Int32,
    sales_rank            Int32
)
ENGINE = MergeTree()
ORDER BY product_id;
