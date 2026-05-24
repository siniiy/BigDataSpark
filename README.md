# BigDataSpark

Анализ больших данных — лабораторная работа №2 — ETL реализованный с помощью Spark

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark — мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

## Задание

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv) в модель данных **звезда** в PostgreSQL, а затем на основе модели данных звезда создать ряд отчётов в ClickHouse.

### Отчёты

1. **Витрина продаж по продуктам** — топ-10 самых продаваемых продуктов, выручка по категориям, рейтинг и отзывы.
2. **Витрина продаж по клиентам** — топ-10 клиентов по сумме покупок, распределение по странам, средний чек.
3. **Витрина продаж по времени** — месячные и годовые тренды, средний размер заказа по месяцам.
4. **Витрина продаж по магазинам** — топ-5 магазинов по выручке, распределение по городам/странам, средний чек.
5. **Витрина продаж по поставщикам** — топ-5 поставщиков по выручке, средняя цена товара, распределение по странам.
6. **Витрина качества продукции** — товары с наивысшим/наименьшим рейтингом, корреляция рейтинга и продаж, самые рецензируемые товары.

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)

---

## Структура репозитория

```
BigDataSpark/
├── исходные данные/          # 10 CSV-файлов с исходными данными (10 000 строк)
├── docker-compose.yml        # PostgreSQL + ClickHouse + Spark
├── sql/
│   ├── postgres/
│   │   ├── 01_create_raw.sql # DDL таблицы mock_data
│   │   └── 02_load_data.sql  # COPY 10 CSV-файлов в PostgreSQL
│   └── clickhouse/
│       └── 01_create_tables.sql  # DDL 6 витрин (MergeTree)
└── spark/
    ├── Dockerfile            # bitnami/spark:3.5 + JDBC-драйверы
    ├── entrypoint.sh         # запускает оба spark-submit последовательно
    └── jobs/
        ├── etl_star_schema.py   # raw → схема «звезда» в PostgreSQL
        └── etl_clickhouse.py    # схема «звезда» → 6 витрин в ClickHouse
```

### Схема «звезда» в PostgreSQL

| Таблица          | Описание                                     |
|------------------|----------------------------------------------|
| `dim_date`       | Измерение времени (год, квартал, месяц и др.)|
| `dim_customer`   | Измерение клиентов                           |
| `dim_seller`     | Измерение продавцов                          |
| `dim_product`    | Измерение продуктов                          |
| `dim_store`      | Измерение магазинов                          |
| `dim_supplier`   | Измерение поставщиков                        |
| `fact_sales`     | Таблица фактов продаж                        |

### Витрины в ClickHouse

| Таблица                   | Описание                          |
|---------------------------|-----------------------------------|
| `mart_product_sales`      | Витрина продаж по продуктам       |
| `mart_customer_sales`     | Витрина продаж по клиентам        |
| `mart_time_sales`         | Витрина продаж по времени         |
| `mart_store_sales`        | Витрина продаж по магазинам       |
| `mart_supplier_sales`     | Витрина продаж по поставщикам     |
| `mart_product_quality`    | Витрина качества продукции        |

---

## Запуск

### Требования

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (с доступом в интернет для скачивания образов и JDBC-JAR-файлов при первой сборке)
- Git

### Шаги

```bash
# 1. Клонировать репозиторий
git clone <repo-url>
cd BigDataSpark

# 2. Собрать образы и запустить все сервисы
docker compose up --build
```

Это единственная команда. Docker Compose автоматически:

1. Запускает **PostgreSQL** → создаёт таблицу `mock_data` → загружает все 10 CSV (10 000 строк). Загрузка занимает до ~2 минут; Spark стартует только после её завершения (`start_period: 120s`).
2. Запускает **ClickHouse** → создаёт 6 пустых витринных таблиц.
3. Собирает и запускает **Spark** (после того, как оба сервиса здоровы):
   - `etl_star_schema.py` — строит схему «звезда» в PostgreSQL (~1–2 мин).
   - `etl_clickhouse.py` — строит 6 витрин в ClickHouse (~1–2 мин).

Spark-контейнер завершит работу после успешного выполнения обоих ETL-джобов (статус `Exited (0)`).

> **Первый запуск** скачивает образы и JDBC-JAR-файлы. Потребуется стабильный интернет; общее время первого `up --build` — около 10–15 минут.

### Ожидаемый вывод Spark-контейнера

```
==============================================
 BigDataSpark — ETL Pipeline
==============================================

[1/2] Star Schema ETL: raw data -> PostgreSQL star schema
  Loaded 10000 rows
  -> dim_date: ...
  -> dim_customer: ...
  ...
  -> fact_sales: 10000 rows written
=== Star Schema ETL complete ===

[2/2] ClickHouse ETL: star schema -> 6 mart tables
  -> bigdata.mart_product_sales: ...
  ...
=== ClickHouse Mart ETL complete ===

==============================================
 All ETL jobs completed successfully!
==============================================
```

---

## Проверка результатов

Все команды ниже используют `docker exec` — работают одинаково в PowerShell, CMD, bash и WSL.

### PostgreSQL — схема «звезда»

```bash
# Количество строк в таблице фактов (ожидается 10 000)
docker exec bigdata_postgres psql -U postgres -d bigdata -c "SELECT COUNT(*) FROM fact_sales;"

# Проверка всех измерений
docker exec bigdata_postgres psql -U postgres -d bigdata -c "SELECT 'dim_customer' t, COUNT(*) n FROM dim_customer UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;"
```

### ClickHouse — витрины

```bash
# Топ-10 самых продаваемых продуктов
docker exec bigdata_clickhouse clickhouse-client --query "SELECT product_name, total_quantity, quantity_rank FROM bigdata.mart_product_sales ORDER BY quantity_rank LIMIT 10"

# Топ-10 клиентов по сумме покупок
docker exec bigdata_clickhouse clickhouse-client --query "SELECT customer_name, country, total_purchases, purchase_rank FROM bigdata.mart_customer_sales ORDER BY purchase_rank LIMIT 10"

# Ежемесячная выручка
docker exec bigdata_clickhouse clickhouse-client --query "SELECT year, month, month_name, total_revenue, order_count FROM bigdata.mart_time_sales ORDER BY year, month"

# Топ-5 магазинов по выручке
docker exec bigdata_clickhouse clickhouse-client --query "SELECT store_name, city, country, total_revenue, revenue_rank FROM bigdata.mart_store_sales ORDER BY revenue_rank LIMIT 5"

# Топ-5 поставщиков по выручке
docker exec bigdata_clickhouse clickhouse-client --query "SELECT supplier_name, country, total_revenue, revenue_rank FROM bigdata.mart_supplier_sales ORDER BY revenue_rank LIMIT 5"

# Продукты по рейтингу (витрина качества)
docker exec bigdata_clickhouse clickhouse-client --query "SELECT product_name, avg_rating, total_reviews, total_sales_quantity, rating_rank FROM bigdata.mart_product_quality ORDER BY rating_rank LIMIT 10"
```

---

## Повторный запуск

Если нужно перезапустить ETL без пересоздания контейнеров PostgreSQL/ClickHouse:

```bash
# Только пересобрать и перезапустить Spark-контейнер
docker compose up --build spark
```

Чтобы сбросить всё с нуля (удалить volumes с данными):

```bash
docker compose down -v
docker compose up --build
```
