#!/bin/bash
set -e

JARS="/opt/spark/extra-jars/postgresql-42.7.3.jar,/opt/spark/extra-jars/clickhouse-jdbc-0.6.5-all.jar"

echo "=============================================="
echo " BigDataSpark — ETL Pipeline"
echo "=============================================="

echo ""
echo "[1/2] Star Schema ETL: raw data -> PostgreSQL star schema"
spark-submit \
  --master "local[*]" \
  --jars "$JARS" \
  /opt/spark/jobs/etl_star_schema.py

echo ""
echo "[2/2] ClickHouse ETL: star schema -> 6 mart tables"
spark-submit \
  --master "local[*]" \
  --jars "$JARS" \
  /opt/spark/jobs/etl_clickhouse.py

echo ""
echo "=============================================="
echo " All ETL jobs completed successfully!"
echo "=============================================="
