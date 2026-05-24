FROM apache/spark:3.5.3-scala2.12-java11-python3-ubuntu

# Install wget for JAR downloads (ubuntu base may not include it)
RUN apt-get update -qq && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*

# Download JDBC driver JARs into Spark's extra-jars folder
RUN mkdir -p /opt/spark/extra-jars \
 && wget -nv -O /opt/spark/extra-jars/postgresql-42.7.3.jar \
        "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" \
 && wget -nv -O /opt/spark/extra-jars/clickhouse-jdbc-0.6.5-all.jar \
        "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-all.jar" \
 && ls -lh /opt/spark/extra-jars/

# Copy ETL jobs and startup script
COPY jobs/         /opt/spark/jobs/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
