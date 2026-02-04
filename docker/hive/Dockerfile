FROM apache/hive:4.0.0

USER root

# Download PostgreSQL JDBC driver
ENV POSTGRES_JDBC_VERSION=42.7.3
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Copy Hive configuration
COPY docker/hive/hive-site.xml /opt/hive/conf/hive-site.xml
COPY docker/hive/core-site.xml /opt/hive/conf/core-site.xml

USER hive
