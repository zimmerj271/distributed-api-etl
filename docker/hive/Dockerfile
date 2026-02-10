FROM apache/hive:4.0.0

USER root

# Download PostgreSQL JDBC driver
ENV POSTGRES_JDBC_VERSION=42.7.3
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Download Hadoop AWS JARs for S3A support
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Copy Hive configuration
COPY docker/hive/hive-site.xml /opt/hive/conf/hive-site.xml
COPY docker/hive/core-site.xml /opt/hive/conf/core-site.xml

USER hive
