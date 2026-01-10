#!/bin/bash
# Download required JAR files for the migration pipeline

JARS_DIR="/home/voltus/Desktop/Analytics/jars"
mkdir -p "$JARS_DIR"
cd "$JARS_DIR"

echo "Downloading JAR dependencies..."

# Iceberg Spark Runtime
wget -nc https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar

# Iceberg AWS Bundle
wget -nc https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

# AWS SDK Bundle
wget -nc https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar

# Hadoop AWS Integration
wget -nc https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget -nc https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar

# MSSQL JDBC Driver
wget -nc https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar

# MySQL Connector (Required for StarRocks JDBC)
wget -nc https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar

echo "All JARs downloaded to $JARS_DIR"
ls -lh
