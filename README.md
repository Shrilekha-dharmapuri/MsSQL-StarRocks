# MSSQL to StarRocks Migration Pipeline

Complete data migration solution using Apache Spark, Iceberg table format, and AWS Glue Data Catalog.

## Overview

This pipeline migrates data from **Microsoft SQL Server (MSSQL)** to **StarRocks** analytical database using **Apache Iceberg** as an intermediate storage layer with **AWS Glue Catalog** for metadata management.

### Architecture

```
MSSQL (Source) → Spark/Iceberg (S3) → AWS Glue Catalog → StarRocks (Target)
```

**Key Features:**
- ✅ Parallel extraction using JDBC partitioning
- ✅ Iceberg table format with ACID guarantees
- ✅ AWS Glue Catalog integration for metadata
- ✅ Automatic schema mapping (MSSQL → StarRocks)
- ✅ Intelligent partition optimization
- ✅ REST API for migration management
- ✅ Retry logic and error handling
- ✅ Migration validation and reporting

---

## Prerequisites

### System Requirements
- **Operating System**: Linux (Ubuntu 20.04+ recommended)
- **Java**: OpenJDK 11
- **Python**: 3.10+
- **Memory**: 16 GB RAM minimum (32 GB recommended)
- **Apache Spark**: 3.5.0 with Hadoop 3

### Database Access
- MSSQL source server credentials
- StarRocks target cluster access
- Network connectivity to both databases

### AWS Resources
- AWS Account with access to:
  - S3 bucket for Iceberg data storage
  - AWS Glue Data Catalog
  - IAM credentials (Access Key + Secret Key)

---

## Installation

### Step 1: Clone or Create Project Directory

```bash
cd /home/voltus/Desktop/Analytics
```

### Step 2: Install Java 11

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version  # Should show version 11
```

### Step 3: Download Apache Spark

```bash
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### Step 4: Download JAR Dependencies

Create a script to download all required JARs:

```bash
cd /home/voltus/Desktop/Analytics/jars

# Download Iceberg runtime
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar

# Download Iceberg AWS bundle
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

# Download AWS SDK
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar

# Download Hadoop AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar

# Download MSSQL JDBC driver
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar

# Verify downloads
ls -lh
```

### Step 5: Install Python Dependencies

```bash
cd /home/voltus/Desktop/Analytics
pip install -r requirements.txt
```

### Step 6: Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
nano .env
```

**Required Configuration:**

```bash
# MSSQL Configuration
MSSQL_HOST=your-mssql-server.database.windows.net
MSSQL_PORT=1433
MSSQL_USER=your_username
MSSQL_PASSWORD=your_password
MSSQL_DATABASE=your_database

# AWS Configuration
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
AWS_REGION=ap-south-1
S3_BUCKET=your-bucket-name

# StarRocks Configuration
STARROCKS_HOST=your-starrocks-host
STARROCKS_PORT=9030
STARROCKS_USER=root
STARROCKS_PASSWORD=your_password
```

---

## Usage

### Option 1: REST API (Recommended)

Start the FastAPI server:

```bash
cd /home/voltus/Desktop/Analytics
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**API Documentation**: Open browser to `http://localhost:8000/docs`

#### Migrate Single Table

```bash
curl -X POST "http://localhost:8000/migrate/table" \
  -H "Content-Type: application/json" \
  -d '{
    "source_schema": "dbo",
    "table_name": "customers",
    "target_schema": "analytics",
    "create_table": true,
    "auto_partition": true
  }'
```

**Response:**
```json
{
  "job_id": 1,
  "status": "pending",
  "message": "Migration job 1 started for table customers"
}
```

#### Check Migration Status

```bash
curl http://localhost:8000/migration/status/1
```

#### Migrate Entire Schema

```bash
curl -X POST "http://localhost:8000/migrate/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "source_schema": "dbo",
    "target_schema": "analytics",
    "create_tables": true
  }'
```

### Option 2: Python Script

Create a migration script:

```python
# migrate.py
from app.services.migration_orchestrator import orchestrator

# Migrate single table
result = orchestrator.migrate_table(
    source_schema="dbo",
    table_name="customers",
    target_schema="analytics",
    create_table=True,
    auto_partition=True
)

print(f"Migration completed: {result['status']}")
print(f"Rows migrated: {result['mssql_row_count']}")
print(f"Duration: {result['duration_seconds']} seconds")

# Validate migration
validation = orchestrator.validate_migration(
    source_schema="dbo",
    table_name="customers",
    target_schema="analytics"
)
print(f"Validation: {validation['is_valid']}")
```

Run the script:

```bash
python migrate.py
```

---

## Verification

### 1. Check Spark Initialization

```bash
python -c "from app.services.spark_pipeline import pipeline; spark = pipeline.get_spark_session(); print('Spark initialized successfully')"
```

### 2. Verify AWS Glue Catalog

```bash
aws glue get-databases --region ap-south-1
```

### 3. Check S3 Data

```bash
aws s3 ls s3://your-bucket-name/warehouse/ --recursive
```

### 4. Query via AWS Athena

Go to AWS Athena console and run:

```sql
SELECT COUNT(*) FROM your_database_glue.customers;
```

### 5. Verify StarRocks Data

```bash
mysql -h your-starrocks-host -P 9030 -u root -p

# In StarRocks:
USE analytics;
SELECT COUNT(*) FROM customers;
```

---

## Architecture Details

### Data Flow

1. **Extract**: MSSQL → Spark DataFrame (JDBC with partitioning)
2. **Transform**: Lowercase column names + audit columns
3. **Write**: Spark → Iceberg tables (Parquet files in S3)
4. **Register**: Iceberg metadata → AWS Glue Catalog
5. **Load**: Iceberg → Spark → StarRocks (JDBC bulk insert)

### Type Mapping (MSSQL → StarRocks)

| MSSQL Type | StarRocks Type | Notes |
|------------|----------------|-------|
| `INT` | `INT` | Direct mapping |
| `BIGINT` | `BIGINT` | Direct mapping |
| `NVARCHAR(n)` | `VARCHAR(n*2)` | Unicode support |
| `DATETIME2` | `DATETIME` | Precision truncated |
| `BIT` | `BOOLEAN` | Boolean values |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` | GUID as string |
| `VARBINARY(MAX)` | `VARBINARY` | Binary data |

See `MIGRATION_GUIDE.md` for complete mapping.

---

## Troubleshooting

### Issue: Cannot connect to MSSQL

```bash
# Test MSSQL connection
python -c "import pymssql; conn = pymssql.connect(server='HOST', port=1433, user='USER', password='PASS', database='DB'); print('Connected!')"
```

### Issue: S3 Access Denied

```bash
# Verify AWS credentials
aws s3 ls s3://your-bucket-name/

# Check IAM permissions for S3 and Glue
```

### Issue: Spark Out of Memory

```bash
# Increase memory in .env
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g
```

### Issue: StarRocks Connection Refused

```bash
# Test StarRocks connectivity
mysql -h your-host -P 9030 -u root -p
```

---

## Project Structure

```
/home/voltus/Desktop/Analytics/
├── app/
│   ├── __init__.py
│   ├── config.py                    # Configuration management
│   ├── main.py                      # FastAPI application
│   └── services/
│       ├── __init__.py
│       ├── spark_pipeline.py        # Core Spark + Iceberg pipeline
│       ├── schema_mapper.py         # MSSQL → StarRocks mapping
│       ├── partition_optimizer.py   # Intelligent partitioning
│       └── migration_orchestrator.py # Migration orchestration
├── jars/                            # JAR dependencies
│   ├── iceberg-spark-runtime-3.5_2.12-1.7.1.jar
│   ├── iceberg-aws-bundle-1.7.1.jar
│   ├── aws-java-sdk-bundle-1.12.648.jar
│   ├── hadoop-aws-3.3.4.jar
│   ├── hadoop-common-3.3.4.jar
│   └── mssql-jdbc-12.4.2.jre11.jar
├── .env                             # Environment configuration
├── .env.example                     # Environment template
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
└── MIGRATION_GUIDE.md              # Comprehensive documentation
```

---

## Performance Tips

- **Small tables (<10K rows)**: No partitioning needed
- **Medium tables (10K-1M rows)**: Use 8-16 partitions
- **Large tables (>1M rows)**: Use 20+ partitions
- **Increase `JDBC_FETCH_SIZE`** for faster reads
- **Monitor Spark UI**: `http://localhost:4040`

---

## Support & Documentation

- **Comprehensive Guide**: See `MIGRATION_GUIDE.md`
- **API Documentation**: `http://localhost:8000/docs` (when server running)
- **Apache Iceberg**: https://iceberg.apache.org/
- **AWS Glue**: https://docs.aws.amazon.com/glue/
- **StarRocks**: https://docs.starrocks.io/

---

## License

This project is for internal use.

**Version**: 1.0.0  
**Last Updated**: 2026-01-07
