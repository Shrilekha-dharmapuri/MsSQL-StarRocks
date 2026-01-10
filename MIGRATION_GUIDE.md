# MSSQL to StarRocks Migration Guide via Iceberg & AWS Glue

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Technology Stack & Versions](#technology-stack--versions)
3. [Migration Flow](#migration-flow)
4. [Detailed Component Setup](#detailed-component-setup)
5. [AWS Glue Catalog Integration](#aws-glue-catalog-integration)
6. [Error Prevention & Best Practices](#error-prevention--best-practices)
7. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

### High-Level Architecture
```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌─────────────┐
│   MSSQL     │─────▶│Apache Iceberg│─────▶│  AWS Glue   │─────▶│ StarRocks   │
│  (Source)   │      │  (S3 Layer)  │      │  Catalog    │      │(Destination)│
└─────────────┘      └──────────────┘      └─────────────┘      └─────────────┘
      │                      │                      │                    │
      │                      │                      │                    │
    JDBC              Apache Spark            Metadata Store         JDBC Write
   Extract            Processing                Registry           Bulk Insert
```

### Data Flow Pipeline

1. **Extraction Phase**: MSSQL → Spark DataFrame (JDBC)
2. **Transformation Phase**: Spark DataFrame → Lowercase identifiers + Audit columns
3. **Iceberg Write Phase**: Spark → Iceberg Tables → S3 Storage
4. **Catalog Registration**: Iceberg metadata → AWS Glue Data Catalog
5. **StarRocks Load Phase**: Iceberg → Spark → StarRocks (JDBC Bulk Insert)

---

## Technology Stack & Versions

### Required JAR Dependencies

All JARs are downloaded via `download_jars.sh` to: `/home/voltus/Desktop/Analytics/jars/`

| Component | Version | Purpose |
|-----------|---------|---------|
| **Apache Spark** | 3.5.0 | Distributed data processing engine |
| **Iceberg Runtime** | 1.7.1 | Iceberg table format support |
| **AWS Bundle** | 1.7.1 | AWS Glue Catalog + S3 integration |
| **MSSQL JDBC** | 12.4.2 | MSSQL connectivity |
| **MySQL JDBC** | 8.2.0 | StarRocks connectivity (MySQL protocol) |

---

## Migration Flow

### Phase 1: MSSQL Extraction

```python
# File: app/services/spark_pipeline.py

# MSSQL JDBC read configuration
jdbc_options = {
    "url": f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false",
    "dbtable": f"[{schema_name}].[{table_name}]",
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "50000"
}

df = spark.read.format("jdbc").options(**jdbc_options).load()
```

### Phase 2-4: Processing, Storage, and Loading
The rest of the flow follows the standard Iceberg → AWS Glue Catalog → StarRocks pipeline as implemented in the `app/services/` directory.

---

## Detailed Component Setup

### 1. Environment Configuration (.env)
Refer to `.env.example` in the root directory for all required MSSQL, AWS, and StarRocks variables.

### 2. Schema Mapping (MSSQL → StarRocks)

| MSSQL Type | StarRocks Type | Notes |
|------------|----------------|-------|
| `BIT` | `BOOLEAN` | Boolean |
| `INT` | `INT` | Signed |
| `BIGINT` | `BIGINT` | Signed |
| `NVARCHAR(n)` | `VARCHAR(n*2)` | Unicode support |
| `DATETIME2` | `DATETIME` | Time truncation |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` | GUID as string |

---

## Summary Checklist
- [ ] Run `./download_jars.sh`
- [ ] Configure your `.env` file
- [ ] Test connection with `python test_mssql.py`
- [ ] Start API: `uvicorn app.main:app --host 0.0.0.0 --port 8000`
- [ ] Trigger migration via POST `/migrate/table`

---

**Document Version**: 1.1
**Last Updated**: 2026-01-07
