# Memory Fix Guide - OutOfMemoryError Resolution

## Problem Summary

Your migration pipeline was experiencing `java.lang.OutOfMemoryError: Java heap space` when processing the `AGENTSVES` table (9,066 rows, 66 columns). This occurred during the Iceberg/Parquet write operation.

## Root Causes Identified

### 1. **DataFrame Memory Leaks**
- DataFrames were cached but never unpersisted
- Multiple DataFrames (source + Iceberg) held in memory simultaneously
- Memory accumulated across multiple table migrations

### 2. **Iceberg Partitioning Overhead**
- Date-based partitioning creates multiple Parquet writers (one per day)
- Each writer allocates heap memory for compression buffers
- With 66 columns, each partition requires significant memory

### 3. **Insufficient Memory Configuration**
- Default 8GB might not be enough for wide tables (66 columns)
- No memory management optimizations configured
- Parquet compression buffers competing for heap space

## Fixes Applied

### ✅ Fix 1: DataFrame Memory Management
**File**: `app/services/migration_orchestrator.py`

Added `unpersist()` calls after each DataFrame operation:
```python
# After writing to Iceberg
df.unpersist()

# After loading to StarRocks
iceberg_df.unpersist()
```

**Impact**: Frees cached DataFrames immediately after use, preventing memory accumulation.

### ✅ Fix 2: Optimized Iceberg Partitioning
**File**: `app/services/spark_pipeline.py`

- Only partition by date if table has > 1000 rows
- Skip partitioning for small tables to reduce memory overhead
- Prevents unnecessary Parquet writer creation

**Impact**: Reduces memory pressure for small/medium tables.

### ✅ Fix 3: Spark Memory Optimizations
**File**: `app/services/spark_pipeline.py`

Added configurations:
- `spark.sql.adaptive.enabled=true` - Adaptive query execution
- `spark.sql.files.maxPartitionBytes=128MB` - Limits partition size
- `spark.sql.parquet.rowGroupSize=64MB` - Reduces Parquet buffer size
- G1GC garbage collector with string deduplication

**Impact**: Better memory utilization and garbage collection.

## Recommended Configuration

### For Systems with 16GB RAM:
```bash
SPARK_DRIVER_MEMORY=6g
SPARK_EXECUTOR_MEMORY=6g
```

### For Systems with 32GB RAM (Recommended):
```bash
SPARK_DRIVER_MEMORY=12g
SPARK_EXECUTOR_MEMORY=12g
```

### For Systems with 64GB+ RAM:
```bash
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g
```

## How to Apply Fixes

1. **Update your `.env` file** with recommended memory settings:
   ```bash
   SPARK_DRIVER_MEMORY=12g
   SPARK_EXECUTOR_MEMORY=12g
   ```

2. **Restart your FastAPI server** to load new configurations:
   ```bash
   # Stop current server (Ctrl+C)
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

3. **Resume migration** from the failed table:
   ```bash
   curl -X POST "http://localhost:8000/migrate/schema" \
     -H "Content-Type: application/json" \
     -d '{
       "source_schema": "dbo",
       "target_schema": "migrated_WorldZone",
       "resume_from": "AGENTSVES",
       "create_tables": true
     }'
   ```
   
   **Note**: `create_tables: true` is safe to use even when resuming because the DDL uses `CREATE TABLE IF NOT EXISTS`, which won't fail if tables already exist. This ensures tables are created if they weren't created before the failure.

## Monitoring Memory Usage

### Check Spark UI:
- Open `http://localhost:4040` (if Spark is running)
- Monitor "Storage" tab for cached DataFrames
- Check "Executors" tab for memory usage

### Watch for Warnings:
- If you see "Truncated the string representation" - table is very wide
- If you see "OutOfMemoryError" - increase memory settings
- If you see "GC overhead" - reduce memory or optimize partitioning

## Additional Recommendations

1. **For Very Large Tables (>1M rows)**:
   - Consider using `only_s3=true` to migrate in phases
   - Process large tables individually, not in batch

2. **For Wide Tables (>50 columns)**:
   - Increase `SPARK_DRIVER_MEMORY` to 16g+
   - Consider reducing `JDBC_NUM_PARTITIONS` to avoid too many concurrent connections

3. **If Memory Issues Persist**:
   - Migrate tables in smaller batches
   - Use `resume_from` parameter to process incrementally
   - Consider increasing system RAM or using a Spark cluster

## Testing the Fix

After applying fixes, test with a problematic table:

```bash
curl -X POST "http://localhost:8000/migrate/table" \
  -H "Content-Type: application/json" \
  -d '{
    "source_schema": "dbo",
    "table_name": "AGENTSVES",
    "target_schema": "migrated_WorldZone",
    "create_table": false,
    "auto_partition": true
  }'
```

Monitor the logs for:
- ✅ "Unpersisted source DataFrame to free memory"
- ✅ "Unpersisted Iceberg DataFrame to free memory"
- ✅ Successful completion without OutOfMemoryError

---

**Last Updated**: 2026-01-09
**Version**: 1.1

