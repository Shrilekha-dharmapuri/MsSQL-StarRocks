# Memory Configuration Guide for 32GB RAM System

## System Specifications
- **Total RAM**: 32GB
- **Available for Spark**: ~26-28GB (after OS and other processes)
- **Recommended Allocation**: Conservative to avoid swapping

## Recommended Memory Settings

### For Normal Tables (< 50 columns):
```bash
SPARK_DRIVER_MEMORY=12g
SPARK_EXECUTOR_MEMORY=12g
```
**Total Spark Memory**: 24GB  
**Reserved for OS**: 8GB ✅ Safe

### For Wide Tables (50-80 columns) - Like AGENTSVES:
```bash
SPARK_DRIVER_MEMORY=14g
SPARK_EXECUTOR_MEMORY=14g
```
**Total Spark Memory**: 28GB  
**Reserved for OS**: 4GB ⚠️ Minimum, but should work

### For Very Wide Tables (80+ columns):
```bash
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g
```
**Total Spark Memory**: 32GB  
**Reserved for OS**: 0GB ⚠️ **Risky** - May cause swapping

## Memory Breakdown for 32GB System

```
Total RAM: 32GB
├── Operating System: ~4GB
├── Other Processes: ~2GB
├── Spark Driver: 12-16GB
├── Spark Executor: 12-16GB
└── Buffer: 0-4GB
```

## Recommended Configuration for Your Case

Since you're migrating tables with up to 66 columns (like AGENTSVES), use:

```bash
# .env file settings
SPARK_DRIVER_MEMORY=14g
SPARK_EXECUTOR_MEMORY=14g
SPARK_EXECUTOR_CORES=4
SPARK_SQL_SHUFFLE_PARTITIONS=8
```

**Why 14g?**
- ✅ Enough for 66-column tables (AGENTSVES)
- ✅ Leaves 4GB for OS (minimum but acceptable)
- ✅ Won't cause swapping on 32GB system
- ✅ Better than 12g for wide tables
- ✅ Safer than 16g (which would use all RAM)

## Memory Usage Estimation

### For AGENTSVES (9,066 rows, 66 columns):
```
JDBC Read (12 partitions): ~3-4 GB
DataFrame Cache: ~2-3 GB
Iceberg Write Buffers: ~3-4 GB
Parquet Compression: ~2-3 GB
StarRocks Write: ~2-3 GB
─────────────────────────────────
Total Peak: ~12-17 GB
With 14g driver + 14g executor = 28GB available ✅
```

## Monitoring Commands

### Check Current Memory Usage:
```bash
# Total system memory
free -h

# Java/Spark processes
ps aux | grep java | grep -E "spark|driver|executor"

# Detailed memory per process
ps -o pid,vsz,rss,comm -p $(pgrep -f spark)
```

### Watch Memory During Migration:
```bash
# In another terminal, monitor memory:
watch -n 2 'free -h && echo "---" && ps aux | grep java | grep spark | head -5'
```

## If You Still Get OutOfMemoryError

### Option 1: Increase to 15g (Maximum Safe)
```bash
SPARK_DRIVER_MEMORY=15g
SPARK_EXECUTOR_MEMORY=15g
```
⚠️ Leaves only 2GB for OS - monitor closely

### Option 2: Use S3-Only Mode
Migrate to S3 first (less memory), then load to StarRocks separately:
```bash
"only_s3": true
```

### Option 3: Process Wide Tables Individually
For tables with 66+ columns, process them one at a time with maximum memory:
```bash
# Temporarily increase to 16g for problematic tables
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g

# Process one table
curl -X POST "http://localhost:8000/migrate/table" ...

# Then reduce back to 14g for normal tables
```

## Best Practices for 32GB System

1. **✅ Close Other Applications**
   - Close browsers, IDEs, etc. during migration
   - Frees up more memory for Spark

2. **✅ Monitor System Memory**
   - Watch `free -h` during migration
   - If swap is being used, reduce Spark memory

3. **✅ Process in Batches**
   - Migrate 10-20 tables at a time
   - Check memory between batches
   - Restart if memory gets fragmented

4. **✅ Use S3-Only for Large Tables**
   - For tables > 1M rows or > 80 columns
   - Migrate to S3 first, then load to StarRocks separately

## Quick Setup

Update your `.env` file:

```bash
# MSSQL Configuration
MSSQL_HOST=your-host
MSSQL_PORT=1433
MSSQL_USER=your_user
MSSQL_PASSWORD=your_password
MSSQL_DATABASE=your_database

# StarRocks Configuration
STARROCKS_HOST=your-host
STARROCKS_PORT=9030
STARROCKS_USER=root
STARROCKS_PASSWORD=your_password

# AWS Configuration
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=ap-south-1
S3_BUCKET=your-bucket
S3_WAREHOUSE_PATH=DMSPipelineData

# Spark Configuration - OPTIMIZED FOR 32GB RAM
SPARK_MASTER=local[*]
SPARK_APP_NAME=MSSQL-Iceberg-Pipeline
SPARK_DRIVER_MEMORY=14g
SPARK_EXECUTOR_MEMORY=14g
SPARK_EXECUTOR_CORES=4
SPARK_SQL_SHUFFLE_PARTITIONS=8

# Other settings...
```

## Summary

**For 32GB RAM System:**
- ✅ **14g driver + 14g executor** = Best balance for wide tables
- ✅ Leaves 4GB for OS (minimum but acceptable)
- ✅ Should handle 66-column tables like AGENTSVES
- ⚠️ Monitor memory usage during migration
- ⚠️ Close other applications if needed

**Start with 14g, increase to 15g only if needed!**

---

**Last Updated**: 2026-01-09

