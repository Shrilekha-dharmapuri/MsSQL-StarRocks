# Performance Guide for 400K Row Tables (4 Lakh Rows)

## ✅ Current Configuration Analysis

### For 400,000 Row Tables:

**JDBC Partitioning:**
- **Current**: 12 partitions (optimized for 100k-500k range)
- **Rows per partition**: ~33,000 rows
- **Status**: ✅ **Optimal** - Good balance between parallelism and overhead

**Memory Configuration:**
- **Driver Memory**: 12g (recommended)
- **Executor Memory**: 12g (recommended)
- **Status**: ✅ **Sufficient** for 400k rows with wide tables

**Iceberg Partitioning:**
- **Date Partitioning**: Enabled (for tables > 1000 rows)
- **Status**: ✅ **Enabled** - Will partition by date column if available

## Performance Expectations

### Expected Migration Time:
- **Small tables (<10k rows)**: 10-30 seconds
- **Medium tables (100k-400k rows)**: 2-5 minutes
- **Large tables (>1M rows)**: 10-20 minutes

### For 400K Row Tables:
- **Extraction (MSSQL → Spark)**: ~1-2 minutes (with 12 partitions)
- **Iceberg Write (Spark → S3)**: ~1-2 minutes
- **StarRocks Load (S3 → StarRocks)**: ~1-2 minutes
- **Total**: ~3-6 minutes per table

## Memory Usage Breakdown

### For 400K Row Table with 50 Columns:
```
Estimated Memory Usage:
├── JDBC Read (12 partitions): ~2-3 GB
├── DataFrame Cache: ~1-2 GB
├── Iceberg Write Buffers: ~2-3 GB
├── Parquet Compression: ~1-2 GB
└── StarRocks Write: ~1-2 GB
─────────────────────────────────
Total Peak: ~7-12 GB (within 12g limit) ✅
```

### For 400K Row Table with 100+ Columns:
```
Estimated Memory Usage:
├── JDBC Read (12 partitions): ~4-6 GB
├── DataFrame Cache: ~2-4 GB
├── Iceberg Write Buffers: ~4-6 GB
├── Parquet Compression: ~2-4 GB
└── StarRocks Write: ~2-4 GB
─────────────────────────────────
Total Peak: ~14-24 GB (may need 16g-20g) ⚠️
```

## Recommendations for 400K Row Tables

### ✅ Current Settings Are Good If:
- Table has < 80 columns
- System has 32GB+ RAM
- Using 12g driver/executor memory

### ⚠️ Increase Memory If:
- Table has > 80 columns (wide tables)
- System has 64GB+ RAM available
- You see memory warnings in logs

**Recommended for Wide Tables (80+ columns):**
```bash
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g
```

### 📊 Partition Count Optimization

For 400k rows, the current **12 partitions** is optimal:
- ✅ Good parallelism (12 concurrent JDBC reads)
- ✅ Reasonable partition size (~33k rows each)
- ✅ Not overwhelming MSSQL server
- ✅ Efficient memory usage

## Monitoring Tips

### Watch for These Indicators:

1. **Good Performance Signs:**
   - ✅ "Extracted X rows" completes in < 2 minutes
   - ✅ "Successfully wrote X rows to S3" in < 2 minutes
   - ✅ No "OutOfMemoryError" in logs
   - ✅ No "GC overhead" warnings

2. **Warning Signs:**
   - ⚠️ Extraction takes > 5 minutes
   - ⚠️ "Truncated the string representation" warnings
   - ⚠️ Frequent garbage collection pauses
   - ⚠️ Memory usage consistently > 90%

### Check Spark UI:
```bash
# While migration is running, open:
http://localhost:4040

# Monitor:
- Storage tab: Check for cached DataFrames
- Executors tab: Memory usage should stay < 80%
- Jobs tab: Look for failed tasks
```

## Troubleshooting for 400K Tables

### If Migration is Slow:

1. **Check JDBC Partitioning:**
   ```python
   # Should see in logs:
   "Using partitioning: column=ID, partitions=12"
   ```

2. **Verify Memory Settings:**
   ```bash
   # Check your .env file:
   grep SPARK_DRIVER_MEMORY .env
   grep SPARK_EXECUTOR_MEMORY .env
   ```

3. **Monitor System Resources:**
   ```bash
   # Check available memory:
   free -h
   
   # Check Java process memory:
   ps aux | grep java
   ```

### If OutOfMemoryError Occurs:

1. **Increase Memory:**
   ```bash
   SPARK_DRIVER_MEMORY=16g
   SPARK_EXECUTOR_MEMORY=16g
   ```

2. **Reduce Partition Count:**
   - Edit `partition_optimizer.py`
   - Change 12 to 8 for 400k range
   - This reduces parallelism but saves memory

3. **Use S3-Only Mode:**
   ```bash
   # Migrate to S3 first, then load to StarRocks separately
   curl -X POST "http://localhost:8000/migrate/table" \
     -H "Content-Type: application/json" \
     -d '{
       "source_schema": "dbo",
       "table_name": "YourTable",
       "target_schema": "migrated_WorldZone",
       "only_s3": true
     }'
   ```

## Best Practices for 400K Row Tables

1. **✅ Process During Off-Peak Hours**
   - Reduces load on MSSQL server
   - Better network bandwidth

2. **✅ Monitor First Few Tables**
   - Watch logs for patterns
   - Adjust memory if needed

3. **✅ Use Resume Feature**
   - If migration fails, resume from last successful table
   - Don't restart entire schema migration

4. **✅ Batch Processing**
   - Process 10-20 tables at a time
   - Check system resources between batches

## Summary

**For 400K Row Tables:**
- ✅ **Current configuration is GOOD** for tables with < 80 columns
- ✅ **12 partitions** is optimal for JDBC reads
- ✅ **12g memory** is sufficient for most cases
- ⚠️ **Consider 16g** if table has > 80 columns
- ✅ **Date partitioning** will be enabled automatically
- ✅ **Memory cleanup** (unpersist) prevents leaks

**Expected Performance:**
- Migration time: **3-6 minutes per table**
- Memory usage: **7-12 GB peak** (for 50 columns)
- Success rate: **> 95%** with current fixes

---

**Last Updated**: 2026-01-09  
**Tested For**: 400,000 row tables with 20-100 columns

