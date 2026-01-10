# Spark Session Crash Fix - Connection Refused After OOM

## Problem Identified

Your logs show a **cascading failure**:

1. **OutOfMemoryError** → Spark JVM crashes
2. **Retry Attempt 2** → Tries to use dead Spark session
3. **Py4JNetworkError** → "Answer from Java side is empty" (JVM is dead)
4. **Connection Refused** → Can't reconnect because JVM process is hanging

## Root Cause

When Spark crashes due to OutOfMemoryError:
- The Java Virtual Machine (JVM) process dies
- The Python connection (Py4J) to Spark is broken
- Retry attempts try to use the dead session
- New session can't start because ports/resources are still locked

## Fixes Applied

### ✅ Fix 1: Smart Error Detection
**File**: `app/services/migration_orchestrator.py`

Now detects Spark crashes:
- `OutOfMemoryError`
- `Connection refused`
- `Py4JNetworkError`
- `Answer from Java side is empty`

### ✅ Fix 2: Force Spark Session Cleanup
When crash is detected:
- Immediately closes the dead Spark session
- Clears the session reference
- Allows clean reinitialization on retry

### ✅ Fix 3: Longer Retry Delay for OOM
- Normal errors: 5 seconds delay
- OOM/Connection errors: **10 seconds delay** (2x)
- Gives JVM time to fully die and release resources

### ✅ Fix 4: Better Session Cleanup
**File**: `app/services/spark_pipeline.py`

- Improved error handling in `get_spark_session()`
- Better cleanup in `close()` method
- Handles exceptions during shutdown gracefully

## How to Use

### Step 1: Increase Memory Settings

**CRITICAL**: The OutOfMemoryError means you need more memory!

Update your `.env` file:

```bash
# For tables with 66+ columns (like AGENTSVES)
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g

# If you have 64GB+ RAM available:
SPARK_DRIVER_MEMORY=20g
SPARK_EXECUTOR_MEMORY=20g
```

### Step 2: Restart FastAPI Server

```bash
# Stop current server (Ctrl+C)
# Then restart:
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Step 3: Resume Migration

The retry logic will now:
1. ✅ Detect the OOM error
2. ✅ Clean up the dead Spark session
3. ✅ Wait 10 seconds (instead of 5)
4. ✅ Reinitialize Spark with fresh session
5. ✅ Retry the migration

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

## What Changed in the Code

### Before (Problem):
```python
except Exception as e:
    # Just retries without cleaning up dead session
    time.sleep(5)  # Too short for JVM to die
    # Tries to use dead Spark session → Connection refused
```

### After (Fixed):
```python
except Exception as e:
    # Detects OOM/connection errors
    if is_oom_or_connection_error:
        pipeline.close()  # Force cleanup
        time.sleep(10)    # Longer delay
    # Fresh Spark session on retry → Works!
```

## Monitoring

### Watch for These Log Messages:

**Good Signs:**
```
✅ "Detected Spark session crash. Forcing cleanup..."
✅ "Spark session closed. Will reinitialize on retry."
✅ "Retrying in 10 seconds..."
✅ "Initializing Spark session with Iceberg and AWS Glue integration..."
✅ "Spark session initialized successfully"
```

**Bad Signs:**
```
❌ "Connection refused" (should be handled now)
❌ "Answer from Java side is empty" (should be handled now)
❌ Still getting OutOfMemoryError (need more memory!)
```

## If Still Failing

### Option 1: Check for Hanging Java Processes

```bash
# Check if Spark Java processes are still running
ps aux | grep java | grep spark

# If found, kill them:
pkill -f "org.apache.spark"

# Then restart migration
```

### Option 2: Use S3-Only Mode First

Migrate to S3 first (less memory intensive), then load to StarRocks separately:

```bash
# Step 1: Migrate to S3 only
curl -X POST "http://localhost:8000/migrate/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "source_schema": "dbo",
    "target_schema": "migrated_WorldZone",
    "resume_from": "AGENTSVES",
    "only_s3": true
  }'

# Step 2: After S3 migration completes, load to StarRocks
# (You'll need to implement a separate endpoint for this, or do it manually)
```

### Option 3: Process Problematic Tables Individually

For very wide tables (66+ columns), process them one at a time:

```bash
curl -X POST "http://localhost:8000/migrate/table" \
  -H "Content-Type: application/json" \
  -d '{
    "source_schema": "dbo",
    "table_name": "AGENTSVES",
    "target_schema": "migrated_WorldZone",
    "create_table": true,
    "auto_partition": true
  }'
```

## Summary

**The Fix:**
- ✅ Detects Spark crashes automatically
- ✅ Cleans up dead sessions properly
- ✅ Longer retry delay for OOM errors
- ✅ Better error handling

**You Still Need To:**
- ⚠️ **Increase memory to 16g-20g** for wide tables
- ⚠️ **Restart the server** to load new settings
- ⚠️ **Resume migration** from AGENTSVES

The retry logic will now handle Spark crashes gracefully, but you still need sufficient memory to prevent the OOM error in the first place!

---

**Last Updated**: 2026-01-09

