#!/usr/bin/env python3
"""
Ultra-conservative S3 to StarRocks loader
Designed for resource-constrained StarRocks servers (4GB RAM)
Uses very small batches and single partition to avoid backend crashes
"""

import sys
import time
import mysql.connector
from app.services.spark_pipeline import pipeline
from app.config import settings

def set_conservative_timeouts():
    """Set conservative timeouts"""
    print("\n⏱️  Setting StarRocks timeouts...")
    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            connect_timeout=30
        )
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL net_read_timeout = 7200")
        cursor.execute("SET GLOBAL net_write_timeout = 7200")
        cursor.execute("SET GLOBAL query_timeout = 14400")
        cursor.close()
        conn.close()
        print("✅ Timeouts set")
    except Exception as e:
        print(f"⚠️  Could not set timeouts: {e}")

def get_row_count(table_name: str, target_schema: str) -> int:
    """Get row count from StarRocks"""
    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=target_schema,
            connect_timeout=30
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{target_schema}`.`{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except:
        return -1

def load_table_conservative(
    table_name: str,
    target_schema: str = "WorldZone_NEW",
    batch_size: int = 500  # VERY SMALL batches for 4GB StarRocks
):
    """
    Load table with ultra-conservative settings for low-memory StarRocks
    """
    
    print("=" * 80)
    print(f"CONSERVATIVE LOAD: {table_name}")
    print("=" * 80)
    print(f"Target: {target_schema}.{table_name}")
    print(f"Batch Size: {batch_size} (conservative for 4GB StarRocks)")
    print(f"Partitions: 1 (single writer to reduce memory pressure)")
    print("=" * 80)
    print()
    
    set_conservative_timeouts()
    
    # Check current state
    current = get_row_count(table_name, target_schema)
    if current > 0:
        print(f"⚠️  Table has {current:,} rows - will APPEND to existing data")
    elif current == 0:
        print(f"✅ Table is empty")
    
    # Read from S3
    print(f"\n📊 Reading from S3...")
    glue_database = f"worldzone_glue{settings.target_schema_suffix}"
    
    try:
        df = pipeline.read_from_iceberg(glue_database, table_name)
        s3_count = df.count()
        print(f"✅ Found {s3_count:,} rows in S3")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    if s3_count == 0:
        print("ℹ️  Empty table")
        return True
    
    # Load with SINGLE partition and small batches
    print(f"\n📥 Loading {s3_count:,} rows...")
    print(f"   Strategy: Single writer, {batch_size} rows per batch")
    print(f"   Batches: ~{(s3_count // batch_size) + 1:,}")
    print(f"   Est. time: {(s3_count / 1000):.0f}-{(s3_count / 500):.0f} minutes")
    print(f"\n   ⏳ Starting load...")
    print()
    
    start_time = time.time()
    
    try:
        # Use SINGLE partition to minimize memory usage
        df_single = df.coalesce(1)
        
        starrocks_url = (
            f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_port}/"
            f"{target_schema}?rewriteBatchedStatements=true&"
            f"useServerPrepStmts=false&cachePrepStmts=false"
        )
        
        df_single.write.format("jdbc") \
            .option("url", starrocks_url) \
            .option("dbtable", table_name) \
            .option("user", settings.starrocks_user) \
            .option("password", settings.starrocks_password) \
            .option("batchsize", batch_size) \
            .option("isolationLevel", "NONE") \
            .option("numPartitions", 1) \
            .mode("append") \
            .save()
        
        elapsed = time.time() - start_time
        print(f"\n✅ COMPLETED in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        
        # Verify
        print(f"\n🔍 Verifying...")
        time.sleep(2)
        
        final_count = get_row_count(table_name, target_schema)
        expected = current + s3_count if current > 0 else s3_count
        
        print(f"   Expected: {expected:,}")
        print(f"   Actual: {final_count:,}")
        
        if final_count == expected:
            print(f"\n🎉 SUCCESS!")
            return True
        else:
            print(f"\n⚠️  Mismatch - check for issues")
            return False
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ FAILED after {elapsed/60:.1f} min")
        print(f"Error: {str(e)[:300]}")
        
        partial = get_row_count(table_name, target_schema)
        if partial > current:
            loaded = partial - current
            print(f"\n⚠️  Partial: {loaded:,} rows loaded before failure")
        
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 load_s3_to_starrocks.py <table_name> [batch_size]")
        print("\nFor 4GB StarRocks, recommended batch_size: 500-2000")
        print("\nExamples:")
        print("  python3 load_s3_to_starrocks.py docdesc 500")
        print("  python3 load_s3_to_starrocks.py glledg 1000")
        sys.exit(1)
    
    table_name = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    
    success = load_table_conservative(
        table_name=table_name,
        batch_size=batch_size
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
