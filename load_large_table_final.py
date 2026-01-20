#!/usr/bin/env python3
"""
Final solution for large tables with proper timeout handling
"""

import sys
import time
import mysql.connector
from app.services.spark_pipeline import pipeline
from app.config import settings

def set_all_timeouts():
    """Set all necessary timeouts"""
    print("\n⏱️  Configuring StarRocks timeouts...")
    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            connect_timeout=30
        )
        cursor = conn.cursor()
        
        # Set all timeouts to 4+ hours
        cursor.execute("SET GLOBAL interactive_timeout = 14400")  # 4 hours
        cursor.execute("SET GLOBAL wait_timeout = 28800")         # 8 hours
        cursor.execute("SET GLOBAL net_read_timeout = 14400")     # 4 hours
        cursor.execute("SET GLOBAL net_write_timeout = 14400")    # 4 hours
        cursor.execute("SET GLOBAL query_timeout = 21600")        # 6 hours
        
        cursor.close()
        conn.close()
        print("✅ All timeouts set to 4+ hours")
        return True
    except Exception as e:
        print(f"⚠️  Warning: {e}")
        return False

def truncate_table(table_name: str, schema: str):
    """Truncate table"""
    print(f"\n🗑️  Truncating {schema}.{table_name}...")
    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=schema,
            connect_timeout=30
        )
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE `{schema}`.`{table_name}`")
        cursor.close()
        conn.close()
        print("✅ Table truncated")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def get_row_count(table_name: str, schema: str) -> int:
    """Get row count"""
    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=schema,
            connect_timeout=30
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except:
        return -1

def load_large_table(
    table_name: str,
    schema: str = "WorldZone_NEW",
    batch_size: int = 5000,
    num_partitions: int = 2
):
    """
    Load large table with optimized settings
    """
    
    print("=" * 80)
    print(f"LOADING LARGE TABLE: {table_name}")
    print("=" * 80)
    print(f"Schema: {schema}")
    print(f"Batch Size: {batch_size:,}")
    print(f"Partitions: {num_partitions}")
    print("=" * 80)
    
    # Set timeouts
    set_all_timeouts()
    
    # Check and truncate if needed
    current = get_row_count(table_name, schema)
    if current > 0:
        print(f"\n⚠️  Table has {current:,} rows (partial load)")
        if not truncate_table(table_name, schema):
            return False
    else:
        print(f"\n✅ Table is empty")
    
    # Read from S3
    print(f"\n📊 Step 1/3: Reading from S3...")
    glue_db = f"worldzone_glue{settings.target_schema_suffix}"
    
    try:
        df = pipeline.read_from_iceberg(glue_db, table_name)
        s3_count = df.count()
        print(f"✅ Found {s3_count:,} rows in S3")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    if s3_count == 0:
        print("ℹ️  Empty table")
        return True
    
    # Repartition
    print(f"\n⚙️  Step 2/3: Repartitioning into {num_partitions} partitions...")
    df_repartitioned = df.repartition(num_partitions)
    print(f"✅ Repartitioned")
    
    # Calculate estimates
    batches_per_partition = (s3_count // num_partitions // batch_size) + 1
    total_batches = batches_per_partition * num_partitions
    est_min = (s3_count / 50000)  # ~50K rows/min
    est_max = (s3_count / 30000)  # ~30K rows/min
    
    # Load
    print(f"\n📥 Step 3/3: Loading {s3_count:,} rows...")
    print(f"   Parallel writers: {num_partitions}")
    print(f"   Batch size: {batch_size:,}")
    print(f"   Est. batches: ~{total_batches:,}")
    print(f"   Est. time: {est_min:.0f}-{est_max:.0f} minutes")
    print(f"\n   ⏳ Starting load at {time.strftime('%H:%M:%S')}...")
    print()
    
    start_time = time.time()
    
    try:
        url = (
            f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_port}/"
            f"{schema}?rewriteBatchedStatements=true"
        )
        
        df_repartitioned.write.format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", settings.starrocks_user) \
            .option("password", settings.starrocks_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", batch_size) \
            .option("isolationLevel", "NONE") \
            .option("numPartitions", num_partitions) \
            .mode("append") \
            .save()
        
        elapsed = time.time() - start_time
        print(f"\n✅ LOAD COMPLETED!")
        print(f"   Duration: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"   Speed: {s3_count/elapsed:.0f} rows/second")
        
        # Verify
        print(f"\n🔍 Verifying...")
        time.sleep(3)
        
        final_count = get_row_count(table_name, schema)
        print(f"   Expected: {s3_count:,}")
        print(f"   Actual: {final_count:,}")
        
        if s3_count == final_count:
            print(f"\n🎉 SUCCESS! All rows loaded correctly")
            return True
        else:
            diff = s3_count - final_count
            pct = (final_count / s3_count) * 100
            print(f"\n⚠️  PARTIAL: {pct:.1f}% loaded, missing {diff:,} rows")
            return False
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ FAILED after {elapsed/60:.1f} minutes")
        print(f"Error: {str(e)[:500]}")
        
        partial = get_row_count(table_name, schema)
        if partial > 0:
            pct = (partial / s3_count) * 100
            print(f"\n⚠️  Partial load: {partial:,} rows ({pct:.1f}%)")
        
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 load_large_table_final.py <table_name> [batch_size] [partitions]")
        print("\nRecommended settings:")
        print("  Small tables (<100K):  batch=2000, partitions=1")
        print("  Medium tables (100K-1M): batch=5000, partitions=2")
        print("  Large tables (>1M):    batch=10000, partitions=4")
        print("\nExamples:")
        print("  python3 load_large_table_final.py userlog 10000 4")
        print("  python3 load_large_table_final.py docdesc 5000 2")
        sys.exit(1)
    
    table_name = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    num_partitions = int(sys.argv[3]) if len(sys.argv) > 3 else 2
    
    success = load_large_table(
        table_name=table_name,
        batch_size=batch_size,
        num_partitions=num_partitions
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
