#!/usr/bin/env python3
"""
Resume loading a table by filtering out already-loaded rows based on unique column
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

        # Set all timeouts to 8+ hours
        cursor.execute("SET GLOBAL interactive_timeout = 28800")  # 8 hours
        cursor.execute("SET GLOBAL wait_timeout = 28800")         # 8 hours
        cursor.execute("SET GLOBAL net_read_timeout = 28800")     # 8 hours
        cursor.execute("SET GLOBAL net_write_timeout = 28800")    # 8 hours
        cursor.execute("SET GLOBAL query_timeout = 28800")        # 8 hours

        cursor.close()
        conn.close()
        print("✅ All timeouts set to 8 hours")
        return True
    except Exception as e:
        print(f"⚠️  Warning: {e}")
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

def get_loaded_ids(table_name: str, schema: str, id_column: str):
    """Get list of already-loaded IDs from StarRocks"""
    print(f"\n📋 Fetching already-loaded {id_column} values from StarRocks...")

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

        # Get all IDs (this might take a while for large tables)
        cursor.execute(f"SELECT DISTINCT {id_column} FROM `{schema}`.`{table_name}`")
        loaded_ids = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        print(f"✅ Found {len(loaded_ids):,} already-loaded IDs")
        return set(loaded_ids)
    except Exception as e:
        print(f"❌ Error: {e}")
        return set()

def resume_load(
    table_name: str,
    id_column: str,
    schema: str = "WorldZone_NEW",
    batch_size: int = 100,
    num_partitions: int = 1
):
    """
    Resume loading a table by filtering out already-loaded rows
    """

    print("=" * 80)
    print(f"RESUMING LOAD: {table_name}")
    print("=" * 80)
    print(f"Schema: {schema}")
    print(f"ID Column: {id_column}")
    print(f"Batch Size: {batch_size:,}")
    print(f"Partitions: {num_partitions}")
    print("=" * 80)

    # Set timeouts
    set_all_timeouts()

    # Check current state
    current = get_row_count(table_name, schema)
    print(f"\n📊 Current rows in StarRocks: {current:,}")

    # Read from S3
    print(f"\n📊 Step 1/4: Reading from S3...")
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

    # Calculate what's missing
    missing = s3_count - current
    pct_complete = (current / s3_count) * 100
    print(f"\n📊 Progress:")
    print(f"   Total in S3: {s3_count:,}")
    print(f"   Already loaded: {current:,} ({pct_complete:.1f}%)")
    print(f"   Missing: {missing:,} rows")

    if missing == 0:
        print(f"\n✅ Table is already complete!")
        return True

    # Get already-loaded IDs
    loaded_ids = get_loaded_ids(table_name, schema, id_column)

    if not loaded_ids:
        print(f"\n⚠️  Could not fetch loaded IDs. Recommend using truncate + reload instead.")
        return False

    # Filter out already-loaded rows
    print(f"\n⚙️  Step 2/4: Filtering out already-loaded rows...")
    df_filtered = df.filter(~df[id_column].isin(loaded_ids))

    # Count remaining rows
    print(f"   Counting remaining rows...")
    remaining_count = df_filtered.count()
    print(f"✅ {remaining_count:,} rows to load")

    if remaining_count == 0:
        print(f"\n✅ No rows to load (table is complete)")
        return True

    # Repartition
    print(f"\n⚙️  Step 3/4: Repartitioning into {num_partitions} partition(s)...")
    df_repartitioned = df_filtered.repartition(num_partitions)
    print(f"✅ Repartitioned")

    # Calculate estimates
    batches_per_partition = (remaining_count // num_partitions // batch_size) + 1
    total_batches = batches_per_partition * num_partitions

    # With small batches (100), expect ~5-10K rows/min
    est_min = (remaining_count / 10000)  # 10K rows/min
    est_max = (remaining_count / 5000)   # 5K rows/min

    # Load
    print(f"\n📥 Step 4/4: Loading {remaining_count:,} remaining rows...")
    print(f"   Parallel writers: {num_partitions}")
    print(f"   Batch size: {batch_size:,}")
    print(f"   Est. batches: ~{total_batches:,}")
    print(f"   Est. time: {est_min:.0f}-{est_max:.0f} minutes ({est_min/60:.1f}-{est_max/60:.1f} hours)")
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
        print(f"   Duration: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes / {elapsed/3600:.1f} hours)")
        print(f"   Speed: {remaining_count/elapsed:.0f} rows/second")

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
            print(f"\n💡 You can run this script again to retry loading the missing rows")
            return False

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ FAILED after {elapsed/60:.1f} minutes ({elapsed/3600:.1f} hours)")
        print(f"Error: {str(e)[:500]}")

        partial = get_row_count(table_name, schema)
        if partial > current:
            new_rows = partial - current
            print(f"\n⚠️  Partial progress: loaded {new_rows:,} additional rows")
            print(f"   Total now: {partial:,} / {s3_count:,} ({partial/s3_count*100:.1f}%)")
            print(f"\n💡 You can run this script again to continue from where it stopped")

        return False

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 resume_load.py <table_name> <id_column> [batch_size] [partitions]")
        print("\nExamples:")
        print("  python3 resume_load.py jobledg SRID")
        print("  python3 resume_load.py jobledg SRID 100 1")
        print("  python3 resume_load.py glledg SRNO 50 1")
        sys.exit(1)

    table_name = sys.argv[1]
    id_column = sys.argv[2]
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    num_partitions = int(sys.argv[4]) if len(sys.argv) > 4 else 1

    success = resume_load(
        table_name=table_name,
        id_column=id_column,
        batch_size=batch_size,
        num_partitions=num_partitions
    )

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
