#!/usr/bin/env python3
"""
Resume loading a table after failure by filtering out already-loaded rows.
Uses batched ID fetching to avoid StarRocks compaction errors.
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

        cursor.execute("SET GLOBAL interactive_timeout = 28800")
        cursor.execute("SET GLOBAL wait_timeout = 28800")
        cursor.execute("SET GLOBAL net_read_timeout = 28800")
        cursor.execute("SET GLOBAL net_write_timeout = 28800")
        cursor.execute("SET GLOBAL query_timeout = 28800")

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
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error getting count: {e}")
        return -1

def get_loaded_ids_batched(table_name: str, schema: str, id_column: str, batch_size: int = 50000):
    """Get loaded IDs in batches to avoid compaction errors"""
    print(f"\n📋 Fetching already-loaded {id_column} values from StarRocks...")

    all_ids = set()
    offset = 0
    max_retries = 3

    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=schema,
            connect_timeout=60
        )
        cursor = conn.cursor()

        # Get total count first
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        total = cursor.fetchone()[0]
        print(f"   Total rows to fetch IDs from: {total:,}")

        while offset < total:
            retries = 0
            while retries < max_retries:
                try:
                    cursor.execute(f"SELECT `{id_column}` FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}")
                    batch_ids = [row[0] for row in cursor.fetchall()]
                    all_ids.update(batch_ids)
                    offset += batch_size
                    pct = min(100, (offset / total) * 100)
                    print(f"   Progress: {len(all_ids):,} / {total:,} ({pct:.0f}%)", end='\r')
                    break
                except Exception as e:
                    retries += 1
                    if 'compacted' in str(e).lower():
                        print(f"\n   ⚠️  Compaction error at offset {offset}, retry {retries}/{max_retries}...")
                        time.sleep(3)
                        # Reconnect
                        try:
                            cursor.close()
                            conn.close()
                        except:
                            pass
                        conn = mysql.connector.connect(
                            host=settings.starrocks_host,
                            port=settings.starrocks_port,
                            user=settings.starrocks_user,
                            password=settings.starrocks_password,
                            database=schema,
                            connect_timeout=60
                        )
                        cursor = conn.cursor()
                    else:
                        raise e

            if retries >= max_retries:
                print(f"\n   ❌ Max retries exceeded at offset {offset}")
                return None

        print(f"\n✅ Fetched {len(all_ids):,} unique IDs")
        cursor.close()
        conn.close()
        return all_ids

    except Exception as e:
        print(f"\n❌ Error fetching IDs: {e}")
        return None

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
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        cursor.close()
        conn.close()
        print("✅ Table truncated")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def resume_from_failure(
    table_name: str,
    id_column: str,
    schema: str = "WorldZone_NEW",
    batch_size: int = 100,
    num_partitions: int = 1,
    force_truncate: bool = False
):
    """Resume loading a table after failure"""

    print("=" * 80)
    print(f"RESUMING LOAD: {table_name}")
    print("=" * 80)
    print(f"Schema: {schema}")
    print(f"ID Column: {id_column}")
    print(f"Batch Size: {batch_size:,}")
    print(f"Partitions: {num_partitions}")
    print(f"Force Truncate: {force_truncate}")
    print("=" * 80)

    # Set timeouts
    set_all_timeouts()

    # Check current state
    current = get_row_count(table_name, schema)
    if current < 0:
        print("❌ Cannot connect to StarRocks")
        return False

    print(f"\n📊 Current rows in StarRocks: {current:,}")

    # Read from S3
    print(f"\n📊 Reading from S3...")
    glue_db = f"worldzone_glue{settings.target_schema_suffix}"

    try:
        df = pipeline.read_from_iceberg(glue_db, table_name)
        s3_count = df.count()
        print(f"✅ Found {s3_count:,} rows in S3")
    except Exception as e:
        print(f"❌ Error reading from S3: {e}")
        return False

    if s3_count == 0:
        print("ℹ️  Empty table in S3")
        return True

    # Calculate progress
    missing = s3_count - current
    if current > 0:
        pct_complete = (current / s3_count) * 100
        print(f"\n📊 Progress:")
        print(f"   Total in S3: {s3_count:,}")
        print(f"   Already loaded: {current:,} ({pct_complete:.1f}%)")
        print(f"   Missing: {missing:,} rows")

        if missing == 0:
            print(f"\n✅ Table is already complete!")
            return True

        if missing < 0:
            print(f"\n⚠️  StarRocks has MORE rows than S3! Possible duplicates.")
            print(f"   Recommend running with --truncate flag")
            if not force_truncate:
                return False

    # Handle truncate or resume
    if force_truncate:
        if not truncate_table(table_name, schema):
            return False
        df_to_load = df
        rows_to_load = s3_count
        current = 0
    elif current > 0:
        # Resume by filtering
        print(f"\n🔄 Fetching loaded IDs to filter...")
        loaded_ids = get_loaded_ids_batched(table_name, schema, id_column)

        if loaded_ids is None:
            print("\n❌ Could not fetch loaded IDs.")
            print("   Try running with --truncate flag to start fresh")
            return False

        if len(loaded_ids) == 0:
            print("   No IDs fetched, loading all rows")
            df_to_load = df
            rows_to_load = s3_count
        else:
            # Use broadcast for efficiency with large ID sets
            print(f"\n⚙️  Filtering out {len(loaded_ids):,} already-loaded rows...")
            loaded_ids_bc = pipeline.spark.sparkContext.broadcast(loaded_ids)
            df_to_load = df.filter(~df[id_column].isin(loaded_ids_bc.value))

            print(f"   Counting remaining rows...")
            rows_to_load = df_to_load.count()
            print(f"✅ {rows_to_load:,} rows remaining to load")

            if rows_to_load == 0:
                print(f"\n✅ No rows to load (table appears complete)")
                return True
    else:
        df_to_load = df
        rows_to_load = s3_count

    # Repartition
    print(f"\n⚙️  Repartitioning into {num_partitions} partition(s)...")
    df_repartitioned = df_to_load.repartition(num_partitions)
    print(f"✅ Repartitioned")

    # Estimate time
    est_min = (rows_to_load / 10000)
    est_max = (rows_to_load / 5000)

    print(f"\n📥 Loading {rows_to_load:,} rows...")
    print(f"   Batch size: {batch_size:,}")
    print(f"   Partitions: {num_partitions}")
    print(f"   Est. time: {est_min:.0f}-{est_max:.0f} minutes")
    print(f"\n   ⏳ Starting at {time.strftime('%H:%M:%S')}...")
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
        print(f"   Duration: {elapsed/60:.1f} minutes")
        print(f"   Speed: {rows_to_load/elapsed:.0f} rows/second")

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
            print(f"   Run this script again to continue")
            return False

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ FAILED after {elapsed/60:.1f} minutes")
        print(f"Error: {str(e)[:500]}")

        partial = get_row_count(table_name, schema)
        if partial > current:
            new_rows = partial - current
            print(f"\n⚠️  Partial: loaded {new_rows:,} rows before failure")
            print(f"   Total now: {partial:,} / {s3_count:,} ({partial/s3_count*100:.1f}%)")
            print(f"\n💡 Run this script again to continue from here")
        else:
            print(f"\n   No additional rows loaded this run")

        return False

def main():
    if len(sys.argv) < 3:
        print("Resume a failed table load")
        print()
        print("Usage: python3 resume_from_failure.py <table_name> <id_column> [batch_size] [partitions] [--truncate]")
        print()
        print("Arguments:")
        print("  table_name   - Name of the table to load")
        print("  id_column    - Column used to identify unique rows (e.g., SRNO, SRID)")
        print("  batch_size   - Rows per batch (default: 100)")
        print("  partitions   - Parallel writers (default: 1)")
        print("  --truncate   - Start fresh (truncate table first)")
        print()
        print("Examples:")
        print("  python3 resume_from_failure.py glledg SRNO")
        print("  python3 resume_from_failure.py jobledg SRID 100 1")
        print("  python3 resume_from_failure.py glledg SRNO --truncate")
        sys.exit(1)

    table_name = sys.argv[1]
    id_column = sys.argv[2]
    force_truncate = '--truncate' in sys.argv

    numeric_args = [a for a in sys.argv[3:] if a != '--truncate' and a.isdigit()]
    batch_size = int(numeric_args[0]) if len(numeric_args) > 0 else 100
    num_partitions = int(numeric_args[1]) if len(numeric_args) > 1 else 1

    success = resume_from_failure(
        table_name=table_name,
        id_column=id_column,
        batch_size=batch_size,
        num_partitions=num_partitions,
        force_truncate=force_truncate
    )

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
