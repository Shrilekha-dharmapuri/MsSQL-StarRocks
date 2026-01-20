#!/usr/bin/env python3
"""
Resume loading a table - loads only missing rows without truncating.
Simple and fast approach using ID-based filtering.
"""

import sys
import time
import mysql.connector
from pyspark.sql import functions as F

def resume_table(table_name: str, id_column: str, batch_size: int = 500):
    """Resume loading a table from where it stopped"""

    from app.services.spark_pipeline import pipeline
    from app.config import settings

    print("=" * 70)
    print(f"RESUMING: {table_name}")
    print("=" * 70)

    schema = "WorldZone_NEW"
    glue_db = f"worldzone_glue{settings.target_schema_suffix}"

    # Set timeouts
    print("\n[1/6] Setting timeouts...")
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
        cursor.close()
        conn.close()
        print("✓ Timeouts set to 8 hours")
    except Exception as e:
        print(f"⚠ Warning: {e}")

    # Get current count in StarRocks
    print("\n[2/6] Checking StarRocks...")
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
    current_count = cursor.fetchone()[0]
    print(f"✓ Current rows in StarRocks: {current_count:,}")

    # Get loaded IDs using simple batched approach
    print(f"\n[3/6] Fetching loaded {id_column} values...")
    loaded_ids = set()
    batch = 100000
    offset = 0

    while True:
        cursor.execute(f"SELECT `{id_column}` FROM `{table_name}` LIMIT {batch} OFFSET {offset}")
        rows = cursor.fetchall()
        if not rows:
            break
        loaded_ids.update(row[0] for row in rows)
        offset += batch
        print(f"   Fetched {len(loaded_ids):,} IDs...", end='\r')

    cursor.close()
    conn.close()
    print(f"\n✓ Fetched {len(loaded_ids):,} loaded IDs")

    # Read from S3
    print(f"\n[4/6] Reading from S3...")
    df = pipeline.read_from_iceberg(glue_db, table_name)
    s3_count = df.count()
    print(f"✓ S3 has {s3_count:,} rows")

    missing = s3_count - current_count
    if missing <= 0:
        print(f"\n✓ Table is complete! ({current_count:,} / {s3_count:,})")
        return True

    print(f"✓ Need to load {missing:,} rows")

    # Filter to only missing rows
    print(f"\n[5/6] Filtering to missing rows...")

    # Broadcast loaded IDs for efficient filtering
    spark = pipeline.spark
    loaded_ids_broadcast = spark.sparkContext.broadcast(loaded_ids)

    # Filter using a UDF for efficiency with large sets
    @F.udf("boolean")
    def not_loaded(id_val):
        return id_val not in loaded_ids_broadcast.value

    df_missing = df.filter(not_loaded(F.col(id_column)))

    # Repartition to single partition for sequential loading
    df_to_load = df_missing.repartition(1)
    print(f"✓ Filtered and repartitioned")

    # Load to StarRocks
    print(f"\n[6/6] Loading {missing:,} rows (batch_size={batch_size})...")
    print(f"   Started at {time.strftime('%H:%M:%S')}")

    start_time = time.time()

    try:
        url = f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_port}/{schema}?rewriteBatchedStatements=true"

        df_to_load.write.format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", settings.starrocks_user) \
            .option("password", settings.starrocks_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", batch_size) \
            .option("isolationLevel", "NONE") \
            .option("numPartitions", 1) \
            .mode("append") \
            .save()

        elapsed = time.time() - start_time

        # Verify
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
        final_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        print(f"\n" + "=" * 70)
        print(f"COMPLETED in {elapsed/60:.1f} minutes")
        print(f"  Before: {current_count:,}")
        print(f"  After:  {final_count:,}")
        print(f"  Added:  {final_count - current_count:,}")
        print(f"  Target: {s3_count:,}")

        if final_count == s3_count:
            print(f"\n✓ SUCCESS - All rows loaded!")
            return True
        else:
            print(f"\n⚠ PARTIAL - Run again to continue ({s3_count - final_count:,} remaining)")
            return False

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n✗ FAILED after {elapsed/60:.1f} minutes")
        print(f"  Error: {str(e)[:200]}")

        # Check partial progress
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
            partial = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            if partial > current_count:
                print(f"\n⚠ Partial progress: {partial:,} rows ({partial - current_count:,} added)")
                print(f"  Run again to continue")
        except:
            pass

        return False


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 resume_table.py <table_name> <id_column> [batch_size]")
        print()
        print("Examples:")
        print("  python3 resume_table.py glledg SRNO")
        print("  python3 resume_table.py jobledg SRID 500")
        sys.exit(1)

    table_name = sys.argv[1]
    id_column = sys.argv[2]
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 500

    success = resume_table(table_name, id_column, batch_size)
    sys.exit(0 if success else 1)
