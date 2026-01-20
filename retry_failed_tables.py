#!/usr/bin/env python3
"""Retry loading data for tables that failed during migration"""

import mysql.connector
import time
from app.config import settings
from app.services.spark_pipeline import pipeline
import boto3

def get_failed_tables(target_schema: str):
    """
    Get list of tables that failed to load by comparing S3 row counts vs StarRocks
    Returns tables that have data in S3 but 0 rows in StarRocks
    """
    print("Step 1: Getting all tables from StarRocks...")
    conn = mysql.connector.connect(
        host=settings.starrocks_host,
        port=settings.starrocks_port,
        user=settings.starrocks_user,
        password=settings.starrocks_password,
        connect_timeout=30
    )
    cursor = conn.cursor()

    # Get all tables with their row counts
    cursor.execute(f"""
        SELECT table_name, table_rows
        FROM information_schema.tables
        WHERE table_schema = '{target_schema}'
        ORDER BY table_name
    """)

    sr_tables = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    conn.close()

    print(f"Found {len(sr_tables)} tables in StarRocks")

    # Get tables from Glue and check S3 row counts
    print("\nStep 2: Checking S3/Glue for actual row counts...")
    glue_client = boto3.client(
        'glue',
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key
    )

    glue_database = f"worldzone_glue{settings.target_schema_suffix}"

    tables = []
    next_token = None

    while True:
        if next_token:
            response = glue_client.get_tables(
                DatabaseName=glue_database,
                NextToken=next_token
            )
        else:
            response = glue_client.get_tables(DatabaseName=glue_database)

        for table in response.get('TableList', []):
            tables.append(table['Name'])

        next_token = response.get('NextToken')
        if not next_token:
            break

    print(f"Found {len(tables)} tables in Glue catalog")

    # Check each table's S3 row count
    print("\nStep 3: Identifying failed tables (have data in S3 but 0 in StarRocks)...")
    failed_tables = []

    for idx, table_name in enumerate(sorted(tables), 1):
        sr_count = sr_tables.get(table_name, -1)

        # Only check tables that have 0 rows in StarRocks
        if sr_count == 0:
            try:
                # Quick check - try to count rows in S3
                df = pipeline.read_from_iceberg(glue_database, table_name)
                s3_count = df.count()

                if s3_count > 0:
                    # This table has data in S3 but 0 in StarRocks - it failed!
                    failed_tables.append({
                        'table': table_name,
                        's3_rows': s3_count,
                        'sr_rows': 0
                    })
                    print(f"  [{idx}/{len(tables)}] {table_name}: S3={s3_count:,} rows, StarRocks=0 ❌ FAILED")
                else:
                    print(f"  [{idx}/{len(tables)}] {table_name}: Empty in both S3 and StarRocks ✓")
            except Exception as e:
                print(f"  [{idx}/{len(tables)}] {table_name}: Error checking S3: {e}")
        else:
            print(f"  [{idx}/{len(tables)}] {table_name}: {sr_count:,} rows in StarRocks ✓")

    return failed_tables

def retry_table(table_name: str, target_schema: str, delay_seconds: int = 3):
    """Retry loading a single table with delay"""

    glue_database = f"worldzone_glue{settings.target_schema_suffix}"

    print(f"\n{'='*70}")
    print(f"Loading: {table_name}")
    print(f"{'='*70}")

    try:
        # Step 1: Read from S3
        print(f"[1/3] Reading from S3/Iceberg...")
        df = pipeline.read_from_iceberg(glue_database, table_name)

        # Step 2: Count rows
        print(f"[2/3] Counting rows...")
        row_count = df.count()
        print(f"✓ Found {row_count:,} rows")

        if row_count == 0:
            print(f"⚠ Skipping {table_name} - table is empty in S3")
            return {"status": "skipped", "table": table_name, "reason": "empty in S3"}

        # Step 3: Load to StarRocks
        print(f"[3/3] Loading {row_count:,} rows to StarRocks...")
        pipeline.load_to_starrocks(df, target_schema, table_name, mode="append")

        # Verify
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
        sr_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if sr_count == row_count:
            print(f"✅ SUCCESS: {sr_count:,} rows loaded")
            return {"status": "success", "table": table_name, "rows": sr_count}
        else:
            print(f"⚠ PARTIAL: S3={row_count:,}, StarRocks={sr_count:,}")
            return {"status": "partial", "table": table_name, "s3_rows": row_count, "sr_rows": sr_count}

    except Exception as e:
        print(f"❌ FAILED: {e}")
        return {"status": "failed", "table": table_name, "error": str(e)[:200]}

    finally:
        # Delay before next table to avoid overwhelming StarRocks
        if delay_seconds > 0:
            print(f"Waiting {delay_seconds}s before next table...")
            time.sleep(delay_seconds)

def main():
    target_schema = "WorldZone_NEW"
    delay_between_tables = 5  # 5 seconds between tables
    large_table_threshold = 200000  # Exclude tables with > 200K rows

    print(f"\n{'='*70}")
    print(f"SETTING UP STARROCKS TIMEOUTS")
    print(f"{'='*70}\n")

    # Ensure StarRocks has appropriate timeouts to prevent connection drops
    try:
        print("Increasing StarRocks timeouts to prevent connection drops...")
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            connect_timeout=30
        )
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL net_read_timeout = 3600")
        cursor.execute("SET GLOBAL net_write_timeout = 3600")
        cursor.execute("SET GLOBAL query_timeout = 7200")
        cursor.close()
        conn.close()
        print("✓ Timeouts increased: net_read=3600s, net_write=3600s, query=7200s\n")
    except Exception as e:
        print(f"⚠ Warning: Could not set timeouts: {e}")
        print("Migration will continue but may experience timeout issues.\n")

    print(f"{'='*70}")
    print(f"FINDING FAILED TABLES")
    print(f"{'='*70}\n")

    all_failed_tables = get_failed_tables(target_schema)

    if not all_failed_tables:
        print("\n✅ No failed tables found! All tables with data in S3 are loaded in StarRocks.")
        return

    # Separate small and large tables
    small_tables = [ft for ft in all_failed_tables if ft['s3_rows'] <= large_table_threshold]
    large_tables = [ft for ft in all_failed_tables if ft['s3_rows'] > large_table_threshold]

    print(f"\n{'='*70}")
    print(f"Found {len(all_failed_tables)} tables that failed to load")
    print(f"  - Small tables (<= 200K rows): {len(small_tables)}")
    print(f"  - Large tables (> 200K rows): {len(large_tables)} [EXCLUDED from retry]")
    print(f"{'='*70}")

    if large_tables:
        print(f"\n⚠ LARGE TABLES (excluded from this retry):")
        for ft in large_tables:
            print(f"  - {ft['table']}: {ft['s3_rows']:,} rows")
        print(f"\nThese will need to be migrated separately with optimized settings.")
        print(f"Save this list for later!")

    if not small_tables:
        print("\n✅ No small/medium tables to retry. Only large tables remain.")
        return

    # Use only small tables for retry
    failed_tables = small_tables

    print(f"\n{'='*70}")
    print(f"Will retry {len(failed_tables)} small/medium tables")
    print(f"{'='*70}")

    # Show first 10 failed tables
    print("\nFailed tables to retry (first 10):")
    for ft in failed_tables[:10]:
        print(f"  - {ft['table']}: {ft['s3_rows']:,} rows in S3, 0 in StarRocks")
    if len(failed_tables) > 10:
        print(f"  ... and {len(failed_tables) - 10} more")

    print(f"\n{'='*70}")
    print(f"Retry strategy:")
    print(f"  - {delay_between_tables}s delay between tables")
    print(f"  - Verify row counts after each load")
    print(f"  - Estimated time: {(len(failed_tables) * delay_between_tables) / 60:.0f} minutes + load time")
    print(f"{'='*70}\n")

    # Auto-proceed without user input for background execution
    # input("Press Enter to start retry, or Ctrl+C to cancel...")
    print("Starting retry automatically...")

    results = {
        "success": [],
        "failed": [],
        "partial": [],
        "skipped": []
    }

    for idx, failed_table in enumerate(failed_tables, 1):
        table_name = failed_table['table']
        print(f"\n[{idx}/{len(failed_tables)}] Processing {table_name}...")

        result = retry_table(table_name, target_schema, delay_between_tables)

        status = result["status"]
        results[status].append(result)

        # Show progress every 10 tables
        if idx % 10 == 0:
            print(f"\n{'='*70}")
            print(f"Progress: {idx}/{len(failed_tables)} tables processed")
            print(f"  ✅ Success: {len(results['success'])}")
            print(f"  ❌ Failed: {len(results['failed'])}")
            print(f"  ⚠ Partial: {len(results['partial'])}")
            print(f"  ⏭ Skipped: {len(results['skipped'])}")
            print(f"{'='*70}\n")

    # Final summary
    print(f"\n{'='*70}")
    print(f"FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"Total tables retried: {len(failed_tables)}")
    print(f"  ✅ Success: {len(results['success'])}")
    print(f"  ❌ Failed: {len(results['failed'])}")
    print(f"  ⚠ Partial: {len(results['partial'])}")
    print(f"  ⏭ Skipped (empty in S3): {len(results['skipped'])}")
    print(f"{'='*70}\n")

    if results['failed']:
        print("Failed tables:")
        for r in results['failed'][:10]:
            print(f"  - {r['table']}: {r.get('error', 'Unknown error')}")
        if len(results['failed']) > 10:
            print(f"  ... and {len(results['failed']) - 10} more")

    if results['partial']:
        print("\nPartial loads (data mismatch):")
        for r in results['partial']:
            print(f"  - {r['table']}: S3={r['s3_rows']}, StarRocks={r['sr_rows']}")

if __name__ == "__main__":
    main()
