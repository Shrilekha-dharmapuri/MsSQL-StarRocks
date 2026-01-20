#!/usr/bin/env python3
"""
MSSQL to S3 (Iceberg) Comparison Script
Compares row counts between source MSSQL database and S3/Iceberg tables.
"""

import sys
import pymssql
from datetime import datetime
from app.services.spark_pipeline import pipeline
from app.config import settings


def get_mssql_tables_with_counts(schema: str = "dbo"):
    """
    Get all tables from MSSQL with their row counts.

    Args:
        schema: MSSQL schema name (default: dbo)

    Returns:
        Dictionary of table_name -> row_count
    """
    print(f"Connecting to MSSQL: {settings.mssql_host}/{settings.mssql_database}...")

    conn = pymssql.connect(
        server=settings.mssql_host,
        port=settings.mssql_port,
        user=settings.mssql_user,
        password=settings.mssql_password,
        database=settings.mssql_database
    )
    cursor = conn.cursor()

    # Get all tables with approximate row counts (faster than COUNT(*))
    # Using sys.dm_db_partition_stats for performance
    query = """
        SELECT
            t.name AS table_name,
            SUM(p.rows) AS row_count
        FROM sys.tables t
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE p.index_id IN (0, 1)  -- Heap or clustered index
        AND s.name = %s
        GROUP BY t.name
        ORDER BY t.name
    """

    cursor.execute(query, (schema,))
    tables = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    return tables


def compare_mssql_to_s3(mssql_schema: str = "dbo", show_matches: bool = False, tables_to_check: list = None):
    """
    Compare all tables between MSSQL and S3/Iceberg.

    Args:
        mssql_schema: MSSQL schema name
        show_matches: If True, show tables that match (default: only show mismatches)
        tables_to_check: Optional list of specific tables to check. If None, checks all.
    """

    print("=" * 90)
    print("MSSQL TO S3 (ICEBERG) COMPARISON")
    print("=" * 90)
    print(f"MSSQL Source: {settings.mssql_host}/{settings.mssql_database} (schema: {mssql_schema})")
    print(f"S3/Iceberg: {settings.iceberg_warehouse_path}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)
    print()

    # Get MSSQL tables
    print("Fetching MSSQL table list and row counts...")
    mssql_tables = get_mssql_tables_with_counts(mssql_schema)
    print(f"Found {len(mssql_tables)} tables in MSSQL")
    print()

    # Glue database name
    glue_database = f"worldzone_glue{settings.target_schema_suffix}"
    print(f"Glue Database: {glue_database}")
    print()

    # Filter tables if specific list provided
    if tables_to_check:
        tables_to_process = [t for t in tables_to_check if t in mssql_tables]
        print(f"Checking {len(tables_to_process)} specified tables...")
    else:
        tables_to_process = list(mssql_tables.keys())
        print(f"Checking all {len(tables_to_process)} tables...")

    print()

    # Results tracking
    results = {
        "perfect_match": [],
        "mismatch": [],
        "empty_both": [],
        "missing_in_s3": [],
        "errors": []
    }

    print(f"{'#':<5} {'Table Name':<40} {'MSSQL Rows':<15} {'S3 Rows':<15} {'Status':<15}")
    print("-" * 95)

    # Compare each table
    for idx, table_name in enumerate(sorted(tables_to_process), 1):
        try:
            mssql_count = mssql_tables.get(table_name, 0)

            # Try to read from S3/Iceberg
            try:
                df = pipeline.read_from_iceberg(glue_database, table_name)
                s3_count = df.count()
            except Exception as e:
                if "Table not found" in str(e) or "does not exist" in str(e).lower():
                    s3_count = -1  # Table doesn't exist in S3
                else:
                    raise e

            # Determine status
            if s3_count == -1:
                status = "❌ NOT IN S3"
                results["missing_in_s3"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "s3_rows": 0
                })
                print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {'N/A':<15} {status:<15}")

            elif mssql_count == 0 and s3_count == 0:
                status = "✅ EMPTY (OK)"
                results["empty_both"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "s3_rows": s3_count
                })
                if show_matches:
                    print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {s3_count:<15,} {status:<15}")

            elif mssql_count == s3_count:
                status = "✅ MATCH"
                results["perfect_match"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "s3_rows": s3_count
                })
                if show_matches:
                    print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {s3_count:<15,} {status:<15}")

            else:
                status = "⚠️ MISMATCH"
                diff = mssql_count - s3_count
                results["mismatch"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "s3_rows": s3_count,
                    "diff": diff
                })
                print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {s3_count:<15,} {status:<15}")

            # Progress indicator every 20 tables
            if idx % 20 == 0:
                print(f"... Processed {idx}/{len(tables_to_process)} tables ...")

        except Exception as e:
            status = "❌ ERROR"
            results["errors"].append({
                "table": table_name,
                "error": str(e)
            })
            print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {'ERROR':<15} {status:<15}")

    # Print summary
    print()
    print("=" * 90)
    print("COMPARISON SUMMARY")
    print("=" * 90)
    print(f"Total tables checked: {len(tables_to_process)}")
    print(f"  ✅ Perfect matches: {len(results['perfect_match'])}")
    print(f"  ✅ Empty (both MSSQL & S3): {len(results['empty_both'])}")
    print(f"  ⚠️  Row count mismatches: {len(results['mismatch'])}")
    print(f"  ❌ Missing in S3: {len(results['missing_in_s3'])}")
    print(f"  ❌ Errors: {len(results['errors'])}")
    print()

    # Calculate totals
    total_mssql_rows = sum(mssql_tables.get(t, 0) for t in tables_to_process)
    total_s3_rows = sum(item['s3_rows'] for item in results['perfect_match'])
    total_s3_rows += sum(item['s3_rows'] for item in results['mismatch'])
    total_s3_rows += sum(item['s3_rows'] for item in results['empty_both'])

    print(f"Total rows in MSSQL: {total_mssql_rows:,}")
    print(f"Total rows in S3: {total_s3_rows:,}")
    print(f"Difference: {total_mssql_rows - total_s3_rows:+,}")
    print()

    # Show mismatches in detail
    if results["mismatch"]:
        print("=" * 90)
        print("MISMATCHED TABLES (Row Count Differences)")
        print("=" * 90)
        for item in sorted(results["mismatch"], key=lambda x: abs(x["diff"]), reverse=True):
            print(f"  {item['table']}:")
            print(f"    MSSQL: {item['mssql_rows']:,} rows")
            print(f"    S3: {item['s3_rows']:,} rows")
            print(f"    Difference: {item['diff']:+,} rows")
            print()

    # Show tables missing in S3
    if results["missing_in_s3"]:
        print("=" * 90)
        print("TABLES MISSING IN S3")
        print("=" * 90)
        for item in sorted(results["missing_in_s3"], key=lambda x: x["mssql_rows"], reverse=True):
            print(f"  {item['table']}: {item['mssql_rows']:,} rows in MSSQL")
        print()

    # Show errors
    if results["errors"]:
        print("=" * 90)
        print("ERRORS DURING COMPARISON")
        print("=" * 90)
        for item in results["errors"]:
            print(f"  {item['table']}: {item['error']}")
        print()

    # Overall status
    total_with_data = len(results["perfect_match"]) + len(results["mismatch"]) + len(results["missing_in_s3"])
    successful = len(results["perfect_match"])

    print("=" * 90)
    print("OVERALL STATUS")
    print("=" * 90)
    if total_with_data > 0:
        success_rate = (successful / total_with_data) * 100
        print(f"Tables with data: {total_with_data}")
        print(f"Successfully migrated to S3: {successful} ({success_rate:.1f}%)")
        print(f"Need attention: {len(results['mismatch']) + len(results['missing_in_s3'])}")
    print("=" * 90)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compare MSSQL and S3/Iceberg table row counts")
    parser.add_argument("--schema", default="dbo", help="MSSQL schema name (default: dbo)")
    parser.add_argument("--show-matches", action="store_true", help="Show tables that match (default: only show issues)")
    parser.add_argument("--tables", nargs="+", help="Specific tables to check (default: all)")

    args = parser.parse_args()

    results = compare_mssql_to_s3(
        mssql_schema=args.schema,
        show_matches=args.show_matches,
        tables_to_check=args.tables
    )

    # Exit code: 0 if all match, 1 if there are issues
    has_issues = len(results["mismatch"]) + len(results["missing_in_s3"]) + len(results["errors"]) > 0
    sys.exit(1 if has_issues else 0)
