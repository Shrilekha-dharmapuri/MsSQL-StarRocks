#!/usr/bin/env python3
"""
MSSQL to StarRocks Comparison Script
Compares row counts between source MSSQL database and target StarRocks database.
"""

import sys
import pymssql
import mysql.connector
from datetime import datetime
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


def get_starrocks_tables_with_counts(target_schema: str):
    """
    Get all tables from StarRocks with their row counts.
    Note: StarRocks stores table names in lowercase.

    Args:
        target_schema: StarRocks schema/database name

    Returns:
        Dictionary of table_name (lowercase) -> row_count
    """
    print(f"Connecting to StarRocks: {settings.starrocks_host}:{settings.starrocks_port}/{target_schema}...")

    conn = mysql.connector.connect(
        host=settings.starrocks_host,
        port=settings.starrocks_port,
        user=settings.starrocks_user,
        password=settings.starrocks_password,
        database=target_schema,
        connect_timeout=30
    )
    cursor = conn.cursor()

    # Get all tables with row counts from information_schema
    cursor.execute(f"""
        SELECT TABLE_NAME, TABLE_ROWS
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = '{target_schema}'
        ORDER BY TABLE_NAME
    """)
    # Store with lowercase keys for case-insensitive matching
    tables = {row[0].lower(): row[1] for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    return tables


def compare_mssql_to_starrocks(
    mssql_schema: str = "dbo",
    starrocks_schema: str = "WorldZone_NEW",
    show_matches: bool = False,
    tables_to_check: list = None
):
    """
    Compare all tables between MSSQL and StarRocks.

    Args:
        mssql_schema: MSSQL schema name
        starrocks_schema: StarRocks database/schema name
        show_matches: If True, show tables that match (default: only show mismatches)
        tables_to_check: Optional list of specific tables to check. If None, checks all.
    """

    print("=" * 95)
    print("MSSQL TO STARROCKS COMPARISON")
    print("=" * 95)
    print(f"MSSQL Source: {settings.mssql_host}/{settings.mssql_database} (schema: {mssql_schema})")
    print(f"StarRocks Target: {settings.starrocks_host}:{settings.starrocks_port}/{starrocks_schema}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 95)
    print()

    # Get MSSQL tables
    print("Fetching MSSQL table list and row counts...")
    mssql_tables = get_mssql_tables_with_counts(mssql_schema)
    print(f"Found {len(mssql_tables)} tables in MSSQL")

    # Get StarRocks tables
    print("Fetching StarRocks table list and row counts...")
    sr_tables = get_starrocks_tables_with_counts(starrocks_schema)
    print(f"Found {len(sr_tables)} tables in StarRocks")
    print()

    # Filter tables if specific list provided
    if tables_to_check:
        tables_to_process = [t for t in tables_to_check if t in mssql_tables]
        print(f"Checking {len(tables_to_process)} specified tables...")
    else:
        tables_to_process = list(mssql_tables.keys())
        print(f"Checking all {len(tables_to_process)} MSSQL tables...")

    print()

    # Results tracking
    results = {
        "perfect_match": [],
        "mismatch": [],
        "empty_both": [],
        "missing_in_sr": [],
        "extra_in_sr": [],
        "errors": []
    }

    print(f"{'#':<5} {'Table Name':<40} {'MSSQL Rows':<15} {'SR Rows':<15} {'Status':<15}")
    print("-" * 95)

    # Compare each MSSQL table
    for idx, table_name in enumerate(sorted(tables_to_process), 1):
        try:
            mssql_count = mssql_tables.get(table_name, 0)
            # StarRocks uses lowercase table names, so do case-insensitive lookup
            sr_count = sr_tables.get(table_name.lower(), -1)  # -1 means not found

            # Determine status
            if sr_count == -1:
                status = "❌ NOT IN SR"
                results["missing_in_sr"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "sr_rows": 0
                })
                print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {'N/A':<15} {status:<15}")

            elif mssql_count == 0 and sr_count == 0:
                status = "✅ EMPTY (OK)"
                results["empty_both"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "sr_rows": sr_count
                })
                if show_matches:
                    print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {sr_count:<15,} {status:<15}")

            elif mssql_count == sr_count:
                status = "✅ MATCH"
                results["perfect_match"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "sr_rows": sr_count
                })
                if show_matches:
                    print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {sr_count:<15,} {status:<15}")

            else:
                status = "⚠️ MISMATCH"
                diff = mssql_count - sr_count
                results["mismatch"].append({
                    "table": table_name,
                    "mssql_rows": mssql_count,
                    "sr_rows": sr_count,
                    "diff": diff
                })
                print(f"{idx:<5} {table_name:<40} {mssql_count:<15,} {sr_count:<15,} {status:<15}")

            # Progress indicator every 20 tables
            if idx % 20 == 0:
                print(f"... Processed {idx}/{len(tables_to_process)} tables ...")

        except Exception as e:
            status = "❌ ERROR"
            results["errors"].append({
                "table": table_name,
                "error": str(e)
            })
            print(f"{idx:<5} {table_name:<40} {'ERROR':<15} {'ERROR':<15} {status:<15}")

    # Check for extra tables in StarRocks (not in MSSQL) - case-insensitive comparison
    mssql_tables_lower = {t.lower() for t in mssql_tables.keys()}
    extra_tables = set(sr_tables.keys()) - mssql_tables_lower
    if extra_tables:
        for table_name in extra_tables:
            results["extra_in_sr"].append({
                "table": table_name,
                "sr_rows": sr_tables[table_name]
            })

    # Print summary
    print()
    print("=" * 95)
    print("COMPARISON SUMMARY")
    print("=" * 95)
    print(f"Total MSSQL tables checked: {len(tables_to_process)}")
    print(f"  ✅ Perfect matches: {len(results['perfect_match'])}")
    print(f"  ✅ Empty (both MSSQL & SR): {len(results['empty_both'])}")
    print(f"  ⚠️  Row count mismatches: {len(results['mismatch'])}")
    print(f"  ❌ Missing in StarRocks: {len(results['missing_in_sr'])}")
    print(f"  ℹ️  Extra in StarRocks (not in MSSQL): {len(results['extra_in_sr'])}")
    print(f"  ❌ Errors: {len(results['errors'])}")
    print()

    # Calculate totals
    total_mssql_rows = sum(mssql_tables.get(t, 0) for t in tables_to_process)
    total_sr_rows = sum(item['sr_rows'] for item in results['perfect_match'])
    total_sr_rows += sum(item['sr_rows'] for item in results['mismatch'])
    total_sr_rows += sum(item['sr_rows'] for item in results['empty_both'])

    print(f"Total rows in MSSQL: {total_mssql_rows:,}")
    print(f"Total rows in StarRocks: {total_sr_rows:,}")
    print(f"Difference: {total_mssql_rows - total_sr_rows:+,}")
    print()

    # Show mismatches in detail
    if results["mismatch"]:
        print("=" * 95)
        print("MISMATCHED TABLES (Row Count Differences)")
        print("=" * 95)
        for item in sorted(results["mismatch"], key=lambda x: abs(x["diff"]), reverse=True):
            pct_diff = ((item["mssql_rows"] - item["sr_rows"]) / item["mssql_rows"] * 100) if item["mssql_rows"] > 0 else 0
            print(f"  {item['table']}:")
            print(f"    MSSQL: {item['mssql_rows']:,} rows")
            print(f"    StarRocks: {item['sr_rows']:,} rows")
            print(f"    Difference: {item['diff']:+,} rows ({pct_diff:+.2f}%)")
            print()

    # Show tables missing in StarRocks
    if results["missing_in_sr"]:
        print("=" * 95)
        print("TABLES MISSING IN STARROCKS")
        print("=" * 95)
        for item in sorted(results["missing_in_sr"], key=lambda x: x["mssql_rows"], reverse=True):
            print(f"  {item['table']}: {item['mssql_rows']:,} rows in MSSQL")
        print()

    # Show extra tables in StarRocks
    if results["extra_in_sr"]:
        print("=" * 95)
        print("EXTRA TABLES IN STARROCKS (not in MSSQL)")
        print("=" * 95)
        for item in sorted(results["extra_in_sr"], key=lambda x: x["sr_rows"], reverse=True):
            print(f"  {item['table']}: {item['sr_rows']:,} rows")
        print()

    # Show errors
    if results["errors"]:
        print("=" * 95)
        print("ERRORS DURING COMPARISON")
        print("=" * 95)
        for item in results["errors"]:
            print(f"  {item['table']}: {item['error']}")
        print()

    # Overall status
    total_with_data = len(results["perfect_match"]) + len(results["mismatch"]) + len(results["missing_in_sr"])
    successful = len(results["perfect_match"])

    print("=" * 95)
    print("OVERALL STATUS")
    print("=" * 95)
    if total_with_data > 0:
        success_rate = (successful / total_with_data) * 100
        print(f"Tables with data: {total_with_data}")
        print(f"Successfully migrated to StarRocks: {successful} ({success_rate:.1f}%)")
        print(f"Need attention: {len(results['mismatch']) + len(results['missing_in_sr'])}")
    print("=" * 95)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compare MSSQL and StarRocks table row counts")
    parser.add_argument("--mssql-schema", default="dbo", help="MSSQL schema name (default: dbo)")
    parser.add_argument("--sr-schema", default="WorldZone_NEW", help="StarRocks schema name (default: WorldZone_NEW)")
    parser.add_argument("--show-matches", action="store_true", help="Show tables that match (default: only show issues)")
    parser.add_argument("--tables", nargs="+", help="Specific tables to check (default: all)")

    args = parser.parse_args()

    results = compare_mssql_to_starrocks(
        mssql_schema=args.mssql_schema,
        starrocks_schema=args.sr_schema,
        show_matches=args.show_matches,
        tables_to_check=args.tables
    )

    # Exit code: 0 if all match, 1 if there are issues
    has_issues = len(results["mismatch"]) + len(results["missing_in_sr"]) + len(results["errors"]) > 0
    sys.exit(1 if has_issues else 0)
