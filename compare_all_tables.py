#!/usr/bin/env python3
"""
Comprehensive S3 to StarRocks comparison for all tables with data.
Compares row counts and identifies discrepancies.
"""

import sys
import mysql.connector
from app.services.spark_pipeline import pipeline
from app.config import settings
import boto3
from datetime import datetime

def compare_all_tables(target_schema="WorldZone_NEW", show_matches=False, tables_to_check=None):
    """
    Compare all tables between S3 and StarRocks
    
    Args:
        target_schema: StarRocks schema name
        show_matches: If True, show tables that match (default: only show mismatches)
        tables_to_check: Optional list of specific tables to check. If None, checks all.
    """
    
    print("=" * 80)
    print("S3 (ICEBERG) TO STARROCKS COMPARISON")
    print("=" * 80)
    print(f"Target Schema: {target_schema}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # Connect to StarRocks
    print("Connecting to StarRocks...")
    conn = mysql.connector.connect(
        host=settings.starrocks_host,
        port=settings.starrocks_port,
        user=settings.starrocks_user,
        password=settings.starrocks_password,
        database=target_schema,
        connect_timeout=30
    )
    cursor = conn.cursor()
    
    # Get all tables from StarRocks with row counts
    print("Fetching StarRocks table list and row counts...")
    cursor.execute(f"""
        SELECT TABLE_NAME, TABLE_ROWS 
        FROM information_schema.tables 
        WHERE TABLE_SCHEMA = '{target_schema}'
        ORDER BY TABLE_NAME
    """)
    sr_tables_info = {row[0]: row[1] for row in cursor.fetchall()}
    
    print(f"Found {len(sr_tables_info)} tables in StarRocks")
    print()
    
    # Filter tables if specific list provided
    if tables_to_check:
        tables_to_process = [t for t in tables_to_check if t in sr_tables_info]
        print(f"Checking {len(tables_to_process)} specified tables...")
    else:
        # Only check tables that have data in StarRocks (or might have failed)
        tables_to_process = list(sr_tables_info.keys())
        print(f"Checking all {len(tables_to_process)} tables...")
    
    print()
    
    # Initialize Glue client
    glue_database = f"worldzone_glue{settings.target_schema_suffix}"
    
    # Results tracking
    results = {
        "perfect_match": [],
        "mismatch": [],
        "empty_both": [],
        "empty_sr_only": [],
        "errors": []
    }
    
    print(f"{'#':<5} {'Table Name':<40} {'S3 Rows':<15} {'SR Rows':<15} {'Status':<15}")
    print("-" * 90)
    
    # Compare each table
    for idx, table_name in enumerate(sorted(tables_to_process), 1):
        try:
            sr_count = sr_tables_info.get(table_name, -1)
            
            # Read from S3/Iceberg
            df = pipeline.read_from_iceberg(glue_database, table_name)
            s3_count = df.count()
            
            # Determine status
            if s3_count == 0 and sr_count == 0:
                status = "✅ EMPTY (OK)"
                results["empty_both"].append({
                    "table": table_name,
                    "s3_rows": s3_count,
                    "sr_rows": sr_count
                })
                if show_matches:
                    print(f"{idx:<5} {table_name:<40} {s3_count:<15,} {sr_count:<15,} {status:<15}")
                    
            elif s3_count == sr_count:
                status = "✅ MATCH"
                results["perfect_match"].append({
                    "table": table_name,
                    "s3_rows": s3_count,
                    "sr_rows": sr_count
                })
                if show_matches:
                    print(f"{idx:<5} {table_name:<40} {s3_count:<15,} {sr_count:<15,} {status:<15}")
                    
            elif sr_count == 0 and s3_count > 0:
                status = "❌ MISSING DATA"
                results["empty_sr_only"].append({
                    "table": table_name,
                    "s3_rows": s3_count,
                    "sr_rows": sr_count,
                    "diff": s3_count
                })
                print(f"{idx:<5} {table_name:<40} {s3_count:<15,} {sr_count:<15,} {status:<15}")
                
            else:
                status = "⚠️ MISMATCH"
                diff = s3_count - sr_count
                results["mismatch"].append({
                    "table": table_name,
                    "s3_rows": s3_count,
                    "sr_rows": sr_count,
                    "diff": diff
                })
                print(f"{idx:<5} {table_name:<40} {s3_count:<15,} {sr_count:<15,} {status:<15}")
            
            # Progress indicator every 10 tables
            if idx % 10 == 0:
                print(f"... Processed {idx}/{len(tables_to_process)} tables ...")
                
        except Exception as e:
            status = "❌ ERROR"
            results["errors"].append({
                "table": table_name,
                "error": str(e)
            })
            print(f"{idx:<5} {table_name:<40} {'ERROR':<15} {sr_count:<15,} {status:<15}")
    
    cursor.close()
    conn.close()
    
    # Print summary
    print()
    print("=" * 80)
    print("COMPARISON SUMMARY")
    print("=" * 80)
    print(f"Total tables checked: {len(tables_to_process)}")
    print(f"  ✅ Perfect matches: {len(results['perfect_match'])}")
    print(f"  ✅ Empty (both S3 & SR): {len(results['empty_both'])}")
    print(f"  ⚠️  Row count mismatches: {len(results['mismatch'])}")
    print(f"  ❌ Missing data (0 in SR, >0 in S3): {len(results['empty_sr_only'])}")
    print(f"  ❌ Errors: {len(results['errors'])}")
    print()
    
    # Show mismatches in detail
    if results["mismatch"]:
        print("=" * 80)
        print("MISMATCHED TABLES (Row Count Differences)")
        print("=" * 80)
        for item in results["mismatch"]:
            print(f"  {item['table']}:")
            print(f"    S3: {item['s3_rows']:,} rows")
            print(f"    StarRocks: {item['sr_rows']:,} rows")
            print(f"    Difference: {item['diff']:+,} rows")
            print()
    
    # Show tables with missing data
    if results["empty_sr_only"]:
        print("=" * 80)
        print("TABLES WITH MISSING DATA (Failed to Load)")
        print("=" * 80)
        for item in sorted(results["empty_sr_only"], key=lambda x: x["s3_rows"], reverse=True):
            print(f"  {item['table']}: {item['s3_rows']:,} rows in S3, 0 in StarRocks")
        print()
    
    # Show errors
    if results["errors"]:
        print("=" * 80)
        print("ERRORS DURING COMPARISON")
        print("=" * 80)
        for item in results["errors"]:
            print(f"  {item['table']}: {item['error']}")
        print()
    
    # Overall status
    total_with_data = len(results["perfect_match"]) + len(results["mismatch"]) + len(results["empty_sr_only"])
    successful = len(results["perfect_match"])
    
    print("=" * 80)
    print("OVERALL STATUS")
    print("=" * 80)
    if total_with_data > 0:
        success_rate = (successful / total_with_data) * 100
        print(f"Tables with data: {total_with_data}")
        print(f"Successfully migrated: {successful} ({success_rate:.1f}%)")
        print(f"Need attention: {len(results['mismatch']) + len(results['empty_sr_only'])}")
    print("=" * 80)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Compare S3 and StarRocks table row counts")
    parser.add_argument("--schema", default="WorldZone_NEW", help="StarRocks schema name")
    parser.add_argument("--show-matches", action="store_true", help="Show tables that match (default: only show issues)")
    parser.add_argument("--tables", nargs="+", help="Specific tables to check (default: all)")
    
    args = parser.parse_args()
    
    results = compare_all_tables(
        target_schema=args.schema,
        show_matches=args.show_matches,
        tables_to_check=args.tables
    )
    
    # Exit code: 0 if all match, 1 if there are issues
    has_issues = len(results["mismatch"]) + len(results["empty_sr_only"]) + len(results["errors"]) > 0
    sys.exit(1 if has_issues else 0)
