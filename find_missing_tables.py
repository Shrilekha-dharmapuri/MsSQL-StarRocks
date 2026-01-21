#!/usr/bin/env python3 python3 find_missing_tables.py --mysql-db c1s1_billing_crm_DEV_1_OR852t7r_DEV --starrocks-schema c1s1_billing_crm_DEV_1_OR852t7r_DEV_migrated

"""
Find Missing Tables Script

Compares MySQL source tables with StarRocks target to identify:
1. Tables that don't exist in StarRocks (not migrated)
2. Tables with row count mismatches (partial migration)

Usage:
    python3 find_missing_tables.py --mysql-db <db_name> --starrocks-schema <schema_name>

    # Output to JSON file for retry script
    python3 find_missing_tables.py --mysql-db <db_name> --starrocks-schema <schema_name> --output missing_tables.json
"""

import argparse
import json
import logging
import sys
from datetime import datetime

import mysql.connector
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_mysql_connection():
    """Create MySQL source connection"""
    return mysql.connector.connect(
        host=settings.mysql_source_host,
        port=settings.mysql_source_port,
        user=settings.mysql_source_user,
        password=settings.mysql_source_password,
        database=settings.mysql_source_database,
        charset='utf8mb4'
    )


def get_starrocks_connection():
    """Create StarRocks connection (uses MySQL protocol)"""
    return mysql.connector.connect(
        host=settings.starrocks_host,
        port=settings.starrocks_port,
        user=settings.starrocks_user,
        password=settings.starrocks_password or '',
    )


def get_mysql_tables(mysql_db: str) -> list:
    """Get all tables from MySQL database"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """, (mysql_db,))

    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    return tables


def get_starrocks_tables(starrocks_schema: str) -> list:
    """Get all tables from StarRocks schema"""
    conn = get_starrocks_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SHOW TABLES FROM `{starrocks_schema}`")
        tables = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.warning(f"Could not get tables from StarRocks schema {starrocks_schema}: {e}")
        tables = []

    cursor.close()
    conn.close()

    return tables


def get_mysql_row_count(mysql_db: str, table_name: str) -> int:
    """Get row count from MySQL table"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{mysql_db}`.`{table_name}`")
        count = cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Could not get row count for MySQL {mysql_db}.{table_name}: {e}")
        count = -1

    cursor.close()
    conn.close()

    return count


def get_starrocks_row_count(starrocks_schema: str, table_name: str) -> int:
    """Get row count from StarRocks table"""
    conn = get_starrocks_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{starrocks_schema}`.`{table_name}`")
        count = cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Could not get row count for StarRocks {starrocks_schema}.{table_name}: {e}")
        count = -1

    cursor.close()
    conn.close()

    return count


def find_missing_tables(mysql_db: str, starrocks_schema: str, check_counts: bool = True):
    """
    Compare MySQL and StarRocks to find missing/failed tables

    Returns:
        dict with 'missing', 'mismatched', and 'successful' tables
    """
    logger.info(f"Fetching tables from MySQL database: {mysql_db}")
    mysql_tables = set(get_mysql_tables(mysql_db))
    logger.info(f"Found {len(mysql_tables)} tables in MySQL")

    logger.info(f"Fetching tables from StarRocks schema: {starrocks_schema}")
    starrocks_tables = set(get_starrocks_tables(starrocks_schema))
    logger.info(f"Found {len(starrocks_tables)} tables in StarRocks")

    # Find missing tables (in MySQL but not in StarRocks)
    missing_tables = mysql_tables - starrocks_tables
    existing_tables = mysql_tables & starrocks_tables

    result = {
        'mysql_database': mysql_db,
        'starrocks_schema': starrocks_schema,
        'total_mysql_tables': len(mysql_tables),
        'total_starrocks_tables': len(starrocks_tables),
        'missing': sorted(list(missing_tables)),
        'mismatched': [],
        'successful': [],
        'checked_at': datetime.now().isoformat()
    }

    if check_counts and existing_tables:
        logger.info(f"Checking row counts for {len(existing_tables)} existing tables...")
        for i, table in enumerate(sorted(existing_tables), 1):
            if i % 50 == 0:
                logger.info(f"Checked {i}/{len(existing_tables)} tables...")

            mysql_count = get_mysql_row_count(mysql_db, table)
            sr_count = get_starrocks_row_count(starrocks_schema, table)

            if mysql_count != sr_count:
                result['mismatched'].append({
                    'table': table,
                    'mysql_count': mysql_count,
                    'starrocks_count': sr_count,
                    'difference': mysql_count - sr_count
                })
            else:
                result['successful'].append({
                    'table': table,
                    'row_count': mysql_count
                })
    else:
        result['successful'] = [{'table': t} for t in sorted(existing_tables)]

    return result


def generate_retry_report(result: dict, mysql_db: str, starrocks_schema: str) -> dict:
    """Generate a report compatible with retry_failed_mysql_tables.py"""
    failed_results = []

    # Add missing tables as failed
    for table in result['missing']:
        failed_results.append({
            'table_name': table,
            'status': 'failed',
            'error': 'Table does not exist in StarRocks'
        })

    # Add mismatched tables as failed
    for item in result['mismatched']:
        failed_results.append({
            'table_name': item['table'],
            'status': 'failed',
            'error': f"Row count mismatch: MySQL={item['mysql_count']}, StarRocks={item['starrocks_count']}"
        })

    report = {
        'source_database': mysql_db,
        'target_schema': starrocks_schema,
        'total_tables': result['total_mysql_tables'],
        'successful': len(result['successful']),
        'failed': len(failed_results),
        'results': failed_results,
        'generated_at': datetime.now().isoformat()
    }

    return report


def main():
    parser = argparse.ArgumentParser(
        description="Find tables missing from StarRocks migration",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--mysql-db", "-m",
        required=True,
        help="MySQL source database name"
    )
    parser.add_argument(
        "--starrocks-schema", "-s",
        required=True,
        help="StarRocks target schema name"
    )
    parser.add_argument(
        "--skip-counts",
        action="store_true",
        help="Skip row count comparison (faster, only checks table existence)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output JSON file path (compatible with retry script)"
    )

    args = parser.parse_args()

    try:
        result = find_missing_tables(
            mysql_db=args.mysql_db,
            starrocks_schema=args.starrocks_schema,
            check_counts=not args.skip_counts
        )

        # Print summary
        print(f"\n{'='*60}")
        print("Migration Comparison Summary")
        print(f"{'='*60}")
        print(f"MySQL Database: {args.mysql_db}")
        print(f"StarRocks Schema: {args.starrocks_schema}")
        print(f"Total MySQL Tables: {result['total_mysql_tables']}")
        print(f"Total StarRocks Tables: {result['total_starrocks_tables']}")
        print(f"Missing Tables: {len(result['missing'])}")
        print(f"Mismatched Tables: {len(result['mismatched'])}")
        print(f"{'='*60}")

        if result['missing']:
            print(f"\nMissing Tables ({len(result['missing'])}):")
            for table in result['missing']:
                print(f"  - {table}")

        if result['mismatched']:
            print(f"\nMismatched Tables ({len(result['mismatched'])}):")
            for item in result['mismatched']:
                print(f"  - {item['table']}: MySQL={item['mysql_count']:,}, StarRocks={item['starrocks_count']:,}")

        # Save to file if requested
        if args.output:
            report = generate_retry_report(result, args.mysql_db, args.starrocks_schema)
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"\nRetry report saved to: {args.output}")
            print(f"Run: python3 retry_failed_mysql_tables.py {args.output}")

        # Return the first missing table name for easy resume
        if result['missing']:
            first_missing = sorted(result['missing'])[0]
            print(f"\nTo resume migration from the first missing table:")
            print(f"python3 migrate_mysql.py --database {args.mysql_db} --target-schema {args.starrocks_schema} --resume-from {first_missing}")

        return 0 if not result['missing'] and not result['mismatched'] else 1

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
