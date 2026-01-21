#!/usr/bin/env python3
"""
Retry Failed MySQL Tables Migration Script

This script retries migration for tables that failed in the initial migration.
It reads the failed tables from the migration report and retries them.

Usage:
    # Retry all failed tables from report (skips already migrated ones):
    python retry_failed_mysql_tables.py --report mysql_migration_report_job_1.json --skip-migrated

    # List failed tables only:
    python retry_failed_mysql_tables.py --report mysql_migration_report_job_1.json --list-only

    # Or specify tables directly:
    python retry_failed_mysql_tables.py --database mydb --target-schema analytics --tables "table1,table2,table3"
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def sanitize_table_name(name: str) -> str:
    """Sanitize table name for StarRocks/Iceberg compatibility"""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    sanitized = re.sub(r'_+', '_', sanitized)
    sanitized = sanitized.strip('_')
    return sanitized.lower()


def get_migrated_tables(target_schema: str) -> set:
    """Get set of tables that already have data in StarRocks"""
    import mysql.connector
    from app.config import settings

    migrated = set()
    try:
        conn = mysql.connector.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=target_schema
        )
        cursor = conn.cursor()

        # Get all tables in the schema
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        # Check which ones have data
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                count = cursor.fetchone()[0]
                if count > 0:
                    migrated.add(table)
            except:
                pass

        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not check migrated tables: {e}")

    return migrated


def get_failed_tables_from_report(report_path: str) -> list:
    """Extract failed table names from migration report"""
    with open(report_path, 'r') as f:
        report = json.load(f)

    failed_tables = []
    source_database = None
    target_schema = None

    for result in report:
        if result.get('status') == 'failed':
            failed_tables.append(result.get('table_name'))
            if not source_database:
                source_database = result.get('source_database')
            if not target_schema:
                target_schema = result.get('target_schema')

    return failed_tables, source_database, target_schema


def retry_migration(
    database: str,
    target_schema: str,
    table_list: list,
    create_tables: bool = True,
    only_s3: bool = False,
    batch_size: int = 10
):
    """Retry migration for specified tables in batches to prevent memory exhaustion"""
    from app.services.migration_orchestrator import orchestrator

    logger.info(f"Retrying migration for {len(table_list)} failed tables in batches of {batch_size}")
    logger.info(f"Database: {database}, Target Schema: {target_schema}")

    all_results = []
    total_successful = 0
    total_failed = 0

    # Process tables in batches
    for i in range(0, len(table_list), batch_size):
        batch = table_list[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(table_list) + batch_size - 1) // batch_size

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tables)")
        logger.info(f"{'='*60}")

        try:
            result = orchestrator.migrate_mysql_schema(
                source_database=database,
                target_schema=target_schema,
                table_list=batch,
                create_tables=create_tables,
                only_s3=only_s3,
            )

            all_results.extend(result['results'])
            total_successful += result['successful']
            total_failed += result['failed']

            logger.info(f"Batch {batch_num} complete: {result['successful']} successful, {result['failed']} failed")

        except Exception as e:
            logger.error(f"Batch {batch_num} failed with error: {e}")
            # Mark all tables in this batch as failed
            for table in batch:
                all_results.append({
                    'table_name': table,
                    'status': 'failed',
                    'error': str(e)[:200]
                })
                total_failed += 1

        # Clean up Spark session between batches to free memory
        try:
            from app.services.spark_pipeline import pipeline
            pipeline.close()
            logger.info("Spark session cleaned up between batches")
        except Exception as e:
            logger.warning(f"Could not clean up Spark session: {e}")

    return {
        'total_tables': len(table_list),
        'successful': total_successful,
        'failed': total_failed,
        'results': all_results
    }


def main():
    parser = argparse.ArgumentParser(
        description="Retry Failed MySQL Tables Migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--report", "-r",
        help="Path to migration report JSON file"
    )
    parser.add_argument(
        "--database", "-d",
        help="MySQL source database name (required if not using --report)"
    )
    parser.add_argument(
        "--target-schema", "-t",
        help="StarRocks target schema name"
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables to retry"
    )
    parser.add_argument(
        "--only-s3",
        action="store_true",
        help="Only migrate to S3/Iceberg, skip StarRocks loading"
    )
    parser.add_argument(
        "--skip-create-tables",
        action="store_true",
        help="Skip creating StarRocks tables"
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only list failed tables, don't retry"
    )
    parser.add_argument(
        "--skip-migrated",
        action="store_true",
        help="Skip tables that already have data in StarRocks"
    )
    parser.add_argument(
        "--exclude-special",
        action="store_true",
        help="Exclude tables with special characters (spaces, dots) in names"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of tables to process per batch (default: 10). Spark restarts between batches to free memory."
    )

    args = parser.parse_args()

    # Get failed tables
    failed_tables = []
    database = args.database
    target_schema = args.target_schema

    if args.report:
        logger.info(f"Reading failed tables from report: {args.report}")
        failed_tables, db_from_report, schema_from_report = get_failed_tables_from_report(args.report)

        if not database:
            database = db_from_report
        if not target_schema:
            target_schema = schema_from_report

        logger.info(f"Found {len(failed_tables)} failed tables in report")

    if args.tables:
        # Override with specified tables
        failed_tables = [t.strip() for t in args.tables.split(',')]
        logger.info(f"Using specified tables: {len(failed_tables)} tables")

    # Exclude tables with special characters if requested
    if args.exclude_special:
        special_tables = [t for t in failed_tables if ' ' in t or '.' in t]
        failed_tables = [t for t in failed_tables if ' ' not in t and '.' not in t]
        if special_tables:
            logger.info(f"Excluding {len(special_tables)} tables with special characters: {special_tables}")

    if not failed_tables:
        logger.error("No tables to retry. Use --report or --tables to specify tables.")
        return 1

    if not database or not target_schema:
        logger.error("Database and target schema are required. Use --database and --target-schema")
        return 1

    # Skip already migrated tables if requested
    if args.skip_migrated and target_schema:
        logger.info("Checking for already migrated tables in StarRocks...")
        migrated = get_migrated_tables(target_schema)

        original_count = len(failed_tables)
        tables_to_retry = []
        already_done = []

        for table in failed_tables:
            sanitized = sanitize_table_name(table)
            if sanitized in migrated:
                already_done.append(table)
            else:
                tables_to_retry.append(table)

        if already_done:
            logger.info(f"Skipping {len(already_done)} tables already migrated: {already_done[:5]}{'...' if len(already_done) > 5 else ''}")

        failed_tables = tables_to_retry
        logger.info(f"Tables to retry: {len(failed_tables)} (skipped {original_count - len(failed_tables)} already migrated)")

    # List tables if requested
    if args.list_only:
        print(f"\nFailed tables to retry ({len(failed_tables)}):")
        for i, table in enumerate(failed_tables, 1):
            print(f"  {i}. {table}")
        return 0

    if not failed_tables:
        logger.info("No tables need to be retried - all are already migrated!")
        return 0

    # Retry migration
    start_time = datetime.now()

    try:
        result = retry_migration(
            database=database,
            target_schema=target_schema,
            table_list=failed_tables,
            create_tables=not args.skip_create_tables,
            only_s3=args.only_s3,
            batch_size=args.batch_size
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n{'='*60}")
        print(f"Retry Migration Summary")
        print(f"{'='*60}")
        print(f"Source Database: {database}")
        print(f"Target Schema: {target_schema}")
        print(f"Total Tables: {result['total_tables']}")
        print(f"Successful: {result['successful']}")
        print(f"Failed: {result['failed']}")
        print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"{'='*60}")

        # List still-failed tables if any
        if result['failed'] > 0:
            print("\nStill Failed Tables:")
            for r in result['results']:
                if r['status'] == 'failed':
                    print(f"  - {r['table_name']}: {r.get('error', 'Unknown error')[:80]}")

        # Save retry report
        report_file = f"mysql_retry_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(result['results'], f, indent=2, default=str)
        print(f"\nReport saved to: {report_file}")

        return 0 if result['failed'] == 0 else 1

    except Exception as e:
        logger.error(f"Retry migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Cleanup
        try:
            from app.services.spark_pipeline import pipeline
            from app.services.schema_mapper import mapper
            from app.services.partition_optimizer import optimizer

            pipeline.close()
            mapper.close()
            optimizer.close()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


if __name__ == "__main__":
    sys.exit(main())
