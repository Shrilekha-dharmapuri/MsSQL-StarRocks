#!/usr/bin/env python3
"""
MySQL to StarRocks Migration Script

This script provides command-line interface for migrating tables from MySQL
to StarRocks using the same pipeline flow:
1. Data is extracted from MySQL and ingested into Apache Spark
2. Spark integrates with Apache Iceberg to generate Parquet files
3. Metadata is managed using the AWS Glue Data Catalog
4. Generated Parquet files and metadata are stored in Amazon S3
5. Data from S3 is then ingested into StarRocks

Usage:
    # Migrate all tables from MySQL database
    python migrate_mysql.py --database mydb --target-schema analytics

    # Migrate specific tables
    python migrate_mysql.py --database mydb --target-schema analytics --tables users,orders,products

    # Migrate to S3 only (skip StarRocks loading)
    python migrate_mysql.py --database mydb --target-schema analytics --only-s3

    # Resume migration from a specific table
    python migrate_mysql.py --database mydb --target-schema analytics --resume-from users

    # Migrate a single table
    python migrate_mysql.py --database mydb --target-schema analytics --table users
"""

import argparse
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def migrate_single_table(
    database: str,
    table_name: str,
    target_schema: str,
    create_table: bool = True,
    auto_partition: bool = True,
    only_s3: bool = False
):
    """Migrate a single table from MySQL to StarRocks"""
    from app.services.migration_orchestrator import orchestrator

    logger.info(f"Starting single table migration: {database}.{table_name}")

    result = orchestrator.migrate_mysql_table(
        source_database=database,
        table_name=table_name,
        target_schema=target_schema,
        create_table=create_table,
        auto_partition=auto_partition,
        only_s3=only_s3
    )

    return result


def migrate_database(
    database: str,
    target_schema: str,
    table_list: list = None,
    create_tables: bool = True,
    only_s3: bool = False,
    resume_from: str = None
):
    """Migrate all (or specified) tables from MySQL database"""
    from app.services.migration_orchestrator import orchestrator

    logger.info(f"Starting database migration: {database} -> {target_schema}")

    result = orchestrator.migrate_mysql_schema(
        source_database=database,
        target_schema=target_schema,
        table_list=table_list,
        create_tables=create_tables,
        only_s3=only_s3,
        resume_from=resume_from
    )

    return result


def list_mysql_tables(database: str):
    """List all tables in MySQL database"""
    from app.services.schema_mapper import mapper

    tables = mapper.get_mysql_tables(database)
    return tables


def validate_migration(database: str, table_name: str, target_schema: str):
    """Validate migration by comparing row counts"""
    from app.services.migration_orchestrator import orchestrator

    result = orchestrator.validate_mysql_migration(
        source_database=database,
        table_name=table_name,
        target_schema=target_schema
    )

    return result


def main():
    parser = argparse.ArgumentParser(
        description="MySQL to StarRocks Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Required arguments
    parser.add_argument(
        "--database", "-d",
        required=True,
        help="MySQL source database name"
    )
    parser.add_argument(
        "--target-schema", "-t",
        required=True,
        help="StarRocks target schema name"
    )

    # Optional arguments
    parser.add_argument(
        "--table",
        help="Single table name to migrate (mutually exclusive with --tables)"
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables to migrate (e.g., users,orders,products)"
    )
    parser.add_argument(
        "--only-s3",
        action="store_true",
        help="Only migrate to S3/Iceberg, skip StarRocks loading"
    )
    parser.add_argument(
        "--resume-from",
        help="Resume migration from this table (inclusive)"
    )
    parser.add_argument(
        "--skip-create-tables",
        action="store_true",
        help="Skip creating StarRocks tables (use if tables already exist)"
    )
    parser.add_argument(
        "--no-partition",
        action="store_true",
        help="Disable automatic partitioning"
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all tables in the MySQL database and exit"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate migration by comparing row counts"
    )
    parser.add_argument(
        "--validate-table",
        help="Validate a specific table migration"
    )

    args = parser.parse_args()

    # Handle list tables
    if args.list_tables:
        logger.info(f"Listing tables in MySQL database: {args.database}")
        tables = list_mysql_tables(args.database)
        print(f"\nFound {len(tables)} tables in {args.database}:")
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table}")
        return 0

    # Handle validation
    if args.validate_table:
        logger.info(f"Validating migration for table: {args.validate_table}")
        result = validate_migration(args.database, args.validate_table, args.target_schema)
        print(f"\nValidation Result:")
        print(f"  Table: {result['table_name']}")
        print(f"  MySQL Count: {result['mysql_count']:,}")
        print(f"  StarRocks Count: {result['starrocks_count']:,}")
        print(f"  Status: {'✓ PASSED' if result['is_valid'] else '✗ FAILED'}")
        if not result['is_valid']:
            print(f"  Difference: {result['difference']:,} rows")
        return 0 if result['is_valid'] else 1

    start_time = datetime.now()

    try:
        # Single table migration
        if args.table:
            if args.tables:
                logger.error("Cannot specify both --table and --tables")
                return 1

            result = migrate_single_table(
                database=args.database,
                table_name=args.table,
                target_schema=args.target_schema,
                create_table=not args.skip_create_tables,
                auto_partition=not args.no_partition,
                only_s3=args.only_s3
            )

            print(f"\n{'='*60}")
            print(f"Migration Result for {args.table}")
            print(f"{'='*60}")
            print(f"Status: {result['status']}")
            if result['status'] == 'success':
                print(f"Rows Migrated: {result.get('mysql_row_count', 'N/A'):,}")
            else:
                print(f"Error: {result.get('error', 'Unknown error')}")

            return 0 if result['status'] == 'success' else 1

        # Multiple tables or all tables migration
        table_list = None
        if args.tables:
            table_list = [t.strip() for t in args.tables.split(',')]
            logger.info(f"Migrating specific tables: {table_list}")

        result = migrate_database(
            database=args.database,
            target_schema=args.target_schema,
            table_list=table_list,
            create_tables=not args.skip_create_tables,
            only_s3=args.only_s3,
            resume_from=args.resume_from
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n{'='*60}")
        print(f"Migration Summary")
        print(f"{'='*60}")
        print(f"Source Database: {args.database}")
        print(f"Target Schema: {args.target_schema}")
        print(f"Total Tables: {result['total_tables']}")
        print(f"Successful: {result['successful']}")
        print(f"Failed: {result['failed']}")
        print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"{'='*60}")

        # List failed tables if any
        if result['failed'] > 0:
            print("\nFailed Tables:")
            for r in result['results']:
                if r['status'] == 'failed':
                    print(f"  - {r['table_name']}: {r.get('error', 'Unknown error')}")

        return 0 if result['failed'] == 0 else 1

    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
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
