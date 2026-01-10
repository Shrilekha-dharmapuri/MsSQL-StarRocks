"""
Migration Orchestrator - High-level pipeline orchestration
Handles schema and table migrations with error handling and reporting
"""

import logging
import json
import time
from typing import List, Dict, Optional
from datetime import datetime
from app.services.spark_pipeline import pipeline
from app.services.schema_mapper import mapper
from app.services.partition_optimizer import optimizer
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MigrationOrchestrator:
    """Orchestrates migrations with retry logic and reporting"""

    def __init__(self):
        self.migration_results = []

    def migrate_table(
        self,
        source_schema: str,
        table_name: str,
        target_schema: str,
        create_table: bool = True,
        auto_partition: bool = True,
        only_s3: bool = False,
    ) -> Dict:
        """
        Migrate a single table with retry logic

        Args:
            source_schema: MSSQL source schema name
            table_name: Table name to migrate
            target_schema: Target schema name (without suffix)
            create_table: Whether to create StarRocks table first
            auto_partition: Whether to auto-detect partition configuration
            only_s3: If True, stops after writing to S3/Iceberg

        Returns:
            Migration result dictionary
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting migration: {source_schema}.{table_name} (S3 Only: {only_s3})")
        logger.info(f"{'='*60}\n")

        result = {
            "source_schema": source_schema,
            "table_name": table_name,
            "target_schema": target_schema,
            "status": "pending",
            "error": None,
            "attempts": 0,
            "only_s3": only_s3
        }

        # Step 1: Create StarRocks table if requested and NOT only_s3
        if create_table and not only_s3:
            # Retry table creation to handle connection timeouts
            for attempt in range(1, 4):  # Try 3 times
                try:
                    logger.info(f"Step 1: Creating StarRocks table (attempt {attempt}/3)...")
                    mapper.create_starrocks_table(
                        source_schema, table_name, target_schema
                    )
                    result["table_created"] = True
                    break  # Success, exit retry loop
                except Exception as e:
                    error_str = str(e)

                    # Check if it's a connection error
                    is_connection_error = (
                        "Can't connect" in error_str or
                        "Connection refused" in error_str or
                        "Lost connection" in error_str or
                        "2003" in error_str or
                        "2013" in error_str or
                        "Timeout" in error_str
                    )

                    if is_connection_error and attempt < 3:
                        logger.warning(f"Connection error on attempt {attempt}: {e}")
                        logger.info(f"Retrying in {attempt * 2} seconds...")
                        time.sleep(attempt * 2)  # Exponential backoff: 2s, 4s
                        continue
                    else:
                        logger.error(f"Failed to create table: {e}")
                        result["status"] = "failed"
                        result["error"] = f"Table creation failed: {str(e)}"
                        return result

        # Step 2: Get partition configuration
        partition_config = None
        if auto_partition:
            try:
                logger.info("Step 2: Analyzing partition strategy...")
                partition_config = optimizer.get_optimal_partition_config(
                    source_schema, table_name
                )
                result["partition_config"] = partition_config
            except Exception as e:
                logger.warning(f"Failed to get partition config: {e}")
                # Continue without partitioning

        # Step 3: Execute migration with retry
        for attempt in range(1, settings.max_retry_attempts + 1):
            result["attempts"] = attempt

            try:
                logger.info(
                    f"Step 3: Executing migration (attempt {attempt}/{settings.max_retry_attempts})..."
                )

                # Custom logic to handle only_s3 inside pipeline if needed, 
                # but here we can just call methods selectively or update pipeline.migrate_table
                
                # Step 3a: Extract from MSSQL
                df = pipeline.extract_mssql_table(source_schema, table_name, partition_config)
                mssql_count = df.count()
                result["mssql_row_count"] = mssql_count

                # Step 3b: Write to Iceberg (S3)
                iceberg_schema = f"{target_schema}{settings.target_schema_suffix}"
                pipeline.write_to_iceberg(df, iceberg_schema, table_name)
                result["iceberg_row_count"] = mssql_count # Initial count
                
                # CRITICAL: Unpersist the cached DataFrame to free memory
                try:
                    df.unpersist()
                    logger.debug("Unpersisted source DataFrame to free memory")
                except Exception as e:
                    logger.warning(f"Failed to unpersist DataFrame: {e}")
                
                # If only_s3 is True, we stop here
                if only_s3:
                    result["status"] = "success"
                    result["message"] = "Data successfully stored in S3/Iceberg. StarRocks step skipped."
                    logger.info(f"✓ S3-Only Migration completed: {mssql_count:,} rows landed in S3.")
                    break

                # Step 3c: Load to StarRocks
                # strictly following Step 5: Data from S3 is then ingested into StarRocks
                logger.info("Reading back from S3/Iceberg for StarRocks ingestion...")
                iceberg_df = pipeline.read_from_iceberg(iceberg_schema, table_name)
                pipeline.load_to_starrocks(iceberg_df, target_schema, table_name)
                
                # CRITICAL: Unpersist the Iceberg DataFrame to free memory
                try:
                    iceberg_df.unpersist()
                    logger.debug("Unpersisted Iceberg DataFrame to free memory")
                except Exception as e:
                    logger.warning(f"Failed to unpersist Iceberg DataFrame: {e}")

                # Success!
                result["status"] = "success"
                logger.info(
                    f"\n✓ Full Migration completed successfully: {table_name}\n"
                )
                break

            except Exception as e:
                error_str = str(e)
                logger.error(f"Attempt {attempt} failed: {error_str}")
                result["error"] = error_str

                # Check if it's an OutOfMemoryError or connection error (Spark crashed)
                is_oom_or_connection_error = (
                    "OutOfMemoryError" in error_str or
                    "Connection refused" in error_str or
                    "Py4JNetworkError" in error_str or
                    "Answer from Java side is empty" in error_str
                )

                # CRITICAL: Force close Spark session if it crashed
                if is_oom_or_connection_error:
                    logger.warning("Detected Spark session crash. Forcing cleanup...")
                    try:
                        pipeline.close()
                        logger.info("Spark session closed. Will reinitialize on retry.")
                    except Exception as close_err:
                        logger.warning(f"Failed to close pipeline after crash: {close_err}")
                        # Force reset the spark reference
                        pipeline.spark = None

                if attempt < settings.max_retry_attempts:
                    # Longer delay for OOM/connection errors to let JVM fully die
                    retry_delay = settings.retry_delay_seconds * 2 if is_oom_or_connection_error else settings.retry_delay_seconds
                    logger.info(
                        f"Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                else:
                    result["status"] = "failed"
                    logger.error(f"\n✗ Migration failed after {attempt} attempts\n")
                    
                    # Final cleanup attempt
                    try:
                        pipeline.close()
                    except Exception as close_err:
                        logger.warning(f"Failed to close pipeline after error: {close_err}")
                        pipeline.spark = None

        self.migration_results.append(result)
        return result

    def migrate_schema(
        self,
        source_schema: str,
        target_schema: str,
        table_list: Optional[List[str]] = None,
        create_tables: bool = True,
        only_s3: bool = False,
        resume_from: Optional[str] = None,
    ) -> Dict:
        """
        Migrate all tables in a schema

        Args:
            source_schema: MSSQL source schema name
            target_schema: Target schema name
            table_list: Optional list of specific tables to migrate (None = all)
            create_tables: Whether to create StarRocks tables
            only_s3: Whether to stop after S3/Iceberg phase
            resume_from: Table name to resume from (inclusive)

        Returns:
            Summary of migration results
        """
        logger.info(f"\n{'#'*60}")
        logger.info(f"# SCHEMA MIGRATION: {source_schema} → {target_schema}")
        if resume_from:
            logger.info(f"# Resuming from: {resume_from}")
        logger.info(f"{'#'*60}\n")

        start_time = datetime.now()

        # Get list of tables if not provided
        if table_list is None:
            logger.info("Discovering tables in source schema...")
            table_list = self._get_schema_tables(source_schema)
            logger.info(f"Found {len(table_list)} tables to migrate")

        # Migrate each table
        successful = 0
        failed = 0

        skipping = True if resume_from else False

        for idx, table_name in enumerate(table_list, 1):
            if skipping:
                if table_name == resume_from:
                    skipping = False
                    logger.info(f"Checking {table_name}: Found resume point. Starting migration.")
                else:
                    logger.info(f"[{idx}/{len(table_list)}] Skipping {table_name} (before resume point)")
                    continue

            logger.info(f"\n[{idx}/{len(table_list)}] Migrating table: {table_name}")

            result = self.migrate_table(
                source_schema=source_schema,
                table_name=table_name,
                target_schema=target_schema,
                create_table=create_tables,
                only_s3=only_s3,
            )

            if result["status"] == "success":
                successful += 1
            else:
                failed += 1

            # CRITICAL: Periodic cleanup every 50 tables to prevent memory buildup
            if idx % 50 == 0:
                logger.info(f"Performing periodic cleanup after {idx} tables...")
                try:
                    # Close MSSQL metadata connection
                    mapper._close_connection()

                    # Force Spark session cleanup
                    logger.info("Restarting Spark session to free memory...")
                    pipeline.close()

                    # Force garbage collection
                    import gc
                    gc.collect()

                    logger.info("Periodic cleanup completed. Continuing migration...")
                    time.sleep(5)  # Brief pause to let resources fully release
                except Exception as e:
                    logger.warning(f"Error during periodic cleanup: {e}")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Summary
        summary = {
            "source_schema": source_schema,
            "target_schema": target_schema,
            "total_tables": len(table_list),
            "successful": successful,
            "failed": failed,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "results": self.migration_results,
        }

        logger.info(f"\n{'#'*60}")
        logger.info(f"# MIGRATION SUMMARY")
        logger.info(f"{'#'*60}")
        logger.info(f"Total Tables: {len(table_list)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"{'#'*60}\n")

        return summary

    def _get_schema_tables(self, schema_name: str) -> List[str]:
        """Get list of all tables in a schema"""
        import pymssql

        conn = pymssql.connect(
            server=settings.mssql_host,
            port=settings.mssql_port,
            user=settings.mssql_user,
            password=settings.mssql_password,
            database=settings.mssql_database,
        )

        cursor = conn.cursor()
        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """

        cursor.execute(query, (schema_name,))
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        return tables

    def save_report(self, filepath: str = "migration_report.json"):
        """Save migration results to JSON file"""
        with open(filepath, "w") as f:
            json.dump(self.migration_results, f, indent=2)
        logger.info(f"Migration report saved to: {filepath}")

    def validate_migration(
        self, source_schema: str, table_name: str, target_schema: str
    ) -> Dict:
        """
        Validate migration by comparing row counts

        Returns:
            Validation result dictionary
        """
        logger.info(f"Validating migration for {table_name}...")

        # Get MSSQL row count
        mssql_count = mapper.get_table_row_count(source_schema, table_name)

        # Get StarRocks row count
        import mysql.connector

        conn = None
        cursor = None

        try:
            conn = mysql.connector.connect(
                host=settings.starrocks_host,
                port=settings.starrocks_port,
                user=settings.starrocks_user,
                password=settings.starrocks_password,
                database=target_schema,
                connect_timeout=30,
                pool_size=5,
                pool_name="starrocks_validation_pool",
                pool_reset_session=True
            )

            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{target_schema}`.`{table_name}`")
            starrocks_count = cursor.fetchone()[0]

            is_valid = mssql_count == starrocks_count

            result = {
                "table_name": table_name,
                "mssql_count": mssql_count,
                "starrocks_count": starrocks_count,
                "is_valid": is_valid,
                "difference": abs(mssql_count - starrocks_count),
            }

            if is_valid:
                logger.info(f"✓ Validation passed: {mssql_count:,} rows match")
            else:
                logger.warning(
                    f"✗ Validation failed: MSSQL={mssql_count:,}, "
                    f"StarRocks={starrocks_count:,}"
                )

            return result

        finally:
            # CRITICAL: Always close connections
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass


# Global orchestrator instance
orchestrator = MigrationOrchestrator()
