"""
FastAPI Application - REST API for MSSQL to StarRocks Migration
Provides endpoints for table and schema migration
"""

import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, List
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from app.services.migration_orchestrator import orchestrator
from app.config import settings
import os

# Configure logging to both file and console
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# File handler with rotation (max 50MB, keep 5 backup files)
file_handler = RotatingFileHandler(
    f"{log_dir}/migration.log",
    maxBytes=50*1024*1024,  # 50MB
    backupCount=5
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="MSSQL to StarRocks Migration API",
    description="Migration pipeline using Apache Iceberg and AWS Glue Catalog",
    version="1.0.0",
)

# Track migration jobs
migration_jobs = {}
job_counter = 0


# Request/Response Models
class TableMigrationRequest(BaseModel):
    """Request model for single table migration"""

    source_schema: str = Field(..., description="MSSQL source schema name")
    table_name: str = Field(..., description="Table name to migrate")
    target_schema: str = Field(..., description="Target schema name (without suffix)")
    create_table: bool = Field(
        default=True, description="Whether to create StarRocks table first"
    )
    auto_partition: bool = Field(
        default=True, description="Whether to auto-detect partition configuration"
    )
    only_s3: bool = Field(
        default=False, description="Whether to stop after uploading to S3/Iceberg"
    )


class SchemaMigrationRequest(BaseModel):
    """Request model for schema migration"""

    source_schema: str = Field(..., description="MSSQL source schema name")
    target_schema: str = Field(..., description="Target schema name")
    table_list: Optional[List[str]] = Field(
        default=None, description="Optional list of specific tables (None = all tables)"
    )
    create_tables: bool = Field(
        default=True, description="Whether to create StarRocks tables"
    )
    only_s3: bool = Field(
        default=False, description="Whether to stop after uploading to S3/Iceberg"
    )
    resume_from: Optional[str] = Field(
        default=None, description="Skip all tables until this table name is found (inclusive)"
    )


class MigrationResponse(BaseModel):
    """Response model for migration requests"""

    job_id: int = Field(..., description="Job ID for tracking")
    status: str = Field(..., description="Job status")
    message: str = Field(..., description="Status message")


class JobStatusResponse(BaseModel):
    """Response model for job status queries"""

    job_id: int
    status: str
    result: Optional[dict] = None


# Background task functions
def run_table_migration(job_id: int, request: TableMigrationRequest):
    """Background task for table migration"""
    try:
        logger.info(f"Starting table migration job {job_id}")

        migration_jobs[job_id]["status"] = "running"

        result = orchestrator.migrate_table(
            source_schema=request.source_schema,
            table_name=request.table_name,
            target_schema=request.target_schema,
            create_table=request.create_table,
            auto_partition=request.auto_partition,
            only_s3=request.only_s3,
        )

        migration_jobs[job_id]["status"] = "completed"
        migration_jobs[job_id]["result"] = result

        logger.info(f"Table migration job {job_id} completed successfully")

    except Exception as e:
        logger.error(f"Table migration job {job_id} failed: {e}")
        migration_jobs[job_id]["status"] = "failed"
        migration_jobs[job_id]["error"] = str(e)


def run_schema_migration(job_id: int, request: SchemaMigrationRequest):
    """Background task for schema migration"""
    try:
        logger.info(f"Starting schema migration job {job_id}")

        migration_jobs[job_id]["status"] = "running"

        result = orchestrator.migrate_schema(
            source_schema=request.source_schema,
            target_schema=request.target_schema,
            table_list=request.table_list,
            create_tables=request.create_tables,
            only_s3=request.only_s3,
            resume_from=request.resume_from,
        )

        migration_jobs[job_id]["status"] = "completed"
        migration_jobs[job_id]["result"] = result

        # Save report
        report_file = f"migration_report_job_{job_id}.json"
        orchestrator.save_report(report_file)
        migration_jobs[job_id]["report_file"] = report_file

        logger.info(f"Schema migration job {job_id} completed successfully")

    except Exception as e:
        logger.error(f"Schema migration job {job_id} failed: {e}")
        migration_jobs[job_id]["status"] = "failed"
        migration_jobs[job_id]["error"] = str(e)


# API Endpoints
@app.get("/", tags=["Root"])
def read_root():
    """Root endpoint with API information"""
    return {
        "name": "MSSQL to StarRocks Migration API",
        "version": "1.0.0",
        "description": "Migration pipeline using Apache Iceberg and AWS Glue Catalog",
        "endpoints": {
            "/health": "Health check",
            "/migrate/table": "Migrate single table",
            "/migrate/schema": "Migrate entire schema",
            "/migration/status/{job_id}": "Check migration job status",
        },
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "MSSQL to StarRocks Migration",
        "configuration": {
            "mssql_host": settings.mssql_host,
            "starrocks_host": settings.starrocks_host,
            "s3_bucket": settings.s3_bucket,
            "aws_region": settings.aws_region,
        },
    }


@app.post("/migrate/table", response_model=MigrationResponse, tags=["Migration"])
def migrate_table(request: TableMigrationRequest, background_tasks: BackgroundTasks):
    """
    Migrate a single table from MSSQL to StarRocks via Iceberg

    This endpoint starts a background migration job and returns immediately with a job ID.
    Use the `/migration/status/{job_id}` endpoint to check progress.
    """
    global job_counter
    job_counter += 1
    job_id = job_counter

    # Initialize job tracking
    migration_jobs[job_id] = {
        "status": "pending",
        "request": request.dict(),
        "result": None,
    }

    # Start background task
    background_tasks.add_task(run_table_migration, job_id, request)

    logger.info(
        f"Created migration job {job_id} for table {request.source_schema}.{request.table_name}"
    )

    return MigrationResponse(
        job_id=job_id,
        status="pending",
        message=f"Migration job {job_id} started for table {request.table_name}",
    )


@app.post("/migrate/schema", response_model=MigrationResponse, tags=["Migration"])
def migrate_schema(request: SchemaMigrationRequest, background_tasks: BackgroundTasks):
    """
    Migrate entire schema from MSSQL to StarRocks via Iceberg

    This endpoint starts a background migration job for all tables in the schema.
    Use the `/migration/status/{job_id}` endpoint to check progress.
    """
    global job_counter
    job_counter += 1
    job_id = job_counter

    # Initialize job tracking
    migration_jobs[job_id] = {
        "status": "pending",
        "request": request.dict(),
        "result": None,
    }

    # Start background task
    background_tasks.add_task(run_schema_migration, job_id, request)

    logger.info(f"Created schema migration job {job_id} for schema {request.source_schema}")

    return MigrationResponse(
        job_id=job_id,
        status="pending",
        message=f"Schema migration job {job_id} started for {request.source_schema}",
    )


@app.get(
    "/migration/status/{job_id}",
    response_model=JobStatusResponse,
    tags=["Migration"],
)
def get_migration_status(job_id: int):
    """
    Get status of a migration job

    Returns the current status and results (if completed) of a migration job.
    """
    if job_id not in migration_jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job = migration_jobs[job_id]

    return JobStatusResponse(
        job_id=job_id, status=job["status"], result=job.get("result")
    )


@app.get("/config", tags=["Configuration"])
def get_configuration():
    """Get current configuration settings (sensitive values masked)"""
    return {
        "mssql": {
            "host": settings.mssql_host,
            "port": settings.mssql_port,
            "database": settings.mssql_database,
            "schema": settings.mssql_schema,
        },
        "starrocks": {
            "host": settings.starrocks_host,
            "port": settings.starrocks_port,
            "database": settings.starrocks_database,
        },
        "aws": {
            "region": settings.aws_region,
            "s3_bucket": settings.s3_bucket,
            "warehouse_path": settings.iceberg_warehouse_path,
        },
        "spark": {
            "master": settings.spark_master,
            "driver_memory": settings.spark_driver_memory,
            "executor_memory": settings.spark_executor_memory,
        },
    }


class S3ToStarRocksRequest(BaseModel):
    """Request model for loading from S3 to StarRocks"""

    target_schema: str = Field(
        default="WorldZone_glue", description="StarRocks target schema name"
    )
    table_list: Optional[List[str]] = Field(
        default=None, description="Specific tables to load (None = all tables)"
    )
    skip_until: Optional[str] = Field(
        default=None, description="Skip tables until this table (exclusive - will start AFTER this table)"
    )
    exclude_tables: Optional[List[str]] = Field(
        default=None, description="List of table names to exclude from migration (e.g., large tables)"
    )


def run_s3_to_starrocks_migration(job_id: int, request: S3ToStarRocksRequest):
    """Background task for S3 to StarRocks migration"""
    import boto3
    from app.services.spark_pipeline import pipeline
    from app.services.schema_mapper import mapper
    import time

    try:
        migration_jobs[job_id]["status"] = "running"
        logger.info(f"Starting S3 to StarRocks migration job {job_id}")

        # Get tables from Glue
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

        # Filter if specific tables requested
        if request.table_list:
            tables = [t for t in tables if t.lower() in [tl.lower() for tl in request.table_list]]

        # Sort tables alphabetically
        tables = sorted(tables)

        # Exclude tables if specified (e.g., large tables)
        if request.exclude_tables:
            exclude_lower = [t.lower() for t in request.exclude_tables]
            original_count = len(tables)
            tables = [t for t in tables if t.lower() not in exclude_lower]
            excluded_count = original_count - len(tables)
            logger.info(f"Excluded {excluded_count} tables: {', '.join(request.exclude_tables[:5])}{'...' if len(request.exclude_tables) > 5 else ''}")

        # Skip tables until specified table (for resuming migrations)
        if request.skip_until:
            skip_table = request.skip_until.lower()
            try:
                skip_index = [t.lower() for t in tables].index(skip_table)
                tables = tables[skip_index + 1:]  # Start from the table AFTER skip_until
                logger.info(f"Skipping tables until '{request.skip_until}' - resuming from table {skip_index + 2}/{len(tables) + skip_index + 1}")
            except ValueError:
                logger.warning(f"Table '{request.skip_until}' not found in table list - processing all tables")

        logger.info(f"Found {len(tables)} tables to load from S3 to StarRocks")

        results = {"success": [], "failed": []}

        for idx, table_name in enumerate(tables, 1):
            try:
                logger.info(f"[{idx}/{len(tables)}] Loading {table_name}...")

                # Create table in StarRocks with retry logic
                max_retries = 3
                for attempt in range(1, max_retries + 1):
                    try:
                        mapper.create_starrocks_table(
                            schema_name=settings.mssql_schema,
                            table_name=table_name,
                            target_schema=request.target_schema
                        )
                        break  # Success
                    except Exception as e:
                        error_str = str(e).lower()
                        if "already exists" in error_str or "1050" in error_str:
                            break  # Table exists, continue
                        elif "lost connection" in error_str or "2013" in error_str or "110" in error_str:
                            if attempt < max_retries:
                                logger.warning(f"Connection error creating table (attempt {attempt}/{max_retries}), retrying in 5s...")
                                time.sleep(5)
                                continue
                        raise  # Other errors or max retries exceeded

                # Read from S3
                df = pipeline.read_from_iceberg(glue_database, table_name)

                # Load to StarRocks (no pre-count to avoid double scan)
                pipeline.load_to_starrocks(
                    df, request.target_schema, table_name, mode="append"
                )

                # Get row count from StarRocks after write
                import mysql.connector
                conn = mysql.connector.connect(
                    host=settings.starrocks_host,
                    port=settings.starrocks_port,
                    user=settings.starrocks_user,
                    password=settings.starrocks_password,
                    database=request.target_schema,
                    connect_timeout=30
                )
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM `{request.target_schema}`.`{table_name}`")
                row_count = cursor.fetchone()[0]
                cursor.close()
                conn.close()

                results["success"].append({"table": table_name, "rows": row_count})
                logger.info(f"✓ {table_name}: {row_count:,} rows loaded")

                # Small delay to prevent connection exhaustion
                time.sleep(0.5)

                # Periodic cleanup
                if idx % 50 == 0:
                    logger.info("Performing periodic cleanup...")
                    mapper._close_connection()
                    pipeline.close()
                    import gc
                    gc.collect()
                    time.sleep(10)

            except Exception as e:
                logger.error(f"✗ Failed to load {table_name}: {e}")
                results["failed"].append({"table": table_name, "error": str(e)})

        migration_jobs[job_id]["status"] = "completed"
        migration_jobs[job_id]["result"] = results

        logger.info(f"S3 to StarRocks migration job {job_id} completed")

    except Exception as e:
        migration_jobs[job_id]["status"] = "failed"
        migration_jobs[job_id]["result"] = {"error": str(e)}
        logger.error(f"S3 to StarRocks migration job {job_id} failed: {e}")


@app.post("/load/s3-to-starrocks", response_model=MigrationResponse, tags=["Migration"])
def load_s3_to_starrocks(
    request: S3ToStarRocksRequest, background_tasks: BackgroundTasks
):
    """
    Load tables from S3/Iceberg to StarRocks

    This endpoint reads tables from AWS Glue Catalog and loads them to StarRocks.
    Use this after completing S3-only migration to load data to StarRocks.
    """
    global job_counter
    job_counter += 1
    job_id = job_counter

    migration_jobs[job_id] = {
        "status": "pending",
        "request": request.dict(),
        "result": None,
    }

    background_tasks.add_task(run_s3_to_starrocks_migration, job_id, request)

    logger.info(f"Created S3 to StarRocks migration job {job_id}")

    return MigrationResponse(
        job_id=job_id,
        status="pending",
        message=f"S3 to StarRocks migration job {job_id} started",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
