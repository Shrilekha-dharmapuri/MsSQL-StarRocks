"""
FastAPI Application - REST API for MSSQL to StarRocks Migration
Provides endpoints for table and schema migration
"""

import logging
from typing import Optional, List
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from app.services.migration_orchestrator import orchestrator
from app.config import settings

logging.basicConfig(level=logging.INFO)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
