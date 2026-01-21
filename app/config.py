"""
Configuration management using Pydantic Settings
Loads environment variables from .env file and provides type-safe configuration access
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # MSSQL Source Database Configuration
    mssql_host: str = Field(..., description="MSSQL server hostname")
    mssql_port: int = Field(default=1433, description="MSSQL server port")
    mssql_user: str = Field(..., description="MSSQL username")
    mssql_password: str = Field(..., description="MSSQL password")
    mssql_database: str = Field(..., description="MSSQL database name")
    mssql_schema: str = Field(default="dbo", description="MSSQL default schema")
    mssql_encrypt: bool = Field(default=True, description="Use encryption for MSSQL connection")
    mssql_trust_server_certificate: bool = Field(
        default=False, description="Trust server certificate"
    )

    # MySQL Source Database Configuration
    mysql_source_host: Optional[str] = Field(default=None, description="MySQL source server hostname")
    mysql_source_port: int = Field(default=3306, description="MySQL source server port")
    mysql_source_user: Optional[str] = Field(default=None, description="MySQL source username")
    mysql_source_password: str = Field(default="", description="MySQL source password")
    mysql_source_database: Optional[str] = Field(default=None, description="MySQL source database name")
    mysql_source_charset: str = Field(default="utf8mb4", description="MySQL source character set")

    # StarRocks Target Database Configuration
    starrocks_host: Optional[str] = Field(default=None, description="StarRocks server hostname")
    starrocks_port: int = Field(default=9030, description="StarRocks query port")
    starrocks_user: Optional[str] = Field(default=None, description="StarRocks username")
    starrocks_password: str = Field(default="", description="StarRocks password")
    starrocks_database: Optional[str] = Field(
        default=None, description="StarRocks default database"
    )

    # AWS Configuration
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    aws_region: str = Field(default="ap-south-1", description="AWS region")
    s3_bucket: Optional[str] = Field(default=None, description="S3 bucket name for Iceberg data")
    s3_warehouse_path: str = Field(
        default="DMSPipelineData", description="S3 path prefix for warehouse"
    )

    # Iceberg Configuration
    iceberg_catalog_type: str = Field(default="glue", description="Iceberg catalog type")
    target_schema_suffix: str = Field(
        default="_glue", description="Suffix to append to target schema names"
    )

    # Apache Spark Configuration
    spark_master: str = Field(default="local[*]", description="Spark master URL")
    spark_app_name: str = Field(
        default="MSSQL-Iceberg-Pipeline", description="Spark application name"
    )
    spark_driver_memory: str = Field(default="8g", description="Spark driver memory")
    spark_executor_memory: str = Field(default="8g", description="Spark executor memory")
    spark_executor_cores: int = Field(default=4, description="Spark executor cores")
    spark_sql_shuffle_partitions: int = Field(
        default=8, description="Number of shuffle partitions"
    )

    # JDBC Configuration
    jdbc_fetch_size: int = Field(
        default=50000, description="JDBC fetch size for reading"
    )
    jdbc_partition_column: str = Field(
        default="id", description="Default partition column"
    )
    jdbc_num_partitions: int = Field(
        default=50, description="Number of JDBC partitions for parallel read"
    )
    jdbc_lower_bound: int = Field(default=0, description="Lower bound for partitioning")
    jdbc_upper_bound: int = Field(
        default=10000000, description="Upper bound for partitioning"
    )

    # Pipeline Configuration
    batch_size: int = Field(default=500000, description="Batch size for bulk inserts")
    max_retry_attempts: int = Field(default=3, description="Maximum retry attempts")
    retry_delay_seconds: int = Field(default=5, description="Delay between retries")
    process_timeout_seconds: int = Field(
        default=7200, description="Process timeout in seconds"
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"

    @property
    def iceberg_warehouse_path(self) -> str:
        """Full S3 warehouse path for Iceberg"""
        return f"s3a://{self.s3_bucket}/{self.s3_warehouse_path}"

    @property
    def mssql_jdbc_url(self) -> str:
        """MSSQL JDBC connection URL"""
        encrypt_str = "true" if self.mssql_encrypt else "false"
        trust_cert_str = "true" if self.mssql_trust_server_certificate else "false"
        return (
            f"jdbc:sqlserver://{self.mssql_host}:{self.mssql_port};"
            f"databaseName={self.mssql_database};"
            f"encrypt={encrypt_str};"
            f"trustServerCertificate={trust_cert_str}"
        )

    @property
    def starrocks_jdbc_url(self) -> str:
        """StarRocks JDBC connection URL"""
        db_part = f"/{self.starrocks_database}" if self.starrocks_database else ""
        return f"jdbc:mysql://{self.starrocks_host}:{self.starrocks_port}{db_part}"

    @property
    def mysql_source_jdbc_url(self) -> str:
        """MySQL Source JDBC connection URL"""
        if not self.mysql_source_host:
            return ""
        # Note: JDBC uses Java encoding names, not MySQL charset names
        # utf8mb4 in MySQL maps to UTF-8 in Java
        # The characterEncoding parameter must use Java charset names
        return (
            f"jdbc:mysql://{self.mysql_source_host}:{self.mysql_source_port}"
            f"/{self.mysql_source_database}?characterEncoding=UTF-8"
            f"&useUnicode=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
            f"&zeroDateTimeBehavior=convertToNull"
        )


# Global settings instance
settings = Settings()
