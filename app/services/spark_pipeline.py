"""
Spark Pipeline Service - Core ETL Logic
Handles MSSQL → Iceberg → StarRocks data migration
"""

import os
import logging
from typing import Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkPipeline:
    """Core Spark pipeline for MSSQL to Iceberg to StarRocks migration"""

    def __init__(self):
        self.spark: Optional[SparkSession] = None

    def get_spark_session(self) -> SparkSession:
        """
        Create or get existing Spark session with Iceberg and AWS Glue configuration
        """
        if self.spark is not None:
            try:
                # Check if the underlying SparkContext is active
                if not self.spark.sparkContext._jsc.sc().isStopped():
                    return self.spark
                logger.warning("Spark session found but context is stopped. Re-initializing...")
                self.spark = None
            except Exception as e:
                logger.warning(f"Error checking Spark session status: {e}. Re-initializing...")
                # Force cleanup on any exception
                try:
                    self.spark.stop()
                except:
                    pass
                self.spark = None

        logger.info("Initializing Spark session with Iceberg and AWS Glue integration...")

        # Set AWS credentials as environment variables
        os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
        os.environ["AWS_REGION"] = settings.aws_region

        # JAR files path
        jars_dir = os.path.join(os.path.dirname(__file__), "../../jars")
        jar_files = [
            "iceberg-spark-runtime-3.5_2.12-1.7.1.jar",
            "iceberg-aws-bundle-1.7.1.jar",
            "aws-java-sdk-bundle-1.12.648.jar",
            "hadoop-aws-3.3.4.jar",
            "hadoop-common-3.3.4.jar",
            "mssql-jdbc-12.4.2.jre11.jar",
            "mysql-connector-j-8.3.0.jar",
        ]
        jars_path = ",".join([os.path.join(jars_dir, jar) for jar in jar_files])

        self.spark = (
            SparkSession.builder.appName(settings.spark_app_name)
            .master(settings.spark_master)
            .config("spark.jars", jars_path)
            # Iceberg extensions
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # AWS Glue Catalog configuration
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.spark_catalog.type", "glue")
            .config(
                "spark.sql.catalog.spark_catalog.warehouse",
                settings.iceberg_warehouse_path,
            )
            .config("spark.sql.catalog.spark_catalog.glue.region", settings.aws_region)
            # S3 configurations
            .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                f"s3.{settings.aws_region}.amazonaws.com",
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            # AWS credentials provider
            .config(
                "spark.hadoop.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            # Performance optimizations
            .config("spark.driver.memory", settings.spark_driver_memory)
            .config("spark.executor.memory", settings.spark_executor_memory)
            .config("spark.executor.cores", str(settings.spark_executor_cores))
            .config(
                "spark.sql.shuffle.partitions",
                str(settings.spark_sql_shuffle_partitions),
            )
            # Memory management for Iceberg/Parquet operations
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            # Reduce memory pressure during Parquet compression
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB per partition
            .config("spark.sql.files.openCostInBytes", "4194304")  # 4MB
            # Iceberg write optimizations
            .config("spark.sql.iceberg.compression-codec", "snappy")
            .config("spark.sql.parquet.compression.codec", "snappy")
            # Limit Parquet row group size to reduce memory usage
            .config("spark.sql.parquet.rowGroupSize", "67108864")  # 64MB (reduced from default)
            # Garbage collection optimization
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:+UseStringDeduplication")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
        return self.spark

    def extract_mssql_table(
        self,
        schema_name: str,
        table_name: str,
        partition_config: Optional[Dict] = None,
    ) -> DataFrame:
        """
        Extract table from MSSQL using JDBC with parallel partitioning

        Args:
            schema_name: MSSQL schema name (e.g., 'dbo')
            table_name: Table name to extract
            partition_config: Optional partition configuration with keys:
                - column: partition column name
                - num_partitions: number of partitions
                - lower_bound: lower bound value
                - upper_bound: upper bound value

        Returns:
            Spark DataFrame with extracted data
        """
        spark = self.get_spark_session()
        logger.info(f"Extracting table: {schema_name}.{table_name} from MSSQL...")

        # Build JDBC read options
        jdbc_options = {
            "url": settings.mssql_jdbc_url,
            "dbtable": f"[{schema_name}].[{table_name}]",
            "user": settings.mssql_user,
            "password": settings.mssql_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "fetchsize": str(settings.jdbc_fetch_size),
        }

        # Add partitioning options if provided
        if partition_config:
            jdbc_options.update(
                {
                    "partitionColumn": partition_config.get("column"),
                    "numPartitions": str(partition_config.get("num_partitions", 20)),
                    "lowerBound": str(partition_config.get("lower_bound", 0)),
                    "upperBound": str(
                        partition_config.get("upper_bound", 10000000)
                    ),
                }
            )
            logger.info(
                f"Using partitioning: column={partition_config.get('column')}, "
                f"partitions={partition_config.get('num_partitions')}"
            )

        # Read from MSSQL
        df = spark.read.format("jdbc").options(**jdbc_options).load()

        # Add audit columns
        df = self._add_audit_columns(df, schema_name)

        # Cache for performance (will be unpersisted after use in orchestrator)
        df = df.cache()
        row_count = df.count()
        logger.info(f"Extracted {row_count:,} rows from {schema_name}.{table_name}")

        return df

    def _add_audit_columns(self, df: DataFrame, source_schema: str) -> DataFrame:
        """Add audit columns to DataFrame"""
        return df.withColumn(
            "migrated_at", F.lit(datetime.now().isoformat())
        ).withColumn("source_schema", F.lit(source_schema))

    def write_to_iceberg(
        self, df: DataFrame, schema_name: str, table_name: str
    ) -> None:
        """
        Steps 2, 3, 4: Write Spark DataFrame to Iceberg table (Parquet in S3 + Glue Metadata)
        """
        spark = self.get_spark_session()

        # AWS Glue requires lowercase identifiers
        glue_schema_name = schema_name.lower()
        table_name_lower = table_name.lower()

        logger.info(
            f"Steps 2-4: Writing to Iceberg: {glue_schema_name}.{table_name_lower} "
            f"(S3 + AWS Glue Catalog)..."
        )

        # Convert column names to lowercase (Glue requirement)
        df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

        # Create namespace if not exists
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{glue_schema_name}"
        )

        full_table_name = f"spark_catalog.{glue_schema_name}.{table_name_lower}"

        # Identify a date/timestamp column for partitioning to achieve the requested S3 folder structure
        # (DMSPipelineData / Date1 / file...)
        date_cols = [c for c, t in df.dtypes if t in ("date", "timestamp")]
        partition_col = None
        if date_cols:
            # Prefer common naming patterns for event/created dates
            preferred = [c for c in date_cols if "create" in c or "date" in c or "time" in c]
            partition_col = preferred[0] if preferred else date_cols[0]
            logger.info(f"Auto-partitioning Iceberg table by date column: {partition_col}")

        try:
            # Get row count before write (to avoid recomputation)
            row_count = df.count()
            
            writer = df.writeTo(full_table_name).using("iceberg")
            
            # Only partition by date if table has significant data and date column exists
            # For small tables, skip partitioning to save memory
            # For medium tables (100k-1M rows), use date partitioning for better query performance
            if partition_col and row_count > 1000:
                # Use days partitioning to create the Date-based folder structure on S3
                # This creates one partition per day, which can be memory-intensive
                # For tables > 100k rows, this is beneficial for query performance
                writer = writer.partitionedBy(F.days(partition_col))
                logger.info(f"Using date partitioning by: {partition_col} (table has {row_count:,} rows)")
            else:
                logger.info("Skipping date partitioning to optimize memory usage")
            
            writer.createOrReplace()
            
            logger.info(
                f"Successfully wrote {row_count:,} rows to S3 and registered in Glue Catalog"
            )
        except Exception as e:
            logger.error(f"Failed to write to Iceberg/S3: {e}")
            raise

    def read_from_iceberg(self, schema_name: str, table_name: str) -> DataFrame:
        """
        Read data from Iceberg table

        Args:
            schema_name: Schema name (lowercase for Glue)
            table_name: Table name (lowercase for Glue)

        Returns:
            Spark DataFrame with Iceberg data
        """
        spark = self.get_spark_session()

        glue_schema_name = schema_name.lower()
        table_name_lower = table_name.lower()
        full_table_name = f"{glue_schema_name}.{table_name_lower}"

        logger.info(f"Reading from Iceberg table: {full_table_name}")
        df = spark.table(full_table_name)

        row_count = df.count()
        logger.info(f"Read {row_count:,} rows from {full_table_name}")
        return df

    def load_to_starrocks(
        self,
        df: DataFrame,
        target_schema: str,
        table_name: str,
        mode: str = "append",
    ) -> None:
        """
        Load DataFrame to StarRocks using JDBC bulk insert

        Args:
            df: Spark DataFrame to load
            target_schema: StarRocks schema name
            table_name: StarRocks table name
            mode: Write mode ('append', 'overwrite')
        """
        logger.info(
            f"Loading to StarRocks: {target_schema}.{table_name} (mode={mode})..."
        )

        # Build StarRocks JDBC URL
        starrocks_url = f"{settings.starrocks_jdbc_url}/{target_schema}"

        # Determine partitions for JDBC write
        # For 20GB, we want higher parallelism, but capped to avoid too many small files/connections
        num_partitions = df.rdd.getNumPartitions()
        if num_partitions > 32: 
            num_partitions = 32 # Safe default cap for StarRocks ingestion
        
        # Ensure at least 4 partitions for performance
        if num_partitions < 4:
            num_partitions = 4
        
        logger.info(f"Writing to StarRocks with {num_partitions} partitions")

        # Write to StarRocks via JDBC
        df.write.format("jdbc").option("url", starrocks_url).option(
            "dbtable", f"`{target_schema}`.`{table_name}`"
        ).option("user", settings.starrocks_user).option(
            "password", settings.starrocks_password
        ).option(
            "driver", "com.mysql.cj.jdbc.Driver"
        ).option(
            "batchsize", str(settings.batch_size)
        ).option(
            "numPartitions", str(num_partitions)
        ).mode(
            mode
        ).save()

        logger.info(
            f"Successfully loaded {df.count():,} rows to StarRocks table "
            f"{target_schema}.{table_name}"
        )

    def migrate_table(
        self,
        source_schema: str,
        table_name: str,
        target_schema: str,
        partition_config: Optional[Dict] = None,
    ) -> Dict:
        """
        Complete 5-step migration pipeline as requested:
        1. MSSQL to Spark
        2. Spark + Iceberg -> Parquet
        3. Metadata -> AWS Glue
        4. Store in S3
        5. S3 -> StarRocks
        """
        start_time = datetime.now()
        logger.info(
            f"Starting 5-step migration: {source_schema}.{table_name} → "
            f"{target_schema}.{table_name}"
        )

        try:
            # Step 1: Extract from MSSQL and ingest into Spark
            logger.info("Step 1: Extracting data from MSSQL to Spark...")
            df = self.extract_mssql_table(source_schema, table_name, partition_config)
            mssql_count = df.count()
            logger.info(f"Ingested {mssql_count:,} rows into Spark")

            # Steps 2, 3, 4: Write to Iceberg (Parquet + Glue + S3)
            logger.info("Steps 2-4: Storing as Iceberg/Parquet in S3 with Glue Metadata...")
            iceberg_schema = f"{target_schema}{settings.target_schema_suffix}"
            self.write_to_iceberg(df, iceberg_schema, table_name)

            # Step 5: Ingest data FROM S3 into StarRocks
            logger.info("Step 5: Ingesting data FROM S3 (via Iceberg) into StarRocks...")
            # We explicitly read back from Iceberg/S3 to fulfill Step 5
            iceberg_df = self.read_from_iceberg(iceberg_schema, table_name)
            iceberg_count = iceberg_df.count()

            self.load_to_starrocks(iceberg_df, target_schema, table_name)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "source_schema": source_schema,
                "target_schema": target_schema,
                "table_name": table_name,
                "mssql_row_count": mssql_count,
                "iceberg_row_count": iceberg_count,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }

            logger.info(
                f"5-step migration completed successfully in {duration:.2f}s"
            )
            return result

        except Exception as e:
            logger.error(f"Migration failed for {source_schema}.{table_name}: {e}")
            raise

    def close(self):
        """Stop Spark session and release resources"""
        if self.spark:
            try:
                logger.info("Stopping Spark session...")
                self.spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")
            finally:
                self.spark = None
                logger.info("Spark session reference cleared")


# Global pipeline instance
pipeline = SparkPipeline()
