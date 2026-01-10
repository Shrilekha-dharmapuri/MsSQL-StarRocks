"""
Partition Optimizer - Intelligent JDBC Partitioning
Auto-detects optimal partition columns and calculates bounds
"""

import logging
import pymssql
from typing import Dict, Optional, List
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PartitionOptimizer:
    """Optimizes JDBC partitioning for parallel data extraction"""

    def __init__(self):
        self.connection = None

    def _get_connection(self):
        """Get MSSQL connection"""
        if not self.connection:
            self.connection = pymssql.connect(
                server=settings.mssql_host,
                port=settings.mssql_port,
                user=settings.mssql_user,
                password=settings.mssql_password,
                database=settings.mssql_database,
            )
        return self.connection

    def get_numeric_columns(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get all numeric columns that can be used for partitioning"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s 
          AND TABLE_NAME = %s
          AND DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint')
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (schema_name, table_name))
        columns = cursor.fetchall()
        cursor.close()

        return columns

    def get_datetime_columns(self, schema_name: str, table_name: str) -> List[str]:
        """Get datetime columns for partitioning"""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s 
          AND TABLE_NAME = %s
          AND DATA_TYPE IN ('datetime', 'datetime2', 'date')
          AND COLUMN_NAME IN ('CreatedAt', 'UpdatedAt', 'created_at', 'updated_at', 'CreateDate', 'ModifiedDate')
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (schema_name, table_name))
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return columns

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get total row count for table"""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def calculate_partition_count(self, row_count: int) -> int:
        """Calculate optimal number of partitions based on row count"""
        if row_count < 10000:
            return 2
        elif row_count < 100000:
            return 4
        elif row_count < 500000:
            # Optimized for 100k-500k range (4 lakh = 400k falls here)
            # 8-12 partitions gives ~33k-50k rows per partition
            return 12
        elif row_count < 1000000:
            return 16
        elif row_count < 10000000:
            return 20
        else:
            # For large tables (>10M rows), aim for ~500k rows per partition
            # Cap at 100 partitions to avoid overwhelming the database
            return min(row_count // 500000, 100)

    def get_column_bounds(
        self, schema_name: str, table_name: str, column_name: str
    ) -> tuple:
        """Get min and max values for a column"""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = f"""
        SELECT 
            MIN([{column_name}]) as min_val,
            MAX([{column_name}]) as max_val
        FROM [{schema_name}].[{table_name}]
        WHERE [{column_name}] IS NOT NULL
        """

        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            return (result[0], result[1])
        else:
            return (0, 1000000)  # Default bounds

    def get_optimal_partition_config(
        self, schema_name: str, table_name: str
    ) -> Optional[Dict]:
        """
        Auto-detect optimal partition configuration for a table

        Returns:
            Dictionary with keys: column, num_partitions, lower_bound, upper_bound
            or None if no suitable partition column found
        """
        logger.info(
            f"Analyzing partition strategy for {schema_name}.{table_name}..."
        )

        # Get row count
        row_count = self.get_table_row_count(schema_name, table_name)
        logger.info(f"Table has {row_count:,} rows")

        # For testing purposes, we want to try partitioning even for small tables
        # if row_count < 10000:
        #     logger.info("Small table, no partitioning needed")
        #     return None

        # Priority 1: Try datetime columns
        datetime_cols = self.get_datetime_columns(schema_name, table_name)
        if datetime_cols:
            col_name = datetime_cols[0]
            logger.info(f"Found datetime column for partitioning: {col_name}")

            # For datetime columns, we'll convert to numeric (epoch) in Spark
            # Here we just return the column name
            num_partitions = self.calculate_partition_count(row_count)

            return {
                "column": col_name,
                "num_partitions": num_partitions,
                "lower_bound": 0,  # Will be determined by Spark
                "upper_bound": 999999999,  # Placeholder for datetime
            }

        # Priority 2: Try numeric columns
        numeric_cols = self.get_numeric_columns(schema_name, table_name)
        if numeric_cols:
            col_name = numeric_cols[0]["COLUMN_NAME"]
            logger.info(f"Found numeric column for partitioning: {col_name}")

            # Get actual bounds
            lower_bound, upper_bound = self.get_column_bounds(
                schema_name, table_name, col_name
            )
            num_partitions = self.calculate_partition_count(row_count)

            logger.info(
                f"Partition config: column={col_name}, partitions={num_partitions}, "
                f"range=[{lower_bound}, {upper_bound}]"
            )

            return {
                "column": col_name,
                "num_partitions": num_partitions,
                "lower_bound": int(lower_bound),
                "upper_bound": int(upper_bound),
            }

        logger.warning(
            "No suitable partition column found, will use single partition"
        )
        return None

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None


# Global optimizer instance
optimizer = PartitionOptimizer()
