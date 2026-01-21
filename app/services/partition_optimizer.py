"""
Partition Optimizer - Intelligent JDBC Partitioning
Auto-detects optimal partition columns and calculates bounds
Supports both MSSQL and MySQL source databases
"""

import logging
import pymssql
import mysql.connector
from typing import Dict, Optional, List
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PartitionOptimizer:
    """Optimizes JDBC partitioning for parallel data extraction from MSSQL or MySQL"""

    def __init__(self):
        self.mssql_connection = None
        self.mysql_connection = None
        # Keep backward compatibility
        self.connection = None

    def _get_connection(self):
        """Get MSSQL connection (backward compatibility)"""
        return self._get_mssql_connection()

    def _get_mssql_connection(self):
        """Get MSSQL connection"""
        if not self.mssql_connection:
            self.mssql_connection = pymssql.connect(
                server=settings.mssql_host,
                port=settings.mssql_port,
                user=settings.mssql_user,
                password=settings.mssql_password,
                database=settings.mssql_database,
            )
            self.connection = self.mssql_connection
        return self.mssql_connection

    def _get_mysql_connection(self):
        """Get MySQL source connection"""
        if not self.mysql_connection:
            self.mysql_connection = mysql.connector.connect(
                host=settings.mysql_source_host,
                port=settings.mysql_source_port,
                user=settings.mysql_source_user,
                password=settings.mysql_source_password,
                database=settings.mysql_source_database,
                charset=settings.mysql_source_charset,
                connect_timeout=30,
                autocommit=True
            )
        return self.mysql_connection

    # ==================== MSSQL Methods ====================

    def get_numeric_columns(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get all numeric columns that can be used for partitioning (MSSQL)"""
        conn = self._get_mssql_connection()
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
        """Get datetime columns for partitioning (MSSQL)"""
        conn = self._get_mssql_connection()
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
        """Get total row count for table (MSSQL)"""
        conn = self._get_mssql_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()

        return count

    # ==================== MySQL Methods ====================

    def get_mysql_numeric_columns(self, database_name: str, table_name: str) -> List[Dict]:
        """Get all numeric columns that can be used for partitioning (MySQL)

        Excludes tinyint(1) which is typically used as boolean in MySQL.
        """
        conn = self._get_mysql_connection()
        cursor = conn.cursor(dictionary=True)

        # Exclude tinyint(1) which is boolean, and bit which is also boolean
        # COLUMN_TYPE gives us the full type like 'tinyint(1)' or 'int(11)'
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint', 'mediumint', 'integer')
          AND NOT (DATA_TYPE = 'tinyint' AND COLUMN_TYPE = 'tinyint(1)')
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (database_name, table_name))
        columns = cursor.fetchall()
        cursor.close()

        return columns

    def get_mysql_datetime_columns(self, database_name: str, table_name: str) -> List[str]:
        """Get datetime columns for partitioning (MySQL)"""
        conn = self._get_mysql_connection()
        cursor = conn.cursor()

        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND DATA_TYPE IN ('datetime', 'timestamp', 'date')
          AND COLUMN_NAME IN ('CreatedAt', 'UpdatedAt', 'created_at', 'updated_at', 'CreateDate', 'ModifiedDate', 'created_date', 'modified_date')
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (database_name, table_name))
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return columns

    def get_mysql_table_row_count(self, database_name: str, table_name: str) -> int:
        """Get total row count for table (MySQL)"""
        conn = self._get_mysql_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM `{database_name}`.`{table_name}`"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def get_mysql_column_bounds(
        self, database_name: str, table_name: str, column_name: str
    ) -> tuple:
        """Get min and max values for a column (MySQL)"""
        conn = self._get_mysql_connection()
        cursor = conn.cursor()

        query = f"""
        SELECT
            MIN(`{column_name}`) as min_val,
            MAX(`{column_name}`) as max_val
        FROM `{database_name}`.`{table_name}`
        WHERE `{column_name}` IS NOT NULL
        """

        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            return (result[0], result[1])
        else:
            return (0, 1000000)  # Default bounds

    def get_mysql_optimal_partition_config(
        self, database_name: str, table_name: str
    ) -> Optional[Dict]:
        """
        Auto-detect optimal partition configuration for a MySQL table

        Returns:
            Dictionary with keys: column, num_partitions, lower_bound, upper_bound
            or None if no suitable partition column found
        """
        logger.info(
            f"Analyzing partition strategy for MySQL {database_name}.{table_name}..."
        )

        # Get row count
        row_count = self.get_mysql_table_row_count(database_name, table_name)
        logger.info(f"MySQL table has {row_count:,} rows")

        # Priority 1: Try numeric columns
        numeric_cols = self.get_mysql_numeric_columns(database_name, table_name)
        if numeric_cols:
            col_name = numeric_cols[0]["COLUMN_NAME"]
            logger.info(f"Found numeric column for partitioning: {col_name}")

            # Get actual bounds
            lower_bound, upper_bound = self.get_mysql_column_bounds(
                database_name, table_name, col_name
            )
            num_partitions = self.calculate_partition_count(row_count)

            logger.info(
                f"MySQL partition config: column={col_name}, partitions={num_partitions}, "
                f"range=[{lower_bound}, {upper_bound}]"
            )

            return {
                "column": col_name,
                "num_partitions": num_partitions,
                "lower_bound": int(lower_bound),
                "upper_bound": int(upper_bound),
            }

        logger.warning(
            "No suitable partition column found for MySQL table, will use single partition"
        )
        return None

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

        # JDBC partitioning only supports numeric columns
        # Datetime columns are used later for Iceberg partitioning in S3, not for JDBC extraction

        # Priority 1: Try numeric columns
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
        """Close all database connections"""
        if self.mssql_connection:
            try:
                self.mssql_connection.close()
            except:
                pass
            self.mssql_connection = None
            self.connection = None

        if self.mysql_connection:
            try:
                self.mysql_connection.close()
            except:
                pass
            self.mysql_connection = None

    def _close_mysql_connection(self):
        """Close MySQL connection only"""
        if self.mysql_connection:
            try:
                self.mysql_connection.close()
            except:
                pass
            self.mysql_connection = None

    def _close_mssql_connection(self):
        """Close MSSQL connection only"""
        if self.mssql_connection:
            try:
                self.mssql_connection.close()
            except:
                pass
            self.mssql_connection = None
            self.connection = None


# Global optimizer instance
optimizer = PartitionOptimizer()
