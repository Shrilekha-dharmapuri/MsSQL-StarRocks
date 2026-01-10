"""
Schema Mapping Service - MSSQL to StarRocks Type Conversion
Handles schema introspection and DDL generation
"""

import logging
import pymssql
from typing import List, Dict, Tuple
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaMapper:
    """Maps MSSQL schemas to StarRocks compatible schemas"""

    # MSSQL to StarRocks type mapping
    TYPE_MAPPING = {
        # Integer types
        "bit": "BOOLEAN",
        "tinyint": "TINYINT",
        "smallint": "SMALLINT",
        "int": "INT",
        "bigint": "BIGINT",
        # Decimal types
        "decimal": "DECIMAL",
        "numeric": "DECIMAL",
        "money": "DECIMAL(19,4)",
        "smallmoney": "DECIMAL(10,4)",
        # Floating point
        "float": "DOUBLE",
        "real": "FLOAT",
        # String types
        "char": "CHAR",
        "varchar": "VARCHAR",
        "nchar": "CHAR",  # Unicode - may need size adjustment
        "nvarchar": "VARCHAR",  # Unicode - may need size adjustment
        "text": "STRING",
        "ntext": "STRING",
        # Date/Time types
        "date": "DATE",
        "time": "VARCHAR(16)",  # StarRocks doesn't have TIME type
        "datetime": "DATETIME",
        "datetime2": "DATETIME",
        "smalldatetime": "DATETIME",
        "datetimeoffset": "VARCHAR(34)",  # Store as string with timezone
        # Binary types
        "binary": "VARBINARY",
        "varbinary": "VARBINARY",
        "image": "VARBINARY",
        # Other types
        "uniqueidentifier": "VARCHAR(36)",  # GUID as string
        "xml": "STRING",
        "json": "JSON",
    }

    def __init__(self):
        self.connection = None

    def _get_connection(self):
        """Get MSSQL connection for metadata queries"""
        if not self.connection:
            logger.info("Connecting to MSSQL for metadata queries...")
            self.connection = pymssql.connect(
                server=settings.mssql_host,
                port=settings.mssql_port,
                user=settings.mssql_user,
                password=settings.mssql_password,
                database=settings.mssql_database,
                timeout=30,  # 30 second timeout
                login_timeout=30
            )
        return self.connection

    def _close_connection(self):
        """Close MSSQL connection to prevent leaks"""
        if self.connection:
            try:
                self.connection.close()
                logger.debug("Closed MSSQL metadata connection")
            except Exception as e:
                logger.warning(f"Error closing MSSQL connection: {e}")
            finally:
                self.connection = None

    def get_table_schema(
        self, schema_name: str, table_name: str
    ) -> List[Dict[str, str]]:
        """
        Get table schema from MSSQL INFORMATION_SCHEMA

        Returns:
            List of column dictionaries with keys: name, type, length, nullable
        """
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (schema_name, table_name))
        columns = cursor.fetchall()
        cursor.close()

        logger.info(
            f"Retrieved {len(columns)} columns for {schema_name}.{table_name}"
        )
        return columns

    def map_mssql_type_to_starrocks(
        self,
        data_type: str,
        char_length: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> str:
        """
        Convert MSSQL data type to StarRocks data type

        Args:
            data_type: MSSQL data type (lowercase)
            char_length: Character maximum length (for string types)
            numeric_precision: Numeric precision (for decimal types)
            numeric_scale: Numeric scale (for decimal types)

        Returns:
            StarRocks type definition
        """
        data_type = data_type.lower()

        # Handle sized types
        if data_type in ["char", "varchar"] and char_length:
            # Limit to StarRocks maximum
            length = min(char_length, 65535)
            base_type = self.TYPE_MAPPING.get(data_type, "VARCHAR")
            return f"{base_type}({length})"

        elif data_type in ["nchar", "nvarchar"] and char_length:
            # Unicode types need double the size
            length = min(char_length * 2, 65535)
            base_type = self.TYPE_MAPPING.get(data_type, "VARCHAR")
            return f"{base_type}({length})"

        elif data_type in ["decimal", "numeric"] and numeric_precision:
            scale = numeric_scale or 0
            return f"DECIMAL({numeric_precision},{scale})"

        elif data_type in ["binary", "varbinary"] and char_length:
            return f"VARBINARY({char_length})"

        # Default mapping
        return self.TYPE_MAPPING.get(data_type, "STRING")

    def generate_create_table_ddl(
        self, schema_name: str, table_name: str, target_schema: str
    ) -> str:
        """
        Generate StarRocks CREATE TABLE DDL from MSSQL schema

        Args:
            schema_name: MSSQL source schema name
            table_name: Table name
            target_schema: StarRocks target schema name

        Returns:
            CREATE TABLE SQL statement
        """
        columns = self.get_table_schema(schema_name, table_name)

        if not columns:
            raise ValueError(
                f"No columns found for table {schema_name}.{table_name}"
            )

        # Build column definitions
        column_defs = []
        for col in columns:
            col_name = col["COLUMN_NAME"]
            mssql_type = col["DATA_TYPE"]
            char_length = col.get("CHARACTER_MAXIMUM_LENGTH")
            num_precision = col.get("NUMERIC_PRECISION")
            num_scale = col.get("NUMERIC_SCALE")
            is_nullable = col["IS_NULLABLE"] == "YES"

            # Map type
            starrocks_type = self.map_mssql_type_to_starrocks(
                mssql_type, char_length, num_precision, num_scale
            )

            # Build column definition
            null_constraint = "" if is_nullable else " NOT NULL"
            column_def = f"`{col_name}` {starrocks_type}{null_constraint}"
            column_defs.append(column_def)

        # Add audit columns
        column_defs.append("`migrated_at` VARCHAR(64)")
        column_defs.append("`source_schema` VARCHAR(255)")

        # Build CREATE TABLE statement
        columns_sql = ",\n  ".join(column_defs)
        ddl = f"""CREATE TABLE IF NOT EXISTS `{target_schema}`.`{table_name}` (
  {columns_sql}
) ENGINE=OLAP
DUPLICATE KEY(`{columns[0]['COLUMN_NAME']}`)
DISTRIBUTED BY HASH(`{columns[0]['COLUMN_NAME']}`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1",
  "compression" = "LZ4"
);"""

        return ddl

    def create_starrocks_table(
        self, schema_name: str, table_name: str, target_schema: str
    ) -> None:
        """
        Create table in StarRocks based on MSSQL schema

        Args:
            schema_name: MSSQL source schema name
            table_name: Table name
            target_schema: StarRocks target schema name
        """
        logger.info(f"Creating StarRocks table: {target_schema}.{table_name}")

        ddl = self.generate_create_table_ddl(schema_name, table_name, target_schema)
        logger.info(f"Generated DDL:\n{ddl}")

        # Execute DDL using MySQL connector (StarRocks uses MySQL protocol)
        import mysql.connector

        conn = None
        cursor = None

        try:
            conn = mysql.connector.connect(
                host=settings.starrocks_host,
                port=settings.starrocks_port,
                user=settings.starrocks_user,
                password=settings.starrocks_password,
                # Do NOT connect to database yet, as it might not exist
                connect_timeout=30,  # 30 second connection timeout
                autocommit=False,
                pool_size=5,  # Connection pooling
                pool_name="starrocks_ddl_pool",
                pool_reset_session=True
            )

            cursor = conn.cursor()

            # Create database if it doesn't exist
            logger.info(f"Ensuring database exists: {target_schema}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{target_schema}`")

            # Now use the database
            cursor.execute(f"USE `{target_schema}`")

            # Create table
            cursor.execute(ddl)
            conn.commit()

            logger.info(f"Successfully created table {target_schema}.{table_name}")

        except mysql.connector.Error as e:
            logger.error(f"MySQL error creating table {target_schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating table {target_schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            # CRITICAL: Always close connections to prevent leaks
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                    logger.debug(f"Closed StarRocks connection for {table_name}")
                except:
                    pass

    def get_primary_key(self, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s 
          AND TABLE_NAME = %s
          AND CONSTRAINT_NAME LIKE 'PK_%'
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (schema_name, table_name))
        pk_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return pk_columns

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get row count for a table"""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None


# Global mapper instance
mapper = SchemaMapper()
