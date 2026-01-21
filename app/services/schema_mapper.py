"""
Schema Mapping Service - MSSQL/MySQL to StarRocks Type Conversion
Handles schema introspection and DDL generation for both source databases
"""

import logging
import pymssql
import mysql.connector
from typing import List, Dict, Tuple, Optional
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaMapper:
    """Maps MSSQL/MySQL schemas to StarRocks compatible schemas"""

    # MSSQL to StarRocks type mapping
    MSSQL_TYPE_MAPPING = {
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
        "time": "VARCHAR(32)",  # StarRocks doesn't have TIME type, Iceberg may store as datetime
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

    # MySQL to StarRocks type mapping
    MYSQL_TYPE_MAPPING = {
        # Integer types
        "tinyint": "TINYINT",
        "smallint": "SMALLINT",
        "mediumint": "INT",
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        # Boolean (MySQL uses TINYINT(1) for boolean) - use TINYINT for compatibility
        "bit": "TINYINT",
        # Decimal types
        "decimal": "DECIMAL",
        "numeric": "DECIMAL",
        "float": "FLOAT",
        "double": "DOUBLE",
        "real": "DOUBLE",
        # String types
        "char": "CHAR",
        "varchar": "VARCHAR",
        "tinytext": "STRING",
        "text": "STRING",
        "mediumtext": "STRING",
        "longtext": "STRING",
        # Binary types
        "binary": "VARBINARY",
        "varbinary": "VARBINARY",
        "tinyblob": "VARBINARY",
        "blob": "VARBINARY",
        "mediumblob": "VARBINARY",
        "longblob": "VARBINARY",
        # Date/Time types
        "date": "DATE",
        "time": "VARCHAR(32)",  # StarRocks doesn't have TIME type, Iceberg may store as datetime
        "datetime": "DATETIME",
        "timestamp": "DATETIME",
        "year": "INT",
        # JSON type
        "json": "JSON",
        # Enum and Set (convert to VARCHAR)
        "enum": "VARCHAR(255)",
        "set": "VARCHAR(1024)",
        # Spatial types (convert to STRING)
        "geometry": "STRING",
        "point": "STRING",
        "linestring": "STRING",
        "polygon": "STRING",
        "multipoint": "STRING",
        "multilinestring": "STRING",
        "multipolygon": "STRING",
        "geometrycollection": "STRING",
    }

    # Keep backward compatibility alias
    TYPE_MAPPING = MSSQL_TYPE_MAPPING

    def __init__(self):
        self.mssql_connection = None
        self.mysql_connection = None
        # Keep backward compatibility
        self.connection = None

    def _get_connection(self):
        """Get MSSQL connection for metadata queries (backward compatibility)"""
        return self._get_mssql_connection()

    def _get_mssql_connection(self):
        """Get MSSQL connection for metadata queries"""
        if not self.mssql_connection:
            logger.info("Connecting to MSSQL for metadata queries...")
            self.mssql_connection = pymssql.connect(
                server=settings.mssql_host,
                port=settings.mssql_port,
                user=settings.mssql_user,
                password=settings.mssql_password,
                database=settings.mssql_database,
                timeout=30,  # 30 second timeout
                login_timeout=30
            )
            # Keep backward compatibility
            self.connection = self.mssql_connection
        return self.mssql_connection

    def _get_mysql_source_connection(self):
        """Get MySQL source connection for metadata queries"""
        if not self.mysql_connection:
            logger.info("Connecting to MySQL source for metadata queries...")
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

    def _close_connection(self):
        """Close MSSQL connection to prevent leaks (backward compatibility)"""
        self._close_mssql_connection()

    def _close_mssql_connection(self):
        """Close MSSQL connection to prevent leaks"""
        if self.mssql_connection:
            try:
                self.mssql_connection.close()
                logger.debug("Closed MSSQL metadata connection")
            except Exception as e:
                logger.warning(f"Error closing MSSQL connection: {e}")
            finally:
                self.mssql_connection = None
                self.connection = None

    def _close_mysql_source_connection(self):
        """Close MySQL source connection to prevent leaks"""
        if self.mysql_connection:
            try:
                self.mysql_connection.close()
                logger.debug("Closed MySQL source metadata connection")
            except Exception as e:
                logger.warning(f"Error closing MySQL source connection: {e}")
            finally:
                self.mysql_connection = None

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

    def get_mysql_table_schema(
        self, database_name: str, table_name: str
    ) -> List[Dict[str, str]]:
        """
        Get table schema from MySQL INFORMATION_SCHEMA

        Args:
            database_name: MySQL database name (acts as schema)
            table_name: Table name

        Returns:
            List of column dictionaries with keys: name, type, length, nullable
        """
        conn = self._get_mysql_source_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (database_name, table_name))
        columns = cursor.fetchall()
        cursor.close()

        logger.info(
            f"Retrieved {len(columns)} columns for MySQL {database_name}.{table_name}"
        )
        return columns

    def map_mysql_type_to_starrocks(
        self,
        data_type: str,
        char_length: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
        column_type: str = None,
    ) -> str:
        """
        Convert MySQL data type to StarRocks data type

        Args:
            data_type: MySQL data type (lowercase)
            char_length: Character maximum length (for string types)
            numeric_precision: Numeric precision (for decimal types)
            numeric_scale: Numeric scale (for decimal types)
            column_type: Full column type definition (e.g., 'int(11) unsigned')

        Returns:
            StarRocks type definition
        """
        data_type = data_type.lower()

        # Handle TINYINT(1) - use TINYINT instead of BOOLEAN for better compatibility
        # StarRocks BOOLEAN is strict, TINYINT is more permissive with values
        if data_type == "tinyint" and column_type and "tinyint(1)" in column_type.lower():
            return "TINYINT"

        # Handle unsigned integers - may need larger type
        is_unsigned = column_type and "unsigned" in column_type.lower() if column_type else False

        # Handle sized string types
        if data_type in ["char", "varchar"]:
            if char_length:
                # Add 20% safety buffer, minimum +50 chars, maximum +500 chars
                buffer = max(50, min(int(char_length * 0.2), 500))
                safe_length = min(char_length + buffer, 65535)
                return f"VARCHAR({safe_length})"
            else:
                return "VARCHAR(255)"

        # Handle decimal types
        elif data_type in ["decimal", "numeric"]:
            if numeric_precision:
                scale = numeric_scale or 0
                return f"DECIMAL({numeric_precision},{scale})"
            else:
                return "DECIMAL(38,10)"

        # Handle unsigned integers - upgrade to larger type if needed
        elif data_type in ["int", "integer"] and is_unsigned:
            return "BIGINT"
        elif data_type == "mediumint" and is_unsigned:
            return "INT"
        elif data_type == "smallint" and is_unsigned:
            return "INT"
        elif data_type == "tinyint" and is_unsigned:
            return "SMALLINT"

        # Handle binary types with length
        elif data_type in ["binary", "varbinary"] and char_length:
            return f"VARBINARY({char_length})"

        # Default mapping
        return self.MYSQL_TYPE_MAPPING.get(data_type, "STRING")

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
            # Add 20% safety buffer for data quality issues (MSSQL allows oversized data)
            # Minimum +50 chars, maximum +500 chars buffer
            buffer = max(50, min(int(char_length * 0.2), 500))
            safe_length = min(char_length + buffer, 65535)
            # Always use VARCHAR for flexibility
            return f"VARCHAR({safe_length})"

        elif data_type in ["nchar", "nvarchar"] and char_length:
            # Unicode types need double the size + safety buffer
            buffer = max(50, min(int(char_length * 0.2), 500))
            safe_length = min(char_length * 2 + buffer, 65535)
            return f"VARCHAR({safe_length})"

        elif data_type in ["decimal", "numeric"] and numeric_precision:
            scale = numeric_scale or 0
            return f"DECIMAL({numeric_precision},{scale})"

        elif data_type in ["binary", "varbinary"] and char_length:
            return f"VARBINARY({char_length})"

        # Default mapping
        return self.MSSQL_TYPE_MAPPING.get(data_type, "STRING")

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

    def generate_mysql_create_table_ddl(
        self, database_name: str, table_name: str, target_schema: str
    ) -> str:
        """
        Generate StarRocks CREATE TABLE DDL from MySQL schema

        Args:
            database_name: MySQL source database name
            table_name: Table name
            target_schema: StarRocks target schema name

        Returns:
            CREATE TABLE SQL statement
        """
        columns = self.get_mysql_table_schema(database_name, table_name)

        if not columns:
            raise ValueError(
                f"No columns found for MySQL table {database_name}.{table_name}"
            )

        # Build column definitions
        column_defs = []
        for col in columns:
            col_name = col["COLUMN_NAME"]
            mysql_type = col["DATA_TYPE"]
            char_length = col.get("CHARACTER_MAXIMUM_LENGTH")
            num_precision = col.get("NUMERIC_PRECISION")
            num_scale = col.get("NUMERIC_SCALE")
            column_type = col.get("COLUMN_TYPE")  # Full type like 'int(11) unsigned'
            is_nullable = col["IS_NULLABLE"] == "YES"

            # Map type
            starrocks_type = self.map_mysql_type_to_starrocks(
                mysql_type, char_length, num_precision, num_scale, column_type
            )

            # Build column definition
            # NOTE: Allow NULL for all columns during migration to avoid data quality rejections
            # StarRocks strictly enforces NOT NULL, but source data may have NULLs despite schema
            column_def = f"`{col_name}` {starrocks_type}"
            column_defs.append(column_def)

        # Add audit columns
        column_defs.append("`migrated_at` VARCHAR(64)")
        column_defs.append("`source_database` VARCHAR(255)")

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

    def create_starrocks_table_from_mysql(
        self, database_name: str, table_name: str, target_schema: str
    ) -> None:
        """
        Create table in StarRocks based on MySQL schema

        Args:
            database_name: MySQL source database name
            table_name: Table name
            target_schema: StarRocks target schema name
        """
        logger.info(f"Creating StarRocks table from MySQL: {target_schema}.{table_name}")

        ddl = self.generate_mysql_create_table_ddl(database_name, table_name, target_schema)
        logger.info(f"Generated DDL:\n{ddl}")

        conn = None
        cursor = None

        try:
            conn = mysql.connector.connect(
                host=settings.starrocks_host,
                port=settings.starrocks_port,
                user=settings.starrocks_user,
                password=settings.starrocks_password,
                connect_timeout=30,
                autocommit=False,
                pool_size=5,
                pool_name="starrocks_mysql_ddl_pool",
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

            logger.info(f"Successfully created table {target_schema}.{table_name} from MySQL source")

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
        """Get primary key columns for a MSSQL table"""
        conn = self._get_mssql_connection()
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

    def get_mysql_primary_key(self, database_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a MySQL table"""
        conn = self._get_mysql_source_connection()
        cursor = conn.cursor()

        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, (database_name, table_name))
        pk_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return pk_columns

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get row count for a MSSQL table"""
        conn = self._get_mssql_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def get_mysql_table_row_count(self, database_name: str, table_name: str) -> int:
        """Get row count for a MySQL table"""
        conn = self._get_mysql_source_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM `{database_name}`.`{table_name}`"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def get_mysql_tables(self, database_name: str) -> List[str]:
        """Get all table names in a MySQL database"""
        conn = self._get_mysql_source_connection()
        cursor = conn.cursor()

        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """

        cursor.execute(query, (database_name,))
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()

        logger.info(f"Found {len(tables)} tables in MySQL database {database_name}")
        return tables

    def close(self):
        """Close all database connections"""
        self._close_mssql_connection()
        self._close_mysql_source_connection()


# Global mapper instance
mapper = SchemaMapper()
