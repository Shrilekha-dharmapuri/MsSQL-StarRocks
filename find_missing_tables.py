#!/usr/bin/env python3
"""
Find which tables are missing from S3/Glue Catalog
Compares MSSQL source tables vs Glue Catalog tables
"""

import boto3
import pymssql
from app.config import settings

def get_mssql_tables():
    """Get all table names from MSSQL"""
    print("Connecting to MSSQL...")
    conn = pymssql.connect(
        server=settings.mssql_host,
        port=settings.mssql_port,
        user=settings.mssql_user,
        password=settings.mssql_password,
        database=settings.mssql_database,
    )

    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{settings.mssql_schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """)

    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    print(f"Found {len(tables)} tables in MSSQL schema '{settings.mssql_schema}'")
    return tables


def get_glue_tables():
    """Get all table names from AWS Glue Catalog"""
    print("Connecting to AWS Glue Catalog...")

    glue_client = boto3.client(
        'glue',
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key
    )

    # The Glue database name - matches what the migration creates
    # Target schema is WorldZone_glue, so Glue DB is worldzone_glue_glue
    glue_database = f"worldzone_glue{settings.target_schema_suffix}"

    print(f"Querying Glue database: {glue_database}")

    tables = []
    next_token = None

    while True:
        if next_token:
            response = glue_client.get_tables(
                DatabaseName=glue_database,
                NextToken=next_token
            )
        else:
            response = glue_client.get_tables(
                DatabaseName=glue_database
            )

        for table in response.get('TableList', []):
            tables.append(table['Name'])

        next_token = response.get('NextToken')
        if not next_token:
            break

    print(f"Found {len(tables)} tables in Glue Catalog database '{glue_database}'")
    return sorted(tables)


def compare_tables():
    """Compare MSSQL tables vs Glue tables and find missing ones"""
    print("\n" + "="*60)
    print("COMPARING MSSQL vs GLUE CATALOG TABLES")
    print("="*60 + "\n")

    # Get tables from both sources
    mssql_tables = get_mssql_tables()
    glue_tables = get_glue_tables()

    # Convert to sets for comparison (lowercase for case-insensitive)
    mssql_set = {t.lower() for t in mssql_tables}
    glue_set = set(glue_tables)

    # Find missing tables
    missing_in_glue = mssql_set - glue_set
    extra_in_glue = glue_set - mssql_set

    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"MSSQL tables: {len(mssql_tables)}")
    print(f"Glue tables:  {len(glue_tables)}")
    print(f"Missing in S3/Glue: {len(missing_in_glue)}")
    print(f"Extra in Glue (unexpected): {len(extra_in_glue)}")

    if missing_in_glue:
        print("\n" + "="*60)
        print("MISSING TABLES IN S3/GLUE:")
        print("="*60)
        for i, table in enumerate(sorted(missing_in_glue), 1):
            # Find original case from MSSQL
            original_name = next((t for t in mssql_tables if t.lower() == table), table)
            print(f"{i}. {original_name}")

        # Save to file
        with open("missing_tables.txt", "w") as f:
            f.write("Missing tables in S3/Glue Catalog:\n")
            f.write("="*60 + "\n")
            for table in sorted(missing_in_glue):
                original_name = next((t for t in mssql_tables if t.lower() == table), table)
                f.write(f"{original_name}\n")

        print(f"\n✓ Missing tables saved to: missing_tables.txt")
    else:
        print("\n✓ All MSSQL tables are present in S3/Glue!")

    if extra_in_glue:
        print("\n" + "="*60)
        print("EXTRA TABLES IN GLUE (not in MSSQL):")
        print("="*60)
        for i, table in enumerate(sorted(extra_in_glue), 1):
            print(f"{i}. {table}")

    print("\n" + "="*60)
    print("MIGRATION COMMANDS FOR MISSING TABLES")
    print("="*60)

    if missing_in_glue:
        print("\nTo migrate missing tables to S3:")
        print("\n```bash")
        for table in sorted(missing_in_glue):
            original_name = next((t for t in mssql_tables if t.lower() == table), table)
            print(f"""curl -X POST "http://localhost:8000/migrate/table" \\
  -H "Content-Type: application/json" \\
  -d '{{"source_schema": "{settings.mssql_schema}", "table_name": "{original_name}", "target_schema": "WorldZone_glue", "only_s3": true, "create_table": false}}'
""")
        print("```")


if __name__ == "__main__":
    try:
        compare_tables()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
