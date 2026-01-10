import pymssql
import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

def test_connection():
    try:
        print("Connecting to MSSQL...")
        conn = pymssql.connect(
            server=os.getenv('MSSQL_HOST'),
            port=int(os.getenv('MSSQL_PORT', 1433)),
            user=os.getenv('MSSQL_USER'),
            password=os.getenv('MSSQL_PASSWORD'),
            database=os.getenv('MSSQL_DATABASE')
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        print(f"✅ Success! MSSQL Version: {row[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
        count = cursor.fetchone()[0]
        print(f"📊 Found {count} tables in the database.")
        
        conn.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
