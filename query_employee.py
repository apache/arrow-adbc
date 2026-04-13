#!/usr/bin/env python3
"""
Query EMPLOYEE table and display results in columnar format

Before running:
  cd arrow-adbc
  source .venv-db2/bin/activate
  export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
  python query_employee.py
"""

# Import the DB2 ADBC driver's DBAPI interface
#
# What is this?
# - adbc_driver_db2: The Python package for DB2 ADBC driver (installed in .venv-db2)
# - .dbapi: A Python DB-API 2.0 compatible interface (standard Python database API)
# - as dbapi: We give it a short name "dbapi" for convenience
#
# This import gives you access to:
# - dbapi.connect() - Function to connect to DB2
# - Connection objects - Manage database connections
# - Cursor objects - Execute queries and fetch results
#
# The DBAPI interface is a Python standard (PEP 249) that makes it easy to work
# with databases using familiar Python patterns (similar to sqlite3, psycopg2, etc.)
import adbc_driver_db2.dbapi as dbapi

def main():
    # Connection string
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    print("=" * 70)
    print("DB2 ADBC Driver - Query EMPLOYEE Table")
    print("=" * 70)
    
    print("\n1. Connecting to DB2...")
    with dbapi.connect(uri) as conn:
        print("   ✓ Connected successfully")
        
        with conn.cursor() as cur:
            # Query EMPLOYEE table
            print("\n2. Executing query: SELECT * FROM DB2INST2.EMPLOYEE...")
            cur.execute("SELECT * FROM DB2INST2.EMPLOYEE ORDER BY ID")
            
            # Get results as Arrow table (columnar format)
            table = cur.fetch_arrow_table()
            
            print(f"   ✓ Retrieved {len(table)} employees")
            print(f"   ✓ Columns: {table.column_names}")
            
            # Display full table
            print("\n" + "=" * 70)
            print("3. Full Table (Arrow Format):")
            print("=" * 70)
            print(table)
            print(f"\nSchema: {table.schema}")
            
            # Display columnar data
            print("\n" + "=" * 70)
            print("4. Columnar Data (Row-to-Column Conversion):")
            print("=" * 70)
            print(f"   IDs:    {table['ID'].to_pylist()}")
            print(f"   Names:  {table['NAME'].to_pylist()}")
            print(f"   Emails: {table['EMAIL'].to_pylist()}")
            
            # Display as dictionary
            print("\n" + "=" * 70)
            print("5. As Dictionary (Column-Oriented):")
            print("=" * 70)
            data_dict = table.to_pydict()
            for col_name, col_values in data_dict.items():
                print(f"   {col_name}: {col_values}")
            
            # Display as Pandas
            print("\n" + "=" * 70)
            print("6. As Pandas DataFrame:")
            print("=" * 70)
            df = table.to_pandas()
            print(df)
            print(f"\n   Shape: {df.shape}")
            print(f"   Dtypes:\n{df.dtypes}")
            
            # Example: Filter data
            print("\n" + "=" * 70)
            print("7. Filtered Query (ID > 1):")
            print("=" * 70)
            cur.execute("""
                SELECT ID, NAME, EMAIL 
                FROM DB2INST2.EMPLOYEE 
                WHERE ID > 1
                ORDER BY NAME
            """)
            filtered_table = cur.fetch_arrow_table()
            print(f"   Found {len(filtered_table)} employees")
            print(f"   IDs:    {filtered_table['ID'].to_pylist()}")
            print(f"   Names:  {filtered_table['NAME'].to_pylist()}")
            print(f"   Emails: {filtered_table['EMAIL'].to_pylist()}")
            
            # Example: Aggregate
            print("\n" + "=" * 70)
            print("8. Aggregate Query:")
            print("=" * 70)
            cur.execute("""
                SELECT 
                    COUNT(*) AS total_employees,
                    MIN(ID) AS min_id,
                    MAX(ID) AS max_id
                FROM DB2INST2.EMPLOYEE
            """)
            agg_table = cur.fetch_arrow_table()
            print(f"   Total employees: {agg_table['TOTAL_EMPLOYEES'][0].as_py()}")
            print(f"   Min ID: {agg_table['MIN_ID'][0].as_py()}")
            print(f"   Max ID: {agg_table['MAX_ID'][0].as_py()}")
    
    print("\n" + "=" * 70)
    print("✓ All queries completed successfully!")
    print("=" * 70)
    print("\nKey Takeaways:")
    print("  • Data is returned in columnar format (Arrow tables)")
    print("  • Access columns directly: table['COLUMN_NAME']")
    print("  • Convert to Pandas for analysis: table.to_pandas()")
    print("  • Convert to dict for column-oriented access: table.to_pydict()")
    print("\nSee PRACTICAL_GUIDE.md for more examples!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        print("\nTroubleshooting:")
        print("  1. Ensure DB2 is running: nc -zv localhost 50000")
        print("  2. Check library path: echo $DYLD_LIBRARY_PATH")
        print("  3. Verify connection string")
        exit(1)

# Made with Bob
