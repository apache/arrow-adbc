#!/usr/bin/env python3
"""
DB2 ADBC Driver - Practical Usage Example

This script demonstrates how to:
1. Connect to DB2
2. Query a table
3. Get results in columnar format (row-to-column conversion)
4. Work with the data

Before running:
  source .venv-db2/bin/activate
  export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
  export ADBC_DB2_LIBRARY=$PWD/build/driver/db2/libadbc_driver_db2.dylib
"""

import sys

def example_1_simple_query():
    """Example 1: Simple query with columnar results"""
    print("=" * 70)
    print("Example 1: Simple Query with Columnar Results")
    print("=" * 70)
    
    import adbc_driver_db2.dbapi as dbapi
    
    # Connection string
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    try:
        # Connect to DB2
        print("\n1. Connecting to DB2...")
        with dbapi.connect(uri) as conn:
            print("✓ Connected successfully")
            
            with conn.cursor() as cur:
                # Execute query
                print("\n2. Executing query...")
                cur.execute("""
                    SELECT 
                        1 AS id,
                        'Alice' AS name,
                        95.5 AS score
                    FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 2, 'Bob', 87.3 FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 3, 'Charlie', 92.1 FROM SYSIBM.SYSDUMMY1
                """)
                
                # Get results as Arrow table (columnar format)
                print("\n3. Fetching results in columnar format...")
                table = cur.fetch_arrow_table()
                
                print(f"✓ Retrieved {len(table)} rows")
                print(f"✓ Columns: {table.column_names}")
                
                # Display the table
                print("\n4. Full table:")
                print(table)
                
                # Access data by column (this is the columnar format!)
                print("\n5. Accessing data by column:")
                print(f"   IDs:    {table['ID'].to_pylist()}")
                print(f"   Names:  {table['NAME'].to_pylist()}")
                print(f"   Scores: {table['SCORE'].to_pylist()}")
                
                # Convert to dictionary (column-oriented)
                print("\n6. As dictionary (column-oriented):")
                data_dict = table.to_pydict()
                for col_name, values in data_dict.items():
                    print(f"   {col_name}: {values}")
                
        print("\n✓ Example 1 completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def example_2_system_catalog():
    """Example 2: Query system catalog tables"""
    print("\n" + "=" * 70)
    print("Example 2: Query System Catalog")
    print("=" * 70)
    
    import adbc_driver_db2.dbapi as dbapi
    
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    try:
        with dbapi.connect(uri) as conn:
            with conn.cursor() as cur:
                print("\n1. Querying system catalog for tables...")
                cur.execute("""
                    SELECT 
                        TABNAME,
                        TYPE,
                        CARD,
                        NPAGES
                    FROM SYSCAT.TABLES 
                    WHERE TABSCHEMA = CURRENT SCHEMA
                    FETCH FIRST 10 ROWS ONLY
                """)
                
                table = cur.fetch_arrow_table()
                
                print(f"\n2. Found {len(table)} tables in current schema")
                
                if len(table) > 0:
                    print("\n3. Table information (columnar format):")
                    print(f"   Names:  {table['TABNAME'].to_pylist()}")
                    print(f"   Types:  {table['TYPE'].to_pylist()}")
                    print(f"   Rows:   {table['CARD'].to_pylist()}")
                    print(f"   Pages:  {table['NPAGES'].to_pylist()}")
                else:
                    print("\n   No tables found in current schema")
                
        print("\n✓ Example 2 completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False


def example_3_with_pandas():
    """Example 3: Convert to Pandas DataFrame"""
    print("\n" + "=" * 70)
    print("Example 3: Convert to Pandas DataFrame")
    print("=" * 70)
    
    import adbc_driver_db2.dbapi as dbapi
    
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    try:
        with dbapi.connect(uri) as conn:
            with conn.cursor() as cur:
                print("\n1. Executing query...")
                cur.execute("""
                    SELECT 
                        'Product A' AS product,
                        100 AS quantity,
                        25.50 AS price
                    FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 'Product B', 200, 15.75 FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 'Product C', 150, 30.00 FROM SYSIBM.SYSDUMMY1
                """)
                
                # Get as Arrow table
                table = cur.fetch_arrow_table()
                
                print("\n2. Converting to Pandas DataFrame...")
                df = table.to_pandas()
                
                print("\n3. DataFrame:")
                print(df)
                
                print("\n4. DataFrame info:")
                print(f"   Shape: {df.shape}")
                print(f"   Columns: {df.columns.tolist()}")
                print(f"   Data types:\n{df.dtypes}")
                
                print("\n5. Calculate total value:")
                df['total_value'] = df['QUANTITY'] * df['PRICE']
                print(df[['PRODUCT', 'TOTAL_VALUE']])
                
        print("\n✓ Example 3 completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False


def example_4_low_level_api():
    """Example 4: Using low-level API for more control"""
    print("\n" + "=" * 70)
    print("Example 4: Low-Level API with Batch Processing")
    print("=" * 70)
    
    import adbc_driver_db2
    import adbc_driver_manager
    import pyarrow as pa
    
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    try:
        print("\n1. Connecting with low-level API...")
        db = adbc_driver_db2.connect(uri=uri)
        conn = adbc_driver_manager.AdbcConnection(db)
        stmt = adbc_driver_manager.AdbcStatement(conn)
        
        print("✓ Connected")
        
        # Set batch size
        print("\n2. Setting batch size to 1000 rows...")
        stmt.set_options(**{
            adbc_driver_db2.StatementOptions.BATCH_ROWS.value: "1000"
        })
        
        # Execute query
        print("\n3. Executing query...")
        stmt.set_sql_query("""
            SELECT 
                CURRENT DATE AS date_col,
                CURRENT TIME AS time_col,
                CURRENT TIMESTAMP AS timestamp_col
            FROM SYSIBM.SYSDUMMY1
        """)
        
        stream, rows_affected = stmt.execute_query()
        print(f"✓ Query executed (rows_affected: {rows_affected})")
        
        # Read results
        print("\n4. Reading results...")
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        table = reader.read_all()
        
        print(f"✓ Retrieved {len(table)} rows")
        print(f"\n5. Schema:")
        print(table.schema)
        
        print(f"\n6. Data:")
        print(table.to_pydict())
        
        # Cleanup
        stmt.close()
        conn.close()
        db.close()
        
        print("\n✓ Example 4 completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all examples"""
    print("\n" + "=" * 70)
    print("DB2 ADBC Driver - Usage Examples")
    print("=" * 70)
    print("\nThese examples demonstrate how to:")
    print("  • Connect to DB2")
    print("  • Execute queries")
    print("  • Get results in columnar format (row-to-column conversion)")
    print("  • Work with Arrow tables and Pandas DataFrames")
    
    results = []
    
    # Run examples
    results.append(("Example 1: Simple Query", example_1_simple_query()))
    results.append(("Example 2: System Catalog", example_2_system_catalog()))
    results.append(("Example 3: Pandas Integration", example_3_with_pandas()))
    results.append(("Example 4: Low-Level API", example_4_low_level_api()))
    
    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(success for _, success in results)
    print("\n" + "=" * 70)
    if all_passed:
        print("✓ All examples completed successfully!")
        return 0
    else:
        print("✗ Some examples failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

# Made with Bob
