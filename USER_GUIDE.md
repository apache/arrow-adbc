# DB2 ADBC Driver - User Guide

This guide shows you how to use the DB2 ADBC driver to connect to DB2 and query data with columnar results.

## Quick Start

### 1. Setup Environment

```bash
cd arrow-adbc
source .venv-db2/bin/activate

# Set library paths (macOS)
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib

# Or for Linux
# export LD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib

# Optional: Set driver library path if not auto-detected
export ADBC_DB2_LIBRARY=$PWD/build/driver/db2/libadbc_driver_db2.dylib
```

### 2. Basic Connection and Query

```python
import adbc_driver_db2
import adbc_driver_manager
import pyarrow as pa

# Connection string
uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Connect to database
db = adbc_driver_db2.connect(uri=uri)
conn = adbc_driver_manager.AdbcConnection(db)

# Create statement
stmt = adbc_driver_manager.AdbcStatement(conn)

# Execute query
stmt.set_sql_query("SELECT * FROM your_table LIMIT 10")
stream, rows_affected = stmt.execute_query()

# Convert to Arrow table (columnar format)
reader = pa.RecordBatchReader._import_from_c(stream.address)
table = reader.read_all()

# Display results
print(table)
print(f"\nSchema: {table.schema}")
print(f"Number of rows: {len(table)}")
print(f"Number of columns: {len(table.schema)}")

# Access data as dictionary (column-oriented)
data_dict = table.to_pydict()
print(f"\nData as dictionary: {data_dict}")

# Cleanup
stmt.close()
conn.close()
db.close()
```

## Detailed Examples

### Example 1: Simple Query with Columnar Results

```python
import adbc_driver_db2
import adbc_driver_manager
import pyarrow as pa

# Connect
uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
db = adbc_driver_db2.connect(uri=uri)
conn = adbc_driver_manager.AdbcConnection(db)
stmt = adbc_driver_manager.AdbcStatement(conn)

# Query
stmt.set_sql_query("""
    SELECT 
        id,
        name,
        age,
        salary
    FROM employees
    WHERE department = 'Engineering'
    ORDER BY salary DESC
    LIMIT 100
""")

stream, _ = stmt.execute_query()
reader = pa.RecordBatchReader._import_from_c(stream.address)
table = reader.read_all()

# Results are in columnar format
print("Columnar data:")
print(f"IDs: {table['id'].to_pylist()}")
print(f"Names: {table['name'].to_pylist()}")
print(f"Ages: {table['age'].to_pylist()}")
print(f"Salaries: {table['salary'].to_pylist()}")

# Cleanup
stmt.close()
conn.close()
db.close()
```

### Example 2: Using DBAPI (More Pythonic)

```python
import adbc_driver_db2.dbapi as dbapi
import pyarrow as pa

# Connect using DBAPI
uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Execute query
        cur.execute("SELECT * FROM your_table WHERE status = 'active'")
        
        # Fetch as Arrow table (columnar)
        table = cur.fetch_arrow_table()
        
        print(f"Retrieved {len(table)} rows")
        print(f"Columns: {table.column_names}")
        
        # Access columns
        for col_name in table.column_names:
            column = table[col_name]
            print(f"\n{col_name}: {column.to_pylist()[:5]}...")  # First 5 values
```

### Example 3: Working with Different Data Types

```python
import adbc_driver_db2.dbapi as dbapi
import pyarrow as pa

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Query with various data types
        cur.execute("""
            SELECT 
                int_col,
                varchar_col,
                decimal_col,
                date_col,
                timestamp_col
            FROM mixed_types_table
            LIMIT 10
        """)
        
        table = cur.fetch_arrow_table()
        
        # Display schema with types
        print("Schema:")
        for field in table.schema:
            print(f"  {field.name}: {field.type}")
        
        # Access data by column
        print("\nData:")
        print(f"Integers: {table['int_col'].to_pylist()}")
        print(f"Strings: {table['varchar_col'].to_pylist()}")
        print(f"Decimals: {table['decimal_col'].to_pylist()}")
        print(f"Dates: {table['date_col'].to_pylist()}")
        print(f"Timestamps: {table['timestamp_col'].to_pylist()}")
```

### Example 4: Converting to Pandas DataFrame

```python
import adbc_driver_db2.dbapi as dbapi
import pandas as pd

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM sales_data WHERE year = 2024")
        
        # Get Arrow table
        table = cur.fetch_arrow_table()
        
        # Convert to Pandas (still columnar internally)
        df = table.to_pandas()
        
        print(df.head())
        print(f"\nShape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Pandas operations
        print(f"\nSummary statistics:")
        print(df.describe())
```

### Example 5: Batch Processing Large Results

```python
import adbc_driver_db2
import adbc_driver_manager
import pyarrow as pa

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

db = adbc_driver_db2.connect(uri=uri)
conn = adbc_driver_manager.AdbcConnection(db)
stmt = adbc_driver_manager.AdbcStatement(conn)

# Set batch size (default is 65536)
stmt.set_options(**{
    adbc_driver_db2.StatementOptions.BATCH_ROWS.value: "10000"
})

# Query large dataset
stmt.set_sql_query("SELECT * FROM large_table")
stream, _ = stmt.execute_query()

# Process in batches
reader = pa.RecordBatchReader._import_from_c(stream.address)
total_rows = 0

for batch in reader:
    print(f"Processing batch with {len(batch)} rows")
    # Process batch...
    total_rows += len(batch)

print(f"Total rows processed: {total_rows}")

stmt.close()
conn.close()
db.close()
```

### Example 6: Parameterized Queries (Prepared Statements)

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Prepare query with parameters
        query = "SELECT * FROM products WHERE category = ? AND price > ?"
        
        # Execute with parameters
        cur.execute(query, ("Electronics", 100.0))
        
        # Fetch results
        table = cur.fetch_arrow_table()
        print(f"Found {len(table)} products")
        print(table)
```

### Example 7: Inserting Data from Arrow Table

```python
import adbc_driver_db2
import adbc_driver_manager
import pyarrow as pa

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

db = adbc_driver_db2.connect(uri=uri)
conn = adbc_driver_manager.AdbcConnection(db)

# Create Arrow table with data
data = pa.table({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'score': [95.5, 87.3, 92.1, 88.9, 91.2]
})

# Ingest data (creates table if it doesn't exist)
conn.adbc_ingest("test_scores", data, mode="create")

# Or append to existing table
# conn.adbc_ingest("test_scores", data, mode="append")

print(f"Inserted {len(data)} rows")

conn.close()
db.close()
```

### Example 8: Transaction Management

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Manual transaction control
with dbapi.connect(uri, autocommit=False) as conn:
    with conn.cursor() as cur:
        try:
            # Multiple operations in transaction
            cur.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)")
            cur.execute("INSERT INTO accounts (id, balance) VALUES (2, 2000)")
            cur.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
            cur.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
            
            # Commit transaction
            conn.commit()
            print("Transaction committed successfully")
            
        except Exception as e:
            # Rollback on error
            conn.rollback()
            print(f"Transaction rolled back: {e}")
```

### Example 9: Getting Metadata

```python
import adbc_driver_db2
import adbc_driver_manager
import pyarrow as pa

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

db = adbc_driver_db2.connect(uri=uri)
conn = adbc_driver_manager.AdbcConnection(db)

# Get driver info
info_codes = [
    adbc_driver_manager.AdbcInfoCode.VENDOR_NAME,
    adbc_driver_manager.AdbcInfoCode.VENDOR_VERSION,
    adbc_driver_manager.AdbcInfoCode.DRIVER_NAME,
    adbc_driver_manager.AdbcInfoCode.DRIVER_VERSION,
]
info_stream = conn.get_info(info_codes)
info_reader = pa.RecordBatchReader._import_from_c(info_stream.address)
info_table = info_reader.read_all()

print("Driver Information:")
for row in info_table.to_pylist():
    print(f"  {row}")

# Get table types
types_stream = conn.get_table_types()
types_reader = pa.RecordBatchReader._import_from_c(types_stream.address)
types_table = types_reader.read_all()

print("\nAvailable table types:")
print(types_table['table_type'].to_pylist())

# Get table schema
schema = conn.get_table_schema(None, None, "your_table")
print(f"\nTable schema:")
for field in schema:
    print(f"  {field.name}: {field.type}")

conn.close()
db.close()
```

## Connection String Options

### Basic Format
```
DATABASE=dbname;UID=username;PWD=password;HOSTNAME=host;PORT=port;PROTOCOL=TCPIP
```

### With SSL
```
DATABASE=dbname;UID=username;PWD=password;HOSTNAME=host;PORT=port;PROTOCOL=TCPIP;SECURITY=SSL;SSLServerCertificate=/path/to/cert.arm
```

### Using Individual Options
```python
import adbc_driver_db2

db = adbc_driver_db2.connect(
    db_kwargs={
        adbc_driver_db2.ConnectionOptions.DATABASE.value: "testdb",
        adbc_driver_db2.ConnectionOptions.HOSTNAME.value: "localhost",
        adbc_driver_db2.ConnectionOptions.PORT.value: "50000",
        adbc_driver_db2.ConnectionOptions.UID.value: "db2inst2",
        adbc_driver_db2.ConnectionOptions.PWD.value: "password",
    }
)
```

## Performance Tips

### 1. Adjust Batch Size
```python
stmt.set_options(**{
    adbc_driver_db2.StatementOptions.BATCH_ROWS.value: "131072"  # Larger batches
})
```

### 2. Use Connection Pooling
```python
# For production, use a connection pool
from contextlib import contextmanager

@contextmanager
def get_connection():
    db = adbc_driver_db2.connect(uri=uri)
    conn = adbc_driver_manager.AdbcConnection(db)
    try:
        yield conn
    finally:
        conn.close()
        db.close()

# Use it
with get_connection() as conn:
    # Your queries here
    pass
```

### 3. Process Data in Batches
```python
# For large datasets, process in batches
reader = pa.RecordBatchReader._import_from_c(stream.address)
for batch in reader:
    # Process each batch
    process_batch(batch)
```

## Error Handling

```python
import adbc_driver_db2.dbapi as dbapi
from adbc_driver_manager import DatabaseError, ProgrammingError

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

try:
    with dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM non_existent_table")
            
except ProgrammingError as e:
    print(f"SQL Error: {e}")
    # Check SQLSTATE code
    if "42S02" in str(e):  # Table not found
        print("Table does not exist")
        
except DatabaseError as e:
    print(f"Database Error: {e}")
    
except Exception as e:
    print(f"Unexpected Error: {e}")
```

## Complete Working Example

Here's a complete example you can run:

```python
#!/usr/bin/env python3
"""
Complete example of using DB2 ADBC driver
"""

import adbc_driver_db2.dbapi as dbapi
import pyarrow as pa

def main():
    # Connection string
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    print("Connecting to DB2...")
    with dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            # Example 1: Simple query
            print("\n1. Simple Query:")
            cur.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            table = cur.fetch_arrow_table()
            print(f"Current timestamp: {table.to_pydict()}")
            
            # Example 2: Query with results
            print("\n2. Query System Catalog:")
            cur.execute("""
                SELECT 
                    TABNAME,
                    TYPE,
                    CARD
                FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = CURRENT SCHEMA
                FETCH FIRST 5 ROWS ONLY
            """)
            
            table = cur.fetch_arrow_table()
            print(f"Found {len(table)} tables")
            print("\nTable names:")
            for name in table['TABNAME'].to_pylist():
                print(f"  - {name}")
            
            # Example 3: Convert to Pandas
            print("\n3. Convert to Pandas:")
            df = table.to_pandas()
            print(df)
            
    print("\nDone!")

if __name__ == "__main__":
    main()
```

## Environment Setup Script

Save this as `setup_env.sh`:

```bash
#!/bin/bash
# Setup environment for DB2 ADBC driver

cd arrow-adbc
source .venv-db2/bin/activate

# Set library path (macOS)
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib

# Or for Linux:
# export LD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib

# Optional: Set driver library
export ADBC_DB2_LIBRARY=$PWD/build/driver/db2/libadbc_driver_db2.dylib

echo "Environment ready!"
echo "Run your Python scripts now."
```

Then use it:
```bash
source setup_env.sh
python your_script.py
```

## Troubleshooting

### Issue: "Library not loaded: libdb2.dylib"
**Solution**: Set DYLD_LIBRARY_PATH (macOS) or LD_LIBRARY_PATH (Linux)
```bash
export DYLD_LIBRARY_PATH=/path/to/.venv-db2/lib/python3.14/site-packages/clidriver/lib
```

### Issue: "Could not load driver"
**Solution**: Set ADBC_DB2_LIBRARY
```bash
export ADBC_DB2_LIBRARY=/path/to/libadbc_driver_db2.dylib
```

### Issue: Connection timeout
**Solution**: Check DB2 is running and accessible
```bash
nc -zv localhost 50000
```

## Additional Resources

- [ADBC Documentation](https://arrow.apache.org/adbc/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [DB2 SQL Reference](https://www.ibm.com/docs/en/db2)
- [Test Examples](python/adbc_driver_db2/tests/)

## Next Steps

1. Try the examples above
2. Adapt them to your use case
3. Check the test files for more examples
4. Read the comprehensive guides:
   - [DB2_TESTING_GUIDE.md](DB2_TESTING_GUIDE.md)
   - [TEST_RESULTS.md](TEST_RESULTS.md)