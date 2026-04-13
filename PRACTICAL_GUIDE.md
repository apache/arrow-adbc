# DB2 ADBC Driver - Practical Guide with Real Examples

This guide uses the actual **EMPLOYEE** table from your DB2 database to demonstrate row-to-column conversion.

## Your Database

**Database**: testdb  
**Table**: DB2INST2.EMPLOYEE  
**Columns**: ID (int), NAME (string), EMAIL (string)  
**Sample Data**:
- ID: [1, 2, 3]
- NAME: ['Alice', 'Bob', 'Charlie']
- EMAIL: ['alice@example.com', 'bob@example.com', 'charlie@example.com']

## Setup

Before running any examples, you need to set up your environment. Here's what each command does:

```bash
# 1. Navigate to the project directory
cd arrow-adbc

# 2. Activate the Python virtual environment
#    This ensures you're using the correct Python packages
source .venv-db2/bin/activate

# 3. Set the library path for DB2 CLI libraries
#    DYLD_LIBRARY_PATH (macOS) tells the system where to find libdb2.dylib
#    The DB2 driver needs these libraries to communicate with DB2
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib

# 4. (Optional) Specify the ADBC driver library location
#    If the driver can't be auto-detected, this tells Python where to find it
export ADBC_DB2_LIBRARY=$PWD/build/driver/db2/libadbc_driver_db2.dylib
```

**What's happening:**
- **Line 1**: Changes to the project directory where all files are located
- **Line 2**: Activates a Python virtual environment that has all required packages (adbc-driver-manager, pyarrow, pandas, etc.)
- **Line 3**: Sets `DYLD_LIBRARY_PATH` so the ADBC driver can find DB2's native libraries (`libdb2.dylib`). Without this, you'll get "Library not loaded" errors
- **Line 4**: Tells the Python package where to find the compiled ADBC driver library. This is usually auto-detected but can be set explicitly

**For Linux users**, replace line 3 with:
```bash
export LD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
```

**Why is this needed?**
The DB2 ADBC driver is a bridge between Python and DB2. It consists of:
1. Python code (adbc_driver_db2 package) - already installed in .venv-db2
2. C++ driver library (libadbc_driver_db2.dylib) - built in build/driver/db2/
3. DB2 CLI libraries (libdb2.dylib) - installed with ibm_db package in .venv-db2

The environment variables connect these pieces together so they can work as one system.

## Example 1: Basic Query with Row-to-Column Conversion

This is the most common use case - querying a table and getting columnar results.

```python
import adbc_driver_db2.dbapi as dbapi

# Connection string
uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Connect and query
with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Execute query on EMPLOYEE table
        cur.execute("SELECT * FROM DB2INST2.EMPLOYEE")
        
        # Get results as Arrow table (columnar format)
        table = cur.fetch_arrow_table()
        
        print(f"Retrieved {len(table)} rows")
        print(f"Columns: {table.column_names}")
        
        # Display full table
        print("\nFull table:")
        print(table)
        
        # Access data by column (ROW-TO-COLUMN CONVERSION!)
        print("\nColumnar data:")
        print(f"IDs:    {table['ID'].to_pylist()}")
        print(f"Names:  {table['NAME'].to_pylist()}")
        print(f"Emails: {table['EMAIL'].to_pylist()}")
```

**Output**:
```
Retrieved 3 rows
Columns: ['ID', 'NAME', 'EMAIL']

Full table:
pyarrow.Table
ID: int32 not null
NAME: string
EMAIL: string
----
ID: [[1,2,3]]
NAME: [["Alice","Bob","Charlie"]]
EMAIL: [["alice@example.com","bob@example.com","charlie@example.com"]]

Columnar data:
IDs:    [1, 2, 3]
Names:  ['Alice', 'Bob', 'Charlie']
Emails: ['alice@example.com', 'bob@example.com', 'charlie@example.com']
```

## Example 2: Filtered Query

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Query with WHERE clause
        cur.execute("""
            SELECT ID, NAME, EMAIL 
            FROM DB2INST2.EMPLOYEE 
            WHERE ID > 1
            ORDER BY NAME
        """)
        
        table = cur.fetch_arrow_table()
        
        print(f"Found {len(table)} employees with ID > 1")
        print("\nFiltered results (columnar):")
        print(f"IDs:    {table['ID'].to_pylist()}")
        print(f"Names:  {table['NAME'].to_pylist()}")
        print(f"Emails: {table['EMAIL'].to_pylist()}")
```

**Output**:
```
Found 2 employees with ID > 1

Filtered results (columnar):
IDs:    [2, 3]
Names:  ['Bob', 'Charlie']
Emails: ['bob@example.com', 'charlie@example.com']
```

## Example 3: Specific Columns

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Select only specific columns
        cur.execute("SELECT NAME, EMAIL FROM DB2INST2.EMPLOYEE")
        
        table = cur.fetch_arrow_table()
        
        print("Employee contact information:")
        names = table['NAME'].to_pylist()
        emails = table['EMAIL'].to_pylist()
        
        for name, email in zip(names, emails):
            print(f"  {name}: {email}")
```

**Output**:
```
Employee contact information:
  Alice: alice@example.com
  Bob: bob@example.com
  Charlie: charlie@example.com
```

## Example 4: Convert to Dictionary (Column-Oriented)

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM DB2INST2.EMPLOYEE")
        table = cur.fetch_arrow_table()
        
        # Convert to dictionary (column-oriented format)
        data_dict = table.to_pydict()
        
        print("Data as dictionary (columnar format):")
        for column_name, column_values in data_dict.items():
            print(f"{column_name}: {column_values}")
```

**Output**:
```
Data as dictionary (columnar format):
ID: [1, 2, 3]
NAME: ['Alice', 'Bob', 'Charlie']
EMAIL: ['alice@example.com', 'bob@example.com', 'charlie@example.com']
```

## Example 5: Convert to Pandas DataFrame

```python
import adbc_driver_db2.dbapi as dbapi
import pandas as pd

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM DB2INST2.EMPLOYEE")
        table = cur.fetch_arrow_table()
        
        # Convert to Pandas DataFrame
        df = table.to_pandas()
        
        print("As Pandas DataFrame:")
        print(df)
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Access columns
        print(f"\nNames column: {df['NAME'].tolist()}")
```

**Output**:
```
As Pandas DataFrame:
   ID     NAME                EMAIL
0   1    Alice  alice@example.com
1   2      Bob    bob@example.com
2   3  Charlie  charlie@example.com

DataFrame shape: (3, 3)
Columns: ['ID', 'NAME', 'EMAIL']

Names column: ['Alice', 'Bob', 'Charlie']
```

## Example 6: Aggregate Query

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Aggregate query
        cur.execute("""
            SELECT 
                COUNT(*) AS total_employees,
                MIN(ID) AS min_id,
                MAX(ID) AS max_id
            FROM DB2INST2.EMPLOYEE
        """)
        
        table = cur.fetch_arrow_table()
        
        print("Aggregate results (columnar):")
        print(f"Total employees: {table['TOTAL_EMPLOYEES'][0].as_py()}")
        print(f"Min ID: {table['MIN_ID'][0].as_py()}")
        print(f"Max ID: {table['MAX_ID'][0].as_py()}")
```

**Output**:
```
Aggregate results (columnar):
Total employees: 3
Min ID: 1
Max ID: 3
```

## Example 7: Join with Another Table

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        # Join EMPLOYEE with EMPLOYEE_SALARY
        cur.execute("""
            SELECT 
                e.ID,
                e.NAME,
                e.EMAIL,
                s.SALARY
            FROM DB2INST2.EMPLOYEE e
            LEFT JOIN DB2INST2.EMPLOYEE_SALARY s ON e.ID = s.EMPLOYEE_ID
            ORDER BY e.ID
        """)
        
        table = cur.fetch_arrow_table()
        
        print(f"Employee data with salaries ({len(table)} rows):")
        print("\nColumnar format:")
        print(f"IDs:      {table['ID'].to_pylist()}")
        print(f"Names:    {table['NAME'].to_pylist()}")
        print(f"Emails:   {table['EMAIL'].to_pylist()}")
        print(f"Salaries: {table['SALARY'].to_pylist()}")
```

## Example 8: Processing Large Results in Batches

```python
import adbc_driver_db2
import adbc_driver_manager
import pyarrow as pa

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Use low-level API for batch processing
db = adbc_driver_db2.connect(uri=uri)
conn = adbc_driver_manager.AdbcConnection(db)
stmt = adbc_driver_manager.AdbcStatement(conn)

# Set batch size
stmt.set_options(**{
    adbc_driver_db2.StatementOptions.BATCH_ROWS.value: "2"  # Small batch for demo
})

# Execute query
stmt.set_sql_query("SELECT * FROM DB2INST2.EMPLOYEE")
stream, _ = stmt.execute_query()

# Process in batches
reader = pa.RecordBatchReader._import_from_c(stream.address)
batch_num = 0

for batch in reader:
    batch_num += 1
    print(f"\nBatch {batch_num}:")
    print(f"  Rows: {len(batch)}")
    print(f"  IDs: {batch['ID'].to_pylist()}")
    print(f"  Names: {batch['NAME'].to_pylist()}")

# Cleanup
stmt.close()
conn.close()
db.close()
```

**Output**:
```
Batch 1:
  Rows: 2
  IDs: [1, 2]
  Names: ['Alice', 'Bob']

Batch 2:
  Rows: 1
  IDs: [3]
  Names: ['Charlie']
```

## Complete Working Script

Save this as `query_employee.py`:

```python
#!/usr/bin/env python3
"""
Query EMPLOYEE table and display results in columnar format
"""

import adbc_driver_db2.dbapi as dbapi

def main():
    # Connection string
    uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
    
    print("Connecting to DB2...")
    with dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            # Query EMPLOYEE table
            print("\nQuerying EMPLOYEE table...")
            cur.execute("SELECT * FROM DB2INST2.EMPLOYEE ORDER BY ID")
            
            # Get results as Arrow table (columnar format)
            table = cur.fetch_arrow_table()
            
            print(f"\n✓ Retrieved {len(table)} employees")
            print(f"✓ Columns: {table.column_names}")
            
            # Display full table
            print("\n" + "=" * 60)
            print("Full Table (Arrow format):")
            print("=" * 60)
            print(table)
            
            # Display columnar data
            print("\n" + "=" * 60)
            print("Columnar Data (Row-to-Column Conversion):")
            print("=" * 60)
            print(f"IDs:    {table['ID'].to_pylist()}")
            print(f"Names:  {table['NAME'].to_pylist()}")
            print(f"Emails: {table['EMAIL'].to_pylist()}")
            
            # Display as dictionary
            print("\n" + "=" * 60)
            print("As Dictionary (Column-Oriented):")
            print("=" * 60)
            data_dict = table.to_pydict()
            for col_name, col_values in data_dict.items():
                print(f"{col_name}: {col_values}")
            
            # Display as Pandas
            print("\n" + "=" * 60)
            print("As Pandas DataFrame:")
            print("=" * 60)
            df = table.to_pandas()
            print(df)
            print(f"\nShape: {df.shape}")
            
    print("\n✓ Done!")

if __name__ == "__main__":
    main()
```

## Running the Examples

### Setup Environment
```bash
cd arrow-adbc
source .venv-db2/bin/activate
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
```

### Run the Script
```bash
python query_employee.py
```

## Key Concepts

### Row-Oriented vs Column-Oriented

**Traditional Row-Oriented (SQL result set)**:
```
Row 1: {ID: 1, NAME: 'Alice', EMAIL: 'alice@example.com'}
Row 2: {ID: 2, NAME: 'Bob', EMAIL: 'bob@example.com'}
Row 3: {ID: 3, NAME: 'Charlie', EMAIL: 'charlie@example.com'}
```

**Column-Oriented (Arrow/ADBC)**:
```
ID:    [1, 2, 3]
NAME:  ['Alice', 'Bob', 'Charlie']
EMAIL: ['alice@example.com', 'bob@example.com', 'charlie@example.com']
```

### Benefits of Columnar Format

1. **Better Performance**: Columnar storage is more efficient for analytics
2. **Compression**: Similar data types compress better
3. **Vectorization**: Modern CPUs can process columns faster
4. **Memory Efficiency**: Load only needed columns
5. **Integration**: Works seamlessly with Pandas, NumPy, and analytics tools

## Next Steps

1. Try the examples with your EMPLOYEE table
2. Modify queries to filter or aggregate data
3. Experiment with joins to other tables
4. Process large datasets in batches
5. Integrate with your data analysis workflow

## Additional Tables in Your Database

You also have these tables available:
- `DB2INST2.EMPLOYEE_INFO` - Additional employee information
- `DB2INST2.EMPLOYEE_SALARY` - Salary data
- `DB2INST2.ADBC_DEMO_ROW_COL` - Demo table for row-to-column conversion

Try querying these tables using the same patterns shown above!

## Troubleshooting

If you encounter issues:

1. **Check connection**: Verify DB2 is running on port 50000
2. **Set library path**: Ensure DYLD_LIBRARY_PATH is set correctly
3. **Verify table exists**: Run `SELECT * FROM SYSCAT.TABLES WHERE TABNAME = 'EMPLOYEE'`
4. **Check permissions**: Ensure user has SELECT permission on the table

## Resources

- [USER_GUIDE.md](USER_GUIDE.md) - Comprehensive user guide
- [TEST_RESULTS.md](TEST_RESULTS.md) - Test results and validation
- [DB2_TESTING_GUIDE.md](DB2_TESTING_GUIDE.md) - Testing documentation