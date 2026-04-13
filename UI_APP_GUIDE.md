# DB2 Query Tool - Web UI

Interactive web interface to query DB2 and view results in columnar format.

## Quick Start

```bash
# 1. Install Streamlit
cd arrow-adbc
source .venv-db2/bin/activate
pip install streamlit

# 2. Set environment
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib

# 3. Run the app
streamlit run db2_ui_app.py
```

The app opens at `http://localhost:8501`

## Features

- 📝 **SQL Query Editor** - Write and execute queries
- 📊 **Table View** - See results as a table
- 📋 **Columnar View** - See row-to-column conversion
- 🔍 **Schema Info** - View column types
- 📥 **Export CSV** - Download results

## Using the App

### 1. Connection (Sidebar)
- Database: testdb
- Hostname: localhost
- Port: 50000
- Username: db2inst2
- Password: password

Click "Test Connection" to verify.

### 2. Quick Actions (Sidebar)
- 📋 List All Tables
- 👥 Query EMPLOYEE Table
- 📊 Employee Statistics

### 3. Write Query
```sql
SELECT * FROM DB2INST2.EMPLOYEE
```

Click "▶️ Execute Query"

### 4. View Results

**Table View**: Traditional table display  
**Columnar Data**: Shows row-to-column conversion
```
ID:    [1, 2, 3]
NAME:  ['Alice', 'Bob', 'Charlie']
EMAIL: ['alice@example.com', 'bob@example.com', 'charlie@example.com']
```

**Schema**: Column types and info  
**Statistics**: Row counts and memory usage

## Example Queries

```sql
-- All employees
SELECT * FROM DB2INST2.EMPLOYEE

-- Filtered
SELECT * FROM DB2INST2.EMPLOYEE WHERE ID > 1

-- Aggregate
SELECT COUNT(*) AS total FROM DB2INST2.EMPLOYEE

-- Join
SELECT e.*, s.SALARY 
FROM DB2INST2.EMPLOYEE e
LEFT JOIN DB2INST2.EMPLOYEE_SALARY s ON e.ID = s.EMPLOYEE_ID
```

## Troubleshooting

**Connection failed**: Check DB2 is running on port 50000  
**Library not loaded**: Set DYLD_LIBRARY_PATH  
**Module not found**: Run `pip install streamlit`

## See Also

- [PRACTICAL_GUIDE.md](PRACTICAL_GUIDE.md) - Query examples with real data
- [USER_GUIDE.md](USER_GUIDE.md) - Comprehensive usage guide