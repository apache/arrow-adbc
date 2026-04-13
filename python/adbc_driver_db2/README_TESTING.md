# Testing the DB2 ADBC Driver

This document provides instructions for testing the DB2 ADBC driver locally.

## Quick Start

From the repository root:

```bash
# Make the script executable (first time only)
chmod +x test_db2_quick.sh

# Run the quick test
./test_db2_quick.sh
```

This script will:
1. Start a DB2 Docker container
2. Build the C++ driver (if needed)
3. Set up a Python virtual environment
4. Run basic connectivity tests

## Manual Testing

### 1. Start DB2 Container

```bash
docker compose up -d db2-test

# Wait for it to be healthy (2-3 minutes)
docker compose ps db2-test
```

### 2. Build the Driver

```bash
# From repository root
mkdir -p build && cd build

cmake ../c \
  -DADBC_DRIVER_DB2=ON \
  -DADBC_BUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Release

cmake --build . --target adbc_driver_db2
```

### 3. Install Python Package

```bash
# Create and activate virtual environment
python3 -m venv .venv-db2
source .venv-db2/bin/activate

# Install dependencies
pip install adbc-driver-manager pyarrow pandas pytest

# Install this package in development mode
cd python/adbc_driver_db2
pip install -e .
```

### 4. Set Environment Variables

```bash
# Connection string
export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Path to driver library
export ADBC_DB2_LIBRARY=/path/to/build/driver/db2/libadbc_driver_db2.so
# On macOS: libadbc_driver_db2.dylib
# On Windows: adbc_driver_db2.dll
```

### 5. Run Tests

```bash
# Quick test
python ../../test_db2_driver.py

# Full test suite
pytest tests/ -v

# Specific test
pytest tests/test_lowlevel.py::test_query_trivial -v
```

## Test Examples

### Basic Connection

```python
import adbc_driver_db2
import adbc_driver_manager

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

db = adbc_driver_db2.connect(uri)
conn = adbc_driver_manager.AdbcConnection(db)

stmt = adbc_driver_manager.AdbcStatement(conn)
stmt.set_sql_query("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
stream, _ = stmt.execute_query()
table = stream.read_all()
print(table)

stmt.close()
conn.close()
db.close()
```

### Using DBAPI

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS one FROM SYSIBM.SYSDUMMY1")
        print(cur.fetchone())
```

### Data Ingestion

```python
import pyarrow as pa
import adbc_driver_db2
import adbc_driver_manager

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

db = adbc_driver_db2.connect(uri)
conn = adbc_driver_manager.AdbcConnection(db)

# Create table
stmt = adbc_driver_manager.AdbcStatement(conn)
stmt.set_sql_query("CREATE TABLE test_data (id INT, name VARCHAR(50))")
stmt.execute_update()

# Ingest data
table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
conn.adbc_ingest("test_data", table, mode="append")

# Query back
stmt.set_sql_query("SELECT * FROM test_data ORDER BY id")
stream, _ = stmt.execute_query()
result = stream.read_all()
print(result)

# Cleanup
stmt.set_sql_query("DROP TABLE test_data")
stmt.execute_update()
stmt.close()
conn.close()
db.close()
```

## Troubleshooting

### Driver Library Not Found

If you see errors about the driver library not being found:

```bash
# Option 1: Set ADBC_DB2_LIBRARY
export ADBC_DB2_LIBRARY=/full/path/to/libadbc_driver_db2.so

# Option 2: Install to standard location
cd build
cmake --install . --prefix ~/.local
export LD_LIBRARY_PATH=~/.local/lib:$LD_LIBRARY_PATH
```

### DB2 CLI Libraries Not Found During Build

The driver needs DB2 CLI libraries. Options:

1. **Install ibm_db Python package** (includes CLI driver):
   ```bash
   pip install ibm_db
   
   # Find clidriver location
   python -c "import importlib, pathlib; p = pathlib.Path(importlib.import_module('ibm_db').__file__).resolve().parent; print(p.parent / 'clidriver' if p.name == 'ibm_db' else p / 'clidriver')"
   
   # Set DB2_HOME
   export DB2_HOME=/path/to/clidriver
   ```

2. **Download IBM Data Server Driver Package**:
   - Visit: https://www.ibm.com/support/pages/download-initial-version-115-clients-and-drivers
   - Extract and set `DB2_HOME` to the installation directory

### Connection Issues

If you can't connect to DB2:

```bash
# Check if DB2 is running
docker compose ps db2-test

# Check logs
docker compose logs db2-test

# Test connection manually
docker compose exec db2-test su - db2inst1 -c "db2 connect to testdb"

# Verify port is accessible
nc -zv localhost 50000
```

### Authentication Errors

Default credentials for Docker container:
- **User**: `db2inst1` or `db2inst2`
- **Password**: `password`
- **Database**: `testdb`

Note: The `.env` file uses `db2inst2`, but the container creates `db2inst1`. Both should work.

## Running Specific Tests

```bash
# Test connection only
pytest tests/test_lowlevel.py::test_query_trivial -v

# Test metadata operations
pytest tests/test_lowlevel.py::test_get_table_types -v

# Test DBAPI
pytest tests/test_dbapi.py -v

# Run with verbose output
pytest tests/ -v -s

# Run with coverage
pytest tests/ --cov=adbc_driver_db2 --cov-report=html
```

## CI/CD Testing

The driver is tested in CI using Docker Compose:

```bash
# Start DB2
docker compose up -d db2-test

# Wait for healthy status
docker compose ps db2-test

# Run tests
pytest python/adbc_driver_db2/tests/ -v

# Cleanup
docker compose down -v
```

## Performance Testing

For performance testing, you can adjust the batch size:

```python
import adbc_driver_db2

db = adbc_driver_db2.connect(uri)
conn = adbc_driver_manager.AdbcConnection(db)
stmt = adbc_driver_manager.AdbcStatement(conn)

# Set batch size (default: 65536)
stmt.set_options(**{
    adbc_driver_db2.StatementOptions.BATCH_ROWS.value: "131072"
})

stmt.set_sql_query("SELECT * FROM large_table")
stream, _ = stmt.execute_query()
# Process batches...
```

## Additional Resources

- [Main Testing Guide](../../DB2_TESTING_GUIDE.md) - Comprehensive testing documentation
- [ADBC Documentation](https://arrow.apache.org/adbc/)
- [DB2 Documentation](https://www.ibm.com/docs/en/db2)

## Getting Help

If you encounter issues:

1. Check the [DB2_TESTING_GUIDE.md](../../DB2_TESTING_GUIDE.md)
2. Review Docker logs: `docker compose logs db2-test`
3. Open an issue on GitHub with:
   - Error messages
   - Connection string (without password)
   - DB2 version
   - Operating system