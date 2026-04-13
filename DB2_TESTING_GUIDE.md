# DB2 Driver Local Testing Guide

This guide will help you test the DB2 ADBC driver locally.

## Prerequisites

1. **Docker** - For running DB2 in a container
2. **Python 3.10+** - For running tests
3. **CMake 3.18+** - For building the C++ driver
4. **C++ compiler** - GCC 8+ or Clang 10+

## Quick Start

### Option 1: Using Docker Compose (Recommended)

1. **Start DB2 container:**
   ```bash
   cd arrow-adbc
   docker compose up -d db2-test
   ```

2. **Wait for DB2 to be ready** (this can take 2-3 minutes):
   ```bash
   docker compose logs -f db2-test
   # Wait until you see "Setup has completed"
   ```

3. **Verify DB2 is healthy:**
   ```bash
   docker compose ps db2-test
   # Should show "healthy" status
   ```

4. **Build and test the driver** (see sections below)

### Option 2: Using Existing DB2 Instance

If you have an existing DB2 instance, update the connection string in `.env`:

```bash
# Edit arrow-adbc/.env
ADBC_DB2_TEST_URI="DATABASE=yourdb;UID=youruser;PWD=yourpass;HOSTNAME=yourhost;PORT=50000;PROTOCOL=TCPIP"
```

## Building the Driver

### Step 1: Build C++ Driver

```bash
cd arrow-adbc

# Create build directory
mkdir -p build
cd build

# Configure with CMake
cmake ../c \
  -DADBC_DRIVER_DB2=ON \
  -DADBC_BUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=$PWD/local

# Build
cmake --build . --target adbc_driver_db2

# Optional: Run C++ tests
ctest -R db2 --output-on-failure
```

**Note:** The DB2 driver requires DB2 CLI libraries. The build will search for:
- `libdb2` or `libdb2cli` on Linux/macOS
- `db2cli.lib` on Windows

If not found automatically, set `DB2_HOME` environment variable:
```bash
export DB2_HOME=/path/to/db2/installation
```

### Step 2: Build Python Package

```bash
cd arrow-adbc

# Create virtual environment
python3 -m venv .venv-db2
source .venv-db2/bin/activate  # On Windows: .venv-db2\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install adbc-driver-manager pyarrow pandas pytest

# Install the DB2 driver package in development mode
cd python/adbc_driver_db2
pip install -e .

# Set environment variable to point to the built driver
export ADBC_DB2_LIBRARY=$PWD/../../build/driver/db2/libadbc_driver_db2.so
# On macOS: export ADBC_DB2_LIBRARY=$PWD/../../build/driver/db2/libadbc_driver_db2.dylib
# On Windows: set ADBC_DB2_LIBRARY=%CD%\..\..\build\driver\db2\Release\adbc_driver_db2.dll
```

## Running Tests

### Quick Test Script

Run the provided test script:

```bash
cd arrow-adbc
source .venv-db2/bin/activate
export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

python test_db2_driver.py
```

Expected output:
```
============================================================
DB2 ADBC Driver Test Suite
============================================================
Target: localhost:50000 (testdb)
User: db2inst2

============================================================
Step 1: Testing Python imports...
============================================================
✓ adbc_driver_manager imported successfully
✓ adbc_driver_db2 imported successfully
✓ pyarrow 14.0.1 available

============================================================
Step 2: Testing DB2 Connection...
============================================================
...
✓ ALL TESTS PASSED
```

### Python Unit Tests

Run the full test suite:

```bash
cd arrow-adbc/python/adbc_driver_db2
source ../../.venv-db2/bin/activate

# Set connection URI
export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Run tests
pytest tests/ -v
```

### C++ Tests

```bash
cd arrow-adbc/build
ctest -R db2 --output-on-failure -V
```

## Testing Different Scenarios

### 1. Basic Connection Test

```python
import adbc_driver_db2
import adbc_driver_manager

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Create database and connection
db = adbc_driver_db2.connect(uri)
conn = adbc_driver_manager.AdbcConnection(db)

# Execute simple query
stmt = adbc_driver_manager.AdbcStatement(conn)
stmt.set_sql_query("SELECT 1 AS one FROM SYSIBM.SYSDUMMY1")
stream, _ = stmt.execute_query()
table = stream.read_all()
print(table)

# Cleanup
stmt.close()
conn.close()
db.close()
```

### 2. DBAPI Test

```python
import adbc_driver_db2.dbapi as dbapi

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
        result = cur.fetchone()
        print(f"Current timestamp: {result[0]}")
```

### 3. Data Ingestion Test

```python
import pyarrow as pa
import adbc_driver_db2
import adbc_driver_manager

uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

db = adbc_driver_db2.connect(uri)
conn = adbc_driver_manager.AdbcConnection(db)

# Create test table
stmt = adbc_driver_manager.AdbcStatement(conn)
stmt.set_sql_query("CREATE TABLE test_ingest (id INTEGER, name VARCHAR(50))")
stmt.execute_update()

# Ingest data
table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
})
conn.adbc_ingest("test_ingest", table, mode="append")

# Verify
stmt.set_sql_query("SELECT * FROM test_ingest ORDER BY id")
stream, _ = stmt.execute_query()
result = stream.read_all()
print(result)

# Cleanup
stmt.set_sql_query("DROP TABLE test_ingest")
stmt.execute_update()
stmt.close()
conn.close()
db.close()
```

## Troubleshooting

### Issue: "libdb2 not found" during build

**Solution:** Install DB2 CLI driver or set DB2_HOME:

```bash
# Option 1: Install ibm_db Python package (includes CLI driver)
pip install ibm_db

# Find the clidriver location
python -c "import importlib, pathlib; p = pathlib.Path(importlib.import_module('ibm_db').__file__).resolve().parent; print(p.parent / 'clidriver' if p.name == 'ibm_db' else p / 'clidriver')"

# Set DB2_HOME to the clidriver path
export DB2_HOME=/path/to/clidriver

# Option 2: Download IBM Data Server Driver Package
# Visit: https://www.ibm.com/support/pages/download-initial-version-115-clients-and-drivers
```

### Issue: "Connection refused" or "SQL30081N"

**Solutions:**
1. Verify DB2 container is running and healthy:
   ```bash
   docker compose ps db2-test
   docker compose logs db2-test
   ```

2. Check if port 50000 is accessible:
   ```bash
   nc -zv localhost 50000
   # or
   telnet localhost 50000
   ```

3. Wait longer for DB2 initialization (can take 2-3 minutes on first start)

### Issue: "SQL1024N" - Database not found

**Solution:** The database might not be created yet. Connect to the container and create it:

```bash
docker compose exec db2-test su - db2inst1
db2 create database testdb
db2 connect to testdb
```

### Issue: Python can't find the driver library

**Solution:** Set the `ADBC_DB2_LIBRARY` environment variable:

```bash
export ADBC_DB2_LIBRARY=/path/to/libadbc_driver_db2.so
```

Or install the driver to a standard location:

```bash
cd arrow-adbc/build
cmake --install . --prefix ~/.local
export LD_LIBRARY_PATH=~/.local/lib:$LD_LIBRARY_PATH
```

### Issue: "Authentication failed" (SQL30082N)

**Solutions:**
1. Verify credentials in connection string
2. For Docker container, default credentials are:
   - User: `db2inst1` or `db2inst2`
   - Password: `password`
   - Database: `testdb`

## Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `ADBC_DB2_TEST_URI` | Connection string for tests | `DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP` |
| `ADBC_DB2_LIBRARY` | Path to driver shared library | `/path/to/libadbc_driver_db2.so` |
| `DB2_HOME` | DB2 installation directory | `/opt/ibm/db2/V11.5` |
| `IBM_DB_HOME` | Alternative to DB2_HOME | `/path/to/clidriver` |

## Connection String Format

DB2 connection strings use semicolon-separated key=value pairs:

```
DATABASE=dbname;HOSTNAME=host;PORT=port;PROTOCOL=TCPIP;UID=user;PWD=password
```

Required parameters:
- `DATABASE` - Database name
- `HOSTNAME` - Server hostname or IP
- `PORT` - Port number (usually 50000)
- `PROTOCOL` - Usually `TCPIP`
- `UID` - Username
- `PWD` - Password

Optional parameters:
- `SECURITY=SSL` - Enable SSL/TLS
- `SSLServerCertificate=/path/to/cert.arm` - SSL certificate path

## Next Steps

1. **Run integration tests:**
   ```bash
   cd arrow-adbc
   pytest python/adbc_driver_db2/tests/ -v
   ```

2. **Test with your own DB2 instance:**
   - Update connection string in `.env`
   - Run tests against your database

3. **Contribute:**
   - Report issues on GitHub
   - Submit pull requests with improvements
   - Add more test cases

## Additional Resources

- [ADBC Documentation](https://arrow.apache.org/adbc/)
- [DB2 Documentation](https://www.ibm.com/docs/en/db2)
- [Arrow ADBC GitHub](https://github.com/apache/arrow-adbc)

## Docker Compose Commands

```bash
# Start DB2
docker compose up -d db2-test

# View logs
docker compose logs -f db2-test

# Check status
docker compose ps db2-test

# Stop DB2
docker compose stop db2-test

# Remove DB2 (including data)
docker compose down -v db2-test

# Restart DB2
docker compose restart db2-test

# Execute commands in DB2 container
docker compose exec db2-test su - db2inst1
```

## License

Licensed under the Apache License, Version 2.0.