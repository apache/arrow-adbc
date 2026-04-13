# Running DB2 Driver in VSCode - Step by Step

This guide shows you how to run Python files in VSCode to query your DB2 database.

## Method 1: Run Python File Directly in VSCode (Recommended)

### Step 1: Open the File
1. In VSCode, open `query_employee.py`
2. You should see the Python code

### Step 2: Select Python Interpreter
1. Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
2. Type "Python: Select Interpreter"
3. Choose: `.venv-db2/bin/python` (the virtual environment)

### Step 3: Set Environment Variables
Create a file `.vscode/launch.json` in your project:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Query DB2",
            "type": "debugpy",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "env": {
                "DYLD_LIBRARY_PATH": "${workspaceFolder}/.venv-db2/lib/python3.14/site-packages/clidriver/lib",
                "ADBC_DB2_LIBRARY": "${workspaceFolder}/build/driver/db2/libadbc_driver_db2.dylib"
            }
        }
    ]
}
```

### Step 4: Run the File
**Option A**: Click the ▶️ Run button in the top-right corner

**Option B**: Press `F5` to run with debugger

**Option C**: Right-click in the editor → "Run Python File in Terminal"

## Method 2: Use VSCode Terminal

### Step 1: Open Terminal in VSCode
- Press `` Ctrl+` `` (backtick) or
- Menu: Terminal → New Terminal

### Step 2: Run Setup Commands
```bash
cd arrow-adbc
source .venv-db2/bin/activate
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
```

### Step 3: Run Python File
```bash
python query_employee.py
```

## Method 3: Interactive Python (Best for Learning)

### Step 1: Open Terminal
Press `` Ctrl+` ``

### Step 2: Start Python Interactive Mode
```bash
cd arrow-adbc
source .venv-db2/bin/activate
export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
python
```

### Step 3: Run Code Interactively
```python
# Import the driver
import adbc_driver_db2.dbapi as dbapi

# Connect
uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
conn = dbapi.connect(uri)
cur = conn.cursor()

# Query
cur.execute("SELECT * FROM DB2INST2.EMPLOYEE")
table = cur.fetch_arrow_table()

# View results
print(table)
print(f"\nIDs: {table['ID'].to_pylist()}")
print(f"Names: {table['NAME'].to_pylist()}")
print(f"Emails: {table['EMAIL'].to_pylist()}")

# Cleanup
cur.close()
conn.close()
```

## Method 4: Jupyter Notebook in VSCode

### Step 1: Install Jupyter
```bash
cd arrow-adbc
source .venv-db2/bin/activate
pip install jupyter ipykernel
```

### Step 2: Create Notebook
1. In VSCode: File → New File
2. Save as `db2_query.ipynb`
3. VSCode will recognize it as a Jupyter notebook

### Step 3: Select Kernel
1. Click "Select Kernel" in top-right
2. Choose `.venv-db2` Python interpreter

### Step 4: Add Code Cells

**Cell 1: Setup**
```python
import os
os.environ['DYLD_LIBRARY_PATH'] = '/Users/nishantavasthi/Documents/arrow-adbc/arrow-adbc/.venv-db2/lib/python3.14/site-packages/clidriver/lib'

import adbc_driver_db2.dbapi as dbapi
import pandas as pd
```

**Cell 2: Connect**
```python
uri = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
conn = dbapi.connect(uri)
print("✅ Connected!")
```

**Cell 3: Query**
```python
cur = conn.cursor()
cur.execute("SELECT * FROM DB2INST2.EMPLOYEE")
table = cur.fetch_arrow_table()
df = table.to_pandas()
df
```

**Cell 4: Columnar View**
```python
print("Columnar format:")
print(f"IDs: {table['ID'].to_pylist()}")
print(f"Names: {table['NAME'].to_pylist()}")
print(f"Emails: {table['EMAIL'].to_pylist()}")
```

### Step 5: Run Cells
Click the ▶️ button next to each cell or press `Shift+Enter`

## Quick Reference

### Files You Can Run
- `query_employee.py` - Query EMPLOYEE table
- `test_db2_driver.py` - Run all tests
- `example_usage.py` - Multiple examples

### VSCode Shortcuts
- `F5` - Run with debugger
- `Ctrl+F5` - Run without debugger
- `` Ctrl+` `` - Open terminal
- `Cmd/Ctrl+Shift+P` - Command palette

### Environment Variables (Required)
```bash
DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
```

## Troubleshooting in VSCode

### Issue: "Module not found"
**Solution**: Select correct Python interpreter
1. `Cmd/Ctrl+Shift+P`
2. "Python: Select Interpreter"
3. Choose `.venv-db2/bin/python`

### Issue: "Library not loaded"
**Solution**: Set environment variable in launch.json (see Method 1, Step 3)

### Issue: Terminal not using virtual environment
**Solution**: 
```bash
source .venv-db2/bin/activate
```

## Recommended Workflow

1. **Open VSCode** in the `arrow-adbc` folder
2. **Open Terminal** (`` Ctrl+` ``)
3. **Activate environment**:
   ```bash
   source .venv-db2/bin/activate
   export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
   ```
4. **Run your file**:
   ```bash
   python query_employee.py
   ```

Or use the launch.json configuration for one-click running!

## Next Steps

1. Try running `query_employee.py` in VSCode
2. Modify the queries to explore your data
3. Create your own Python files
4. Use Jupyter notebooks for interactive exploration

See [PRACTICAL_GUIDE.md](PRACTICAL_GUIDE.md) for more query examples!