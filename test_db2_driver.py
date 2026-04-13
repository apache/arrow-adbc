#!/usr/bin/env python3
"""
Simple DB2 ADBC Driver Test
Tests the DB2 driver against localhost:50000
"""

import os
import sys

# Test configuration
TEST_URI = "DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

def test_imports():
    """Test if required modules are available"""
    print("=" * 60)
    print("Step 1: Testing Python imports...")
    print("=" * 60)
    
    try:
        import adbc_driver_manager
        print("✓ adbc_driver_manager imported successfully")
        print(f"  Location: {adbc_driver_manager.__file__}")
    except ImportError as e:
        print(f"✗ adbc_driver_manager not found: {e}")
        print("\nInstall with: pip install adbc-driver-manager")
        return False
    
    try:
        import adbc_driver_db2
        print("✓ adbc_driver_db2 imported successfully")
        print(f"  Location: {adbc_driver_db2.__file__}")
    except ImportError as e:
        print(f"✗ adbc_driver_db2 not found: {e}")
        print("\nBuild and install the driver first")
        return False
    
    try:
        import pyarrow
        print(f"✓ pyarrow {pyarrow.__version__} available")
    except ImportError:
        print("⚠ pyarrow not available (optional for DBAPI)")
    
    return True

def test_connection():
    """Test DB2 connection"""
    print("\n" + "=" * 60)
    print("Step 2: Testing DB2 Connection...")
    print("=" * 60)
    print(f"Connection URI: {TEST_URI[:50]}...")
    
    try:
        import adbc_driver_db2
        import adbc_driver_manager as mgr
        
        print("\nInitializing database...")
        db = adbc_driver_db2.connect(uri=TEST_URI)
        print("✓ Database initialized")
        
        print("Opening connection...")
        conn = mgr.AdbcConnection(db)
        print("✓ Connection opened")
        
        print("Creating statement...")
        stmt = mgr.AdbcStatement(conn)
        print("✓ Statement created")
        
        print("\nExecuting test query: SELECT 1 AS one FROM SYSIBM.SYSDUMMY1")
        stmt.set_sql_query("SELECT 1 AS one FROM SYSIBM.SYSDUMMY1")
        stream, rows_affected = stmt.execute_query()
        print(f"✓ Query executed (rows_affected: {rows_affected})")
        
        print("Reading results...")
        import pyarrow as pa
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        table = reader.read_all()
        print(f"✓ Results retrieved: {table}")
        print(f"  Schema: {table.schema}")
        print(f"  Num rows: {len(table)}")
        print(f"  Data: {table.to_pydict()}")
        
        print("\nCleaning up...")
        stmt.close()
        conn.close()
        db.close()
        print("✓ Cleanup complete")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Connection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_metadata():
    """Test metadata operations"""
    print("\n" + "=" * 60)
    print("Step 3: Testing Metadata Operations...")
    print("=" * 60)
    
    try:
        import adbc_driver_db2
        import adbc_driver_manager as mgr
        
        db = adbc_driver_db2.connect(uri=TEST_URI)
        conn = mgr.AdbcConnection(db)
        
        print("\nGetting driver info...")
        info_codes = [
            mgr.AdbcInfoCode.VENDOR_NAME,
            mgr.AdbcInfoCode.VENDOR_VERSION,
            mgr.AdbcInfoCode.DRIVER_NAME,
            mgr.AdbcInfoCode.DRIVER_VERSION,
        ]
        info_stream = conn.get_info(info_codes)
        import pyarrow as pa
        info_reader = pa.RecordBatchReader._import_from_c(info_stream.address)
        info_table = info_reader.read_all()
        print("✓ Driver info retrieved:")
        for row in info_table.to_pylist():
            print(f"  {row}")
        
        print("\nGetting table types...")
        types_stream = conn.get_table_types()
        types_reader = pa.RecordBatchReader._import_from_c(types_stream.address)
        types_table = types_reader.read_all()
        print(f"✓ Table types: {types_table.to_pydict()}")
        
        conn.close()
        db.close()
        
        return True
        
    except Exception as e:
        print(f"\n✗ Metadata test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("\n" + "=" * 60)
    print("DB2 ADBC Driver Test Suite")
    print("=" * 60)
    print(f"Target: localhost:50000 (testdb)")
    print(f"User: db2inst2")
    
    results = []
    
    # Test 1: Imports
    if test_imports():
        results.append(("Imports", True))
        
        # Test 2: Connection
        if test_connection():
            results.append(("Connection", True))
            
            # Test 3: Metadata
            if test_metadata():
                results.append(("Metadata", True))
            else:
                results.append(("Metadata", False))
        else:
            results.append(("Connection", False))
            results.append(("Metadata", False))
    else:
        results.append(("Imports", False))
        results.append(("Connection", False))
        results.append(("Metadata", False))
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    all_passed = all(passed for _, passed in results)
    print("\n" + ("=" * 60))
    if all_passed:
        print("✓ ALL TESTS PASSED")
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        return 1

if __name__ == "__main__":
    sys.exit(main())

# Made with Bob
