#!/usr/bin/env python3
"""
DB2 ADBC Driver - Interactive Web UI

A simple web interface to query DB2 and view results in columnar format.

To run:
  cd arrow-adbc
  source .venv-db2/bin/activate
  export DYLD_LIBRARY_PATH=$PWD/.venv-db2/lib/python3.14/site-packages/clidriver/lib
  pip install streamlit
  streamlit run db2_ui_app.py
"""

import streamlit as st
import adbc_driver_db2.dbapi as dbapi
import pandas as pd
import traceback

# Page configuration
st.set_page_config(
    page_title="DB2 Query Tool",
    page_icon="🗄️",
    layout="wide"
)

# Title
st.title("🗄️ DB2 ADBC Query Tool")
st.markdown("Interactive tool to query DB2 and view results in columnar format")

# Sidebar for connection settings
st.sidebar.header("Connection Settings")

# Connection inputs
database = st.sidebar.text_input("Database", "testdb")
hostname = st.sidebar.text_input("Hostname", "localhost")
port = st.sidebar.text_input("Port", "50000")
username = st.sidebar.text_input("Username", "db2inst2")
password = st.sidebar.text_input("Password", "password", type="password")

# Build connection string
uri = f"DATABASE={database};UID={username};PWD={password};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP"

# Connection status
if st.sidebar.button("Test Connection"):
    try:
        with dbapi.connect(uri) as conn:
            st.sidebar.success("✅ Connected successfully!")
    except Exception as e:
        st.sidebar.error(f"❌ Connection failed: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("### Quick Actions")

# Initialize session state for query
if 'query' not in st.session_state:
    st.session_state['query'] = "SELECT * FROM DB2INST2.EMPLOYEE"

# Quick query buttons
if st.sidebar.button("📋 List All Tables"):
    st.session_state['query'] = """SELECT TABSCHEMA, TABNAME, TYPE, CARD 
FROM SYSCAT.TABLES 
WHERE TABSCHEMA NOT LIKE 'SYS%' 
ORDER BY TABSCHEMA, TABNAME
FETCH FIRST 50 ROWS ONLY"""

if st.sidebar.button("👥 Query EMPLOYEE Table"):
    st.session_state['query'] = "SELECT * FROM DB2INST2.EMPLOYEE ORDER BY ID"

if st.sidebar.button("📊 Employee Statistics"):
    st.session_state['query'] = """SELECT 
    COUNT(*) AS total_employees,
    MIN(ID) AS min_id,
    MAX(ID) AS max_id
FROM DB2INST2.EMPLOYEE"""

# Main query area
st.header("SQL Query")

# Query input
query = st.text_area(
    "Enter your SQL query:",
    value=st.session_state['query'],
    height=150,
    help="Enter any valid DB2 SQL query"
)

# Update session state
st.session_state['query'] = query

# Execute button
col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    execute_button = st.button("▶️ Execute Query", type="primary")
with col2:
    clear_button = st.button("🗑️ Clear")
    if clear_button:
        st.session_state['query'] = ""
        st.rerun()

# Execute query
if execute_button and query.strip():
    try:
        with st.spinner("Executing query..."):
            # Connect and execute
            with dbapi.connect(uri) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    
                    # Get results as Arrow table
                    table = cur.fetch_arrow_table()
                    
                    # Display results
                    st.success(f"✅ Query executed successfully! Retrieved {len(table)} rows")
                    
                    # Tabs for different views
                    tab1, tab2, tab3, tab4 = st.tabs(["📊 Table View", "📋 Columnar Data", "🔍 Schema", "📈 Statistics"])
                    
                    with tab1:
                        st.subheader("Table View")
                        # Convert to Pandas for display
                        df = table.to_pandas()
                        st.dataframe(df, use_container_width=True)
                        
                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv"
                        )
                    
                    with tab2:
                        st.subheader("Columnar Data (Row-to-Column Conversion)")
                        st.markdown("This shows how data is stored in columnar format:")
                        
                        # Display each column
                        data_dict = table.to_pydict()
                        for col_name, col_values in data_dict.items():
                            with st.expander(f"📌 {col_name}", expanded=True):
                                st.code(f"{col_name}: {col_values}", language="python")
                    
                    with tab3:
                        st.subheader("Schema Information")
                        st.markdown("**Arrow Schema:**")
                        st.code(str(table.schema), language="text")
                        
                        st.markdown("**Column Details:**")
                        schema_data = []
                        for field in table.schema:
                            schema_data.append({
                                "Column": field.name,
                                "Type": str(field.type),
                                "Nullable": field.nullable
                            })
                        st.table(pd.DataFrame(schema_data))
                    
                    with tab4:
                        st.subheader("Statistics")
                        df = table.to_pandas()
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Rows", len(df))
                        with col2:
                            st.metric("Total Columns", len(df.columns))
                        with col3:
                            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB")
                        
                        st.markdown("**Data Types:**")
                        st.dataframe(pd.DataFrame({
                            "Column": df.columns,
                            "Type": df.dtypes.astype(str),
                            "Non-Null Count": df.count()
                        }), use_container_width=True)
                        
                        # Numeric columns statistics
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.markdown("**Numeric Columns Summary:**")
                            st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                    
    except Exception as e:
        st.error(f"❌ Error executing query: {e}")
        with st.expander("Show error details"):
            st.code(traceback.format_exc())

# Footer
st.markdown("---")
st.markdown("""
### 💡 Tips
- Use the quick action buttons in the sidebar for common queries
- Results are displayed in **columnar format** (Arrow tables)
- Switch between tabs to see different views of your data
- Download results as CSV from the Table View tab

### 📚 Example Queries
```sql
-- List all employees
SELECT * FROM DB2INST2.EMPLOYEE

-- Filter employees
SELECT * FROM DB2INST2.EMPLOYEE WHERE ID > 1

-- Aggregate data
SELECT COUNT(*) as total FROM DB2INST2.EMPLOYEE

-- Join tables
SELECT e.*, s.SALARY 
FROM DB2INST2.EMPLOYEE e
LEFT JOIN DB2INST2.EMPLOYEE_SALARY s ON e.ID = s.EMPLOYEE_ID
```
""")

# Made with Bob
