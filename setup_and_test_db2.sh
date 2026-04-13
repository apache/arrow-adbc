#!/bin/bash
# DB2 Driver Setup and Test Script
# This script helps diagnose and test the DB2 ADBC driver

set -e

echo "=========================================="
echo "DB2 ADBC Driver Setup & Test"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [ ! -f "compose.yaml" ]; then
    echo -e "${RED}Error: Must run from arrow-adbc directory${NC}"
    exit 1
fi

echo -e "${BLUE}Step 1: Checking Prerequisites${NC}"
echo "=========================================="

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓ Python found: $PYTHON_VERSION${NC}"
else
    echo -e "${RED}✗ Python 3 not found${NC}"
    exit 1
fi

# Check if driver is built
if [ -f "build/driver/db2/libadbc_driver_db2.dylib" ] || [ -f "build/driver/db2/libadbc_driver_db2.so" ]; then
    echo -e "${GREEN}✓ DB2 driver library found${NC}"
    if [ -f "build/driver/db2/libadbc_driver_db2.dylib" ]; then
        DRIVER_PATH="$PWD/build/driver/db2/libadbc_driver_db2.dylib"
    else
        DRIVER_PATH="$PWD/build/driver/db2/libadbc_driver_db2.so"
    fi
    echo "  Location: $DRIVER_PATH"
else
    echo -e "${RED}✗ DB2 driver not built${NC}"
    echo "  Run: mkdir -p build && cd build && cmake ../c -DADBC_DRIVER_DB2=ON && cmake --build . --target adbc_driver_db2"
    exit 1
fi

# Check virtual environment
if [ -d ".venv-db2" ]; then
    echo -e "${GREEN}✓ Virtual environment found${NC}"
else
    echo -e "${YELLOW}⚠ Virtual environment not found, creating...${NC}"
    python3 -m venv .venv-db2
fi

echo ""
echo -e "${BLUE}Step 2: Setting up Python Environment${NC}"
echo "=========================================="

# Activate virtual environment
source .venv-db2/bin/activate

# Install/upgrade dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q adbc-driver-manager pyarrow pandas pytest 2>/dev/null || {
    echo -e "${YELLOW}⚠ Some packages may have failed to install${NC}"
}

# Install DB2 driver package
cd python/adbc_driver_db2
pip install -q -e . 2>/dev/null || {
    echo -e "${YELLOW}⚠ DB2 driver package installation had warnings${NC}"
}
cd ../..

echo -e "${GREEN}✓ Python environment ready${NC}"

echo ""
echo -e "${BLUE}Step 3: Checking DB2 Availability${NC}"
echo "=========================================="

# Check if port 50000 is accessible
if nc -zv localhost 50000 2>&1 | grep -q "succeeded\|open"; then
    echo -e "${GREEN}✓ DB2 is accessible on localhost:50000${NC}"
    DB2_AVAILABLE=true
else
    echo -e "${YELLOW}⚠ No DB2 instance found on localhost:50000${NC}"
    DB2_AVAILABLE=false
    
    # Check Docker
    if command -v docker &> /dev/null; then
        echo ""
        echo -e "${BLUE}Docker is available. Options:${NC}"
        echo "  1. Start DB2 with Docker (recommended)"
        echo "  2. Connect to existing DB2 instance"
        echo "  3. Skip testing (just verify setup)"
        echo ""
        read -p "Choose option (1/2/3): " choice
        
        case $choice in
            1)
                echo ""
                echo "Starting DB2 container..."
                echo "Note: This requires Docker to be running"
                
                # Try docker compose
                if docker compose version &> /dev/null; then
                    docker compose up -d db2-test
                elif docker-compose --version &> /dev/null; then
                    docker-compose up -d db2-test
                else
                    echo -e "${RED}✗ Docker Compose not found${NC}"
                    echo "Install Docker Compose or start Docker Desktop"
                    exit 1
                fi
                
                echo ""
                echo "Waiting for DB2 to be ready (this takes 2-3 minutes)..."
                timeout=180
                elapsed=0
                while [ $elapsed -lt $timeout ]; do
                    if nc -zv localhost 50000 2>&1 | grep -q "succeeded\|open"; then
                        echo -e "${GREEN}✓ DB2 is ready!${NC}"
                        DB2_AVAILABLE=true
                        break
                    fi
                    sleep 5
                    elapsed=$((elapsed + 5))
                    echo "  Waiting... ($elapsed/$timeout seconds)"
                done
                
                if [ $elapsed -ge $timeout ]; then
                    echo -e "${RED}✗ DB2 failed to start within $timeout seconds${NC}"
                    echo "Check logs with: docker compose logs db2-test"
                    exit 1
                fi
                ;;
            2)
                echo ""
                echo "Enter DB2 connection details:"
                read -p "Hostname [localhost]: " DB2_HOST
                DB2_HOST=${DB2_HOST:-localhost}
                read -p "Port [50000]: " DB2_PORT
                DB2_PORT=${DB2_PORT:-50000}
                read -p "Database name: " DB2_DATABASE
                read -p "Username: " DB2_USER
                read -sp "Password: " DB2_PASSWORD
                echo ""
                
                export ADBC_DB2_TEST_URI="DATABASE=$DB2_DATABASE;UID=$DB2_USER;PWD=$DB2_PASSWORD;HOSTNAME=$DB2_HOST;PORT=$DB2_PORT;PROTOCOL=TCPIP"
                
                # Test connection
                if nc -zv $DB2_HOST $DB2_PORT 2>&1 | grep -q "succeeded\|open"; then
                    echo -e "${GREEN}✓ Can reach $DB2_HOST:$DB2_PORT${NC}"
                    DB2_AVAILABLE=true
                else
                    echo -e "${RED}✗ Cannot reach $DB2_HOST:$DB2_PORT${NC}"
                    exit 1
                fi
                ;;
            3)
                echo ""
                echo "Skipping DB2 connection test"
                DB2_AVAILABLE=false
                ;;
            *)
                echo "Invalid choice"
                exit 1
                ;;
        esac
    else
        echo ""
        echo -e "${YELLOW}Docker not available.${NC}"
        echo "To test the driver, you need a DB2 instance running."
        echo ""
        echo "Options:"
        echo "  1. Install Docker and run: docker compose up -d db2-test"
        echo "  2. Connect to an existing DB2 instance"
        echo "  3. Install DB2 locally"
        echo ""
        read -p "Do you have an existing DB2 instance? (y/n): " has_db2
        
        if [ "$has_db2" = "y" ]; then
            echo ""
            echo "Enter DB2 connection details:"
            read -p "Hostname: " DB2_HOST
            read -p "Port [50000]: " DB2_PORT
            DB2_PORT=${DB2_PORT:-50000}
            read -p "Database name: " DB2_DATABASE
            read -p "Username: " DB2_USER
            read -sp "Password: " DB2_PASSWORD
            echo ""
            
            export ADBC_DB2_TEST_URI="DATABASE=$DB2_DATABASE;UID=$DB2_USER;PWD=$DB2_PASSWORD;HOSTNAME=$DB2_HOST;PORT=$DB2_PORT;PROTOCOL=TCPIP"
            DB2_AVAILABLE=true
        else
            echo ""
            echo "Setup verification complete. To test, you'll need to:"
            echo "  1. Start a DB2 instance"
            echo "  2. Set ADBC_DB2_TEST_URI environment variable"
            echo "  3. Run: python test_db2_driver.py"
            exit 0
        fi
    fi
fi

# Set environment variables
export ADBC_DB2_LIBRARY="$DRIVER_PATH"
if [ -z "$ADBC_DB2_TEST_URI" ]; then
    export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
fi

echo ""
echo -e "${BLUE}Step 4: Environment Configuration${NC}"
echo "=========================================="
echo "ADBC_DB2_LIBRARY=$ADBC_DB2_LIBRARY"
echo "ADBC_DB2_TEST_URI=$ADBC_DB2_TEST_URI"

if [ "$DB2_AVAILABLE" = true ]; then
    echo ""
    echo -e "${BLUE}Step 5: Running Tests${NC}"
    echo "=========================================="
    echo ""
    
    python test_db2_driver.py
    TEST_RESULT=$?
    
    echo ""
    echo "=========================================="
    if [ $TEST_RESULT -eq 0 ]; then
        echo -e "${GREEN}✓ All tests passed!${NC}"
        echo ""
        echo "Next steps:"
        echo "  • Run full test suite: pytest python/adbc_driver_db2/tests/ -v"
        echo "  • Run C++ tests: cd build && ctest -R db2 --output-on-failure"
        echo "  • See DB2_TESTING_GUIDE.md for more information"
    else
        echo -e "${RED}✗ Some tests failed${NC}"
        echo ""
        echo "Troubleshooting:"
        echo "  • Check DB2 logs: docker compose logs db2-test"
        echo "  • Verify connection string"
        echo "  • See DB2_TESTING_GUIDE.md for help"
    fi
    
    exit $TEST_RESULT
else
    echo ""
    echo -e "${GREEN}✓ Setup complete!${NC}"
    echo ""
    echo "To run tests when DB2 is available:"
    echo "  1. Start DB2 instance"
    echo "  2. Set connection: export ADBC_DB2_TEST_URI='DATABASE=...;UID=...;PWD=...;HOSTNAME=...;PORT=...;PROTOCOL=TCPIP'"
    echo "  3. Run: python test_db2_driver.py"
    exit 0
fi

# Made with Bob
