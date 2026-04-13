#!/bin/bash
# Quick test script for DB2 ADBC driver
# Usage: ./test_db2_quick.sh

set -e

echo "=========================================="
echo "DB2 ADBC Driver Quick Test"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [ ! -f "compose.yaml" ]; then
    echo -e "${RED}Error: Must run from arrow-adbc directory${NC}"
    exit 1
fi

# Step 1: Check Docker
echo "Step 1: Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not found. Please install Docker.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker found${NC}"

# Step 2: Start DB2 container
echo ""
echo "Step 2: Starting DB2 container..."
if docker compose ps db2-test | grep -q "Up"; then
    echo -e "${GREEN}✓ DB2 container already running${NC}"
else
    echo "Starting DB2 (this may take 2-3 minutes)..."
    docker compose up -d db2-test
    
    echo "Waiting for DB2 to be healthy..."
    timeout=180
    elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if docker compose ps db2-test | grep -q "healthy"; then
            echo -e "${GREEN}✓ DB2 is ready${NC}"
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
fi

# Step 3: Check if build exists
echo ""
echo "Step 3: Checking C++ driver build..."
if [ ! -f "build/driver/db2/libadbc_driver_db2.so" ] && [ ! -f "build/driver/db2/libadbc_driver_db2.dylib" ]; then
    echo -e "${YELLOW}⚠ Driver not built. Building now...${NC}"
    
    mkdir -p build
    cd build
    
    cmake ../c \
        -DADBC_DRIVER_DB2=ON \
        -DADBC_BUILD_TESTS=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=$PWD/local
    
    cmake --build . --target adbc_driver_db2 -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)
    
    cd ..
    echo -e "${GREEN}✓ Driver built successfully${NC}"
else
    echo -e "${GREEN}✓ Driver already built${NC}"
fi

# Step 4: Setup Python environment
echo ""
echo "Step 4: Setting up Python environment..."
if [ ! -d ".venv-db2" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv-db2
fi

source .venv-db2/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install -q --upgrade pip
pip install -q adbc-driver-manager pyarrow pandas pytest 2>/dev/null || true

# Install DB2 driver package
cd python/adbc_driver_db2
pip install -q -e .
cd ../..

echo -e "${GREEN}✓ Python environment ready${NC}"

# Step 5: Set environment variables
echo ""
echo "Step 5: Configuring environment..."
export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Detect OS and set library path
if [[ "$OSTYPE" == "darwin"* ]]; then
    export ADBC_DB2_LIBRARY="$PWD/build/driver/db2/libadbc_driver_db2.dylib"
else
    export ADBC_DB2_LIBRARY="$PWD/build/driver/db2/libadbc_driver_db2.so"
fi

if [ ! -f "$ADBC_DB2_LIBRARY" ]; then
    echo -e "${RED}✗ Driver library not found at: $ADBC_DB2_LIBRARY${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Environment configured${NC}"
echo "  URI: $ADBC_DB2_TEST_URI"
echo "  Library: $ADBC_DB2_LIBRARY"

# Step 6: Run tests
echo ""
echo "Step 6: Running tests..."
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
    echo "  1. Run full test suite: pytest python/adbc_driver_db2/tests/ -v"
    echo "  2. Run C++ tests: cd build && ctest -R db2 --output-on-failure"
    echo "  3. See DB2_TESTING_GUIDE.md for more information"
else
    echo -e "${RED}✗ Some tests failed${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check DB2 logs: docker compose logs db2-test"
    echo "  2. Verify connection: docker compose exec db2-test su - db2inst1 -c 'db2 connect to testdb'"
    echo "  3. See DB2_TESTING_GUIDE.md for more help"
fi

exit $TEST_RESULT

# Made with Bob
