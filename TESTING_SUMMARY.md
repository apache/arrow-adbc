# DB2 Driver Testing - Quick Reference

## 🚀 Fastest Way to Test

```bash
cd arrow-adbc
./test_db2_quick.sh
```

This automated script handles everything: starting DB2, building the driver, setting up Python, and running tests.

## 📋 What's Available

### Documentation
- **[DB2_TESTING_GUIDE.md](DB2_TESTING_GUIDE.md)** - Comprehensive testing guide with troubleshooting
- **[python/adbc_driver_db2/README_TESTING.md](python/adbc_driver_db2/README_TESTING.md)** - Python-specific testing instructions

### Test Files
- **[test_db2_driver.py](test_db2_driver.py)** - Simple standalone test script
- **[test_db2_quick.sh](test_db2_quick.sh)** - Automated test runner
- **[python/adbc_driver_db2/tests/](python/adbc_driver_db2/tests/)** - Full test suite

### Configuration
- **[.env](.env)** - Environment variables (line 59: `ADBC_DB2_TEST_URI`)
- **[compose.yaml](compose.yaml)** - Docker Compose config (lines 343-359: `db2-test` service)

## 🎯 Quick Commands

### Start DB2
```bash
docker compose up -d db2-test
docker compose logs -f db2-test  # Watch logs
```

### Build Driver
```bash
mkdir -p build && cd build
cmake ../c -DADBC_DRIVER_DB2=ON -DADBC_BUILD_TESTS=ON
cmake --build . --target adbc_driver_db2
```

### Run Tests
```bash
# Quick test
python test_db2_driver.py

# Full Python tests
cd python/adbc_driver_db2
pytest tests/ -v

# C++ tests
cd build
ctest -R db2 --output-on-failure
```

## 🔧 Environment Setup

### Required Environment Variables
```bash
# Connection string
export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"

# Driver library path (if not installed system-wide)
export ADBC_DB2_LIBRARY=/path/to/build/driver/db2/libadbc_driver_db2.so
```

### Python Virtual Environment
```bash
python3 -m venv .venv-db2
source .venv-db2/bin/activate
pip install adbc-driver-manager pyarrow pandas pytest
cd python/adbc_driver_db2 && pip install -e .
```

## 🐛 Common Issues

| Issue | Solution |
|-------|----------|
| `libdb2 not found` during build | Install `ibm_db` package or set `DB2_HOME` |
| Connection refused | Wait for DB2 to be healthy (2-3 min), check `docker compose ps` |
| Driver library not found | Set `ADBC_DB2_LIBRARY` environment variable |
| Authentication failed | Use `db2inst1` or `db2inst2` with password `password` |

## 📊 Test Coverage

The test suite covers:
- ✅ Basic connectivity
- ✅ Query execution (SELECT, INSERT, UPDATE, DELETE)
- ✅ Metadata operations (GetInfo, GetTableTypes, GetObjects)
- ✅ Data ingestion (Arrow tables → DB2)
- ✅ Transaction management (commit/rollback)
- ✅ DBAPI compatibility
- ✅ Error handling
- ✅ Type mapping (Arrow ↔ DB2)

## 🔍 Verification Checklist

Before submitting changes, verify:

- [ ] `./test_db2_quick.sh` passes
- [ ] `pytest python/adbc_driver_db2/tests/ -v` passes
- [ ] `ctest -R db2` passes (C++ tests)
- [ ] No memory leaks (run with ASAN if possible)
- [ ] Documentation is updated
- [ ] Code follows project style guidelines

## 📚 Additional Resources

- [ADBC Specification](https://arrow.apache.org/adbc/current/format/specification.html)
- [DB2 CLI Documentation](https://www.ibm.com/docs/en/db2/11.5?topic=interface-db2-call-level-cli-odbc)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)

## 🤝 Contributing

When adding new features or fixing bugs:

1. Add tests to `python/adbc_driver_db2/tests/`
2. Update documentation
3. Run full test suite
4. Submit PR with clear description

## 💡 Tips

- **Use Docker Compose**: Easiest way to get a DB2 instance
- **Check logs**: `docker compose logs db2-test` for DB2 issues
- **Incremental testing**: Test small changes frequently
- **Use virtual environments**: Isolate dependencies
- **Read error messages**: DB2 provides detailed SQLSTATE codes

## 🎓 Learning Path

1. Start with `test_db2_driver.py` - understand basic flow
2. Read `DB2_TESTING_GUIDE.md` - comprehensive overview
3. Explore `python/adbc_driver_db2/tests/` - see all test patterns
4. Review C++ code in `c/driver/db2/` - understand implementation
5. Check CI workflows in `.github/workflows/` - see automated testing

## 📞 Getting Help

- **Documentation**: Start with [DB2_TESTING_GUIDE.md](DB2_TESTING_GUIDE.md)
- **Issues**: Check existing GitHub issues
- **Community**: Ask on Apache Arrow mailing list
- **Logs**: Always include error messages and logs when reporting issues

---

**Last Updated**: 2026-04-06  
**Driver Version**: See `python/adbc_driver_db2/adbc_driver_db2/_version.py`  
**DB2 Version**: 11.5+ (tested with Docker image `icr.io/db2_community/db2:latest`)