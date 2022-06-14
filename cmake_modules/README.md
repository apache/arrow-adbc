# CMake Build Utilities

Common CMake utilities taken from Apache Arrow. These should be kept
in sync with Arrow upstream without modification. Exceptions:

- All flags were renamed to avoid colliding with Arrow flags (since
  ArrowOptions.cmake will also define these options when
  `find_package(Arrow)`).
- DefineOptions.cmake has different flags.
