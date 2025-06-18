# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

macro(set_option_category name)
  set(ADBC_OPTION_CATEGORY ${name})
  list(APPEND "ADBC_OPTION_CATEGORIES" ${name})
endmacro()

function(check_description_length name description)
  foreach(description_line ${description})
    string(LENGTH ${description_line} line_length)
    if(${line_length} GREATER 80)
      message(FATAL_ERROR "description for ${name} contained a\n\
        line ${line_length} characters long!\n\
        (max is 80). Split it into more lines with semicolons")
    endif()
  endforeach()
endfunction()

function(list_join lst glue out)
  if("${${lst}}" STREQUAL "")
    set(${out}
        ""
        PARENT_SCOPE)
    return()
  endif()

  list(GET ${lst} 0 joined)
  list(REMOVE_AT ${lst} 0)
  foreach(item ${${lst}})
    set(joined "${joined}${glue}${item}")
  endforeach()
  set(${out}
      ${joined}
      PARENT_SCOPE)
endfunction()

macro(define_option name description default)
  check_description_length(${name} ${description})
  list_join(description "\n" multiline_description)

  option(${name} "${multiline_description}" ${default})

  list(APPEND "ADBC_${ADBC_OPTION_CATEGORY}_OPTION_NAMES" ${name})
  set("${name}_OPTION_DESCRIPTION" ${description})
  set("${name}_OPTION_DEFAULT" ${default})
  set("${name}_OPTION_TYPE" "bool")
endmacro()

macro(define_option_string name description default)
  check_description_length(${name} ${description})
  list_join(description "\n" multiline_description)

  set(${name}
      ${default}
      CACHE STRING "${multiline_description}")

  list(APPEND "ADBC_${ADBC_OPTION_CATEGORY}_OPTION_NAMES" ${name})
  set("${name}_OPTION_DESCRIPTION" ${description})
  set("${name}_OPTION_DEFAULT" "\"${default}\"")
  set("${name}_OPTION_TYPE" "string")
  set("${name}_OPTION_POSSIBLE_VALUES" ${ARGN})

  list_join("${name}_OPTION_POSSIBLE_VALUES" "|" "${name}_OPTION_ENUM")
  if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
    set_property(CACHE ${name} PROPERTY STRINGS "${name}_OPTION_POSSIBLE_VALUES")
  endif()
endmacro()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  #----------------------------------------------------------------------
  set_option_category("Compile and link")

  define_option_string(ADBC_BUILD_WARNING_LEVEL
                       "CHECKIN to enable Werror, PRODUCTION otherwise" "")

  define_option_string(ADBC_CXXFLAGS
                       "Compiler flags to append when compiling ADBC C++ libraries" "")
  define_option_string(ADBC_GO_BUILD_TAGS
                       "Build tags to append when compiling ADBC Go libraries" "")

  define_option(ADBC_BUILD_STATIC "Build static libraries" ON)

  define_option(ADBC_BUILD_SHARED "Build shared libraries" ON)

  define_option_string(ADBC_GIT_ID "The Arrow git commit id (if any)" "")

  define_option_string(ADBC_GIT_DESCRIPTION "The Arrow git commit description (if any)"
                       "")

  define_option(ADBC_USE_CCACHE "Use ccache when compiling (if available)" ON)

  define_option(ADBC_RPATH_ORIGIN "Build Arrow libraries with RATH set to \$ORIGIN" OFF)

  define_option(ADBC_INSTALL_NAME_RPATH
                "Build Arrow libraries with install_name set to @rpath" ON)

  define_option(ADBC_GGDB_DEBUG "Pass -ggdb flag to debug builds" ON)

  define_option(ADBC_WITH_VENDORED_FMT "Use vendored copy of fmt" ON)

  define_option(ADBC_WITH_VENDORED_NANOARROW "Use vendored copy of nanoarrow" ON)

  #----------------------------------------------------------------------
  set_option_category("Test and benchmark")

  define_option(ADBC_BUILD_EXAMPLES "Build the Arrow examples" OFF)

  define_option(ADBC_BUILD_TESTS "Build the Arrow googletest unit tests" OFF)

  define_option(ADBC_BUILD_INTEGRATION "Build the Arrow integration test executables" OFF)

  define_option(ADBC_BUILD_BENCHMARKS "Build the Arrow micro benchmarks" OFF)

  if(ADBC_BUILD_SHARED)
    set(ADBC_TEST_LINKAGE_DEFAULT "shared")
  else()
    set(ADBC_TEST_LINKAGE_DEFAULT "static")
  endif()

  define_option_string(ADBC_TEST_LINKAGE
                       "Linkage of Arrow libraries with unit tests executables."
                       "${ADBC_TEST_LINKAGE_DEFAULT}"
                       "shared"
                       "static")

  define_option(ADBC_DRIVER_MGR_TEST_MANIFEST_USER_LEVEL
                "Build driver manager manifest user-level tests" OFF)

  define_option(ADBC_DRIVER_MGR_TEST_MANIFEST_SYSTEM_LEVEL
                "Build driver manager manifest system-level tests" OFF)

  #----------------------------------------------------------------------
  set_option_category("Lint")

  define_option(ADBC_GENERATE_COVERAGE "Build with C++ code coverage enabled" OFF)

  #----------------------------------------------------------------------
  set_option_category("Checks")

  define_option(ADBC_TEST_MEMCHECK "Run the test suite using valgrind --tool=memcheck"
                OFF)

  define_option(ADBC_USE_ASAN "Enable Address Sanitizer checks" OFF)

  define_option(ADBC_USE_TSAN "Enable Thread Sanitizer checks" OFF)

  define_option(ADBC_USE_UBSAN "Enable Undefined Behavior sanitizer checks" OFF)

  #----------------------------------------------------------------------
  set_option_category("Thirdparty toolchain")

  # Determine how we will look for dependencies
  # * AUTO: Guess which packaging systems we're running in and pull the
  #   dependencies from there. Build any missing ones through the
  #   ExternalProject setup. This is the default unless the CONDA_PREFIX
  #   environment variable is set, in which case the CONDA method is used
  # * BUNDLED: Build dependencies through CMake's ExternalProject facility. If
  #   you wish to build individual dependencies from source instead of using
  #   one of the other methods, pass -D$NAME_SOURCE=BUNDLED
  # * SYSTEM: Use CMake's find_package and find_library without any custom
  #   paths. If individual packages are on non-default locations, you can pass
  #   $NAME_ROOT arguments to CMake, or set environment variables for the same
  #   with CMake 3.11 and higher.  If your system packages are in a non-default
  #   location, or if you are using a non-standard toolchain, you can also pass
  #   ADBC_PACKAGE_PREFIX to set the *_ROOT variables to look in that
  #   directory
  # * CONDA: Same as SYSTEM but set all *_ROOT variables to
  #   ENV{CONDA_PREFIX}. If this is run within an active conda environment,
  #   then ENV{CONDA_PREFIX} will be used for dependencies unless
  #   ADBC_DEPENDENCY_SOURCE is set explicitly to one of the other options
  # * VCPKG: Searches for dependencies installed by vcpkg.
  # * BREW: Use SYSTEM but search for select packages with brew.
  if(NOT "$ENV{CONDA_PREFIX}" STREQUAL "")
    set(ADBC_DEPENDENCY_SOURCE_DEFAULT "CONDA")
  else()
    set(ADBC_DEPENDENCY_SOURCE_DEFAULT "AUTO")
  endif()
  define_option_string(ADBC_DEPENDENCY_SOURCE
                       "Method to use for acquiring arrow's build dependencies"
                       "${ADBC_DEPENDENCY_SOURCE_DEFAULT}"
                       "AUTO"
                       "BUNDLED"
                       "SYSTEM"
                       "CONDA"
                       "VCPKG"
                       "BREW")

  #----------------------------------------------------------------------
  if(MSVC_TOOLCHAIN)
    set_option_category("MSVC")

    define_option(MSVC_LINK_VERBOSE
                  "Pass verbose linking options when linking libraries and executables"
                  OFF)

    define_option(ADBC_USE_STATIC_CRT "Build Arrow with statically linked CRT" OFF)
  endif()

  #----------------------------------------------------------------------
  set_option_category("Advanced developer")

  define_option(ADBC_BUILD_CONFIG_SUMMARY_JSON
                "Summarize build configuration in a JSON file" ON)

  define_option(ADBC_DEFINE_COMMON_ENTRYPOINTS
                "Define the Adbc functions in static/shared driver libraries" ON)

  #----------------------------------------------------------------------
  set_option_category("Project components")

  define_option(ADBC_DRIVER_FLIGHTSQL "Build the Flight SQL driver" OFF)
  define_option(ADBC_DRIVER_MANAGER "Build the driver manager" OFF)
  define_option(ADBC_DRIVER_POSTGRESQL "Build the PostgreSQL driver" OFF)
  define_option(ADBC_DRIVER_SQLITE "Build the SQLite driver" OFF)
  define_option(ADBC_DRIVER_SNOWFLAKE "Build the Snowflake driver" OFF)
  define_option(ADBC_DRIVER_BIGQUERY "Build the BigQuery driver" OFF)

  define_option(ADBC_INTEGRATION_DUCKDB "Build the test suite for DuckDB" OFF)
endif()

macro(validate_config)
  foreach(category ${ADBC_OPTION_CATEGORIES})
    set(option_names ${ADBC_${category}_OPTION_NAMES})

    foreach(name ${option_names})
      set(possible_values ${${name}_OPTION_POSSIBLE_VALUES})
      set(value "${${name}}")
      if(possible_values)
        if(NOT "${value}" IN_LIST possible_values)
          message(FATAL_ERROR "Configuration option ${name} got invalid value '${value}'. "
                              "Allowed values: ${${name}_OPTION_ENUM}.")
        endif()
      endif()
    endforeach()

  endforeach()
endmacro()

macro(config_summary_message)
  message(STATUS "---------------------------------------------------------------------")
  message(STATUS "ADBC version: ${ADBC_VERSION}")
  message(STATUS)
  message(STATUS "Build configuration summary:")

  message(STATUS "  CMake version: ${CMAKE_VERSION}")
  message(STATUS "  Generator: ${CMAKE_GENERATOR}")
  message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
  message(STATUS "  Source directory: ${CMAKE_CURRENT_SOURCE_DIR}")
  message(STATUS "  Install prefix: ${CMAKE_INSTALL_PREFIX}")
  if(${CMAKE_EXPORT_COMPILE_COMMANDS})
    message(STATUS "  Compile commands: ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json"
    )
  endif()

  foreach(category ${ADBC_OPTION_CATEGORIES})

    message(STATUS)
    message(STATUS "${category} options:")
    message(STATUS)

    set(option_names ${ADBC_${category}_OPTION_NAMES})

    foreach(name ${option_names})
      set(value "${${name}}")
      if("${value}" STREQUAL "")
        set(value "\"\"")
      endif()

      set(description ${${name}_OPTION_DESCRIPTION})

      if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
        set(summary "=${value} [default=${${name}_OPTION_ENUM}]")
      else()
        set(summary "=${value} [default=${${name}_OPTION_DEFAULT}]")
      endif()

      message(STATUS "  ${name}${summary}")
      foreach(description_line ${description})
        message(STATUS "      ${description_line}")
      endforeach()
    endforeach()

  endforeach()

endmacro()

macro(config_summary_json)
  set(summary "${CMAKE_CURRENT_BINARY_DIR}/cmake_summary.json")
  message(STATUS "  Outputting build configuration summary to ${summary}")
  file(WRITE ${summary} "{\n")

  foreach(category ${ADBC_OPTION_CATEGORIES})
    foreach(name ${ADBC_${category}_OPTION_NAMES})
      file(APPEND ${summary} "\"${name}\": \"${${name}}\",\n")
    endforeach()
  endforeach()

  file(APPEND ${summary} "\"generator\": \"${CMAKE_GENERATOR}\",\n")
  file(APPEND ${summary} "\"build_type\": \"${CMAKE_BUILD_TYPE}\",\n")
  file(APPEND ${summary} "\"source_dir\": \"${CMAKE_CURRENT_SOURCE_DIR}\",\n")
  if(${CMAKE_EXPORT_COMPILE_COMMANDS})
    file(APPEND ${summary} "\"compile_commands\": "
                           "\"${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json\",\n")
  endif()
  file(APPEND ${summary} "\"install_prefix\": \"${CMAKE_INSTALL_PREFIX}\",\n")
  file(APPEND ${summary} "\"arrow_version\": \"${ADBC_VERSION}\"\n")
  file(APPEND ${summary} "}\n")
endmacro()

macro(config_summary_cmake_setters path)
  file(WRITE ${path} "# Options used to build arrow:")

  foreach(category ${ADBC_OPTION_CATEGORIES})
    file(APPEND ${path} "\n\n## ${category} options:")
    foreach(name ${ADBC_${category}_OPTION_NAMES})
      set(description ${${name}_OPTION_DESCRIPTION})
      foreach(description_line ${description})
        file(APPEND ${path} "\n### ${description_line}")
      endforeach()
      file(APPEND ${path} "\nset(${name} \"${${name}}\")")
    endforeach()
  endforeach()

endmacro()

#----------------------------------------------------------------------
# Compute default values for omitted variables

if(NOT ADBC_GIT_ID)
  execute_process(COMMAND "git" "log" "-n1" "--format=%H"
                  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                  OUTPUT_VARIABLE ADBC_GIT_ID
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
if(NOT ADBC_GIT_DESCRIPTION)
  execute_process(COMMAND "git" "describe" "--tags" "--dirty"
                  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                  ERROR_QUIET
                  OUTPUT_VARIABLE ADBC_GIT_DESCRIPTION
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
