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

find_program(GO_BIN "go" REQUIRED)
message(STATUS "Detecting Go executable: Found ${GO_BIN}")

set(ADBC_GO_PACKAGE_INIT
    [=[
get_filename_component(_IMPORT_PREFIX "${CMAKE_CURRENT_LIST_FILE}" PATH)
get_filename_component(_IMPORT_PREFIX "${_IMPORT_PREFIX}" PATH)
get_filename_component(_IMPORT_PREFIX "${_IMPORT_PREFIX}" PATH)
get_filename_component(_IMPORT_PREFIX "${_IMPORT_PREFIX}" PATH)
if(_IMPORT_PREFIX STREQUAL "/")
  set(_IMPORT_PREFIX "")
endif()

function(adbc_add_shared_library target_name base_name)
  set(shared_base_name
    "${CMAKE_SHARED_LIBRARY_PREFIX}${base_name}${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(prefix "${_IMPORT_PREFIX}/${ADBC_INSTALL_LIBDIR}")
  add_library(${target_name} SHARED IMPORTED)
  if(WINDOWS)
    set(import_base_name
      "${CMAKE_IMPORT_LIBRARY_PREFIX}${base_name}${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    set_target_properties(${target_name}
      PROPERTIES
      IMPORTED_IMPLIB "${prefix}/${import_base_name}"
      IMPORTED_LOCATION "${_IMPORT_PREFIX}/bin/${shared_base_name}")
  else()
    set_target_properties(${target_name}
      PROPERTIES
      IMPORTED_LOCATION "${prefix}/${shared_base_name}.${ADBC_FULL_SO_VERSION}"
      IMPORTED_SONAME "${prefix}/${shared_base_name}.${ADBC_SO_VERSION}")
  endif()
endfunction()

function(adbc_add_static_library target_name base_name)
  set(static_base_name
    "${CMAKE_STATIC_LIBRARY_PREFIX}${base_name}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  add_library(${target_name} STATIC IMPORTED)
  target_compile_definitions(${target_name} ADBC_STATIC)
  if(WINDOWS)
    set_target_properties(${target_name}
      PROPERTIES
      IMPORTED_LOCATION "${_IMPORT_PREFIX}/bin/${static_base_name}")
  else()
    set(prefix "${_IMPORT_PREFIX}/${ADBC_INSTALL_LIBDIR}")
    set_target_properties(${target_name}
      PROPERTIES
      IMPORTED_LOCATION "${prefix}/${static_base_name}")
  endif()
endfunction()
]=])

function(add_go_lib GO_MOD_DIR GO_LIBNAME)
  set(options)
  set(one_value_args
      BUILD_TAGS
      SHARED_LINK_FLAGS
      CMAKE_PACKAGE_NAME
      PKG_CONFIG_NAME
      BUILD_STATIC
      BUILD_SHARED)
  set(multi_value_args SOURCES DEFINES OUTPUTS)

  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS})
  endif()

  # Allow overriding ADBC_BUILD_SHARED and ADBC_BUILD_STATIC
  if(DEFINED ARG_BUILD_SHARED)
    set(BUILD_SHARED ${ARG_BUILD_SHARED})
  else()
    set(BUILD_SHARED ${ADBC_BUILD_SHARED})
  endif()
  if(DEFINED ARG_BUILD_STATIC)
    set(BUILD_STATIC ${ARG_BUILD_STATIC})
  else()
    set(BUILD_STATIC ${ADBC_BUILD_STATIC})
  endif()

  set(BUILD_TAGS)

  if(DEFINED ARG_BUILD_TAGS)
    set(BUILD_TAGS "${ARG_BUILD_TAGS}")
  endif()

  if(NOT "${ADBC_GO_BUILD_TAGS}" STREQUAL "")
    if(NOT "${BUILD_TAGS}" STREQUAL "")
      set(BUILD_TAGS "${BUILD_TAGS},")
    endif()
    set(BUILD_TAGS "${BUILD_TAGS}${ADBC_GO_BUILD_TAGS}")
  endif()

  if(NOT "${BUILD_TAGS}" STREQUAL "")
    set(GO_BUILD_TAGS "-tags=${BUILD_TAGS}")
  endif()

  list(TRANSFORM ARG_SOURCES PREPEND "${GO_MOD_DIR}/")

  # go asan only works on linux/amd64 and linux/arm64
  if(ADBC_USE_ASAN)
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "ARM64")
      if(NOT APPLE AND NOT MSVC_TOOLCHAIN)
        set(GO_BUILD_FLAGS "-asan")
      endif()
    endif()
  endif()

  # Go gcflags for disabling optimizations and inlining if debug
  separate_arguments(GO_BUILD_FLAGS
                     NATIVE_COMMAND
                     "${GO_BUILD_FLAGS} -buildvcs=true $<$<CONFIG:DEBUG>:-gcflags=\"-N -l\">"
  )

  # if we're building debug mode then change the default CGO_CFLAGS and CGO_CXXFLAGS from "-g O2" to "-g3"
  set(GO_FLAGS "$<$<CONFIG:Debug>:-g3>")
  foreach(DEFINE ${ARG_DEFINES})
    string(APPEND GO_FLAGS " -D${DEFINE}")
  endforeach()

  set(GO_ENV_VARS)
  list(APPEND GO_ENV_VARS "CGO_ENABLED=1")
  list(APPEND GO_ENV_VARS "CGO_CFLAGS=${GO_FLAGS}")
  list(APPEND GO_ENV_VARS "CGO_CXXFLAGS=${GO_FLAGS}")

  if(BUILD_SHARED)
    set(LIB_NAME_SHARED
        "${CMAKE_SHARED_LIBRARY_PREFIX}${GO_LIBNAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")

    set(ADBC_VERSION_SCRIPT_LINK_FLAG
        "-Wl,--version-script=${REPOSITORY_ROOT}/c/symbols.map")

    check_linker_flag(CXX ${ADBC_VERSION_SCRIPT_LINK_FLAG}
                      CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
    if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
      set(EXTLDFLAGS ",--version-script=${REPOSITORY_ROOT}/c/symbols.map")
    endif()

    if(NOT APPLE)
      set(EXTLDFLAGS "${EXTLDFLAGS},-soname,${LIB_NAME_SHARED}.${ADBC_SO_VERSION}")
    endif()

    if(DEFINED EXTLDFLAGS)
      set(EXTLDFLAGS "'-extldflags=-Wl${EXTLDFLAGS}'")
    endif()

    if(DEFINED ARG_SHARED_LINK_FLAGS)
      separate_arguments(ARG_SHARED_LINK_FLAGS NATIVE_COMMAND "${ARG_SHARED_LINK_FLAGS}")
    endif()

    set(GO_LDFLAGS
        "-ldflags;\"${ARG_SHARED_LINK_FLAGS};-X;github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase.infoDriverVersion=v${ADBC_VERSION};-a;${EXTLDFLAGS}\""
    )

    set(LIBOUT_SHARED "${CMAKE_CURRENT_BINARY_DIR}/${LIB_NAME_SHARED}")

    if(CMAKE_OSX_ARCHITECTURES STREQUAL "x86_64")
      list(APPEND GO_ENV_VARS "GOARCH=amd64")
    elseif(CMAKE_OSX_ARCHITECTURES STREQUAL "arm64")
      list(APPEND GO_ENV_VARS "GOARCH=arm64")
    endif()

    add_custom_command(OUTPUT "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
                       WORKING_DIRECTORY ${GO_MOD_DIR}
                       DEPENDS ${ARG_SOURCES}
                       COMMAND ${CMAKE_COMMAND} -E env ${GO_ENV_VARS} ${GO_BIN} build
                               ${GO_BUILD_TAGS} "${GO_BUILD_FLAGS}" -o
                               ${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}
                               -buildmode=c-shared ${GO_LDFLAGS} .
                       COMMAND ${CMAKE_COMMAND} -E remove -f
                               "${LIBOUT_SHARED}.${ADBC_SO_VERSION}.0.h"
                       COMMENT "Building Go Shared lib ${GO_LIBNAME}"
                       COMMAND_EXPAND_LISTS)

    add_custom_command(OUTPUT "${LIBOUT_SHARED}.${ADBC_SO_VERSION}" "${LIBOUT_SHARED}"
                       DEPENDS "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
                       WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                       COMMAND ${CMAKE_COMMAND} -E create_symlink
                               "${LIB_NAME_SHARED}.${ADBC_FULL_SO_VERSION}"
                               "${LIB_NAME_SHARED}.${ADBC_SO_VERSION}"
                       COMMAND ${CMAKE_COMMAND} -E create_symlink
                               "${LIB_NAME_SHARED}.${ADBC_SO_VERSION}"
                               "${LIB_NAME_SHARED}")

    add_custom_target(${GO_LIBNAME}_target ALL
                      DEPENDS "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
                              "${LIBOUT_SHARED}.${ADBC_SO_VERSION}" "${LIBOUT_SHARED}")
    add_library(${GO_LIBNAME}_shared SHARED IMPORTED GLOBAL)
    set_target_properties(${GO_LIBNAME}_shared
                          PROPERTIES IMPORTED_LOCATION
                                     "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
                                     IMPORTED_SONAME "${LIB_NAME_SHARED}")
    add_dependencies(${GO_LIBNAME}_shared ${GO_LIBNAME}_target)
    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${GO_LIBNAME}_shared)
    endif()

    if(ADBC_RPATH_ORIGIN)
      if(APPLE)
        set(_lib_install_rpath "@loader_path")
      else()
        set(_lib_install_rpath "\$ORIGIN")
      endif()
      set_target_properties(${GO_LIBNAME}_shared PROPERTIES INSTALL_RPATH
                                                            ${_lib_install_rpath})
    endif()

    if(APPLE)
      if(ADBC_INSTALL_NAME_RPATH)
        set(_lib_install_name "@rpath")
      else()
        set(_lib_install_name "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
      endif()
      set_target_properties(${GO_LIBNAME}_shared PROPERTIES INSTALL_NAME_DIR
                                                            "${_lib_install_name}")
    endif()

    if(CMAKE_VERSION VERSION_LESS "3.21")
      if(WIN32)
        install(PROGRAMS $<TARGET_FILE:${GO_LIBNAME}_shared> ${INSTALL_IS_OPTIONAL}
                DESTINATION ${RUNTIME_INSTALL_DIR})
      else()
        install(PROGRAMS $<TARGET_FILE:${GO_LIBNAME}_shared> ${INSTALL_IS_OPTIONAL}
                TYPE LIB)
      endif()
    else()
      install(IMPORTED_RUNTIME_ARTIFACTS
              ${GO_LIBNAME}_shared
              ${INSTALL_IS_OPTIONAL}
              RUNTIME
              DESTINATION
              ${RUNTIME_INSTALL_DIR}
              LIBRARY
              DESTINATION
              ${CMAKE_INSTALL_LIBDIR})
    endif()
    if(WIN32)
      # This symlink doesn't get installed
      install(FILES "${LIBOUT_SHARED}.${ADBC_SO_VERSION}" TYPE BIN)
    else()
      install(FILES "${LIBOUT_SHARED}" "${LIBOUT_SHARED}.${ADBC_SO_VERSION}" TYPE LIB)
    endif()
  endif()

  if(BUILD_STATIC)
    set(LIBNAME_STATIC
        "${CMAKE_STATIC_LIBRARY_PREFIX}${GO_LIBNAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LIBOUT_STATIC "${CMAKE_CURRENT_BINARY_DIR}/${LIBNAME_STATIC}")
    if(CMAKE_VERSION VERSION_LESS "3.20")
      string(REGEX REPLACE "\\..+$" ".h" LIBOUT_HEADER "${LIBOUT_STATIC}")
    else()
      cmake_path(REPLACE_EXTENSION
                 LIBOUT_STATIC
                 ".h"
                 OUTPUT_VARIABLE
                 LIBOUT_HEADER)
    endif()
    add_custom_command(OUTPUT "${LIBOUT_STATIC}"
                       WORKING_DIRECTORY ${GO_MOD_DIR}
                       DEPENDS ${ARG_SOURCES}
                       COMMAND ${CMAKE_COMMAND} -E env "${GO_ENV_VARS}" ${GO_BIN} build
                               "${GO_BUILD_TAGS}" -o "${LIBOUT_STATIC}"
                               -buildmode=c-archive "${GO_BUILD_FLAGS}" .
                       COMMAND ${CMAKE_COMMAND} -E remove -f "${LIBOUT_HEADER}"
                       COMMENT "Building Go Static lib ${GO_LIBNAME}"
                       COMMAND_EXPAND_LISTS)

    add_custom_target(${GO_LIBNAME}_static_target ALL DEPENDS "${LIBOUT_STATIC}")
    add_library(${GO_LIBNAME}_static STATIC IMPORTED)
    set_target_properties(${GO_LIBNAME}_static PROPERTIES IMPORTED_LOCATION
                                                          "${LIBOUT_STATIC}")
    add_dependencies(${GO_LIBNAME}_static ${GO_LIBNAME}_static_target)
    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${GO_LIBNAME}_static)
    endif()

    install(FILES "${LIBOUT_STATIC}" TYPE LIB)
  endif()

  if(ARG_CMAKE_PACKAGE_NAME)
    arrow_install_cmake_package(${ARG_CMAKE_PACKAGE_NAME} "")
  endif()

  if(ARG_PKG_CONFIG_NAME)
    arrow_add_pkg_config("${ARG_PKG_CONFIG_NAME}")
  endif()

  # Modify variable in calling scope
  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS}
        ${${ARG_OUTPUTS}}
        PARENT_SCOPE)
  endif()
endfunction()
