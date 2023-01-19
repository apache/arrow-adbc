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

function(install_cmake_package PACKAGE_NAME EXPORT_NAME)
  set(CONFIG_CMAKE "${PACKAGE_NAME}Config.cmake")
  set(BUILT_CONFIG_CMAKE "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_CMAKE}")
  configure_package_config_file("${CONFIG_CMAKE}.in" "${BUILT_CONFIG_CMAKE}"
                                INSTALL_DESTINATION "${ARROW_CMAKE_DIR}/${PACKAGE_NAME}")
  set(CONFIG_VERSION_CMAKE "${PACKAGE_NAME}ConfigVersion.cmake")
  set(BUILT_CONFIG_VERSION_CMAKE "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_VERSION_CMAKE}")
  write_basic_package_version_file("${BUILT_CONFIG_VERSION_CMAKE}"
                                   COMPATIBILITY SameMajorVersion)
  set(INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PACKAGE_NAME}")
  install(FILES "${BUILT_CONFIG_CMAKE}" "${BUILT_CONFIG_VERSION_CMAKE}"
          DESTINATION "${INSTALL_CMAKEDIR}")
  set(TARGETS_CMAKE "${PACKAGE_NAME}Targets.cmake")
  install(EXPORT ${EXPORT_NAME}
          DESTINATION "${INSTALL_CMAKEDIR}"
          NAMESPACE "${PACKAGE_NAME}::"
          FILE "${TARGETS_CMAKE}"
          EXPORT_LINK_INTERFACE_LIBRARIES)
endfunction()

function(add_go_shared_lib GO_MOD_DIR GO_LIBNAME)
  set(options)
  set(one_value_args BUILD_TAGS SHARED_LINK_FLAGS CMAKE_PACKAGE_NAME PKG_CONFIG_NAME BUILD_STATIC BUILD_SHARED)
  set(multi_value_args SOURCES OUTPUTS)

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

  if(DEFINED ARG_BUILD_TAGS)
    set(GO_BUILD_TAGS "-tags=${ARG_BUILD_TAGS}")
  endif()

  list(TRANSFORM ARG_SOURCES PREPEND "${GO_MOD_DIR}/")

  if(BUILD_SHARED)
    set(LIB_NAME_SHARED
      "${CMAKE_SHARED_LIBRARY_PREFIX}${GO_LIBNAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")

    set(ARG_SHARED_LINK_FLAGS
        "${ARG_SHARED_LINK_FLAGS} -extldflags -Wl,-soname,${LIB_NAME_SHARED}.${ADBC_SO_VERSION}")
    if(DEFINED ARG_SHARED_LINK_FLAGS)
      separate_arguments(ARG_SHARED_LINK_FLAGS NATIVE_COMMAND "${ARG_SHARED_LINK_FLAGS}")
      set(GO_LDFLAGS "-ldflags=\"${ARG_SHARED_LINK_FLAGS}\"")
    endif()

    set(LIBOUT_SHARED "${CMAKE_CURRENT_BINARY_DIR}/${LIB_NAME_SHARED}")
  
    add_custom_command(OUTPUT "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
                      WORKING_DIRECTORY ${GO_MOD_DIR}
                      DEPENDS ${ARG_SOURCES}
                      COMMAND ${GO_BIN} build "${GO_BUILD_TAGS}" -o
                              "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}" -buildmode=c-shared
                              "${GO_LDFLAGS}" .
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
                              "${LIB_NAME_SHARED}.${ADBC_SO_VERSION}" "${LIB_NAME_SHARED}")

    add_custom_target(${GO_LIBNAME}_target ALL
                      DEPENDS "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
                              "${LIBOUT_SHARED}.${ADBC_SO_VERSION}" "${LIBOUT_SHARED}")
    add_library(${GO_LIBNAME}_shared SHARED IMPORTED)
    set_target_properties(${GO_LIBNAME}_shared
                          PROPERTIES IMPORTED_LOCATION "${LIBOUT_SHARED}.${ADBC_FULL_SO_VERSION}"
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
      set_target_properties(${GO_LIBNAME}_shared PROPERTIES INSTALL_RPATH ${_lib_install_rpath})
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

    install(IMPORTED_RUNTIME_ARTIFACTS
            ${GO_LIBNAME}_shared
            ${INSTALL_IS_OPTIONAL}
            RUNTIME
            DESTINATION
            ${RUNTIME_INSTALL_DIR}
            LIBRARY
            DESTINATION
            ${CMAKE_INSTALL_LIBDIR})
    install(FILES "${LIBOUT_SHARED}.${ADBC_SO_VERSION}" TYPE LIB)
  endif()

  if(BUILD_STATIC)
    set(LIBNAME_STATIC "${CMAKE_STATIC_LIBRARY_PREFIX}${GO_LIBNAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LIBOUT_STATIC "${CMAKE_CURRENT_BINARY_DIR}/${LIBNAME_STATIC}")
    cmake_path(REPLACE_EXTENSION LIBOUT_STATIC ".h" OUTPUT_VARIABLE LIBOUT_HEADER)    
    add_custom_command(OUTPUT "${LIBOUT_STATIC}"
                      WORKING_DIRECTORY ${GO_MOD_DIR}
                      DEPENDS ${ARG_SOURCES}
                      COMMAND ${GO_BIN} build "${GO_BUILD_TAGS}" -o
                              "${LIBOUT_STATIC}" -buildmode=c-archive .
                      COMMAND ${CMAKE_COMMAND} -E remove -f
                              "${LIBOUT_HEADER}"
                      COMMENT "Building Go Static lib ${GO_LIBNAME}"
                      COMMAND_EXPAND_LISTS)

    add_custom_target(${GO_LIBNAME}_static_target ALL
                      DEPENDS "${LIBOUT_STATIC}")
    add_library(${GO_LIBNAME}_static STATIC IMPORTED)
    set_target_properties(${GO_LIBNAME}_static
                          PROPERTIES IMPORTED_LOCATION "${LIBOUT_STATIC}")
    add_dependencies(${GO_LIBNAME}_static ${GO_LIBNAME}_static_target)
    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${GO_LIBNAME}_static)
    endif()

    install(FILES "${LIBOUT_STATIC}" TYPE LIB)
  endif()
  # if(ARG_CMAKE_PACKAGE_NAME)
  #   install_cmake_package(${ARG_CMAKE_PACKAGE_NAME} ${GO_LIBNAME}_export)
  # endif()

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
