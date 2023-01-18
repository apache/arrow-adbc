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

function(add_go_shared_lib GO_MOD_DIR GO_LIBNAME)
  set(options)
  set(one_value_args
      BUILD_TAGS
      SHARED_LINK_FLAGS
      PKG_CONFIG_NAME)
  set(multi_value_args
      SOURCES)
    
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  set(LIB_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${GO_LIBNAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")

  if(DEFINED ARG_BUILD_TAGS)
    set(GO_BUILD_TAGS "-tags=${ARG_BUILD_TAGS}")
  endif()
  if(DEFINED ARG_SHARED_LINK_FLAGS)    
    set(ARG_SHARED_LINK_FLAGS "${ARG_SHARED_LINK_FLAGS} -extldflags -Wl,-soname,${LIB_NAME}.${ADBC_SO_VERSION}")
    separate_arguments(ARG_SHARED_LINK_FLAGS NATIVE_COMMAND "${ARG_SHARED_LINK_FLAGS}")    
    set(GO_LDFLAGS "-ldflags=\"${ARG_SHARED_LINK_FLAGS}\"")
  endif()

  set(LIBOUT
      "${CMAKE_CURRENT_BINARY_DIR}/${LIB_NAME}")

  
  add_custom_command(OUTPUT "${LIBOUT}.${ADBC_FULL_SO_VERSION}" "${CMAKE_SHARED_LIBRARY_PREFIX}${GO_LIBNAME}.h"
    DEPENDS ${ARG_SOURCES}
    WORKING_DIRECTORY ${GO_MOD_DIR}
    COMMAND env ${GO_BIN} build "${GO_BUILD_TAGS}" -o "${LIBOUT}.${ADBC_FULL_SO_VERSION}" -buildmode=c-shared "${GO_LDFLAGS}" .    
    COMMENT "Building Go Shared lib ${GO_LIBNAME}"
    COMMAND_EXPAND_LISTS)
  
  add_custom_command(OUTPUT "${LIBOUT}.${ADBC_SO_VERSION}" "${LIBOUT}"
    DEPENDS "${LIBOUT}.${ADBC_FULL_SO_VERSION}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${LIB_NAME}.${ADBC_FULL_SO_VERSION}" "${LIB_NAME}.${ADBC_SO_VERSION}"
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${LIB_NAME}.${ADBC_SO_VERSION}" "${LIB_NAME}"
  )

  add_custom_target(${GO_LIBNAME}_target ALL DEPENDS "${LIBOUT}.${ADBC_FULL_SO_VERSION}" "${LIBOUT}.${ADBC_SO_VERSION}" "${LIBOUT}")
  add_library(${GO_LIBNAME} SHARED IMPORTED GLOBAL)
  add_dependencies(${GO_LIBNAME} ${GO_LIBNAME}_target)
  set_target_properties(${GO_LIBNAME} 
    PROPERTIES IMPORTED_LOCATION ${LIBOUT}               
               IMPORTED_SONAME "${LIB_NAME}.${ADBC_FULL_SO_VERSION}")

  if(ADBC_RPATH_ORIGIN)
    if(APPLE)
      set(_lib_install_rpath "@loader_path")
    else()
      set(_lib_install_rpath "\$ORIGIN")
    endif()
    set_target_properties(${GO_LIBNAME} PROPERTIES INSTALL_RPATH ${_lib_install_rpath})
  endif()

  if(APPLE)
    if(ADBC_INSTALL_NAME_RPATH)
      set(_lib_install_name "@rpath")
    else()
      set(_lib_install_name "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
    endif()
  set_target_properties(${GO_LIBNAME}
                        PROPERTIES BUILD_WITH_INSTALL_RPATH ON INSTALL_NAME_DIR
                                                               "${_lib_install_name}")
  endif()
  
  install(IMPORTED_RUNTIME_ARTIFACTS ${GO_LIBNAME} ${INSTALL_IS_OPTIONAL}        
        RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(FILES "${LIBOUT}.${ADBC_SO_VERSION}" TYPE LIB)

endfunction()
