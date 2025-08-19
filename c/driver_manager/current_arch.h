// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#if __has_include(<bit>)
#include <bit>
#ifdef __cpp_lib_endian
#define HAS_ENDIAN 1
#endif
#endif

#ifndef HAS_ENDIAN
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <machine/endian.h>
#elif defined(sun) || defined(__sun)
#include <sys/byteorder.h>
#elif !defined(_AIX)
#include <endian.h>
#endif
#endif

namespace adbc {

const std::string& CurrentArch() {
#if defined(_WIN32)
  static const std::string platform = "windows";
#elif defined(__APPLE__)
  static const std::string platform = "macos";
#elif defined(__FreeBSD__)
  static const std::string platform = "freebsd";
#elif defined(__OpenBSD__)
  static const std::string platform = "openbsd";
#elif defined(__linux__)
  static const std::string platform = "linux";
#else
  static const std::string platform = "unknown";
#endif

#ifdef HAS_ENDIAN
  constexpr bool is_little_endian = (std::endian::native == std::endian::little);
#else
#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__)
  constexpr bool is_little_endian = true;
#else
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  constexpr bool is_little_endian = true;
#else
  constexpr bool is_little_endian = false;
#endif
#endif
#endif

#if defined(__x86_64__) || defined(__amd64__) || defined(_M_X64) || defined(_M_AMD64)
  static const std::string arch = "amd64";
#elif defined(__aarch64__) || defined(_M_ARM64) || defined(__ARM_ARCH_ISA_A64)
  if constexpr (!is_little_endian) {
    static const std::string arch = "arm64be";
  } else {
    static const std::string arch = "arm64";
  }
#elif defined(__i386__) || defined(_M_IX86) || defined(_M_X86)
  static const std::string arch = "x86";
#elif defined(__arm__) || defined(_M_ARM)
  if constexpr (!is_little_endian) {
    static const std::string arch = "armbe";
  } else {
    static const std::string arch = "arm";
  }
#elif defined(__riscv) || defined(_M_RISCV)
#if defined(__riscv_xlen) && __riscv_xlen == 64
  static const std::string arch = "riscv64";
#else
  static const std::string arch = "riscv";
#endif
#elif defined(__ppc64__) || defined(__powerpc64__)
  if constexpr (is_little_endian) {
    static const std::string arch = "powerpc64le";
  } else {
    static const std::string arch = "powerpc64";
  }
#elif defined(__powerpc__) || defined(__ppc__) || defined(_M_PPC)
  static const std::string arch = "powerpc";
#elif defined(__s390x__) || defined(_M_S390)
  static const std::string arch = "s390x";
#elif defined(__sparc__) || defined(__sparc)
#if defined(_LP64) || defined(__LP64__)
  static const std::string arch = "sparc64";
#else
  static const std::string arch = "sparc";
#endif
#elif defined(__wasm__)
  static const std::string arch = "wasm";
#else
  static const std::string arch = "unknown";
#endif

// musl doesn't actually define any preprocessor macro for itself
// but apparently it doesn't define __USE_GNU inside of features.h
// while gcc DOES define that.
// see https://stackoverflow.com/questions/58177815/how-to-actually-detect-musl-libc
#if defined(_WIN32) || defined(__APPLE__) || defined(__FreeBSD__)
#else
#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#include <features.h>  // NOLINT [build/include]
#ifndef __USE_GNU
#define __MUSL__
#endif
#undef _GNU_SOURCE /* don't contaminate other includes unnecessarily */
#else
#include <features.h>  // NOLINT [build/include]
#ifndef __USE_GNU
#define __MUSL__
#endif
#endif
#endif

#if defined(__MINGW32__) || defined(__MINGW64__)
  static const std::string target = "_mingw";
#elif defined(__MUSL__)
  static const std::string target = "_musl";
#else
  static const std::string target = "";
#endif

  static const std::string result = platform + "_" + arch + target;
  return result;
}

}  // namespace adbc
