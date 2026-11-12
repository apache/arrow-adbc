// boost.int128 - amalgamated single-header build.
// Auto-generated: all internal boost/int128 headers inlined into one file
// for use in Compiler Explorer and other single-file environments.
// Do not edit by hand; regenerate with extra/amalgamate.py.
//
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

// ===== BEGIN boost/int128.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_HPP
#define BOOST_INT128_HPP

// ===== BEGIN boost/int128/int128.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_INT128_HPP
#define BOOST_INT128_INT128_HPP

// ===== BEGIN boost/int128/detail/fwd.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_FWD_HPP
#define BOOST_INT128_DETAIL_FWD_HPP

// ===== BEGIN boost/int128/detail/config.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_CONFIG_HPP
#define BOOST_INT128_DETAIL_CONFIG_HPP

// A handful of detail-namespace entities are exercised directly by the module
// test suite. BOOST_int128EST_EXPORT exports them only when the module is built
// for testing (BOOST_INT128_EXPORT_TESTING), so the normal module API stays limited
// to the public interface. It expands to nothing in ordinary (header) builds.
#if defined(BOOST_INT128_BUILD_MODULE) && defined(BOOST_INT128_EXPORT_TESTING)
#  define BOOST_int128EST_EXPORT export
#else
#  define BOOST_int128EST_EXPORT
#endif

// The SYCL device target (spir64) has no native 128-bit integer, so force the portable
// code path on the device pass. This mirrors a user-supplied BOOST_INT128_NO_BUILTIN_INT128
// and keeps host/device selection consistent even though __x86_64__ stays defined on device.
#if defined(__SYCL_DEVICE_ONLY__) && !defined(BOOST_INT128_NO_BUILTIN_INT128)
#  define BOOST_INT128_NO_BUILTIN_INT128
#endif

// Use 128-bit integers.
// The SYCL device target (spir64) has no native 128-bit integer, so on the device pass
// we fall back to the portable path (the same one used on platforms without __int128).
#if (defined(BOOST_HAS_INT128) || (defined(__SIZEOF_INT128__) && !defined(_MSC_VER))) && !defined(__SYCL_DEVICE_ONLY__) && !defined(BOOST_INT128_NO_BUILTIN_INT128)

#define BOOST_INT128_HAS_INT128

#define BOOST_INT128_BUILTIN_CONSTEXPR constexpr

namespace boost {
namespace int128 {
namespace detail {

// A module consumer receives these aliases from the import, so only declare them
// in ordinary builds and in the module interface unit itself; declaring them again
// in a consumer would give a second, distinct type and break overload resolution.
#if !defined(BOOST_INT128_BUILD_MODULE) || defined(BOOST_INT128_INTERFACE_UNIT)

// Avoids pedantic warnings
#ifdef __GNUC__

BOOST_int128EST_EXPORT __extension__ using builtin_i128 = __int128 ;
BOOST_int128EST_EXPORT __extension__ using builtin_u128 = unsigned __int128 ;

#else

BOOST_int128EST_EXPORT using builtin_i128 = __int128 ;
BOOST_int128EST_EXPORT using builtin_u128 = unsigned __int128;

#endif

#endif // declare builtin aliases

} // namespace detail
} // namespace int128
} // namespace boost

#elif __has_include(<__msvc_int128.hpp>) && _MSVC_LANG >= 202002L && !defined(__SYCL_DEVICE_ONLY__)

#ifndef BOOST_INT128_BUILD_MODULE
#include <__msvc_int128.hpp>
#endif

#define BOOST_INT128_HAS_MSVC_INT128

#if _MSC_VER >= 1945
#define BOOST_INT128_BUILTIN_CONSTEXPR constexpr
#else
#define BOOST_INT128_BUILTIN_CONSTEXPR inline
#endif

namespace boost {
namespace int128 {
namespace detail {

// See the note above: skip the re-declaration in a module consumer.
#if !defined(BOOST_INT128_BUILD_MODULE) || defined(BOOST_INT128_INTERFACE_UNIT)

BOOST_int128EST_EXPORT using builtin_i128 = std::_Signed128;
BOOST_int128EST_EXPORT using builtin_u128 = std::_Unsigned128;

#endif

} // namespace detail
} // namespace int128
} // namespace boost

#endif // builtin 128-bit detection

// Determine endianness
#if defined(_WIN32)

#define BOOST_INT128_ENDIAN_BIG_BYTE 0
#define BOOST_INT128_ENDIAN_LITTLE_BYTE 1

#elif defined(__BYTE_ORDER__)

#define BOOST_INT128_ENDIAN_BIG_BYTE (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define BOOST_INT128_ENDIAN_LITTLE_BYTE (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

#else

#error Could not determine endian type. Please file an issue at https://github.com/cppalliance/INT128 with your architecture

#endif // Determine endianness

// Is constant evaluated detection
#ifdef __cpp_lib_is_constant_evaluated
#  define BOOST_INT128_HAS_IS_CONSTANT_EVALUATED
#endif

#ifdef __has_builtin
#  if __has_builtin(__builtin_is_constant_evaluated)
#    define BOOST_INT128_HAS_BUILTIN_IS_CONSTANT_EVALUATED
#  endif
#endif

//
// MSVC also supports __builtin_is_constant_evaluated if it's recent enough:
//
#if defined(_MSC_FULL_VER) && (_MSC_FULL_VER >= 192528326)
#  define BOOST_INT128_HAS_BUILTIN_IS_CONSTANT_EVALUATED
#endif

//
// As does GCC-9:
//
#if defined(__GNUC__) && (__GNUC__ >= 9) && !defined(BOOST_INT128_HAS_BUILTIN_IS_CONSTANT_EVALUATED)
#  define BOOST_INT128_HAS_BUILTIN_IS_CONSTANT_EVALUATED
#endif

#if defined(BOOST_INT128_HAS_IS_CONSTANT_EVALUATED) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))
#  define BOOST_INT128_IS_CONSTANT_EVALUATED(x) std::is_constant_evaluated()
#elif defined(BOOST_INT128_HAS_BUILTIN_IS_CONSTANT_EVALUATED) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))
#  define BOOST_INT128_IS_CONSTANT_EVALUATED(x) __builtin_is_constant_evaluated()
#else
#  define BOOST_INT128_IS_CONSTANT_EVALUATED(x) false
#  define BOOST_INT128_NO_CONSTEVAL_DETECTION
#endif

// https://github.com/llvm/llvm-project/issues/55638
#if defined(__clang__) && __cplusplus > 202002L && __clang_major__ < 17
#  undef BOOST_INT128_IS_CONSTANT_EVALUATED
#  define BOOST_INT128_IS_CONSTANT_EVALUATED(x) false
#  define BOOST_INT128_NO_CONSTEVAL_DETECTION
#endif

#if defined(_MSC_VER)
#  define BOOST_INT128_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#  define BOOST_INT128_FORCE_INLINE __attribute__((always_inline)) inline
#else
#  define BOOST_INT128_FORCE_INLINE inline
#endif

// MinGW defines the MSVC platform macros (_M_AMD64, _M_IX86, _M_ARM64) for source
// compatibility, but it provides the GNU intrinsics rather than the MSVC ones. Every
// guard selecting an MSVC-only intrinsic (__shiftleft128, _umul128, __umulh, _BitScan*,
// __popcnt*, ...) therefore has to exclude GNU-mode compilers with !defined(__GNUC__).
#ifdef __x86_64__

#ifndef BOOST_INT128_BUILD_MODULE
#  include <x86intrin.h>
#  include <emmintrin.h>
#endif

#  ifdef __ADX__
#    define BOOST_INT128_ADD_CARRY _addcarryx_u64
#    define BOOST_INT128_SUB_BORROW _subborrow_u64
#  else
#    define BOOST_INT128_ADD_CARRY _addcarry_u64
#    define BOOST_INT128_SUB_BORROW _subborrow_u64
#  endif

#elif defined(_M_AMD64)

#ifndef BOOST_INT128_BUILD_MODULE
#  include <intrin.h>
#endif

#  ifdef __ADX__
#    define BOOST_INT128_ADD_CARRY _addcarryx_u64
#    define BOOST_INT128_SUB_BORROW _subborrow_u64
#  else
#    define BOOST_INT128_ADD_CARRY _addcarry_u64
#    define BOOST_INT128_SUB_BORROW _subborrow_u64
#  endif

#elif defined(__i386__)

#ifndef BOOST_INT128_BUILD_MODULE
#  include <emmintrin.h>
#endif

#elif defined(_M_IX86)

#ifndef BOOST_INT128_BUILD_MODULE
#  include <intrin.h>
#endif

#endif // Platform macros

// Hardware 128-bit by 64-bit unsigned division via the x86-64 DIV instruction
// Excluded on the CUDA and SYCL device passes (the device target is not x86-64)
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__)) && !defined(_MSC_VER) && !defined(__CUDA_ARCH__) && !defined(__SYCL_DEVICE_ONLY__)
#  define BOOST_INT128_HAS_X86_64_DIVQ
#endif

// The builtin is only constexpr from clang-7 or GCC-10
#ifdef __has_builtin
#  if __has_builtin(__builtin_sub_overflow) && ((defined(__clang__) && __clang_major__ >= 7) || (defined(__GNUC__) && __GNUC__ >= 10))
#    define BOOST_INT128_HAS_BUILTIN_SUB_OVERFLOW
#  endif
#  if __has_builtin(__builtin_add_overflow) && ((defined(__clang__) && __clang_major__ >= 7) || (defined(__GNUC__) && __GNUC__ >= 10))
#    define BOOST_INT128_HAS_BUILTIN_ADD_OVERFLOW
#  endif
#endif

#if defined(__cpp_if_constexpr) && __cpp_if_constexpr >= 201606L
#  define BOOST_INT128_HAS_IF_CONSTEXPR
#endif // if constexpr detection

#ifndef BOOST_INT128_BUILD_MODULE
#include <cassert>
#endif

#define BOOST_INT128_ASSERT(x) assert(x)
#define BOOST_INT128_ASSERT_MSG(expr, msg) assert((expr)&&(msg))

#ifdef _MSC_VER
#  define BOOST_INT128_ASSUME(expr) __assume(expr)
#elif defined(__clang__)
#  define BOOST_INT128_ASSUME(expr) __builtin_assume(expr)
#elif defined(__GNUC__)
#  if __GNUC__ >= 5 && __GNUC__ < 13
#    define BOOST_INT128_ASSUME(expr) if (expr) {} else { __builtin_unreachable(); }
#  else
#    define BOOST_INT128_ASSUME(expr) __attribute__((assume(expr)))
#  endif
#elif defined(__has_cpp_attribute)
#  if __has_cpp_attribute(assume)
#    define BOOST_INT128_ASSUME(expr) [[assume(expr)]]
#  else
#    define BOOST_INT128_ASSUME(expr) BOOST_INT128_ASSERT(expr)
#  endif
#else
#  define BOOST_INT128_ASSUME(expr) BOOST_INT128_ASSERT(expr)
#endif

#if defined(__has_builtin)
#define BOOST_INT128_HAS_BUILTIN(x) __has_builtin(x)
#else
#define BOOST_INT128_HAS_BUILTIN(x) false
#endif

#if BOOST_INT128_HAS_BUILTIN(__builtin_expect)
#  define BOOST_INT128_LIKELY(x) __builtin_expect(x, 1)
#  define BOOST_INT128_UNLIKELY(x) __builtin_expect(x, 0)
#else
#  define BOOST_INT128_LIKELY(x) x
#  define BOOST_INT128_UNLIKELY(x) x
#endif

#if !defined(__cpp_if_constexpr) || (__cpp_if_constexpr < 201606L)
#  define BOOST_INT128_NO_CXX17_IF_CONSTEXPR
#endif

#ifndef BOOST_INT128_NO_CXX17_IF_CONSTEXPR
#  define BOOST_INT128_IF_CONSTEXPR if constexpr
#else
#  define BOOST_INT128_IF_CONSTEXPR if
#endif

#if defined(__GNUC__) || defined(__clang__)
#  define BOOST_INT128_UNREACHABLE __builtin_unreachable()
#elif defined(_MSC_VER)
#  define BOOST_INT128_UNREACHABLE __assume(0)
#else
#  define BOOST_INT128_UNREACHABLE std::abort()
#endif

#ifdef BOOST_INT128_BUILD_MODULE
#  define BOOST_INT128_INLINE_CONSTEXPR inline constexpr
#  define BOOST_INT128_EXPORT export
#else
#  define BOOST_INT128_INLINE_CONSTEXPR static constexpr
#  define BOOST_INT128_EXPORT
#endif

// Detect if we can throw or not
// First check if the user said no explicitly
// Then check if it's been disabled elsewhere

#ifdef BOOST_INT128_DISABLE_EXCEPTIONS

#  define BOOST_INT128_THROW_EXCEPTION(expr)

#else

#  ifdef _MSC_VER
#    ifdef _CPPUNWIND
#      define BOOST_INT128_THROW_EXCEPTION(expr) throw expr;
#    else
#      define BOOST_INT128_THROW_EXCEPTION(expr)
#      define BOOST_INT128_DISABLE_EXCEPTIONS
#    endif
#  else
#    ifdef __EXCEPTIONS
#      define BOOST_INT128_THROW_EXCEPTION(expr) throw expr;
#    else
#      define BOOST_INT128_THROW_EXCEPTION(expr)
#      define BOOST_INT128_DISABLE_EXCEPTIONS
#    endif
#endif

#endif // Exceptions

#if defined(__cpp_impl_three_way_comparison) && __cpp_impl_three_way_comparison >= 201907L && __has_include(<compare>)
#  define BOOST_INT128_HAS_SPACESHIP_OPERATOR
#  ifndef BOOST_INT128_BUILD_MODULE
#    include <compare>
#  endif
#endif

// GPU device support. CUDA is auto-detected via __CUDACC__ (opt-in with
// BOOST_INT128_ENABLE_CUDA). SYCL is fully opt-in via BOOST_INT128_ENABLE_SYCL;
// <sycl/sycl.hpp> must be included before <boost/int128.hpp> so SYCL_EXTERNAL exists.
#if defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)
#  define BOOST_INT128_CUDA_ENABLED __host__ __device__
#  define BOOST_INT128_HAS_GPU_SUPPORT
#elif defined(BOOST_INT128_ENABLE_SYCL)
#  define BOOST_INT128_SYCL_ENABLED SYCL_EXTERNAL
#  define BOOST_INT128_HAS_GPU_SUPPORT
#endif

#ifndef BOOST_INT128_CUDA_ENABLED
#  define BOOST_INT128_CUDA_ENABLED
#endif
#ifndef BOOST_INT128_SYCL_ENABLED
#  define BOOST_INT128_SYCL_ENABLED
#endif

// Exactly one sub-macro is ever non-empty; expands to "__host__ __device__" (CUDA),
// "SYCL_EXTERNAL" (SYCL), or nothing (host).
#define BOOST_INT128_HOST_DEVICE BOOST_INT128_CUDA_ENABLED BOOST_INT128_SYCL_ENABLED

#endif // BOOST_INT128_DETAIL_CONFIG_HPP
// ===== END boost/int128/detail/config.hpp =====

namespace boost {
namespace int128 {

BOOST_INT128_EXPORT struct uint128;
BOOST_INT128_EXPORT struct int128;

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_FWD_HPP
// ===== END boost/int128/detail/fwd.hpp =====
// ===== BEGIN boost/int128/detail/int128_imp.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_INT128_HPP
#define BOOST_INT128_DETAIL_INT128_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/fwd.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// ===== BEGIN boost/int128/detail/traits.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_TRAITS_HPP
#define BOOST_INT128_DETAIL_TRAITS_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <type_traits>
#include <cstdint>

#endif

namespace boost {
namespace int128 {
namespace detail {

template <typename T>
struct signed_integer
{
    static constexpr bool value = (std::is_signed<T>::value && std::is_integral<T>::value)
    #ifdef BOOST_INT128_HAS_INT128
    || std::is_same<T, builtin_i128>::value;
    #else
    ;
    #endif
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_signed_integer_v = signed_integer<T>::value;

template <typename T>
struct unsigned_integer
{
    static constexpr bool value = (std::is_unsigned<T>::value && std::is_integral<T>::value)
    #ifdef BOOST_INT128_HAS_INT128
    || std::is_same<T, builtin_u128>::value;
    #else
    ;
    #endif
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_unsigned_integer_v = unsigned_integer<T>::value;

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_any_integer_v = signed_integer<T>::value || unsigned_integer<T>::value;

template <typename T>
struct floating_point
{
    static constexpr bool value = std::is_same<T, float>::value || std::is_same<T, double>::value
    #ifndef BOOST_INT128_HAS_GPU_SUPPORT
    || std::is_same<T, long double>::value;
    #else
    ;
    #endif
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_floating_point_v = floating_point<T>::value;

// The type integral promotion gives an operand of type T, which is the result type of a
// shift with T on the left. The rule depends on the rank of T and not only on its size, so
// long and char32_t differ where both are the width of an int; asking the compiler is exact
template <typename T>
struct promoted
{
    using type = decltype(+T{});
};

// Unary plus on a bool draws a warning from MSVC, and the answer is always int
template <>
struct promoted<bool>
{
    using type = int;
};

template <typename T>
using promoted_t = typename promoted<T>::type;

// Decides if we can use a u32 or u64 implementation for some operations

#ifdef BOOST_INT128_HAS_INT128

template <typename T>
using evaluation_type_t = std::conditional_t<sizeof(T) <= sizeof(std::uint32_t), std::uint32_t,
                            std::conditional_t<sizeof(T) <= sizeof(std::uint64_t), std::uint64_t, builtin_u128>>;

#else

template <typename T>
using evaluation_type_t = std::conditional_t<sizeof(T) <= sizeof(std::uint32_t), std::uint32_t, std::uint64_t>;

#endif

} // namespace detail
} // namespace int128
} // namespace boost

#define BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT typename SignedInteger, std::enable_if_t<detail::is_signed_integer_v<SignedInteger>, bool> = true
#define BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT typename UnsignedInteger, std::enable_if_t<detail::is_unsigned_integer_v<UnsignedInteger>, bool> = true
#define BOOST_INT128_DEFAULTED_INTEGER_CONCEPT typename Integer, std::enable_if_t<detail::is_any_integer_v<Integer>, bool> = true
#define BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT typename Float, std::enable_if_t<detail::is_floating_point_v<Float>, bool> = true

#define BOOST_INT128_SIGNED_INTEGER_CONCEPT typename SignedInteger, std::enable_if_t<detail::is_signed_integer_v<SignedInteger>, bool>
#define BOOST_INT128_UNSIGNED_INTEGER_CONCEPT typename UnsignedInteger, std::enable_if_t<detail::is_unsigned_integer_v<UnsignedInteger>, bool>
#define BOOST_INT128_INTEGER_CONCEPT typename Integer, std::enable_if_t<detail::is_any_integer_v<Integer>, bool>
#define BOOST_INT128_FLOATING_POINT_CONCEPT typename Float, std::enable_if_t<detail::is_floating_point_v<Float>, bool>

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

#define BOOST_INT128_DEFAULTED_SIGNED_128BIT_INTEGER_CONCEPT typename SignedInteger, std::enable_if_t<std::is_same<SignedInteger, detail::builtin_i128>::value, bool> = true
#define BOOST_INT128_DEFAULTED_UNSIGNED_128BIT_INTEGER_CONCEPT typename UnsignedInteger, std::enable_if_t<std::is_same<UnsignedInteger, detail::builtin_u128>::value, bool> = true
#define BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT typename Integer, std::enable_if_t<std::is_same<Integer, detail::builtin_u128>::value || std::is_same<Integer, detail::builtin_i128>::value, bool> = true

#define BOOST_INT128_SIGNED_128BIT_INTEGER_CONCEPT typename SignedInteger, std::enable_if_t<std::is_same<SignedInteger, detail::builtin_i128>::value, bool>
#define BOOST_INT128_UNSIGNED_128BIT_INTEGER_CONCEPT typename UnsignedInteger, std::enable_if_t<std::is_same<UnsignedInteger, detail::builtin_u128>::value, bool>
#define BOOST_INT128_128BIT_INTEGER_CONCEPT typename Integer, std::enable_if_t<std::is_same<Integer, detail::builtin_u128>::value || std::is_same<Integer, detail::builtin_i128>::value, bool>

#endif

#endif // BOOST_INT128_DETAIL_TRAITS_HPP
// ===== END boost/int128/detail/traits.hpp =====
// ===== BEGIN boost/int128/detail/constants.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_CONSTANTS_HPP
#define BOOST_INT128_DETAIL_CONSTANTS_HPP

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstdint>
#include <limits>

#endif

namespace boost {
namespace int128 {
namespace detail {

BOOST_INT128_INLINE_CONSTEXPR std::uint64_t low_word_mask {(std::numeric_limits<std::uint64_t>::max)()};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR T offset_value_v = static_cast<T>(18446744073709551616.0); // UINT64_MAX as double

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_CONSTANTS_HPP
// ===== END boost/int128/detail/constants.hpp =====
// ===== BEGIN boost/int128/detail/clz.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_CLZ_HPP
#define BOOST_INT128_DETAIL_CLZ_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <limits>
#include <cstdint>

#endif

namespace boost {
namespace int128 {
namespace detail {

// The whole impl namespace is exported when building the module for testing so
// the low-level bit helpers can be exercised directly; it is an ordinary
// namespace in every other build.
BOOST_int128EST_EXPORT namespace impl {

#if !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

// See: http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogDeBruijn
BOOST_INT128_INLINE_CONSTEXPR int index64[64] = {
    0, 47,  1, 56, 48, 27,  2, 60,
    57, 49, 41, 37, 28, 16,  3, 61,
    54, 58, 35, 52, 50, 42, 21, 44,
    38, 32, 29, 23, 17, 11,  4, 62,
    46, 55, 26, 59, 40, 36, 15, 53,
    34, 51, 20, 43, 31, 22, 10, 45,
    25, 39, 14, 33, 19, 30,  9, 24,
    13, 18,  8, 12,  7,  6,  5, 63
};

#endif

BOOST_INT128_HOST_DEVICE constexpr int bit_scan_reverse(std::uint64_t bb) noexcept
{
    #if defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)

    constexpr int index64[64] = {
        0, 47,  1, 56, 48, 27,  2, 60,
        57, 49, 41, 37, 28, 16,  3, 61,
        54, 58, 35, 52, 50, 42, 21, 44,
        38, 32, 29, 23, 17, 11,  4, 62,
        46, 55, 26, 59, 40, 36, 15, 53,
        34, 51, 20, 43, 31, 22, 10, 45,
        25, 39, 14, 33, 19, 30,  9, 24,
        13, 18,  8, 12,  7,  6,  5, 63
    };

    #endif

    constexpr auto debruijn64 {UINT64_C(0x03f79d71b4cb0a89)};

    BOOST_INT128_ASSUME(bb != 0); // LCOV_EXCL_LINE

    bb |= bb >> 1;
    bb |= bb >> 2;
    bb |= bb >> 4;
    bb |= bb >> 8;
    bb |= bb >> 16;
    bb |= bb >> 32;

    return index64[(bb * debruijn64) >> 58];
}

#if !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

BOOST_INT128_INLINE_CONSTEXPR int countl_mod37[37] = {
    32, 31, 6, 30, 9, 5, 0, 29,
    16, 8, 2, 4, 21, 0, 19, 28,
    25, 15, 0, 7, 10, 1, 17, 3,
    22, 20, 26, 0, 11, 18, 23,
    27, 12, 24, 13, 14, 0
};

#endif

BOOST_INT128_HOST_DEVICE constexpr int backup_countl_impl(std::uint32_t x) noexcept
{
    #if defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)

    constexpr int countl_mod37[37] = {
        32, 31, 6, 30, 9, 5, 0, 29,
        16, 8, 2, 4, 21, 0, 19, 28,
        25, 15, 0, 7, 10, 1, 17, 3,
        22, 20, 26, 0, 11, 18, 23,
        27, 12, 24, 13, 14, 0
    };

    #endif

    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;

    return countl_mod37[x % 37];
}

#if BOOST_INT128_HAS_BUILTIN(__builtin_clz) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

constexpr int countl_impl(unsigned int x) noexcept
{
    return x ? __builtin_clz(x) : std::numeric_limits<unsigned int>::digits;
}

constexpr int countl_impl(unsigned long x) noexcept
{
    return x ? __builtin_clzl(x) : std::numeric_limits<unsigned long>::digits;
}

constexpr int countl_impl(unsigned long long x) noexcept
{
    return x ? __builtin_clzll(x) : std::numeric_limits<unsigned long long>::digits;
}

#elif (defined(_M_AMD64) || defined(_M_ARM64)) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

constexpr int countl_impl(std::uint32_t x) noexcept
{
    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return backup_countl_impl(x); // LCOV_EXCL_LINE
    }
    else
    {
        unsigned long r {};

        if (_BitScanReverse(&r, x))
        {
            return 31 - static_cast<int>(r);
        }
        else
        {
            return 32;
        }
    }
}

constexpr int countl_impl(std::uint64_t x) noexcept
{
    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return x ? bit_scan_reverse(static_cast<std::uint64_t>(x)) ^ 63 : std::numeric_limits<std::uint64_t>::digits; // LCOV_EXCL_LINE
    }
    else
    {
        unsigned long r {};

        if (_BitScanReverse64(&r, x))
        {
            return 63 - static_cast<int>(r);
        }
        else
        {
            return 64;
        }
    }
}

#elif defined(_M_IX86) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

constexpr int countl_impl(std::uint32_t x) noexcept
{
    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return backup_countl_impl(x); // LCOV_EXCL_LINE
    }
    else
    {
        unsigned long r {};

        if (_BitScanReverse(&r, x))
        {
            return 31 - static_cast<int>(r);
        }
        else
        {
            return 32;
        }
    }
}

BOOST_INT128_HOST_DEVICE constexpr int countl_impl(std::uint64_t x) noexcept
{
    return x ? bit_scan_reverse(static_cast<std::uint64_t>(x)) ^ 63 : std::numeric_limits<std::uint64_t>::digits;
}

#else

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr int countl_impl(T x) noexcept
{
    return x ? bit_scan_reverse(static_cast<std::uint64_t>(x)) ^ 63 : std::numeric_limits<T>::digits;
}

BOOST_INT128_HOST_DEVICE constexpr int countl_impl(std::uint32_t x) noexcept
{
    return backup_countl_impl(x);
}


#endif

} // namespace impl

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr int countl_zero(T x) noexcept
{
    static_assert(std::numeric_limits<T>::is_integer && !std::numeric_limits<T>::is_signed,
                  "Can only count with unsigned integers");

    return impl::countl_impl(x);
}

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_CLZ_HPP
// ===== END boost/int128/detail/clz.hpp =====
// ===== BEGIN boost/int128/detail/common_mul.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_COMMON_MUL_HPP
#define BOOST_INT128_DETAIL_COMMON_MUL_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/fwd.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstdint>

#endif

namespace boost {
namespace int128 {
namespace detail {

template <typename>
struct ctor_high_word
{
    using type = std::uint64_t;
};

template <>
struct ctor_high_word<int128>
{
    using type = std::int64_t;
};

template <typename T>
using ctor_high_word_t = typename ctor_high_word<T>::type;

// High 64 bits of the 64x64 -> 128 product, computed with four 32-bit partial products
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::uint64_t umulh_generic(const std::uint64_t a, const std::uint64_t b) noexcept
{
    const std::uint64_t a_lo {a & UINT32_MAX};
    const std::uint64_t a_hi {a >> 32U};
    const std::uint64_t b_lo {b & UINT32_MAX};
    const std::uint64_t b_hi {b >> 32U};

    const std::uint64_t lo_lo {a_lo * b_lo};
    const std::uint64_t hi_lo {a_hi * b_lo};
    const std::uint64_t lo_hi {a_lo * b_hi};
    const std::uint64_t hi_hi {a_hi * b_hi};

    const std::uint64_t cross {(lo_lo >> 32U) + (hi_lo & UINT32_MAX) + (lo_hi & UINT32_MAX)};

    return hi_hi + (hi_lo >> 32U) + (lo_hi >> 32U) + (cross >> 32U);
}

// Full 64x64 -> 128 product
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::uint64_t umul(const std::uint64_t a, const std::uint64_t b, std::uint64_t& hi) noexcept
{
    #ifndef BOOST_INT128_NO_CONSTEVAL_DETECTION

    if (!BOOST_INT128_IS_CONSTANT_EVALUATED(a))
    {
        #if defined(BOOST_INT128_HAS_INT128)

        const detail::builtin_u128 product {static_cast<detail::builtin_u128>(a) * static_cast<detail::builtin_u128>(b)};
        hi = static_cast<std::uint64_t>(product >> 64U);
        return static_cast<std::uint64_t>(product);

        #elif defined(_M_AMD64) && !defined(__GNUC__) && !defined(__CUDA_ARCH__) && !defined(__SYCL_DEVICE_ONLY__)

        return _umul128(a, b, &hi);

        #elif defined(_M_ARM64) && !defined(__GNUC__) && !defined(__CUDA_ARCH__) && !defined(__SYCL_DEVICE_ONLY__)

        hi = __umulh(a, b);
        return a * b;

        #endif
    }

    #endif

    hi = umulh_generic(a, b);
    return a * b;
}

// Low 128 bits of a 128x128 product
template <typename ReturnType, typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr ReturnType low_word_mul(const T& lhs, const T& rhs) noexcept
{
    std::uint64_t result_high {};
    const std::uint64_t result_low {umul(lhs.low, rhs.low, result_high)};

    result_high += lhs.low * static_cast<std::uint64_t>(rhs.high);
    result_high += static_cast<std::uint64_t>(lhs.high) * rhs.low;

    return ReturnType{static_cast<ctor_high_word_t<ReturnType>>(result_high), result_low};
}

// Low 128 bits of a 128x64 product
template <typename ReturnType, typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr ReturnType low_word_mul(const T& lhs, const std::uint64_t rhs) noexcept
{
    std::uint64_t result_high {};
    const std::uint64_t result_low {umul(lhs.low, rhs, result_high)};

    result_high += static_cast<std::uint64_t>(lhs.high) * rhs;

    return ReturnType{static_cast<ctor_high_word_t<ReturnType>>(result_high), result_low};
}

// Low 128 bits of a 128x32 product
template <typename ReturnType, typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr ReturnType low_word_mul(const T& lhs, const std::uint32_t rhs) noexcept
{
    return low_word_mul<ReturnType>(lhs, static_cast<std::uint64_t>(rhs));
}

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_COMMON_MUL_HPP
// ===== END boost/int128/detail/common_mul.hpp =====
// ===== BEGIN boost/int128/detail/common_div.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_COMMON_DIV_HPP
#define BOOST_INT128_DETAIL_COMMON_DIV_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/clz.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/common_mul.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstdint>
#include <cstring>

#endif

namespace boost {
namespace int128 {
namespace detail {

#if defined(__clang__)
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wassume"
#endif

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr void half_word_div(const T& lhs, const std::uint32_t rhs, T& quotient, T& remainder) noexcept
{
    using high_word_type = decltype(T{}.high);

    BOOST_INT128_ASSUME(rhs != 0); // LCOV_EXCL_LINE

    // Use Barrett reduction-inspired approach
    const std::uint64_t divisor {rhs};

    const auto q_high {static_cast<std::uint64_t>(lhs.high) / divisor};
    auto r {static_cast<std::uint64_t>(lhs.high) % divisor};

    const auto low_high {static_cast<std::uint32_t>(lhs.low >> 32U)};
    const auto low_low {static_cast<std::uint32_t>(lhs.low)};

    r = (r << 32U) | low_high;
    const auto q_mid {r / divisor};
    r %= divisor;

    r = (r << 32U) | low_low;
    const auto q_low {r / divisor};
    r %= divisor;

    quotient.high = static_cast<high_word_type>(q_high);
    quotient.low = (q_mid << 32U) | q_low;
    remainder.low = r;
}

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr void half_word_div(const T& lhs, const std::uint32_t rhs, T& quotient) noexcept
{
    using high_word_type = decltype(T{}.high);

    BOOST_INT128_ASSUME(rhs != 0); // LCOV_EXCL_LINE

    quotient.high = static_cast<high_word_type>(static_cast<std::uint64_t>(lhs.high) / rhs);
    auto remainder {((static_cast<std::uint64_t>(lhs.high) % rhs) << 32) | (lhs.low >> 32)};
    quotient.low = (remainder / rhs) << 32;
    remainder = ((remainder % rhs) << 32) | (lhs.low & UINT32_MAX);
    quotient.low |= (remainder / rhs) & UINT32_MAX;
}

// Portable 128-bit by 64-bit unsigned division producing a 64-bit quotient and remainder.
// This is the classic Hacker's Delight divlu (two 32-bit "digit" steps over 64-bit words).
// Precondition: u1 < d so the quotient is guaranteed to fit in 64 bits. It is constexpr-safe
// and serves as the fallback for udiv_2by1 on every target without a hardware 128/64 divide.
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::uint64_t divlu(std::uint64_t u1, std::uint64_t u0, std::uint64_t d, std::uint64_t& r) noexcept
{
    constexpr std::uint64_t b {UINT64_C(1) << 32U}; // Number base (2^32)

    BOOST_INT128_ASSUME(u1 < d); // LCOV_EXCL_LINE

    // D.1: normalize so that the divisor's most significant bit is set
    const auto s {countl_zero(d)};
    d <<= s;

    const auto vn1 {d >> 32U};
    const auto vn0 {d & UINT32_MAX};

    // Shift the dividend left by s. The (64 - s) shift is undefined when s == 0, so guard it.
    const auto un32 {s == 0 ? u1 : ((u1 << s) | (u0 >> (64 - s)))};
    const auto un10 {u0 << s};

    const auto un1 {un10 >> 32U};
    const auto un0 {un10 & UINT32_MAX};

    // First quotient digit
    auto q1 {un32 / vn1};
    auto rhat {un32 - (q1 * vn1)};

    while (q1 >= b || (q1 * vn0) > ((b * rhat) + un1))
    {
        --q1;
        rhat += vn1;
        if (rhat >= b)
        {
            break;
        }
    }

    const auto un21 {(un32 * b) + un1 - (q1 * d)};

    // Second quotient digit
    auto q0 {un21 / vn1};
    rhat = un21 - (q0 * vn1);

    while (q0 >= b || (q0 * vn0) > ((b * rhat) + un0))
    {
        --q0;
        rhat += vn1;
        if (rhat >= b)
        {
            break;
        }
    }

    // The remainder is shifted back down by the normalization amount
    r = ((un21 * b) + un0 - (q0 * d)) >> s;
    return (q1 * b) + q0;
}

#if defined(BOOST_INT128_HAS_X86_64_DIVQ)

// Inline asm cannot appear in a constexpr function body before C++20, so the x86-64 DIV
// instruction is wrapped in a non-constexpr helper that udiv_2by1 only calls at runtime.
BOOST_INT128_FORCE_INLINE std::uint64_t udiv_2by1_divq(const std::uint64_t u1, const std::uint64_t u0, const std::uint64_t d, std::uint64_t& r) noexcept
{
    std::uint64_t q {};
    __asm__("divq %[d]" : "=a"(q), "=d"(r) : [d] "r"(d), "a"(u0), "d"(u1) : "cc");
    return q;
}

#endif // BOOST_INT128_HAS_X86_64_DIVQ

// Divides the 128-bit value (u1:u0) by d, returning a 64-bit quotient and the true remainder.
// Precondition: u1 < d. Mirrors common_mul.hpp::umul: a hardware instruction at runtime where
// one exists, and the portable divlu in constexpr evaluation and everywhere else.
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::uint64_t udiv_2by1(const std::uint64_t u1, const std::uint64_t u0, const std::uint64_t d, std::uint64_t& r) noexcept
{
    BOOST_INT128_ASSUME(u1 < d); // LCOV_EXCL_LINE

    #if (defined(BOOST_INT128_HAS_X86_64_DIVQ) || (defined(_M_AMD64) && !defined(__GNUC__) && !defined(__clang__) && _MSC_VER >= 1920)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (!BOOST_INT128_IS_CONSTANT_EVALUATED(u1))
    {
        #if defined(BOOST_INT128_HAS_X86_64_DIVQ)

        return udiv_2by1_divq(u1, u0, d, r);

        #else

        return _udiv128(u1, u0, d, &r);

        #endif
    }

    #endif

    return divlu(u1, u0, d, r);
}

#if defined(_MSC_VER)
#  pragma warning(push)
#  pragma warning(disable : 4127) // Pre c++17 the if constexpr remainder part will hit this
#endif

// Divides the 128-bit value (uh:ul) by the 128-bit divisor (vh:vl) where vh != 0. Because the
// divisor is >= 2^64 the quotient is guaranteed to fit in a single 64-bit word, which is
// returned. When need_remainder is true the 128-bit remainder is written to (rem_hi:rem_lo).
//
// This is one normalized quotient digit (Knuth Algorithm D specialized to a 2-word divisor).
// The top-limb estimate qhat (reusing udiv_2by1, a hardware divq on x86-64) is bounded by
// Knuth Theorem B to q <= qhat <= q + 2; the D3 refinement against d0 tightens it to q <= qhat
// <= q + 1, and the conditional add-back then corrects the remaining off-by-one.
template <bool need_remainder>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::uint64_t div3by2(const std::uint64_t uh, const std::uint64_t ul,
    const std::uint64_t vh, const std::uint64_t vl, std::uint64_t& rem_hi, std::uint64_t& rem_lo) noexcept
{
    BOOST_INT128_ASSUME(vh != 0); // LCOV_EXCL_LINE

    // D.1: normalize so the divisor's most significant bit is set
    const auto s {countl_zero(vh)};
    const auto cs {64 - s};

    std::uint64_t d1 {};
    std::uint64_t d0 {};
    std::uint64_t u2 {};
    std::uint64_t u1 {};
    std::uint64_t u0 {};

    if (s == 0)
    {
        d1 = vh;
        d0 = vl;
        u2 = 0;
        u1 = uh;
        u0 = ul;
    }
    else
    {
        d1 = (vh << s) | (vl >> cs);
        d0 = vl << s;
        u2 = uh >> cs;
        u1 = (uh << s) | (ul >> cs);
        u0 = ul << s;
    }

    BOOST_INT128_ASSUME(u2 <= d1); // LCOV_EXCL_LINE

    // D.3: estimate the single quotient digit qhat = floor((u2:u1) / d1), clamped to 2^64 - 1.
    // rhat is the remainder of that estimate.
    std::uint64_t qhat {};
    std::uint64_t rhat {};
    bool rhat_overflow {false};
    if (u2 < d1)
    {
        qhat = udiv_2by1(u2, u1, d1, rhat);
    }
    else
    {
        // u2 == d1: floor((u2:u1)/d1) clamps to 2^64 - 1, leaving rhat == u1 + d1 (may carry).
        qhat = UINT64_MAX;
        rhat = u1 + d1;
        rhat_overflow = rhat < u1;
    }

    std::uint64_t qd0_hi {};
    auto qd0_lo {umul(qhat, d0, qd0_hi)};

    // Refine qhat against d0 (Knuth D3). The top-limb estimate alone can exceed the true quotient
    // by up to 2; this brings it down to at most one too large, which the add-back below corrects.
    // At most two iterations run, and only while the running remainder rhat stays below 2^64.
    if (!rhat_overflow)
    {
        while (qd0_hi > rhat || (qd0_hi == rhat && qd0_lo > u0))
        {
            --qhat;
            rhat += d1;
            const auto rhat_carry {rhat < d1};
            qd0_lo = umul(qhat, d0, qd0_hi);
            if (rhat_carry)
            {
                break;
            }
        }
    }

    // D.4: multiply and subtract (u2:u1:u0) - qhat * (d1:d0). qd0 already holds qhat * d0.
    std::uint64_t qd1_hi {};
    const auto qd1_lo {umul(qhat, d1, qd1_hi)};

    const auto p0 {qd0_lo};
    const auto p1 {qd0_hi + qd1_lo};
    const auto p2 {qd1_hi + static_cast<std::uint64_t>(p1 < qd0_hi)};

    const auto r0 {u0 - p0};
    const auto borrow0 {static_cast<std::uint64_t>(u0 < p0)};
    const auto t1 {u1 - p1};
    auto r1 {t1 - borrow0};
    const auto borrow1 {static_cast<std::uint64_t>(u1 < p1) + static_cast<std::uint64_t>(t1 < borrow0)};

    // D.5/D.6: if the top limb borrowed, qhat was one too large. Correct it and add the divisor
    // back into the remainder. The probability of this branch is small.
    auto r0_final {r0};
    if (BOOST_INT128_UNLIKELY((u2 < p2) || ((u2 - p2) < borrow1)))
    {
        --qhat;                                                  // LCOV_EXCL_LINE
        const auto sum0 {r0 + d0};                               // LCOV_EXCL_LINE
        r0_final = sum0;                                         // LCOV_EXCL_LINE
        r1 = r1 + d1 + static_cast<std::uint64_t>(sum0 < r0);    // LCOV_EXCL_LINE
    }

    BOOST_INT128_IF_CONSTEXPR (need_remainder)
    {
        if (s == 0)
        {
            rem_hi = r1;
            rem_lo = r0_final;
        }
        else
        {
            rem_lo = (r0_final >> s) | (r1 << cs);
            rem_hi = r1 >> s;
        }
    }
    else
    {
        static_cast<void>(rem_hi);
        static_cast<void>(rem_lo);
    }

    return qhat;
}

#if defined(_MSC_VER)
#  pragma warning(pop)
#endif

namespace impl {

#if defined(_MSC_VER)
#  pragma warning(push)
#  pragma warning(disable : 4127) // Pre c++17 the if constexpr remainder part will hit this
#elif defined(__GNUC__) && __GNUC__ == 5
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Warray-bounds"
#endif

// See: The Art of Computer Programming Volume 2 (Semi-numerical algorithms) section 4.3.1
// Algorithm D: Division of Non-negative integers
//
// Divides the m word dividend u by the n word divisor v (m >= n >= 2, v[n - 1] != 0) on 32-bit words
// and writes the quotient to q. With need_remainder the remainder is left in u, otherwise u is scratch.
// The word counts are template parameters so the same routine serves 128-bit operands here and the
// 256-bit operands of Boost.Decimal.
template <bool need_remainder, std::size_t u_size, std::size_t v_size, std::size_t q_size>
BOOST_INT128_HOST_DEVICE constexpr void knuth_divide(std::uint32_t (&u)[u_size], const std::size_t m,
                            const std::uint32_t (&v)[v_size], const std::size_t n,
                            std::uint32_t (&q)[q_size]) noexcept
{
    static_assert(v_size >= 2, "Algorithm D needs at least two divisor words");
    static_assert(u_size >= v_size, "The dividend can not be narrower than the divisor");
    static_assert(q_size >= u_size, "The quotient buffer must be as wide as the dividend");

    // D.1: normalize so the top word of the divisor has its most significant bit set
    const auto s {countl_zero(v[n - 1])};
    const auto complement_s {32 - s};
    const bool needs_shift {s > 0};

    // Create normalized versions of u and v
    std::uint32_t un[u_size + 1] {};
    std::uint32_t vn[v_size] {};

    for (std::size_t i {n - 1}; i > 0; --i)
    {
        vn[i] = needs_shift ? ((v[i] << s) | (v[i - 1] >> complement_s)) : v[i];
    }
    vn[0] = needs_shift ? (v[0] << s) : v[0];

    un[m] = needs_shift ? (u[m - 1] >> complement_s) : 0;
    for (std::size_t i {m - 1}; i > 0; --i)
    {
        un[i] = needs_shift ? ((u[i] << s) | (u[i - 1] >> complement_s)) : u[i];
    }
    un[0] = needs_shift ? (u[0] << s) : u[0];

    // D.2
    for (std::size_t j {m - n}; j != static_cast<std::size_t>(-1); --j)
    {
        // D.3
        const auto dividend {(static_cast<std::uint64_t>(un[j+n]) << 32) | un[j+n-1]};
        const auto divisor {static_cast<std::uint64_t>(vn[n-1])};
        auto q_hat {dividend / divisor};
        auto r_hat {dividend % divisor};

        while (q_hat > UINT32_MAX ||
               (q_hat * vn[n-2]) > ((r_hat << 32) | un[j+n-2]))
        {
            --q_hat;
            r_hat += vn[n-1];
            if (r_hat > UINT32_MAX)
            {
                break;
            }
        }

        // D.4
        std::int64_t borrow {};
        for (std::size_t i {}; i < n; ++i)
        {
            const auto p {q_hat * vn[i]};
            const auto p_lo {static_cast<std::uint32_t>(p & UINT32_MAX)};
            const auto p_hi {static_cast<std::uint32_t>(p >> 32)};

            borrow += static_cast<std::int64_t>(un[j+i]) - static_cast<std::int64_t>(p_lo);
            un[j+i] = static_cast<std::uint32_t>(borrow & UINT32_MAX);
            borrow >>= 32;

            borrow -= p_hi;
        }
        borrow += un[j+n];
        un[j+n] = static_cast<std::uint32_t>(borrow & UINT32_MAX);

        // D.5
        q[j] = static_cast<std::uint32_t>(q_hat & UINT32_MAX);
        if (BOOST_INT128_UNLIKELY(borrow < 0))
        {
            // D.6
            // The probability of hitting this path is about 4.7e-10
            --q[j];                                                             // LCOV_EXCL_LINE
            std::uint64_t carry {};                                             // LCOV_EXCL_LINE
            for (std::size_t i = 0; i < n; ++i)                                 // LCOV_EXCL_LINE
            {                                                                   // LCOV_EXCL_LINE
                carry += static_cast<std::uint64_t>(un[j+i]) + vn[i];           // LCOV_EXCL_LINE
                un[j+i] = static_cast<std::uint32_t>(carry & UINT32_MAX);       // LCOV_EXCL_LINE
                carry >>= 32U;                                                  // LCOV_EXCL_LINE
            }                                                                   // LCOV_EXCL_LINE
            un[j+n] += static_cast<std::uint32_t>(carry & UINT32_MAX);          // LCOV_EXCL_LINE
        }
    }

    // D.8
    // If we are only calculating division we can completely skip this step
    BOOST_INT128_IF_CONSTEXPR (need_remainder)
    {
        if (s > 0)
        {
            for (std::size_t i {}; i < n-1; i++)
            {
                u[i] = (un[i] >> s) | (un[i+1] << (32 - s));
            }
            u[n-1] = un[n-1] >> s;
        }
        else
        {
            for (std::size_t i {}; i < n; i++)
            {
                u[i] = un[i];
            }
        }

        // Clear anything left in u
        for (std::size_t i {n}; i < m; i++)
        {
            u[i] = 0;
        }
    }
}

#if defined(_MSC_VER)
#  pragma warning(pop)
#elif defined(__GNUC__) && __GNUC__ == 5
#  pragma GCC diagnostic pop
#endif

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::size_t to_words(const T& x, std::uint32_t (&words)[4]) noexcept
{
    #if !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && !BOOST_INT128_ENDIAN_BIG_BYTE
    if (!BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        std::memcpy(&words, &x, sizeof(T));
    }
    else
    #endif
    {
        words[0] = static_cast<std::uint32_t>(x.low & UINT32_MAX);                              // LCOV_EXCL_LINE
        words[1] = static_cast<std::uint32_t>(x.low >> 32);                                     // LCOV_EXCL_LINE
        words[2] = static_cast<std::uint32_t>(static_cast<std::uint64_t>(x.high) & UINT32_MAX); // LCOV_EXCL_LINE
        words[3] = static_cast<std::uint32_t>(static_cast<std::uint64_t>(x.high) >> 32);        // LCOV_EXCL_LINE
    }

    BOOST_INT128_ASSERT_MSG(x != static_cast<T>(0), "Division by 0");

    std::size_t word_count {4};
    while (words[word_count - 1U] == 0U)
    {
        word_count--;
    }

    return word_count;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::size_t to_words(const std::uint64_t x, std::uint32_t (&words)[2]) noexcept
{
    #if !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && !BOOST_INT128_ENDIAN_BIG_BYTE
    if (!BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        std::memcpy(&words, &x, sizeof(std::uint64_t));
    }
    else
    #endif
    {
        words[0] = static_cast<std::uint32_t>(x & UINT32_MAX);  // LCOV_EXCL_LINE
        words[1] = static_cast<std::uint32_t>(x >> 32);         // LCOV_EXCL_LINE
    }

    return x > UINT32_MAX ? 2 : 1;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr std::size_t to_words(const std::uint32_t x, std::uint32_t (&words)[1]) noexcept
{
    words[0] = x;

    return 1;
}

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr T from_words(const std::uint32_t (&words)[4]) noexcept
{
    const auto low {static_cast<std::uint64_t>(words[0]) | (static_cast<std::uint64_t>(words[1]) << 32)};
    const auto high {static_cast<std::uint64_t>(words[2]) | (static_cast<std::uint64_t>(words[3]) << 32)};

    return {static_cast<ctor_high_word_t<T>>(high), low};
}

} // namespace impl

// We only need to take the time to process the remainder in the modulo case
// In the division case it is a waste of cycles
//
// 128/64 -> 128-bit quotient (and optional 64-bit remainder) by two-step long division.
// The leading 64/64 yields the high quotient word and a remainder r < rhs, which satisfies
// the udiv_2by1 precondition for the low quotient word. This covers every rhs (including
// rhs <= UINT32_MAX) through the single hardware-or-portable udiv_2by1 primitive.

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr void one_word_div(const T& lhs, const std::uint64_t rhs, T& quotient) noexcept
{
    using high_word_type = decltype(T{}.high);

    BOOST_INT128_ASSUME(rhs != 0); // LCOV_EXCL_LINE

    const auto u_high {static_cast<std::uint64_t>(lhs.high)};

    quotient.high = static_cast<high_word_type>(u_high / rhs);
    auto r {u_high % rhs};
    quotient.low = udiv_2by1(r, lhs.low, rhs, r);
}

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr void one_word_div(const T& lhs, const std::uint64_t rhs, T& quotient, T& remainder) noexcept
{
    using high_word_type = decltype(T{}.high);

    BOOST_INT128_ASSUME(rhs != 0); // LCOV_EXCL_LINE

    const auto u_high {static_cast<std::uint64_t>(lhs.high)};

    quotient.high = static_cast<high_word_type>(u_high / rhs);
    auto r {u_high % rhs};
    quotient.low = udiv_2by1(r, lhs.low, rhs, r);

    remainder.high = static_cast<high_word_type>(0);
    remainder.low = r;
}

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr void one_word_div(const T& lhs, const std::uint32_t rhs, T& quotient, T& remainder) noexcept
{
    one_word_div(lhs, static_cast<std::uint64_t>(rhs), quotient, remainder);
}

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr void one_word_div(const T& lhs, const std::uint32_t rhs, T& quotient) noexcept
{
    one_word_div(lhs, static_cast<std::uint64_t>(rhs), quotient);
}

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4127) // Conditional expression is constant is true pre-C++17
#  pragma warning(disable : 4804) // Unsafe comparison with bool
#endif

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr T knuth_div(const T& dividend, const T& divisor) noexcept
{
    BOOST_INT128_ASSUME(divisor != static_cast<T>(0));

    std::uint64_t rem_hi {};
    std::uint64_t rem_lo {};

    const auto q {div3by2<false>(static_cast<std::uint64_t>(dividend.high), dividend.low,
                                 static_cast<std::uint64_t>(divisor.high), divisor.low, rem_hi, rem_lo)};

    return T{static_cast<ctor_high_word_t<T>>(0), q};
}

template <typename T>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr T knuth_div(const T& dividend, const T& divisor, T& remainder) noexcept
{
    BOOST_INT128_ASSUME(divisor != static_cast<T>(0));

    std::uint64_t rem_hi {};
    std::uint64_t rem_lo {};

    const auto q {div3by2<true>(static_cast<std::uint64_t>(dividend.high), dividend.low,
                                static_cast<std::uint64_t>(divisor.high), divisor.low, rem_hi, rem_lo)};

    remainder = T{static_cast<ctor_high_word_t<T>>(rem_hi), rem_lo};

    return T{static_cast<ctor_high_word_t<T>>(0), q};
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

#if defined(__clang__)
#  pragma clang diagnostic pop
#endif


} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_COMMON_DIV_HPP
// ===== END boost/int128/detail/common_div.hpp =====
// ===== BEGIN boost/int128/detail/float_conversion.hpp =====
// Copyright 2026 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_FLOAT_CONVERSION_HPP
#define BOOST_INT128_DETAIL_FLOAT_CONVERSION_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/constants.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/clz.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstdint>
#include <limits>
#include <type_traits>

#endif

namespace boost {
namespace int128 {
namespace detail {

// 2^exp as a T, exactly, for 0 <= exp <= 127. Splitting at 2^64 keeps both factors inside the
// range of an exact conversion from a 64-bit integer, so no rounding happens here
template <typename T>
BOOST_INT128_HOST_DEVICE constexpr T exact_power_of_two(const int exp) noexcept
{
    return exp < 64 ? static_cast<T>(UINT64_C(1) << exp)
                    : static_cast<T>(UINT64_C(1) << (exp - 64)) * offset_value_v<T>;
}

// A significand of 64 bits or more holds each word exactly, and holds the scaled high word
// exactly as well, so the addition is the only rounding and the two term form is already
// correctly rounded. x87 80-bit (64), IEEE binary128 (113) and IBM double-double (106) all
// take this path
template <typename T>
BOOST_INT128_HOST_DEVICE constexpr T unsigned_words_to_float_impl(const std::uint64_t high, const std::uint64_t low,
                                                                 std::true_type) noexcept
{
    return static_cast<T>(high) * offset_value_v<T> + static_cast<T>(low);
}

// float and double cannot hold the high word or the sum exactly, so high * 2^64 + low rounds
// as many as three times and lands up to one ulp away from the correctly rounded result.
// Round the 128-bit value to exactly digits bits here instead, once, then apply an exact
// power of two. See the note on ties in the body
template <typename T>
BOOST_INT128_HOST_DEVICE constexpr T unsigned_words_to_float_impl(const std::uint64_t high, const std::uint64_t low,
                                                                 std::false_type) noexcept
{
    // Anything below 2^64 is one conversion the compiler already rounds correctly
    if (high == UINT64_C(0))
    {
        return static_cast<T>(low);
    }

    constexpr int digits {std::numeric_limits<T>::digits};
    constexpr int residue_bits {64 - digits};
    constexpr std::uint64_t residue_mask {(UINT64_C(1) << residue_bits) - UINT64_C(1)};
    constexpr std::uint64_t halfway {UINT64_C(1) << (residue_bits - 1)};

    // Normalize so bit 127 of the pair is set, which puts the significand at the top of the
    // high word. high is non-zero here, so the distance is always less than 64
    const auto shift {static_cast<unsigned>(countl_zero(high))};
    const auto norm_high {shift == 0U ? high : ((high << shift) | (low >> (64U - shift)))};
    const auto norm_low {shift == 0U ? low : (low << shift)};

    const auto significand {norm_high >> residue_bits};
    const auto residue {norm_high & residue_mask};

    // Round to nearest, ties to even. The discarded part is residue * 2^64 + norm_low, so it
    // is above the halfway point when residue is, and it is exactly the halfway point only
    // when residue equals halfway and every lower bit is clear
    const bool round_up {residue > halfway ||
                         (residue == halfway && (norm_low != UINT64_C(0) || (significand & UINT64_C(1)) != UINT64_C(0)))};

    const auto rounded {significand + (round_up ? UINT64_C(1) : UINT64_C(0))};

    // rounded holds digits bits, or digits + 1 when it carried, in which case it is a power of
    // two. Either way the conversion and the scaling are exact, so the product is the value
    // rounded exactly once. It overflows to infinity precisely when the correctly rounded
    // result does, which is the required behavior for round to nearest
    return static_cast<T>(rounded) * exact_power_of_two<T>(static_cast<int>(64U - shift) + residue_bits);
}

// Converts the 128-bit value (high, low) to T, correctly rounded to nearest with ties to even
template <typename T>
BOOST_INT128_HOST_DEVICE constexpr T unsigned_words_to_float(const std::uint64_t high, const std::uint64_t low) noexcept
{
    return unsigned_words_to_float_impl<T>(high, low,
                                           std::integral_constant<bool, (std::numeric_limits<T>::digits >= 64)>{});
}

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr T signed_words_to_float(const std::int64_t high, const std::uint64_t low) noexcept
{
    if (high < 0)
    {
        // Two's complement magnitude of the full 128-bit value.
        // INT128_MIN needs no special case since it yields high = 2^63, low = 0
        const auto abs_low {~low + UINT64_C(1)};
        const auto abs_high {~static_cast<std::uint64_t>(high) + (abs_low == UINT64_C(0) ? UINT64_C(1) : UINT64_C(0))};

        // The magnitude is rounded once and the negation is exact
        return -unsigned_words_to_float<T>(abs_high, abs_low);
    }

    return unsigned_words_to_float<T>(static_cast<std::uint64_t>(high), low);
}

// The other direction: a value in [0, 2^64) truncated toward zero into a 64-bit word.
// A cast straight to an unsigned type takes the compiler's unsigned conversion path, which
// subtracts 2^63 and puts the top bit back afterwards. Clang 7 emits that subtraction inside the
// window where it has already set the x87 control word to single precision, so an 80-bit long
// double loses every significand bit past the 24th. Splitting at 2^63 here keeps both conversions
// inside the signed range, where the conversion is one instruction on every compiler
template <typename T>
BOOST_INT128_HOST_DEVICE constexpr std::uint64_t float_to_uint64(const T value) noexcept
{
    constexpr T two_63 {static_cast<T>(UINT64_C(1) << 63)};

    // value - two_63 is exact: it is a multiple of the ulp of value, and it is below 2^63
    return value < two_63
           ? static_cast<std::uint64_t>(static_cast<std::int64_t>(value))
           : static_cast<std::uint64_t>(static_cast<std::int64_t>(value - two_63)) | (UINT64_C(1) << 63);
}

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_FLOAT_CONVERSION_HPP
// ===== END boost/int128/detail/float_conversion.hpp =====

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstdint>
#include <cstring>

#endif

namespace boost {
namespace int128 {

struct
    #if (defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)) && !defined(_M_IX86)
    alignas(alignof(detail::builtin_i128))
    #endif
int128
{
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE
    std::uint64_t low {};
    std::uint64_t high {};
    #else

    #ifdef __GNUC__
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wreorder"
    #endif

    std::uint64_t high {};
    std::uint64_t low {};

    #ifdef __GNUC__
    #  pragma GCC diagnostic pop
    #endif

    #endif

    // Defaulted basic construction
    constexpr int128() noexcept = default;
    constexpr int128(const int128&) noexcept = default;
    constexpr int128(int128&&) noexcept = default;
    constexpr int128& operator=(const int128&) noexcept = default;
    constexpr int128& operator=(int128&&) noexcept = default;

    // Requires a conversion file to be implemented
    BOOST_INT128_HOST_DEVICE constexpr int128(const uint128& v) noexcept;

    // Construct from integral types
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    BOOST_INT128_HOST_DEVICE constexpr int128(const std::int64_t hi, const std::uint64_t lo) noexcept : low{lo}, high{static_cast<std::uint64_t>(hi)} {}

    template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128(const SignedInteger v) noexcept : low {static_cast<std::uint64_t>(v)}, high {v < 0 ? ~UINT64_C(0) : UINT64_C(0)} {}

    template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128(const UnsignedInteger v) noexcept : low {static_cast<std::uint64_t>(v)}, high {} {}

    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128(const detail::builtin_i128 v) noexcept : low {static_cast<std::uint64_t>(v & static_cast<detail::builtin_i128>(detail::low_word_mask))}, high {static_cast<std::uint64_t>(v >> static_cast<detail::builtin_i128>(64U))} {}
    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128(const detail::builtin_u128 v) noexcept : low {static_cast<std::uint64_t>(v & static_cast<detail::builtin_u128>(detail::low_word_mask))}, high {static_cast<std::uint64_t>(v >> static_cast<detail::builtin_u128>(64U))} {}

    #endif // BOOST_INT128_HAS_INT128

    #else // Big endian

    BOOST_INT128_HOST_DEVICE constexpr int128(const std::int64_t hi, const std::uint64_t lo) noexcept : high{static_cast<std::uint64_t>(hi)}, low{lo} {}

    template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128(const SignedInteger v) noexcept : high{v < 0 ? ~UINT64_C(0) : UINT64_C(0)}, low{static_cast<std::uint64_t>(v)} {}

    template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128(const UnsignedInteger v) noexcept : high {}, low {static_cast<std::uint64_t>(v)} {}

    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128(const detail::builtin_i128 v) noexcept : high {static_cast<std::uint64_t>(v >> 64U)}, low {static_cast<std::uint64_t>(v & detail::low_word_mask)} {}
    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128(const detail::builtin_u128 v) noexcept : high {static_cast<std::uint64_t>(v >> 64U)}, low {static_cast<std::uint64_t>(v & detail::low_word_mask)} {}

    #endif // BOOST_INT128_HAS_INT128

    #endif // BOOST_INT128_ENDIAN_LITTLE_BYTE

    // Construct from floating-point types
    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128(Float f) noexcept;

    // The high word read as a signed value.
    // Every operation whose meaning depends on the sign of the value goes through
    // this rather than reading high directly.
    BOOST_INT128_HOST_DEVICE constexpr std::int64_t signed_high() const noexcept { return static_cast<std::int64_t>(high); }

    // Integer Conversion operators
    BOOST_INT128_HOST_DEVICE explicit constexpr operator bool() const noexcept { return low || high; }

    template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr operator SignedInteger() const noexcept { return static_cast<SignedInteger>(low); }

    #ifdef _MSC_VER
    #  pragma warning(push)
    #  pragma warning(disable:4127)
    #endif

    template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr operator UnsignedInteger() const noexcept
    {
        BOOST_INT128_IF_CONSTEXPR (std::is_same<UnsignedInteger, bool>::value)
        {
            return low || high;
        }
        else
        {
            return static_cast<UnsignedInteger>(low);
        }
    }

    #ifdef _MSC_VER
    #  pragma warning(pop)
    #endif

    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR operator detail::builtin_i128() const noexcept { return static_cast<detail::builtin_i128>(static_cast<detail::builtin_u128>(high) << static_cast<detail::builtin_u128>(64)) | static_cast<detail::builtin_i128>(low); }

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR operator detail::builtin_u128() const noexcept { return (static_cast<detail::builtin_u128>(high) << static_cast<detail::builtin_u128>(64)) | static_cast<detail::builtin_u128>(low); }

    #endif // BOOST_INT128_HAS_INT128

    // Conversion to float
    // Uses the builtin 128-bit conversion where one exists, and otherwise converts
    // the unsigned magnitude as high * 2^64 + low before applying the sign.
    // See detail/float_conversion.hpp for why the sign handling is required
    BOOST_INT128_HOST_DEVICE constexpr operator float() const noexcept;
    BOOST_INT128_HOST_DEVICE constexpr operator double() const noexcept;

    // Long double does not exist on the CUDA or SYCL (spir64) device
    #if !defined(BOOST_INT128_HAS_GPU_SUPPORT)
    constexpr operator long double() const noexcept;
    #endif

    // Compound Or
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator|=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator|=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator|=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound And
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator&=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator&=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator&=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound XOR
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator^=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator^=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator^=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Left Shift
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator<<=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator<<=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator<<=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Right Shift
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator>>=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator>>=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator>>=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Prefix and postfix increment
    BOOST_INT128_HOST_DEVICE constexpr int128& operator++() noexcept;
    BOOST_INT128_HOST_DEVICE constexpr int128 operator++(int) noexcept;

    // Prefix and postfix decrment
    BOOST_INT128_HOST_DEVICE constexpr int128& operator--() noexcept;
    BOOST_INT128_HOST_DEVICE constexpr int128 operator--(int) noexcept;

    // Compound Addition
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator+=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator+=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator+=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Subtraction
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator-=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator-=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator-=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Multiplication
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator*=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator*=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator*=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Division
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator/=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator/=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator/=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Modulo
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator%=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr int128& operator%=(int128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline int128& operator%=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound assignment with floating point types.
    // Matches the builtin: this value is converted to Float, the operation is applied in
    // floating point, and the result is converted back, truncating toward zero.
    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator+=(Float rhs) noexcept;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator-=(Float rhs) noexcept;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator*=(Float rhs) noexcept;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr int128& operator/=(Float rhs) noexcept;

    // The builtin does not allow a floating point operand for these, so neither do we.
    // Without these the implicit floating point constructor would silently truncate rhs
    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE int128& operator%=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE int128& operator&=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE int128& operator|=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE int128& operator^=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE int128& operator<<=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE int128& operator>>=(Float rhs) = delete;
};

namespace detail {

// Builds an int128 from the raw two's complement words
// Enables vectorization
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 from_bits(const std::uint64_t hi, const std::uint64_t lo) noexcept
{
    int128 result {};
    result.high = hi;
    result.low = lo;
    return result;
}

} // namespace detail

//=====================================
// Float Conversion Operators
//=====================================

// When the builtin 128-bit type exists we convert through it since the compiler
// runtime (__floattisf and friends) is correctly rounded. The portable fallback
// converts the unsigned magnitude and applies the sign; see detail/float_conversion.hpp
// for why the raw words can not be composed directly for negative values

BOOST_INT128_HOST_DEVICE constexpr int128::operator float() const noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !defined(BOOST_INT128_HAS_GPU_SUPPORT)

    return static_cast<float>(static_cast<detail::builtin_i128>(*this));

    #else

    return detail::signed_words_to_float<float>(signed_high(), low);

    #endif
}

BOOST_INT128_HOST_DEVICE constexpr int128::operator double() const noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !defined(BOOST_INT128_HAS_GPU_SUPPORT)

    return static_cast<double>(static_cast<detail::builtin_i128>(*this));

    #else

    return detail::signed_words_to_float<double>(signed_high(), low);

    #endif
}

#if !defined(BOOST_INT128_HAS_GPU_SUPPORT)

constexpr int128::operator long double() const noexcept
{
    #if defined(BOOST_INT128_HAS_INT128)

    return static_cast<long double>(static_cast<detail::builtin_i128>(*this));

    #else

    return detail::signed_words_to_float<long double>(signed_high(), low);

    #endif
}

#endif

//=====================================
// Float Construction
//=====================================

// Inverse of operator(Float).
// NaN -> 0;
// f >= 2^127 -> INT128_MAX;
// f < -2^127 -> INT128_MIN.
template <BOOST_INT128_FLOATING_POINT_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128::int128(Float f) noexcept
{
    constexpr Float two_32 {static_cast<Float>(UINT64_C(1) << 32)};
    constexpr Float two_64 {two_32 * two_32};
    constexpr Float two_127 {two_64 * static_cast<Float>(UINT64_C(1) << 63)};

    // NaN: leave default-initialized (zero). NaN compares false to everything,
    // so neither >= 0 nor <= 0 holds.
    if (!(f >= Float{0}) && !(f <= Float{0}))
    {
        return;
    }

    if (f >= two_127)
    {
        high = UINT64_C(0x7FFFFFFFFFFFFFFF);
        low = UINT64_MAX;
        return;
    }

    if (f <= -two_127)
    {
        high = UINT64_C(0x8000000000000000);
        low = UINT64_C(0);
        return;
    }

    const bool negative {f < Float{0}};
    const Float abs_f {negative ? -f : f};

    std::uint64_t h {detail::float_to_uint64(abs_f / two_64)};
    const Float remainder {abs_f - static_cast<Float>(h) * two_64};
    std::uint64_t l {detail::float_to_uint64(remainder)};

    if (negative)
    {
        // Two's complement negation of (h, l): new_l = -l (with wraparound),
        // new_h = ~h if a borrow occurred (l != 0), else ~h + 1.
        const bool low_was_zero {l == UINT64_C(0)};
        l = UINT64_C(0) - l;
        h = ~h + (low_was_zero ? UINT64_C(1) : UINT64_C(0));
    }

    high = h;
    low = l;
}

//=====================================
// Unary Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const int128 value) noexcept
{
    return value;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator-(const int128 value) noexcept
{
    // Spelled with the constructor rather than from_bits: clang folds the low word
    // of the low == 0 arm away here, and loses that if the members are written.
    return (value.low == 0) ? int128{static_cast<std::int64_t>(UINT64_C(0) - value.high), 0} :
                              int128{static_cast<std::int64_t>(~value.high), ~value.low + 1};
}

//=====================================
// Equality Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator==(const int128 lhs, const bool rhs) noexcept
{
    return lhs.high == 0 && lhs.low == static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator==(const bool lhs, const int128 rhs) noexcept
{
    return rhs.high == 0 && rhs.low == static_cast<std::uint64_t>(lhs);
}

#if defined(__clang__)
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wsign-conversion"
#  pragma clang diagnostic ignored "-Wsign-compare"
#elif defined(__GNUC__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wsign-conversion"
#  pragma GCC diagnostic ignored "-Wsign-compare"
#endif

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator==(const int128 lhs, const int128 rhs) noexcept
{
    // x64 and ARM64 like the values in opposite directions

    #if defined(__aarch64__) || defined(_M_ARM64) || defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)

    return lhs.low == rhs.low && lhs.high == rhs.high;

    #else

    return lhs.high == rhs.high && lhs.low == rhs.low;

    #endif
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const int128 lhs, const SignedInteger rhs) noexcept
{
    return lhs.high == (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)) && lhs.low == static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const SignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.high == (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)) && rhs.low == static_cast<std::uint64_t>(lhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high == 0 && lhs.low == static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.high == 0 && rhs.low == static_cast<std::uint64_t>(lhs);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs == static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) == rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Inequality Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const int128 lhs, const int128 rhs) noexcept
{
    // x64 and ARM64 like the values in opposite directions

    #if defined(__aarch64__) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IX86)

    return lhs.low != rhs.low || lhs.high != rhs.high;

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high != rhs.high || lhs.low != rhs.low;
    }
    else
    {
        detail::builtin_i128 builtin_lhs {};
        detail::builtin_i128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs != builtin_rhs;
    }

    #else

    return lhs.high != rhs.high || lhs.low != rhs.low;

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const int128 lhs, const bool rhs) noexcept
{
    return lhs.high != 0 || lhs.low != static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const bool lhs, const int128 rhs) noexcept
{
    return rhs.high != 0 || rhs.low != static_cast<std::uint64_t>(lhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const int128 lhs, const SignedInteger rhs) noexcept
{
    return lhs.high != (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)) || lhs.low != static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const SignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.high != (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)) || rhs.low != static_cast<std::uint64_t>(lhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high != 0 || lhs.low != static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.high != 0 || rhs.low != static_cast<std::uint64_t>(lhs);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs != static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) != rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Less than Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator<(const int128 lhs, const int128 rhs) noexcept
{
    // On ARM macs only with the clang compiler is casting to __int128 uniformly better (and seemingly cost free)
    #if defined(__aarch64__) && defined(__APPLE__) && defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_i128>(lhs) < static_cast<detail::builtin_i128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low < rhs.low : lhs.signed_high() < rhs.signed_high();
    }
    else
    {
        detail::builtin_i128 builtin_lhs {};
        detail::builtin_i128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs < builtin_rhs;
    }

    #else

    return lhs.high == rhs.high ? lhs.low < rhs.low : lhs.signed_high() < rhs.signed_high();

    #endif
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.signed_high() < 0 || (lhs.high == 0 && lhs.low < static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.signed_high() > 0 || (rhs.high == 0 && static_cast<std::uint64_t>(lhs) < rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const int128 lhs, const SignedInteger rhs) noexcept
{
    if (lhs.signed_high() < 0)
    {
        return rhs >= 0 ? true : lhs < static_cast<int128>(rhs);
    }

    if (lhs.signed_high() > 0 || rhs < 0)
    {
        return false;
    }

    return lhs.low < static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const SignedInteger lhs, const int128 rhs) noexcept
{
    if (rhs.signed_high() < 0)
    {
        return lhs >= 0 ? false : static_cast<int128>(lhs) < rhs;
    }

    // rhs is positive
    if (rhs.signed_high() > 0 || lhs < 0)
    {
        return true;
    }

    return static_cast<std::uint64_t>(lhs) < rhs.low;
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs < static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) < rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Greater than Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator>(const int128 lhs, const int128 rhs) noexcept
{
    // On ARM macs only with the clang compiler is casting to __int128 uniformly better (and seemingly cost free)
    #if defined(__aarch64__) && defined(__APPLE__) && defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_i128>(lhs) > static_cast<detail::builtin_i128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low > rhs.low : lhs.signed_high() > rhs.signed_high();
    }
    else
    {
        detail::builtin_i128 builtin_lhs {};
        detail::builtin_i128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs > builtin_rhs;
    }

    #else

    return lhs.high == rhs.high ? lhs.low > rhs.low : lhs.signed_high() > rhs.signed_high();

    #endif
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const int128 lhs, const SignedInteger rhs) noexcept
{
    return !(lhs < rhs) && !(lhs == rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const SignedInteger lhs, const int128 rhs) noexcept
{
    return !(lhs < rhs) && !(lhs == rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.signed_high() > 0 || (lhs.high == 0 && lhs.low > static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.signed_high() < 0 || (rhs.high == 0 && static_cast<std::uint64_t>(lhs) > rhs.low);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs > static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) > rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Less Equal Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const int128 lhs, const int128 rhs) noexcept
{
    // On ARM macs only with the clang compiler is casting to __int128 uniformly better (and seemingly cost free)
    #if defined(__aarch64__) && defined(__APPLE__) && defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_i128>(lhs) <= static_cast<detail::builtin_i128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low <= rhs.low : lhs.signed_high() <= rhs.signed_high();
    }
    else
    {
        detail::builtin_i128 builtin_lhs {};
        detail::builtin_i128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs <= builtin_rhs;
    }

    #else

    return lhs.high == rhs.high ? lhs.low <= rhs.low : lhs.signed_high() <= rhs.signed_high();

    #endif
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const int128 lhs, const SignedInteger rhs) noexcept
{
    return !(lhs > rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const SignedInteger lhs, const int128 rhs) noexcept
{
    return !(lhs > rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.signed_high() < 0 || (lhs.high == 0 && lhs.low <= static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.signed_high() > 0 || (rhs.high == 0 && static_cast<std::uint64_t>(lhs) <= rhs.low);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs <= static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) <= rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Greater Equal Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const int128 lhs, const int128 rhs) noexcept
{
    // On ARM macs only with the clang compiler is casting to __int128 uniformly better (and seemingly cost free)
    #if defined(__aarch64__) && defined(__APPLE__) && defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_i128>(lhs) >= static_cast<detail::builtin_i128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low >= rhs.low : lhs.signed_high() >= rhs.signed_high();
    }
    else
    {
        detail::builtin_i128 builtin_lhs {};
        detail::builtin_i128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs >= builtin_rhs;
    }

    #else

    return lhs.high == rhs.high ? lhs.low >= rhs.low : lhs.signed_high() >= rhs.signed_high();

    #endif
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const int128 lhs, const SignedInteger rhs) noexcept
{
    return !(lhs < rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const SignedInteger lhs, const int128 rhs) noexcept
{
    return !(lhs < rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.signed_high() > 0 || (lhs.high == 0 && lhs.low >= static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return rhs.signed_high() < 0 || (rhs.high == 0 && static_cast<std::uint64_t>(lhs) >= rhs.low);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs >= static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) >= rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Spaceship Operator
//=====================================

#ifdef BOOST_INT128_HAS_SPACESHIP_OPERATOR

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const int128 lhs, const int128 rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const int128 lhs, const SignedInteger rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const SignedInteger lhs, const int128 rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

#endif

//=====================================
// Not Operator
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator~(const int128 rhs) noexcept
{
    return detail::from_bits(~rhs.high, ~rhs.low);
}

//=====================================
// Or Operator
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator|(const int128 lhs, const int128 rhs) noexcept
{
    return detail::from_bits(lhs.high | rhs.high, lhs.low | rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator|(const int128 lhs, const SignedInteger rhs) noexcept
{
    return detail::from_bits(lhs.high | (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), lhs.low | static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator|(const SignedInteger lhs, const int128 rhs) noexcept
{
    return detail::from_bits(rhs.high | (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), static_cast<std::uint64_t>(lhs) | rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator|(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return detail::from_bits(lhs.high, lhs.low | static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator|(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return detail::from_bits(rhs.high, static_cast<std::uint64_t>(lhs) | rhs.low);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator|(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs | static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator|(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) | rhs;
}


#endif // BOOST_INT128_HAS_INT128

//=====================================
// Compound OR Operator
//=====================================

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator|=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this | rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator|=(const int128 rhs) noexcept
{
    *this = *this | rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator|=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this | rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// And Operator
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator&(const int128 lhs, const int128 rhs) noexcept
{
    return detail::from_bits(lhs.high & rhs.high, lhs.low & rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator&(const int128 lhs, const SignedInteger rhs) noexcept
{
    return detail::from_bits(lhs.high & (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), lhs.low & static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator&(const SignedInteger lhs, const int128 rhs) noexcept
{
    return detail::from_bits(rhs.high & (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), static_cast<std::uint64_t>(lhs) & rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator&(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return {0, lhs.low & static_cast<std::uint64_t>(rhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator&(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return {0, static_cast<std::uint64_t>(lhs) & rhs.low};
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator&(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs & static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator&(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) & rhs;
}


#endif // BOOST_INT128_HAS_INT128

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator&=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this & rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Compound And Operator
//=====================================

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator&=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this & rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator&=(const int128 rhs) noexcept
{
    *this = *this & rhs;
    return *this;
}

//=====================================
// XOR Operator
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator^(const int128 lhs, const int128 rhs) noexcept
{
    return detail::from_bits(lhs.high ^ rhs.high, lhs.low ^ rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator^(const int128 lhs, const SignedInteger rhs) noexcept
{
    return detail::from_bits(lhs.high ^ (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), lhs.low ^ static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator^(const SignedInteger lhs, const int128 rhs) noexcept
{
    return detail::from_bits(rhs.high ^ (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), static_cast<std::uint64_t>(lhs) ^ rhs.low);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator^(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return detail::from_bits(lhs.high, lhs.low ^ static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator^(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return detail::from_bits(rhs.high, static_cast<std::uint64_t>(lhs) ^ rhs.low);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator^(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs ^ static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator^(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) ^ rhs;
}


#endif // BOOST_INT128_HAS_INT128

//=====================================
// Compound XOR Operator
//=====================================

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator^=(Integer rhs) noexcept
{
    *this = static_cast<int128>(*this ^ rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator^=(int128 rhs) noexcept
{
    *this = *this ^ rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator^=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this ^ rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Left Shift Operator
//=====================================

namespace detail {

template <typename Integer>
BOOST_INT128_HOST_DEVICE constexpr int128 default_ls_impl(const int128 lhs, const Integer rhs) noexcept
{
    static_assert(std::is_integral<Integer>::value, "Only builtin types allowed");

    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators. In a
    // constant expression the compiler diagnoses it; at runtime it is unspecified.
    if (rhs == 0)
    {
        return lhs;
    }

    if (rhs == 64)
    {
        return detail::from_bits(lhs.low, 0);
    }

    if (rhs > 64)
    {
        return detail::from_bits(lhs.low << (rhs - 64), 0);
    }

    // For shifts < 64
    std::uint64_t high_part = (lhs.high << rhs) |
                              (lhs.low >> (64 - rhs));

    return detail::from_bits(high_part, lhs.low << rhs);
}

template <typename Integer>
BOOST_INT128_HOST_DEVICE int128 intrinsic_ls_impl(const int128 lhs, const Integer rhs) noexcept
{
    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators; delegate
    // straight to the native type so we produce identical results.
    #ifdef BOOST_INT128_HAS_INT128

    // Left-shifting a negative builtin_i128 is UB pre-C++20
    #  if defined(__aarch64__)

    #if defined(__GNUC__) && __GNUC__ >= 8
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wclass-memaccess"
    #endif

    builtin_u128 value;
    std::memcpy(&value, &lhs, sizeof(builtin_u128));
    const auto res {value << rhs};

    int128 return_value;
    std::memcpy(&return_value, &res, sizeof(int128));
    return return_value;

    #if defined(__GNUC__) && __GNUC__ >= 8
    #  pragma GCC diagnostic pop
    #endif

    #  else

    return int128{static_cast<builtin_u128>(lhs) << rhs};

    #  endif

    #elif defined(_M_AMD64) && !defined(__GNUC__)

    if (rhs >= 64)
    {
        return detail::from_bits(lhs.low << (rhs - 64), 0);
    }
    else
    {
        int128 res;
        res.high = __shiftleft128(lhs.low, lhs.high, static_cast<unsigned char>(rhs));
        res.low = lhs.low << rhs;

        return res;
    }

    #else

    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        return lhs;
    }
    if (rhs == 64)
    {
        return detail::from_bits(lhs.low, 0);
    }

    if (rhs > 64)
    {
        return detail::from_bits(lhs.low << (rhs - 64), 0);
    }

    // For shifts < 64
    const auto high_part = (lhs.high << rhs) |
                           (lhs.low >> (64 - rhs));

    return detail::from_bits(high_part, lhs.low << rhs);

    #endif
}

} // namespace detail

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator<<(const int128 lhs, const Integer rhs) noexcept
{
    #ifndef BOOST_INT128_NO_CONSTEVAL_DETECTION

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return detail::default_ls_impl(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        return detail::intrinsic_ls_impl(lhs, rhs);
    }

    #else

    return detail::default_ls_impl(lhs, rhs);

    #endif
}

BOOST_INT128_HOST_DEVICE constexpr int128 operator<<(const int128 lhs, const int128 rhs) noexcept
{
    // Out-of-range counts (negative, >= 128, or with the high word set) are
    // undefined, matching the built-in operators; forward to the scalar overload.
    return lhs << rhs.low;
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_u128 operator<<(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs << static_cast<detail::builtin_u128>(rhs.low);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_i128 operator<<(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs << static_cast<detail::builtin_u128>(rhs.low);
}

#endif

// A shift takes its value and its result type from the left operand after integral promotion,
// and only the count from the right, exactly as the builtin does

BOOST_INT128_EXPORT template <typename Integer, std::enable_if_t<detail::is_any_integer_v<Integer> && (sizeof(Integer) * 8 <= 64), bool> = true>
BOOST_INT128_HOST_DEVICE constexpr detail::promoted_t<Integer> operator<<(const Integer lhs, const int128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return static_cast<detail::promoted_t<Integer>>(lhs) << rhs.low;
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4804) // Unsafe use of type bool in operation
#endif // _MSC_VER

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator<<=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this << rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator<<=(const int128 rhs) noexcept
{
    *this = *this << rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator<<=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this << rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

#ifdef _MSC_VER
#pragma warning(pop)
#endif // _MSC_VER

//=====================================
// Right Shift Operator
//=====================================

namespace detail {

template <typename Integer>
BOOST_INT128_HOST_DEVICE constexpr int128 default_rs_impl(const int128 lhs, const Integer rhs) noexcept
{
    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators. In a
    // constant expression the compiler diagnoses it; at runtime it is unspecified.
    if (rhs == 0)
    {
        return lhs;
    }

    if (rhs >= 64)
    {
        return detail::from_bits(lhs.signed_high() < 0 ? ~UINT64_C(0) : UINT64_C(0),
                                 static_cast<std::uint64_t>(lhs.signed_high() >> (rhs - 64)));
    }

    // For shifts < 64
    const auto high_to_low {lhs.high << (64 - rhs)};
    const auto low_shifted {lhs.low >> rhs};
    const auto low_part {high_to_low | low_shifted};

    return detail::from_bits(static_cast<std::uint64_t>(lhs.signed_high() >> rhs), low_part);
}

template <typename Integer>
BOOST_INT128_HOST_DEVICE int128 intrinsic_rs_impl(const int128 lhs, const Integer rhs) noexcept
{
    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators; delegate
    // straight to the native type so we produce identical results.
    #ifdef BOOST_INT128_HAS_INT128

    #  if defined(__aarch64__)

    #if defined(__GNUC__) && __GNUC__ >= 8
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wclass-memaccess"
    #endif

    builtin_i128 value;
    std::memcpy(&value, &lhs, sizeof(builtin_i128));
    const auto res {value >> rhs};

    int128 return_value;
    std::memcpy(&return_value, &res, sizeof(int128));
    return return_value;

    #if defined(__GNUC__) && __GNUC__ >= 8
    #  pragma GCC diagnostic pop
    #endif

    #  else

    return static_cast<builtin_i128>(lhs) >> rhs;

    #  endif

    #elif defined(_M_AMD64) && !defined(__GNUC__)

    if (rhs >= 64)
    {
        return detail::from_bits(lhs.signed_high() < 0 ? ~UINT64_C(0) : UINT64_C(0),
                                 static_cast<std::uint64_t>(lhs.signed_high() >> (rhs - 64)));
    }
    else
    {
        int128 res;
        res.low = __shiftright128(lhs.low, lhs.high, static_cast<unsigned char>(rhs));
        res.high = static_cast<std::uint64_t>(lhs.signed_high() >> rhs);

        return res;
    }

    #else

    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        return lhs;
    }

    if (rhs >= 64)
    {
        return detail::from_bits(lhs.signed_high() < 0 ? ~UINT64_C(0) : UINT64_C(0),
                                 static_cast<std::uint64_t>(lhs.signed_high() >> (rhs - 64)));
    }

    // For shifts < 64
    const auto high_to_low {lhs.high << (64 - rhs)};
    const auto low_shifted {lhs.low >> rhs};
    const auto low_part {high_to_low | low_shifted};

    return detail::from_bits(static_cast<std::uint64_t>(lhs.signed_high() >> rhs), low_part);

    #endif
}

} // namespace detail

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator>>(const int128 lhs, const Integer rhs) noexcept
{
    #ifndef BOOST_INT128_NO_CONSTEVAL_DETECTION

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return detail::default_rs_impl(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        return detail::intrinsic_rs_impl(lhs, rhs);
    }

    #else

    return detail::default_rs_impl(lhs, rhs);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator>>(const int128 lhs, const int128 rhs) noexcept
{
    // Out-of-range counts (negative, >= 128, or with the high word set) are
    // undefined, matching the built-in operators; forward to the scalar overload.
    return lhs >> rhs.low;
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_u128 operator>>(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs >> static_cast<detail::builtin_u128>(rhs.low);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_i128 operator>>(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs >> static_cast<detail::builtin_u128>(rhs.low);
}

#endif

// A shift takes its value and its result type from the left operand after integral promotion,
// and only the count from the right, exactly as the builtin does

BOOST_INT128_EXPORT template <typename Integer, std::enable_if_t<detail::is_any_integer_v<Integer> && (sizeof(Integer) * 8 <= 64), bool> = true>
BOOST_INT128_HOST_DEVICE constexpr detail::promoted_t<Integer> operator>>(const Integer lhs, const int128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return static_cast<detail::promoted_t<Integer>>(lhs) >> rhs.low;
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4804) // Unsafe use of type bool in operation
#endif // _MSC_VER

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator>>=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this >> rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator>>=(const int128 rhs) noexcept
{
    *this = *this >> rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator>>=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this >> rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

#ifdef _MSC_VER
#pragma warning(pop)
#endif // _MSC_VER

//=====================================
// Increment Operators
//=====================================

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator++() noexcept
{
    if (++low == UINT64_C(0))
    {
        ++high;
    }

    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128 int128::operator++(int) noexcept
{
    const auto temp {*this};
    ++(*this);
    return temp;
}

//=====================================
// Decrement Operators
//=====================================

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator--() noexcept
{
    if (low-- == UINT64_C(0))
    {
        --high;
    }

    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128 int128::operator--(int) noexcept
{
    const auto temp {*this};
    --(*this);
    return temp;
}

//=====================================
// Addition Operators
//=====================================

namespace detail {

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 library_add(const int128 lhs, const int128 rhs) noexcept
{
    const auto new_low {lhs.low + rhs.low};
    const auto new_high {lhs.high +
                                        rhs.high +
                                        static_cast<std::uint64_t>(new_low < lhs.low)};

    return detail::from_bits(new_high, new_low);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_add(const int128 lhs, const int128 rhs) noexcept
{
    #if (defined(__x86_64__) || (defined(__aarch64__) && !defined(__APPLE__))) && !defined(_WIN32) && defined(BOOST_INT128_HAS_INT128)

    // Compute in the unsigned domain so that overflow wraps modulo 2^128
    return int128{static_cast<detail::builtin_u128>(lhs) + static_cast<detail::builtin_u128>(rhs)};

    #elif defined(BOOST_INT128_HAS_BUILTIN_ADD_OVERFLOW)

    std::uint64_t result_low {};
    std::uint64_t result_high {};

    result_high = lhs.high + rhs.high + __builtin_add_overflow(lhs.low, rhs.low, &result_low);

    return detail::from_bits(result_high, result_low);

    #elif defined(_M_AMD64) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return library_add(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        int128 result {};
        const auto carry {BOOST_INT128_ADD_CARRY(0, lhs.low, rhs.low, &result.low)};
        BOOST_INT128_ADD_CARRY(carry, lhs.high, rhs.high, &result.high);

        return result;
    }

    #else

    return library_add(lhs, rhs);

    #endif
}

template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_add(const int128 lhs, const Integer rhs) noexcept
{
    const auto new_low {lhs.low + rhs};
    const auto new_high {lhs.high + static_cast<std::uint64_t>(new_low < lhs.low)};

    return detail::from_bits(new_high, new_low);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 library_sub(const int128 lhs, const int128 rhs) noexcept
{
    const auto new_low {lhs.low - rhs.low};
    const auto new_high {lhs.high - rhs.high - static_cast<std::uint64_t>(lhs.low < rhs.low)};

    return detail::from_bits(new_high, new_low);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_sub(const int128 lhs, const int128 rhs) noexcept
{
    #if defined(BOOST_INT128_HAS_BUILTIN_SUB_OVERFLOW) && (!defined(__aarch64__) || defined(__APPLE__) || !defined(BOOST_INT128_HAS_INT128)) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

    // __builtin_sub_overflow is marked constexpr so we don't need if consteval handling
    std::uint64_t result_low {};
    const auto result_high {lhs.high - rhs.high - static_cast<std::uint64_t>(__builtin_sub_overflow(lhs.low, rhs.low, &result_low))};

    return detail::from_bits(result_high, result_low);

    #elif defined(__aarch64__) && !defined(__APPLE__) && defined(BOOST_INT128_HAS_INT128)

    // Unsigned wrap for consistent two's-complement semantics
    return int128{static_cast<detail::builtin_u128>(lhs) - static_cast<detail::builtin_u128>(rhs)};

    #elif defined(_M_AMD64) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return library_sub(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        int128 result {};
        const auto borrow {BOOST_INT128_SUB_BORROW(0, lhs.low, rhs.low, &result.low)};
        BOOST_INT128_SUB_BORROW(borrow, lhs.high, rhs.high, &result.high);

        return result;
    }

    #else

    return library_sub(lhs, rhs);

    #endif
}

template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_sub(const int128 lhs, const Integer rhs) noexcept
{
    const auto new_low {lhs.low - rhs};
    const auto new_high {lhs.high - static_cast<std::uint64_t>(new_low > lhs.low)};
    return detail::from_bits(new_high, new_low);
}

}

// On s390x with multiple different versions of GCC and language standards
// doing addition via subtraction is >10% faster in the benchmarks
#if defined(__s390__) || defined(__s390x__)

BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const int128 lhs, const int128 rhs) noexcept
{
    return detail::default_sub(lhs, -rhs);
}

#else

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const int128 lhs, const int128 rhs) noexcept
{
    return detail::default_add(lhs, rhs);
}

#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return detail::default_add(lhs, rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return detail::default_add(rhs, lhs);
}

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4146) // Unary minus applied to unsigned type
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const int128 lhs, const SignedInteger rhs) noexcept
{
    // Negate in the unsigned domain so INT64_MIN does not overflow (UBSAN)
    return rhs < 0 ? detail::default_sub(lhs, -static_cast<std::uint64_t>(rhs)) :
                     detail::default_add(lhs, static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator+(const SignedInteger lhs, const int128 rhs) noexcept
{
    return lhs < 0 ? detail::default_sub(rhs, -static_cast<std::uint64_t>(lhs)) :
                     detail::default_add(rhs, static_cast<std::uint64_t>(lhs));
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator+(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return detail::default_add(lhs, static_cast<int128>(rhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator+(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return detail::default_add(rhs, static_cast<int128>(lhs));
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator+=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this + rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator+=(const int128 rhs) noexcept
{
    *this = *this + rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator+=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this + rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Subtraction Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator-(const int128 lhs, const int128 rhs) noexcept
{
    return detail::default_sub(lhs, rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator-(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    return detail::default_sub(lhs, rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator-(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    return detail::default_add(-rhs, lhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator-(const int128 lhs, const SignedInteger rhs) noexcept
{
    return detail::default_sub(lhs, static_cast<int128>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator-(const SignedInteger lhs, const int128 rhs) noexcept
{
    return detail::default_sub(static_cast<int128>(lhs), rhs);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator-(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs - static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator-(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) - rhs;
}

#endif

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator-=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this - rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator-=(const int128 rhs) noexcept
{
    *this = *this - rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator-=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this - rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Absolute Value function
//=====================================

// Branch-free two's complement absolute value: (x ^ mask) - mask, where mask is all
// ones for a negative value and zero otherwise. abs(min()) is min(), which matches the
// behavior of the builtin signed integer types.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 abs(const int128 value) noexcept
{
    const auto sign_word {static_cast<std::uint64_t>(value.signed_high() >> 63)};
    const auto mask {detail::from_bits(sign_word, sign_word)};

    return (value ^ mask) - mask;
}

//=====================================
// Multiplication Operators
//=====================================

namespace detail {

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_mul(const int128 lhs, const std::uint64_t rhs) noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !defined(__s390__) && !defined(__s390x__)

    return int128{static_cast<detail::builtin_u128>(lhs) * static_cast<detail::builtin_u128>(rhs)};

    #else

    return low_word_mul<int128>(lhs, rhs);

    #endif
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_mul(const int128 lhs, const std::uint32_t rhs) noexcept
{
    return default_mul(lhs, static_cast<std::uint64_t>(rhs));
}

#if defined(_M_AMD64) && !defined(__GNUC__)

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE int128 msvc_amd64_mul(const int128 lhs, const int128 rhs) noexcept
{
    int128 result {};
    result.low = _umul128(lhs.low, rhs.low, &result.high);
    result.high += lhs.low * rhs.high;
    result.high += lhs.high * rhs.low;

    return result;
}

#endif

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr int128 default_mul(const int128 lhs, const int128 rhs) noexcept
{
    #if ((defined(__aarch64__) && defined(__APPLE__)) || defined(__x86_64__) || defined(__PPC__) || defined(__powerpc__)) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    #  if !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return low_word_mul<int128>(lhs, rhs);
    }
    else
    {
        #  pragma GCC diagnostic push
        #  pragma GCC diagnostic ignored "-Wclass-memaccess"

        detail::builtin_u128 new_lhs {};
        detail::builtin_u128 new_rhs {};

        std::memcpy(&new_lhs, &lhs, sizeof(detail::builtin_u128));
        std::memcpy(&new_rhs, &rhs, sizeof(detail::builtin_u128));

        const auto res {new_lhs * new_rhs};
        int128 library_res {};

        std::memcpy(&library_res, &res, sizeof(detail::builtin_u128));

        return library_res;

        #  pragma GCC diagnostic pop
    }

    #  elif defined(BOOST_INT128_HAS_INT128)

    // Unsigned wrap for consistent two's-complement semantics
    return int128{static_cast<detail::builtin_u128>(lhs) * static_cast<detail::builtin_u128>(rhs)};

    #  else

    return low_word_mul<int128>(lhs, rhs);

    #  endif

    #elif defined(__aarch64__) && defined(BOOST_INT128_HAS_INT128)

    return int128{static_cast<detail::builtin_u128>(lhs) * static_cast<detail::builtin_u128>(rhs)};

    #elif defined(_M_AMD64) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(rhs))
    {
        return low_word_mul<int128>(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        return msvc_amd64_mul(lhs, rhs);
    }

    #elif defined(BOOST_INT128_HAS_INT128) && !defined(__s390__) && !defined(__s390x__)

    // Multiply in the unsigned domain to avoid signed-overflow UB, then reinterpret the bits.
    return int128{static_cast<detail::builtin_u128>(lhs) * static_cast<detail::builtin_u128>(rhs)};

    #else

    return low_word_mul<int128>(lhs, rhs);

    #endif
}

} // namespace detail

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator*(const int128 lhs, const int128 rhs) noexcept
{
    return detail::default_mul(lhs, rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator*(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    using local_eval_type = detail::evaluation_type_t<UnsignedInteger>;
    return detail::default_mul(lhs, static_cast<local_eval_type>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator*(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    using local_eval_type = detail::evaluation_type_t<UnsignedInteger>;
    return detail::default_mul(rhs, static_cast<local_eval_type>(lhs));
}

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4146) // Unary minus applied to unsigned
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator*(const int128 lhs, const SignedInteger rhs) noexcept
{
    return rhs < 0 ? -detail::default_mul(lhs, -static_cast<std::uint64_t>(rhs)) :
                      detail::default_mul(lhs, static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator*(const SignedInteger lhs, const int128 rhs) noexcept
{
    return lhs < 0 ? -detail::default_mul(rhs, -static_cast<std::uint64_t>(lhs)) :
                      detail::default_mul(rhs, static_cast<std::uint64_t>(lhs));
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator*(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return detail::default_mul(lhs, static_cast<int128>(rhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator*(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return detail::default_mul(rhs, static_cast<int128>(lhs));
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator*=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this * rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator*=(const int128 rhs) noexcept
{
    *this = *this * rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator*=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this * rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Division Operator
//=====================================

#if defined(__clang__)
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wassume"
#endif

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator/(const int128 lhs, const int128 rhs) noexcept
{
    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    constexpr int128 min_val {INT64_MIN, 0};
    const auto abs_lhs {abs(lhs)};
    const auto abs_rhs {abs(rhs)};

    if (lhs != min_val && abs_lhs < abs_rhs)
    {
        return {0,0};
    }

    const auto negative_res {(lhs.signed_high() < 0) != (rhs.signed_high() < 0)};

    // Narrow fast path: when the divisor magnitude fits in 64 bits, divide the magnitudes with
    // the hardware-accelerated one_word_div and reapply the sign. This reuses the abs values
    // computed above and beats native signed division (the out-of-line __divti3) for this case.
    if (abs_rhs.high == 0)
    {
        int128 quotient {};

        if (abs_lhs.high == 0)
        {
            quotient = {0, abs_lhs.low / abs_rhs.low};
        }
        else
        {
            detail::one_word_div(abs_lhs, abs_rhs.low, quotient);
        }

        return negative_res ? -quotient : quotient;
    }

    #if defined(BOOST_INT128_HAS_INT128)

    return static_cast<int128>(static_cast<detail::builtin_i128>(lhs) / static_cast<detail::builtin_i128>(rhs));

    #else

    const auto quotient {detail::knuth_div(abs_lhs, abs_rhs)};
    return negative_res ? -quotient : quotient;

    #endif
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator/(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    const auto abs_lhs {abs(lhs)};

    int128 quotient {};
    detail::one_word_div(abs_lhs, static_cast<eval_type>(rhs), quotient);
    return lhs < 0 ? -quotient : quotient;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator/(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    if (rhs.high != 0 && rhs.high != ~UINT64_C(0))
    {
        return {0,0};
    }
    else
    {
        auto abs_rhs {abs(rhs)};
        // rhs == -2^64 has |rhs| greater than any 64-bit lhs, so the quotient is 0 (also avoids /0)
        if (abs_rhs.high != 0)
        {
            return {0, 0};
        }
        const auto res {static_cast<std::uint64_t>(lhs) / abs_rhs.low};
        const int128 result {0, res};
        return rhs < 0 ? -result : result;
    }
}

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4146) // Unary minus applied to unsigned type
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator/(const int128 lhs, const SignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    int128 quotient {};

    constexpr int128 min_val {INT64_MIN, 0};
    const auto negative_res {static_cast<bool>((lhs.signed_high() < 0) ^ (rhs < 0))};
    // Negate in the unsigned domain so INT64_MIN does not overflow (UBSAN)
    const auto abs_rhs {rhs < 0 ? -static_cast<eval_type>(rhs) : static_cast<eval_type>(rhs)};
    const auto abs_lhs {abs(lhs)};

    if (lhs != min_val && abs_lhs < abs_rhs)
    {
        return {0, 0};
    }

    detail::one_word_div(abs_lhs, abs_rhs, quotient);

    return negative_res ? -quotient : quotient;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator/(const SignedInteger lhs, const int128 rhs) noexcept
{
    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    if (rhs.high != 0 && rhs.high != ~UINT64_C(0))
    {
        return {0,0};
    }
    else
    {
        const auto negative_res {static_cast<bool>((rhs.signed_high() < 0) ^ (lhs < 0))};
        const auto abs_rhs {abs(rhs)};
        // rhs == -2^64 has |rhs| greater than any 64-bit lhs, so the quotient is 0 (also avoids /0)
        if (abs_rhs.high != 0)
        {
            return {0, 0};
        }
        // Negate in the unsigned domain so INT64_MIN does not overflow (UBSAN)
        const auto abs_lhs {lhs < 0 ? -static_cast<std::uint64_t>(lhs) : static_cast<std::uint64_t>(lhs)};
        const int128 res {0, abs_lhs / abs_rhs.low};

        return negative_res ? -res : res;
    }
}

#if defined(__clang__)
#  pragma clang diagnostic pop
#endif

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator/(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return static_cast<int128>(static_cast<detail::builtin_i128>(lhs) / rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator/(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs / static_cast<detail::builtin_i128>(rhs));
}

#elif defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE inline int128 operator/(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs / static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE inline int128 operator/(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) / rhs;
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator/=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this / rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator/=(const int128 rhs) noexcept
{
    *this = *this / rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator/=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this / rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

#if defined(__clang__)
#  pragma clang diagnostic pop
#elif defined(__GNUC__)
#  pragma GCC diagnostic pop
#endif

//=====================================
// Modulo Operator
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(int128 lhs, UnsignedInteger rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(UnsignedInteger lhs, int128 rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(int128 lhs, SignedInteger rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(SignedInteger lhs, int128 rhs) noexcept;

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator%(int128 lhs, int128 rhs) noexcept;

template <BOOST_INT128_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(const int128 lhs, const UnsignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    int128 quotient {};
    int128 remainder {};

    const auto abs_lhs {abs(lhs)};

    detail::one_word_div(abs_lhs, static_cast<eval_type>(rhs), quotient, remainder);

    return lhs < 0 ? -remainder : remainder;
}

template <BOOST_INT128_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(const UnsignedInteger lhs, const int128 rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    const auto abs_rhs {abs(rhs)};

    if (abs_rhs > lhs)
    {
        return lhs;
    }

    const int128 remainder {0, static_cast<eval_type>(lhs) % abs_rhs.low};

    return remainder;
}

template <BOOST_INT128_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(const int128 lhs, const SignedInteger rhs) noexcept
{
    return lhs % static_cast<int128>(rhs);
}

template <BOOST_INT128_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128 operator%(const SignedInteger lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) % rhs;
}

BOOST_INT128_HOST_DEVICE constexpr int128 operator%(const int128 lhs, const int128 rhs) noexcept
{
    if (rhs == 0)
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    constexpr int128 min_val {INT64_MIN, 0};
    const auto abs_lhs {abs(lhs)};
    const auto abs_rhs {abs(rhs)};

    if (lhs != min_val && rhs != min_val && abs_rhs > abs_lhs)
    {
        return lhs;
    }

    const auto is_neg {lhs < 0};

    // Narrow fast path: when the divisor magnitude fits in 64 bits, take the remainder of the
    // magnitudes with the hardware-accelerated one_word_div and reapply the dividend's sign.
    if (abs_rhs.high == 0)
    {
        int128 remainder {};

        if (abs_lhs.high == 0)
        {
            remainder = int128{0, abs_lhs.low % abs_rhs.low};
        }
        else
        {
            int128 quotient {};
            detail::one_word_div(abs_lhs, abs_rhs.low, quotient, remainder);
        }

        return is_neg ? -remainder : remainder;
    }

    #if defined(BOOST_INT128_HAS_INT128)

    return static_cast<int128>(static_cast<detail::builtin_i128>(lhs) % static_cast<detail::builtin_i128>(rhs));

    #else

    int128 remainder {};
    detail::knuth_div(abs_lhs, abs_rhs, remainder);
    return is_neg ? -remainder : remainder;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator%(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return static_cast<detail::builtin_i128>(lhs) % rhs;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR int128 operator%(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return lhs % static_cast<detail::builtin_i128>(rhs);
}


#elif defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE inline int128 operator%(const int128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs % static_cast<int128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE inline int128 operator%(const detail::builtin_i128 lhs, const int128 rhs) noexcept
{
    return static_cast<int128>(lhs) % rhs;
}


#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator%=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this % rhs);
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator%=(const int128 rhs) noexcept
{
    *this = *this % rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline int128& int128::operator%=(const Integer rhs) noexcept
{
    *this = static_cast<int128>(*this % rhs);
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Built-in Integer Compound Assignment
//=====================================

// Compound assignment with a built-in integer on the left.
// The builtin applies the operation to the common type of the two operands and converts
// the result back to the type of the left operand, so each of these is the binary operator
// above followed by that conversion, which matches what the builtin 128-bit integer does.
// detail/traits.hpp defines which types Integer may be

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4804) // Unsafe use of type bool in operation
#endif

#define BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(op, compound_op)                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>                                     \
    BOOST_INT128_HOST_DEVICE constexpr Integer& operator compound_op(Integer& lhs, const int128 rhs) noexcept \
    {                                                                                                         \
        lhs = static_cast<Integer>(lhs op rhs);                                                               \
        return lhs;                                                                                           \
    }

BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(|, |=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(&, &=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(^, ^=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(+, +=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(-, -=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(*, *=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(/, /=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(%, %=)

// The shifts take the value from the left operand alone, so only the count comes from rhs
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(<<, <<=)
BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP(>>, >>=)

#undef BOOST_INT128_DETAIL_I128_INTEGER_COMPOUND_OP

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

//=====================================
// Floating Point Operators
//=====================================

// The usual arithmetic conversions convert the integer operand to the floating point type
// before the operation is applied, so each of these computes exactly what the builtin
// 128-bit integer computes for the same expression.
// detail/traits.hpp defines which types Float may be

#ifdef __GNUC__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wfloat-equal"
#endif

#define BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(op, return_type)                                       \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE constexpr return_type operator op(const int128 lhs, const Float rhs) noexcept \
    {                                                                                                    \
        return static_cast<Float>(lhs) op rhs;                                                           \
    }                                                                                                    \
                                                                                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE constexpr return_type operator op(const Float lhs, const int128 rhs) noexcept \
    {                                                                                                    \
        return lhs op static_cast<Float>(rhs);                                                           \
    }

BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(+, Float)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(-, Float)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(*, Float)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(/, Float)

BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(==, bool)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(!=, bool)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(<, bool)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(<=, bool)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(>, bool)
BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(>=, bool)

// Mixing an integer and a floating point type yields a partial ordering because of NaN
#ifdef BOOST_INT128_HAS_SPACESHIP_OPERATOR

BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP(<=>, std::partial_ordering)

#endif // BOOST_INT128_HAS_SPACESHIP_OPERATOR

#undef BOOST_INT128_DETAIL_I128_FLOAT_BINARY_OP

// Compound assignment converts the result back to int128, truncating toward zero.
// A result that is NaN or outside the range of the type saturates as the floating point
// constructor does, rather than being undefined as it is for the builtin

#define BOOST_INT128_DETAIL_I128_FLOAT_COMPOUND_OP(op, compound_op)                                     \
    template <BOOST_INT128_FLOATING_POINT_CONCEPT>                                                       \
    BOOST_INT128_HOST_DEVICE constexpr int128& int128::operator compound_op(const Float rhs) noexcept     \
    {                                                                                                    \
        *this = static_cast<int128>(static_cast<Float>(*this) op rhs);                                    \
        return *this;                                                                                     \
    }                                                                                                    \
                                                                                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE constexpr Float& operator compound_op(Float& lhs, const int128 rhs) noexcept  \
    {                                                                                                    \
        lhs compound_op static_cast<Float>(rhs);                                                          \
        return lhs;                                                                                       \
    }

BOOST_INT128_DETAIL_I128_FLOAT_COMPOUND_OP(+, +=)
BOOST_INT128_DETAIL_I128_FLOAT_COMPOUND_OP(-, -=)
BOOST_INT128_DETAIL_I128_FLOAT_COMPOUND_OP(*, *=)
BOOST_INT128_DETAIL_I128_FLOAT_COMPOUND_OP(/, /=)

#undef BOOST_INT128_DETAIL_I128_FLOAT_COMPOUND_OP

#ifdef __GNUC__
#  pragma GCC diagnostic pop
#endif

// The builtin allows no floating point operand for the modulo, bitwise and shift operators.
// Deleting them keeps that a compile error here, rather than letting the implicit floating
// point constructor silently truncate the operand

#define BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(op)                                                   \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE int128 operator op(int128 lhs, Float rhs) = delete;                          \
                                                                                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE int128 operator op(Float lhs, int128 rhs) = delete;

BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(%)
BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(&)
BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(|)
BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(^)
BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(<<)
BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP(>>)

#undef BOOST_INT128_DETAIL_I128_FLOAT_DELETED_OP

namespace detail {

template <bool>
class numeric_limits_impl_i128
{
public:

        // Member constants
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = true;
    static constexpr bool is_integer = true;
    static constexpr bool is_exact = true;
    static constexpr bool has_infinity = false;
    static constexpr bool has_quiet_NaN = false;
    static constexpr bool has_signaling_NaN = false;

    // C++23 deprecated the following two members
    #if defined(__GNUC__) && __cplusplus > 202002L
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    #elif defined(_MSC_VER)
    #  pragma warning(push)
    #  pragma warning(disable:4996)
    #endif

    static constexpr std::float_denorm_style has_denorm = std::denorm_absent;
    static constexpr bool has_denorm_loss = false;

    #if defined(__GNUC__) && __cplusplus > 202002L
    #  pragma GCC diagnostic pop
    #elif defined(_MSC_VER)
    #  pragma warning(pop)
    #endif

    static constexpr std::float_round_style round_style = std::round_toward_zero;
    static constexpr bool is_iec559 = false;
    static constexpr bool is_bounded = true;
    static constexpr bool is_modulo = true;
    static constexpr int digits = 127;
    static constexpr int digits10 = 38;
    static constexpr int max_digits10 = 0;
    static constexpr int radix = 2;
    static constexpr int min_exponent = 0;
    static constexpr int min_exponent10 = 0;
    static constexpr int max_exponent = 0;
    static constexpr int max_exponent10 = 0;
    static constexpr bool traps = std::numeric_limits<std::uint64_t>::traps;
    static constexpr bool tinyness_before = false;

    // Member functions
    BOOST_INT128_HOST_DEVICE static constexpr auto (min)        () -> boost::int128::int128 { return {INT64_MIN, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto lowest       () -> boost::int128::int128 { return {INT64_MIN, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto (max)        () -> boost::int128::int128 { return {INT64_MAX, UINT64_MAX}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto epsilon      () -> boost::int128::int128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto round_error  () -> boost::int128::int128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto infinity     () -> boost::int128::int128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto quiet_NaN    () -> boost::int128::int128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto signaling_NaN() -> boost::int128::int128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto denorm_min   () -> boost::int128::int128 { return {0, 0}; }
};

#if !defined(__cpp_inline_variables) || __cpp_inline_variables < 201606L

template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_specialized;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_signed;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_integer;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_exact;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::has_infinity;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::has_quiet_NaN;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::has_signaling_NaN;

// These members were deprecated in C++23; suppress the deprecation warning rather
// than dropping the definitions.
#if defined(__GNUC__) && __cplusplus > 202002L
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#  pragma warning(push)
#  pragma warning(disable:4996)
#endif

template <bool b> constexpr std::float_denorm_style numeric_limits_impl_i128<b>::has_denorm;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::has_denorm_loss;

#if defined(__GNUC__) && __cplusplus > 202002L
#  pragma GCC diagnostic pop
#elif defined(_MSC_VER)
#  pragma warning(pop)
#endif

template <bool b> constexpr std::float_round_style numeric_limits_impl_i128<b>::round_style;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_iec559;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_bounded;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::is_modulo;
template <bool b> constexpr int numeric_limits_impl_i128<b>::digits;
template <bool b> constexpr int numeric_limits_impl_i128<b>::digits10;
template <bool b> constexpr int numeric_limits_impl_i128<b>::max_digits10;
template <bool b> constexpr int numeric_limits_impl_i128<b>::radix;
template <bool b> constexpr int numeric_limits_impl_i128<b>::min_exponent;
template <bool b> constexpr int numeric_limits_impl_i128<b>::min_exponent10;
template <bool b> constexpr int numeric_limits_impl_i128<b>::max_exponent;
template <bool b> constexpr int numeric_limits_impl_i128<b>::max_exponent10;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::traps;
template <bool b> constexpr bool numeric_limits_impl_i128<b>::tinyness_before;

#endif // !defined(__cpp_inline_variables) || __cpp_inline_variables < 201606L

} // namespace detail

} // namespace int128
} // namespace boost

namespace std {

#ifdef __clang__
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wmismatched-tags"
#endif

template <>
class numeric_limits<boost::int128::int128> :
    public boost::int128::detail::numeric_limits_impl_i128<true> {};

#ifdef __clang__
#  pragma clang diagnostic pop
#endif

} // namespace std

#endif // BOOST_INT128_DETAIL_INT128_HPP
// ===== END boost/int128/detail/int128_imp.hpp =====
// ===== BEGIN boost/int128/detail/uint128_imp.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_UINT128_IMP_HPP
#define BOOST_INT128_DETAIL_UINT128_IMP_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/fwd.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/traits.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/constants.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/clz.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/common_mul.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/common_div.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/float_conversion.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstdint>
#include <cstring>
#include <climits>

#endif

namespace boost {
namespace int128 {

BOOST_INT128_EXPORT struct
    #if (defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)) && !defined(_M_IX86)
    alignas(alignof(detail::builtin_u128))
    #endif
uint128
{
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE
    std::uint64_t low {};
    std::uint64_t high {};
    #else

    #ifdef __GNUC__
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wreorder"
    #endif

    std::uint64_t high {};
    std::uint64_t low {};

    #ifdef __GNUC__
    #  pragma GCC diagnostic pop
    #endif

    #endif // BOOST_INT128_ENDIAN_LITTLE_BYTE

    // Defaulted basic construction
    constexpr uint128() noexcept = default;
    constexpr uint128(const uint128&) noexcept = default;
    constexpr uint128(uint128&&) noexcept = default;
    constexpr uint128& operator=(const uint128&) noexcept = default;
    constexpr uint128& operator=(uint128&&) noexcept = default;

    // Requires a conversion file to be implemented
    BOOST_INT128_HOST_DEVICE constexpr uint128(const int128& v) noexcept;

    // Construct from integral types
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    BOOST_INT128_HOST_DEVICE constexpr uint128(const std::uint64_t hi, const std::uint64_t lo) noexcept : low {lo}, high {hi} {}

    template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128(const SignedInteger v) noexcept : low {static_cast<std::uint64_t>(v)}, high {v < 0 ? UINT64_MAX : UINT64_C(0)} {}

    template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128(const UnsignedInteger v) noexcept : low {static_cast<std::uint64_t>(v)}, high {} {}

    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128(const detail::builtin_i128 v) noexcept :
        low {static_cast<std::uint64_t>(v)},
        high {static_cast<std::uint64_t>(static_cast<detail::builtin_u128>(v) >> static_cast<detail::builtin_u128>(64U))} {}

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128(const detail::builtin_u128 v) noexcept :
        low {static_cast<std::uint64_t>(v)},
        high {static_cast<std::uint64_t>(v >> static_cast<detail::builtin_i128>(64U))} {}

    #endif // BOOST_INT128_HAS_INT128

    #else // Big endian

    BOOST_INT128_HOST_DEVICE constexpr uint128(const std::uint64_t hi, const std::uint64_t lo) noexcept : high {hi}, low {lo} {}

    template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128(const SignedInteger v) noexcept : high {v < 0 ? UINT64_MAX : UINT64_C(0)}, low {static_cast<std::uint64_t>(v)} {}

    template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128(const UnsignedInteger v) noexcept : high {}, low {static_cast<std::uint64_t>(v)} {}

    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128(const detail::builtin_i128 v) noexcept :
        high {static_cast<std::uint64_t>(static_cast<detail::builtin_u128>(v) >> 64U)},
        low {static_cast<std::uint64_t>(v)} {}

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128(const detail::builtin_u128 v) noexcept :
        high {static_cast<std::uint64_t>(v >> 64U)},
        low {static_cast<std::uint64_t>(v)} {}

    #endif // BOOST_INT128_HAS_INT128

    #endif // BOOST_INT128_ENDIAN_LITTLE_BYTE

    // Construct from floating-point types
    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128(Float f) noexcept;

    // Integer conversion operators
    BOOST_INT128_HOST_DEVICE explicit constexpr operator bool() const noexcept {return low || high; }

    template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr operator SignedInteger() const noexcept { return static_cast<SignedInteger>(low); }

    #ifdef _MSC_VER
    #  pragma warning(push)
    #  pragma warning(disable:4127)
    #endif

    template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr operator UnsignedInteger() const noexcept
    {
        BOOST_INT128_IF_CONSTEXPR (std::is_same<UnsignedInteger, bool>::value)
        {
            return low || high;
        }
        else
        {
            return static_cast<UnsignedInteger>(low);
        }
    }

    #ifdef _MSC_VER
    #  pragma warning(pop)
    #endif

    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR operator detail::builtin_i128() const noexcept { return static_cast<detail::builtin_i128>(static_cast<detail::builtin_u128>(high) << static_cast<detail::builtin_u128>(64)) | static_cast<detail::builtin_i128>(low); }

    BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR operator detail::builtin_u128() const noexcept { return (static_cast<detail::builtin_u128>(high) << static_cast<detail::builtin_u128>(64)) | static_cast<detail::builtin_u128>(low); }

    #endif // BOOST_INT128_HAS_INT128

    // Conversion to float
    // Uses the builtin 128-bit conversion where one exists, and otherwise composes
    // the words as high * 2^64 + low with an exact 2^64 constant, which is the value
    // ldexp(static_cast<T>(high), 64) + static_cast<T>(low) computes
    BOOST_INT128_HOST_DEVICE constexpr operator float() const noexcept;
    BOOST_INT128_HOST_DEVICE constexpr operator double() const noexcept;

    // long doubles do not exist on the CUDA or SYCL (spir64) device
    #if !defined(BOOST_INT128_HAS_GPU_SUPPORT)
    constexpr operator long double() const noexcept;
    #endif

    // Compound OR
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator|=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator|=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator|=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound AND
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator&=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator&=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator&=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound XOR
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator^=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator^=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator^=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Left Shift
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator<<=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator<<=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator<<=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Right Shift
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator>>=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator>>=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator>>=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator++() noexcept;
    BOOST_INT128_HOST_DEVICE constexpr uint128 operator++(int) noexcept;
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator--() noexcept;
    BOOST_INT128_HOST_DEVICE constexpr uint128 operator--(int) noexcept;

    // Compound Addition
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator+=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator+=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator+=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Subtraction
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator-=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator-=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator-=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Multiplication
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator*=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator*=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator*=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound Division
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator/=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator/=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator/=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound modulo
    template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator%=(Integer rhs) noexcept;

    BOOST_INT128_HOST_DEVICE constexpr uint128& operator%=(uint128 rhs) noexcept;

    #ifdef BOOST_INT128_HAS_MSVC_INT128

    template <BOOST_INT128_DEFAULTED_128BIT_INTEGER_CONCEPT>
    BOOST_INT128_HOST_DEVICE inline uint128& operator%=(Integer rhs) noexcept;

    #endif // BOOST_INT128_HAS_MSVC_INT128

    // Compound assignment with floating point types.
    // Matches the builtin: this value is converted to Float, the operation is applied in
    // floating point, and the result is converted back, truncating toward zero.
    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator+=(Float rhs) noexcept;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator-=(Float rhs) noexcept;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator*=(Float rhs) noexcept;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE constexpr uint128& operator/=(Float rhs) noexcept;

    // The builtin does not allow a floating point operand for these, so neither do we.
    // Without these the implicit floating point constructor would silently truncate rhs
    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE uint128& operator%=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE uint128& operator&=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE uint128& operator|=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE uint128& operator^=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE uint128& operator<<=(Float rhs) = delete;

    template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>
    BOOST_INT128_HOST_DEVICE uint128& operator>>=(Float rhs) = delete;
};

//=====================================
// Absolute Value function
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 abs(const uint128 value) noexcept
{
    return value;
}

//=====================================
// Float Conversion Operators
//=====================================

// When the builtin 128-bit type exists we convert through it since the compiler
// runtime (__floatuntisf and friends) is correctly rounded. The portable fallback
// composes the words as high * 2^64 + low; see detail/float_conversion.hpp

BOOST_INT128_HOST_DEVICE constexpr uint128::operator float() const noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !defined(BOOST_INT128_HAS_GPU_SUPPORT)

    return static_cast<float>(static_cast<detail::builtin_u128>(*this));

    #else

    return detail::unsigned_words_to_float<float>(high, low);

    #endif
}

BOOST_INT128_HOST_DEVICE constexpr uint128::operator double() const noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !defined(BOOST_INT128_HAS_GPU_SUPPORT)

    return static_cast<double>(static_cast<detail::builtin_u128>(*this));

    #else

    return detail::unsigned_words_to_float<double>(high, low);

    #endif
}

#if !defined(BOOST_INT128_HAS_GPU_SUPPORT)

constexpr uint128::operator long double() const noexcept
{
    #if defined(BOOST_INT128_HAS_INT128)

    return static_cast<long double>(static_cast<detail::builtin_u128>(*this));

    #else

    return detail::unsigned_words_to_float<long double>(high, low);

    #endif
}

#endif // BOOST_INT128_HAS_GPU_SUPPORT

//=====================================
// Float Construction
//=====================================

// Inverse of operator(Float): decompose f into (high, low) by dividing by 2^64.
// NaN/negative -> 0
// overflow -> UINT128_MAX.
template <BOOST_INT128_FLOATING_POINT_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128::uint128(Float f) noexcept
{
    constexpr Float two_32 {static_cast<Float>(UINT64_C(1) << 32)};
    constexpr Float two_64 {two_32 * two_32};

    // !(f >= 0) catches both NaN and negative values without using <cmath>
    if (!(f >= Float{0}))
    {
        return;
    }

    // Overflow test: f >= 2^128 iff f / 2^64 >= 2^64. Comparing scaled values
    // avoids materializing 2^128 as a Float, which overflows to +infinity for
    // `float` and is therefore not constant-evaluable on older compilers.
    const Float scaled {f / two_64};
    if (scaled >= two_64)
    {
        high = UINT64_MAX;
        low = UINT64_MAX;
        return;
    }

    high = detail::float_to_uint64(scaled);
    const Float remainder {f - static_cast<Float>(high) * two_64};
    low = detail::float_to_uint64(remainder);
}

//=====================================
// Unary Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const uint128 value) noexcept
{
    return value;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const uint128 value) noexcept
{
    return {~value.high + static_cast<std::uint64_t>(value.low == UINT64_C(0)), ~value.low + UINT64_C(1)};
}

//=====================================
// Equality Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator==(const uint128 lhs, const bool rhs) noexcept
{
    return lhs.high == UINT64_C(0) && lhs.low == static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator==(const bool lhs, const uint128 rhs) noexcept
{
    return rhs.high == UINT64_C(0) && rhs.low == static_cast<std::uint64_t>(lhs);
}

#if defined(__clang__)
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wsign-conversion"
#elif defined(__GNUC__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const uint128 lhs, const SignedInteger rhs) noexcept
{
    const uint128 rhs_u {rhs};
    return lhs.high == rhs_u.high && lhs.low == rhs_u.low;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const SignedInteger lhs, const uint128 rhs) noexcept
{
    const uint128 lhs_u {lhs};
    return lhs_u.high == rhs.high && lhs_u.low == rhs.low;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high == UINT64_C(0) && lhs.low == static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return rhs.high == UINT64_C(0) && rhs.low == static_cast<std::uint64_t>(lhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator==(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(__aarch64__) || defined(_M_ARM64) || defined(_M_AMD64)

    return lhs.low == rhs.low && lhs.high == rhs.high;

    #elif defined(__x86_64__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_u128>(lhs) == static_cast<detail::builtin_u128>(rhs);

    #elif (defined(__i386__) || defined(_M_IX86) || defined(_M_AMD64)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__SSE2__)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.low == rhs.low && lhs.high == rhs.high;
    }
    else
    {
        __m128i a = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&lhs));
        __m128i b = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&rhs));
        __m128i cmp = _mm_cmpeq_epi32(a, b);

        return _mm_movemask_ps(_mm_castsi128_ps(cmp)) == 0xF;
    }

    #else

    return lhs.high == rhs.high && lhs.low == rhs.low;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs == static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) == rhs;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs == static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) == rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Inequality Operators
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const uint128 lhs, const bool rhs) noexcept
{
    return lhs.high != UINT64_C(0) || lhs.low != static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const bool lhs, const uint128 rhs) noexcept
{
    return rhs.high != UINT64_C(0) || rhs.low != static_cast<std::uint64_t>(lhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const uint128 lhs, const SignedInteger rhs) noexcept
{
    const uint128 rhs_u {rhs};
    return lhs.high != rhs_u.high || lhs.low != rhs_u.low;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const SignedInteger lhs, const uint128 rhs) noexcept
{
    const uint128 lhs_u {lhs};
    return lhs_u.high != rhs.high || lhs_u.low != rhs.low;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high != UINT64_C(0) || lhs.low != static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return rhs.high != UINT64_C(0) || rhs.low != static_cast<std::uint64_t>(lhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(__aarch64__) || defined(_M_ARM64) || defined(_M_AMD64)

    return lhs.low != rhs.low || lhs.high != rhs.high;

    #elif defined(__x86_64__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_u128>(lhs) != static_cast<detail::builtin_u128>(rhs);

    #elif (defined(__i386__) || defined(_M_IX86)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__SSE2__)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.low != rhs.low || lhs.high != rhs.high;
    }
    else
    {
        __m128i a = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&lhs));
        __m128i b = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&rhs));
        __m128i cmp = _mm_cmpeq_epi32(a, b);

        return _mm_movemask_ps(_mm_castsi128_ps(cmp)) != 0xF;
    }

    #else

    return lhs.high != rhs.high || lhs.low != rhs.low;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_BUILTIN_CONSTEXPR)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs != static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) != rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs != static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) != rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Less than Operators
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const uint128 lhs, const SignedInteger rhs) noexcept
{
    const uint128 rhs_u {rhs};
    return lhs.high == rhs_u.high ? lhs.low < rhs_u.low : lhs.high < rhs_u.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const SignedInteger lhs, const uint128 rhs) noexcept
{
    const uint128 lhs_u {lhs};
    return lhs_u.high == rhs.high ? lhs_u.low < rhs.low : lhs_u.high < rhs.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high == UINT64_C(0) && lhs.low < static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return rhs.high > UINT64_C(0) || static_cast<std::uint64_t>(lhs) < rhs.low;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator<(const uint128 lhs, const uint128 rhs) noexcept
{
    // On ARM macs only with the clang compiler is casting to unsigned __int128 uniformly better (and seemingly cost free)
    #if defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_u128>(lhs) < static_cast<detail::builtin_u128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low < rhs.low : lhs.high < rhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        detail::builtin_u128 builtin_lhs {};
        detail::builtin_u128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs < builtin_rhs;
    }

    #elif (defined(__i386__) || defined(_M_IX86) || defined(__arm__)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low < rhs.low : lhs.high < rhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        std::uint32_t l[4] {};
        std::uint32_t r[4] {};
        std::memcpy(l, &lhs, sizeof(lhs));
        std::memcpy(r, &rhs, sizeof(rhs));

        if (l[3] != r[3])
        {
            return l[3] < r[3];
        }
        else if (l[2] != r[2])
        {
            return l[2] < r[2];
        }
        else if (l[1] != r[1])
        {
            return l[1] < r[1];
        }
        else
        {
            return l[0] < r[0];
        }
    }

    #else

    return lhs.high == rhs.high ? lhs.low < rhs.low : lhs.high < rhs.high;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs < static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) < rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs < static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) < rhs;
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// Less Equal Operators
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const uint128 lhs, const SignedInteger rhs) noexcept
{
    const uint128 rhs_u {rhs};
    return lhs.high == rhs_u.high ? lhs.low <= rhs_u.low : lhs.high < rhs_u.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const SignedInteger lhs, const uint128 rhs) noexcept
{
    const uint128 lhs_u {lhs};
    return lhs_u.high == rhs.high ? lhs_u.low <= rhs.low : lhs_u.high < rhs.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high == UINT64_C(0) && lhs.low <= static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return rhs.high > UINT64_C(0) || static_cast<std::uint64_t>(lhs) <= rhs.low;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_u128>(lhs) <= static_cast<detail::builtin_u128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low <= rhs.low : lhs.high <= rhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        detail::builtin_u128 builtin_lhs {};
        detail::builtin_u128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs <= builtin_rhs;
    }

    #elif (defined(__i386__) || defined(_M_IX86) || defined(__arm__)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? lhs.low <= rhs.low : lhs.high <= rhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        std::uint32_t l[4] {};
        std::uint32_t r[4] {};
        std::memcpy(l, &lhs, sizeof(lhs));
        std::memcpy(r, &rhs, sizeof(rhs));

        if (l[3] != r[3])
        {
            return l[3] < r[3];
        }
        else if (l[2] != r[2])
        {
            return l[2] < r[2];
        }
        else if (l[1] != r[1])
        {
            return l[1] < r[1];
        }
        else
        {
            return l[0] <= r[0];
        }
    }

    #else

    return lhs.high == rhs.high ? lhs.low <= rhs.low : lhs.high <= rhs.high;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs <= static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) <= rhs;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs <= static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) <= rhs;
}


#endif // BOOST_INT128_HAS_INT128

//=====================================
// Greater Than Operators
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const uint128 lhs, const SignedInteger rhs) noexcept
{
    const uint128 rhs_u {rhs};
    return lhs.high == rhs_u.high ? lhs.low > rhs_u.low : lhs.high > rhs_u.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const SignedInteger lhs, const uint128 rhs) noexcept
{
    const uint128 lhs_u {lhs};
    return lhs_u.high == rhs.high ? lhs_u.low > rhs.low : lhs_u.high > rhs.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high > UINT64_C(0) || lhs.low > static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return rhs.high == UINT64_C(0) && static_cast<std::uint64_t>(lhs) > rhs.low;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator>(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_u128>(lhs) > static_cast<detail::builtin_u128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? rhs.low < lhs.low : rhs.high < lhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        detail::builtin_u128 builtin_lhs {};
        detail::builtin_u128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs > builtin_rhs;
    }

    #elif (defined(__i386__) || defined(_M_IX86) || defined(__arm__)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? rhs.low < lhs.low : rhs.high < lhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        std::uint32_t l[4] {};
        std::uint32_t r[4] {};
        std::memcpy(l, &lhs, sizeof(lhs));
        std::memcpy(r, &rhs, sizeof(rhs));

        if (l[3] != r[3])
        {
            return l[3] > r[3];
        }
        else if (l[2] != r[2])
        {
            return l[2] > r[2];
        }
        else if (l[1] != r[1])
        {
            return l[1] > r[1];
        }
        else
        {
            return l[0] > r[0];
        }
    }

    #else

    return lhs.high == rhs.high ? rhs.low < lhs.low : rhs.high < lhs.high;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs > static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) > rhs;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs > static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) > rhs;
}


#endif // BOOST_INT128_HAS_INT128

//=====================================
// Greater-equal Operators
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const uint128 lhs, const SignedInteger rhs) noexcept
{
    const uint128 rhs_u {rhs};
    return lhs.high == rhs_u.high ? lhs.low >= rhs_u.low : lhs.high > rhs_u.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const SignedInteger lhs, const uint128 rhs) noexcept
{
    const uint128 lhs_u {lhs};
    return lhs_u.high == rhs.high ? lhs_u.low >= rhs.low : lhs_u.high > rhs.high;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return lhs.high > UINT64_C(0) || lhs.low >= static_cast<std::uint64_t>(rhs);
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return rhs.high == UINT64_C(0) && static_cast<std::uint64_t>(lhs) >= rhs.low;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<detail::builtin_u128>(lhs) >= static_cast<detail::builtin_u128>(rhs);

    #elif defined(__x86_64__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && defined(__GNUC__) && !defined(__clang__) && defined(BOOST_INT128_HAS_INT128)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? rhs.low <= lhs.low : rhs.high <= lhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        detail::builtin_u128 builtin_lhs {};
        detail::builtin_u128 builtin_rhs {};

        std::memcpy(&builtin_lhs, &lhs, sizeof(builtin_lhs));
        std::memcpy(&builtin_rhs, &rhs, sizeof(builtin_rhs));

        return builtin_lhs >= builtin_rhs;
    }

    #elif (defined(__i386__) || defined(_M_IX86) || defined(__arm__)) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return lhs.high == rhs.high ? rhs.low <= lhs.low : rhs.high <= lhs.high; // LCOV_EXCL_LINE
    }
    else
    {
        std::uint32_t l[4] {};
        std::uint32_t r[4] {};
        std::memcpy(l, &lhs, sizeof(lhs));
        std::memcpy(r, &rhs, sizeof(rhs));

        if (l[3] != r[3])
        {
            return l[3] > r[3];
        }
        else if (l[2] != r[2])
        {
            return l[2] > r[2];
        }
        else if (l[1] != r[1])
        {
            return l[1] > r[1];
        }
        else
        {
            return l[0] >= r[0];
        }
    }

    #else

    return lhs.high == rhs.high ? rhs.low <= lhs.low : rhs.high <= lhs.high;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs >= static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) >= rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs >= static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) >= rhs;
}


#endif // BOOST_INT128_HAS_INT128

//=====================================
// Spaceship Operator
//=====================================

#ifdef BOOST_INT128_HAS_SPACESHIP_OPERATOR

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const uint128 lhs, const uint128 rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const SignedInteger lhs, const uint128 rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const uint128 lhs, const SignedInteger rhs) noexcept
{
    if (lhs < rhs)
    {
        return std::strong_ordering::less;
    }
    else if (lhs == rhs)
    {
        return std::strong_ordering::equivalent;
    }
    else
    {
        return std::strong_ordering::greater;
    }
}

#endif

//=====================================
// Not Operator
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator~(const uint128 rhs) noexcept
{
    return {~rhs.high, ~rhs.low};
}

//=====================================
// OR Operator
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator|(const uint128 lhs, const SignedInteger rhs) noexcept
{
    return {lhs.high | (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), lhs.low | static_cast<std::uint64_t>(rhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator|(const SignedInteger lhs, const uint128 rhs) noexcept
{
    return {rhs.high | (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), rhs.low | static_cast<std::uint64_t>(lhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator|(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return {lhs.high, lhs.low | static_cast<std::uint64_t>(rhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator|(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return {rhs.high, rhs.low | static_cast<std::uint64_t>(lhs)};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator|(const uint128 lhs, const uint128 rhs) noexcept
{
    return {lhs.high | rhs.high, lhs.low | rhs.low};
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator|(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs | static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator|(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) | rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator|(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs | static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator|(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) | rhs;
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator|=(const Integer rhs) noexcept
{
    *this = *this | rhs;
    return *this;
}
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator|=(const uint128 rhs) noexcept
{
    *this = *this | rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator|=(const Integer rhs) noexcept
{
    *this = *this | rhs;
    return *this;
}

#endif

//=====================================
// AND Operator
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator&(const uint128 lhs, const SignedInteger rhs) noexcept
{
    return {lhs.high & (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), lhs.low & static_cast<std::uint64_t>(rhs)};
}

template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator&(const SignedInteger lhs, const uint128 rhs) noexcept
{
    return {rhs.high & (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), rhs.low & static_cast<std::uint64_t>(lhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator&(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return {UINT64_C(0), lhs.low & static_cast<std::uint64_t>(rhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator&(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return {UINT64_C(0), rhs.low & static_cast<std::uint64_t>(lhs)};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator&(const uint128 lhs, const uint128 rhs) noexcept
{
    return {lhs.high & rhs.high, lhs.low & rhs.low};
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator&(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs & static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator&(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) & rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator&(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs & static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator&(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) & rhs;
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator&=(const Integer rhs) noexcept
{
    *this = *this & rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator&=(const uint128 rhs) noexcept
{
    *this = *this & rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator&=(Integer rhs) noexcept
{
    *this = *this & rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128


//=====================================
// XOR Operator
//=====================================

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator^(const uint128 lhs, const SignedInteger rhs) noexcept
{
    return {lhs.high ^ (rhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), lhs.low ^ static_cast<std::uint64_t>(rhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator^(const SignedInteger lhs, const uint128 rhs) noexcept
{
    return {rhs.high ^ (lhs < 0 ? ~UINT64_C(0) : UINT64_C(0)), rhs.low ^ static_cast<std::uint64_t>(lhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator^(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return {lhs.high, lhs.low ^ static_cast<std::uint64_t>(rhs)};
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator^(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return {rhs.high, rhs.low ^ static_cast<std::uint64_t>(lhs)};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator^(const uint128 lhs, const uint128 rhs) noexcept
{
    return {lhs.high ^ rhs.high, lhs.low ^ rhs.low};
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator^(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs ^ static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator^(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) ^ rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator^(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs ^ static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator^(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) ^ rhs;
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator^=(const Integer rhs) noexcept
{
    *this = *this ^ rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator^=(const uint128 rhs) noexcept
{
    *this = *this ^ rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator^=(Integer rhs) noexcept
{
    *this = *this ^ rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Left Shift Operator
//=====================================

namespace detail {

template <typename Integer>
BOOST_INT128_HOST_DEVICE constexpr uint128 default_ls_impl(const uint128 lhs, const Integer rhs) noexcept
{
    static_assert(std::is_integral<Integer>::value, "Needs to be a builtin type");

    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators. In a
    // constant expression the compiler diagnoses it; at runtime it is unspecified.
    if (rhs == 0)
    {
        return lhs;
    }

    if (rhs == 64)
    {
        return {lhs.low, 0};
    }

    if (rhs > 64)
    {
        return {lhs.low << (rhs - 64), 0};
    }

    return {
        (lhs.high << rhs) | (lhs.low >> (64 - rhs)),
        lhs.low << rhs
    };
}

template <typename T>
BOOST_INT128_HOST_DEVICE uint128 intrinsic_ls_impl(const uint128 lhs, const T rhs) noexcept
{
    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators; delegate
    // straight to the native type so we produce identical results.
    #ifdef BOOST_INT128_HAS_INT128

    #  if defined(__aarch64__)

        #if defined(__GNUC__) && __GNUC__ >= 8
        #  pragma GCC diagnostic push
        #  pragma GCC diagnostic ignored "-Wclass-memaccess"
        #endif

        builtin_u128 value;
        std::memcpy(&value, &lhs, sizeof(builtin_u128));
        const auto res {value << rhs};

        uint128 return_value;
        std::memcpy(&return_value, &res, sizeof(uint128));
        return return_value;

        #if defined(__GNUC__) && __GNUC__ >= 8
        #  pragma GCC diagnostic pop
        #endif

    #  else

        return static_cast<builtin_u128>(lhs) << rhs;

    #  endif

    #else

    if (rhs == 0)
    {
        return lhs;
    }

    if (rhs == 64)
    {
        return {lhs.low, 0};
    }

    if (rhs > 64)
    {
        return {lhs.low << (rhs - 64), 0};
    }

    return {
        (lhs.high << rhs) | (lhs.low >> (64 - rhs)),
        lhs.low << rhs
    };

    #endif
}

} // namespace detail

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator<<(const uint128 lhs, const Integer rhs) noexcept
{
    #ifndef BOOST_INT128_NO_CONSTEVAL_DETECTION

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return detail::default_ls_impl(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        return detail::intrinsic_ls_impl(lhs, rhs);
    }

    #else

    return detail::default_ls_impl(lhs, rhs);

    #endif
}

// A number of different overloads to ensure that we return the same type as the builtins would

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator<<(const uint128 lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts (>= 128 or with the high word set) are undefined,
    // matching the built-in operators; forward the count to the scalar overload.
    return lhs << rhs.low;
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_u128 operator<<(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs << static_cast<detail::builtin_u128>(rhs.low);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_i128 operator<<(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs << static_cast<detail::builtin_u128>(rhs.low);
}

#endif

// A shift takes its value and its result type from the left operand after integral promotion,
// and only the count from the right, exactly as the builtin does

BOOST_INT128_EXPORT template <typename Integer, std::enable_if_t<detail::is_any_integer_v<Integer> && (sizeof(Integer) * 8 <= 64), bool> = true>
BOOST_INT128_HOST_DEVICE constexpr detail::promoted_t<Integer> operator<<(const Integer lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return static_cast<detail::promoted_t<Integer>>(lhs) << rhs.low;
}

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator<<=(const Integer rhs) noexcept
{
    *this = *this << rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator<<=(const uint128 rhs) noexcept
{
    *this = *this << rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator<<=(Integer rhs) noexcept
{
    *this = *this << rhs;
    return *this;
}

#endif

//=====================================
// Right Shift Operator
//=====================================

namespace detail {

template <typename Integer>
BOOST_INT128_HOST_DEVICE constexpr uint128 default_rs_impl(const uint128 lhs, const Integer rhs) noexcept
{
    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators. In a
    // constant expression the compiler diagnoses it; at runtime it is unspecified.
    if (rhs == 0)
    {
        return lhs;
    }

    if (rhs == 64)
    {
        return {0, lhs.high};
    }

    if (rhs > 64)
    {
        return {0, lhs.high >> (rhs - 64)};
    }

    return {
        lhs.high >> rhs,
        (lhs.low >> rhs) | (lhs.high << (64 - rhs))
    };
}

template <typename Integer>
BOOST_INT128_HOST_DEVICE uint128 intrinsic_rs_impl(const uint128 lhs, const Integer rhs) noexcept
{
    // A shift by a negative amount or by an amount >= 128 (the operand width) is
    // undefined behavior, exactly as for the built-in shift operators; delegate
    // straight to the native type so we produce identical results.
    #ifdef BOOST_INT128_HAS_INT128

    #  ifdef __aarch64__

        #if defined(__GNUC__) && __GNUC__ >= 8
        #  pragma GCC diagnostic push
        #  pragma GCC diagnostic ignored "-Wclass-memaccess"
        #endif

        builtin_u128 value;
        std::memcpy(&value, &lhs, sizeof(builtin_u128));
        const auto res {value >> rhs};

        uint128 return_value;
        std::memcpy(&return_value, &res, sizeof(uint128));
        return return_value;

        #if defined(__GNUC__) && __GNUC__ >= 8
        #  pragma GCC diagnostic pop
        #endif

    #  else
        return static_cast<builtin_u128>(lhs) >> rhs;
    #  endif

    #else

    if (rhs == 0)
    {
        return lhs;
    }

    if (rhs == 64)
    {
        return {0, lhs.high};
    }

    if (rhs < 64)
    {
        const auto result_low {(lhs.low >> rhs) | (lhs.high << (64 - rhs))};
        const auto result_high {lhs.high >> rhs};
        return {result_high, result_low};
    }

    return {0, lhs.high >> (rhs - 64)};

    #endif
}

} // namespace detail

BOOST_INT128_EXPORT template <typename Integer, std::enable_if_t<std::is_integral<Integer>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator>>(const uint128 lhs, const Integer rhs) noexcept
{
    #ifndef BOOST_INT128_NO_CONSTEVAL_DETECTION

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return detail::default_rs_impl(lhs, rhs); // LCOV_EXCL_LINE
    }
    else
    {
        return detail::intrinsic_rs_impl(lhs, rhs);
    }

    #else

    return detail::default_rs_impl(lhs, rhs);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator>>(const uint128 lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts (>= 128 or with the high word set) are undefined,
    // matching the built-in operators; forward the count to the scalar overload.
    return lhs >> rhs.low;
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_u128 operator>>(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs >> static_cast<detail::builtin_u128>(rhs.low);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR detail::builtin_i128 operator>>(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return lhs >> static_cast<detail::builtin_u128>(rhs.low);
}

#endif

// A shift takes its value and its result type from the left operand after integral promotion,
// and only the count from the right, exactly as the builtin does

BOOST_INT128_EXPORT template <typename Integer, std::enable_if_t<detail::is_any_integer_v<Integer> && (sizeof(Integer) * 8 <= 64), bool> = true>
BOOST_INT128_HOST_DEVICE constexpr detail::promoted_t<Integer> operator>>(const Integer lhs, const uint128 rhs) noexcept
{
    // Out-of-range counts are undefined, matching the built-in operators.
    return static_cast<detail::promoted_t<Integer>>(lhs) >> rhs.low;
}

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator>>=(const Integer rhs) noexcept
{
    *this = *this >> rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator>>=(const uint128 rhs) noexcept
{
    *this = *this >> rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator>>=(Integer rhs) noexcept
{
    *this = *this >> rhs;
    return *this;
}

#endif

//=====================================
// Increment Operator
//=====================================

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator++() noexcept
{
    if (++low == UINT64_C(0))
    {
        ++high;
    }

    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128 uint128::operator++(int) noexcept
{
    const auto temp {*this};
    ++(*this);
    return temp;
}

//=====================================
// Decrement Operator
//=====================================

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator--() noexcept
{
    if (--low == UINT64_MAX)
    {
        --high;
    }

    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128 uint128::operator--(int) noexcept
{
    const auto temp {*this};
    --(*this);
    return temp;
}

//=====================================
// Addition Operator
//=====================================

namespace impl {

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr uint128 default_add(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(BOOST_INT128_HAS_BUILTIN_ADD_OVERFLOW) && (defined(__i386__) || (defined(__aarch64__) && !defined(__APPLE__)) || defined(__arm__) || (defined(__s390__) || defined(__s390x__)))

    uint128 res {};
    res.high = lhs.high + rhs.high + __builtin_add_overflow(lhs.low, rhs.low, &res.low);

    return res;

    #elif (defined(__x86_64__) || (defined(__aarch64__) && !defined(__APPLE__))) && !defined(_MSC_VER) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<uint128>(static_cast<detail::builtin_u128>(lhs) + static_cast<detail::builtin_u128>(rhs));

    #else

    uint128 temp {lhs.high + rhs.high, lhs.low + rhs.low};

    if (temp.low < lhs.low)
    {
        ++temp.high;
    }

    return temp;

    #endif
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr uint128 default_add(const uint128 lhs, const std::uint64_t rhs) noexcept
{
    #if defined(BOOST_INT128_HAS_BUILTIN_ADD_OVERFLOW) && (defined(__i386__) || (defined(__aarch64__) && !defined(__APPLE__)) || defined(__arm__) || (defined(__s390__) || defined(__s390x__)))

    uint128 res {};
    res.high = lhs.high + __builtin_add_overflow(lhs.low, rhs, &res.low);

    return res;

    #else

    uint128 temp {lhs.high, lhs.low + rhs};

    if (temp.low < lhs.low)
    {
        ++temp.high;
    }

    return temp;

    #endif
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr uint128 default_sub(const uint128 lhs, const uint128 rhs) noexcept
{
    #if defined(BOOST_INT128_HAS_BUILTIN_SUB_OVERFLOW) && (defined(__i386__) || defined(__arm__) || (defined(__s390__) || defined(__s390x__)))

    uint128 res {};
    res.high = lhs.high - rhs.high - __builtin_sub_overflow(lhs.low, rhs.low, &res.low);

    return res;

    #elif (defined(__x86_64__) || (defined(__aarch64__) && !defined(__APPLE__))) && !defined(_MSC_VER) && defined(BOOST_INT128_HAS_INT128)

    return static_cast<uint128>(static_cast<detail::builtin_u128>(lhs) - static_cast<detail::builtin_u128>(rhs));

    #else

    uint128 temp {lhs.high - rhs.high, lhs.low - rhs.low};

    // Check for carry
    if (lhs.low < rhs.low)
    {
        --temp.high;
    }

    return temp;

    #endif
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr uint128 default_sub(const uint128 lhs, const std::uint64_t rhs) noexcept
{
    #if defined(BOOST_INT128_HAS_BUILTIN_SUB_OVERFLOW) && (defined(__i386__) || (defined(__aarch64__) && !defined(__APPLE__)) || defined(__arm__) || (defined(__s390__) || defined(__s390x__)))

    uint128 res {};
    res.high = lhs.high - __builtin_sub_overflow(lhs.low, rhs, &res.low);

    return res;

    #else

    uint128 temp {lhs.high, lhs.low - rhs};

    // Check for carry
    if (lhs.low < rhs)
    {
        --temp.high;
    }

    return temp;

    #endif
}

} // namespace impl

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4146)
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const uint128 lhs, const SignedInteger rhs) noexcept
{
    return rhs < 0 ? impl::default_sub(lhs, -static_cast<std::uint64_t>(rhs)) :
                     impl::default_add(lhs, static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const SignedInteger lhs, const uint128 rhs) noexcept
{
    return lhs < 0 ? impl::default_sub(rhs, -static_cast<std::uint64_t>(lhs)) :
                     impl::default_add(rhs, static_cast<std::uint64_t>(lhs));
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return impl::default_add(lhs, static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return impl::default_add(rhs, static_cast<std::uint64_t>(lhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const uint128 lhs, const uint128 rhs) noexcept
{
    return impl::default_add(lhs, rhs);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator+(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return impl::default_add(lhs, static_cast<uint128>(rhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator+(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return impl::default_add(static_cast<uint128>(lhs), rhs);
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator+(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return impl::default_add(lhs, static_cast<uint128>(rhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator+(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return impl::default_add(static_cast<uint128>(lhs), rhs);
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator+=(const Integer rhs) noexcept
{
    *this = *this + rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator+=(const uint128 rhs) noexcept
{
    *this = *this + rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator+=(const Integer rhs) noexcept
{
    *this = *this + rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128


//=====================================
// Subtraction Operator
//=====================================

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4146)
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const uint128 lhs, const SignedInteger rhs) noexcept
{
    return rhs < 0 ? impl::default_add(lhs, -static_cast<std::uint64_t>(rhs)) :
                     impl::default_sub(lhs, static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const SignedInteger lhs, const uint128 rhs) noexcept
{
    return lhs < 0 ? impl::default_sub(-rhs, -static_cast<std::uint64_t>(lhs)) :
                     impl::default_add(-rhs, static_cast<std::uint64_t>(lhs));
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return impl::default_sub(lhs, static_cast<std::uint64_t>(rhs));
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return impl::default_add(-rhs, static_cast<std::uint64_t>(lhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const uint128 lhs, const uint128 rhs) noexcept
{
    return impl::default_sub(lhs, rhs);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator-(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs - static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator-(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) - rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator-(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs - static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator-(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) - rhs;
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator-=(const Integer rhs) noexcept
{
    *this = *this - rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator-=(const uint128 rhs) noexcept
{
    *this = *this - rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator-=(const Integer rhs) noexcept
{
    *this = *this - rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Multiplication Operator
//=====================================

#if defined(__GNUC__) && __GNUC__ >= 8
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif

namespace detail {

#if defined(_M_AMD64) && !defined(__GNUC__)

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE uint128 msvc_mul(const uint128 lhs, const uint128 rhs) noexcept
{
    uint128 result {};
    result.low = _umul128(lhs.low, rhs.low, &result.high);
    result.high += lhs.low * rhs.high;
    result.high += lhs.high * rhs.low;

    return result;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE uint128 msvc_mul(const uint128 lhs, const std::uint64_t rhs) noexcept
{
    uint128 result {};
    result.low = _umul128(lhs.low, rhs, &result.high);
    result.high += lhs.high * rhs;

    return result;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE uint128 msvc_mul(const uint128 lhs, const std::uint32_t rhs) noexcept
{
    uint128 result {};
    result.low = _umul128(lhs.low, static_cast<std::uint64_t>(rhs), &result.high);
    result.high += lhs.high * static_cast<std::uint64_t>(rhs);

    return result;
}

#elif defined(_M_ARM64) && !defined(__GNUC__)

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE uint128 msvc_mul(const uint128 lhs, const uint128 rhs) noexcept
{
    const auto low_low{lhs.low * rhs.low};
    const auto high_low_low{__umulh(lhs.low, rhs.low)};

    const auto low_high{lhs.low * rhs.high};
    const auto high_low{lhs.high * rhs.low};

    const auto high{high_low + low_high + high_low_low};

    return {high, low_low};
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE uint128 msvc_mul(const uint128 lhs, const std::uint64_t rhs) noexcept
{
    const auto low{lhs.low * rhs};
    const auto high{__umulh(lhs.low, rhs) + (lhs.high * rhs)};

    return {high, low};
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE uint128 msvc_mul(const uint128 lhs, const std::uint32_t rhs) noexcept
{
    const auto low{lhs.low * rhs};
    const auto high{__umulh(lhs.low, static_cast<std::uint64_t>(rhs)) + (lhs.high * rhs)};

    return {high, low};
}

#endif // MSVC implementations

template <typename UnsignedInteger>
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr uint128 default_mul(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    #if (defined(__aarch64__) || defined(__x86_64__) || defined(__PPC__) || defined(__powerpc__)) && defined(__GNUC__) && defined(BOOST_INT128_HAS_INT128)

    #  if !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (!BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        detail::builtin_u128 new_lhs {};
        detail::builtin_u128 new_rhs {};

        std::memcpy(&new_lhs, &lhs, sizeof(uint128));
        std::memcpy(&new_rhs, &rhs, sizeof(UnsignedInteger));

        const auto res {new_lhs * new_rhs};

        uint128 library_res {};

        std::memcpy(&library_res, &res, sizeof(uint128));

        return library_res;
    }

    #  elif defined(BOOST_INT128_HAS_INT128)
    #    define BOOST_INT128_HIDE_MUL

        return static_cast<uint128>(static_cast<detail::builtin_u128>(lhs) * static_cast<detail::builtin_u128>(rhs));

    #  endif

    // s390x intentionally falls through to the synthetic low_word_mul below. Casting to builtin_u128
    // makes GCC reconstruct the value through a vector-unit stack round-trip that is several times
    // slower, and the memcpy path is unsafe for the narrow (scalar rhs) overloads on big-endian.
    #elif (defined(_M_AMD64) || defined(_M_ARM64)) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (!BOOST_INT128_IS_CONSTANT_EVALUATED(lhs))
    {
        return msvc_mul(lhs, rhs);
    }

    #elif defined(BOOST_INT128_HAS_INT128) && !defined(__s390__) && !defined(__s390x__)
    #  define BOOST_INT128_HIDE_MUL

    return static_cast<uint128>(static_cast<detail::builtin_u128>(lhs) * static_cast<detail::builtin_u128>(rhs));

    #endif

    // We need to hide this if we use a non-const eval method above to avoid a litany of cross-platform warnings
    #ifndef BOOST_INT128_HIDE_MUL

    return low_word_mul<uint128>(lhs, rhs);

    #else
    #undef BOOST_INT128_HIDE_MUL
    #endif //BOOST_INT128_HIDE_MUL
}

} // namespace detail

#if defined(__GNUC__) && __GNUC__ >= 8
#  pragma GCC diagnostic pop
#endif

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4146)
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator*(const uint128 lhs, const SignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;

    const auto abs_rhs {rhs < 0 ? -static_cast<eval_type>(rhs) : static_cast<eval_type>(rhs)};
    const auto res {detail::default_mul(lhs, abs_rhs)};

    return rhs < 0 ? -res : res;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator*(const SignedInteger lhs, const uint128 rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;

    const auto abs_lhs {lhs < 0 ? -static_cast<eval_type>(lhs) : static_cast<eval_type>(lhs)};
    const auto res {detail::default_mul(rhs, abs_lhs)};

    return lhs < 0 ? -res : res;
}

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator*(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    return detail::default_mul(lhs, static_cast<std::uint64_t>(rhs));
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator*(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    return detail::default_mul(rhs, static_cast<std::uint64_t>(lhs));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator*(const uint128 lhs, const uint128 rhs) noexcept
{
    return detail::default_mul(lhs, rhs);
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator*(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    const detail::builtin_u128 rhs_bits {static_cast<detail::builtin_u128>(rhs)};
    const bool rhs_negative {static_cast<std::int64_t>(static_cast<std::uint64_t>(rhs_bits >> static_cast<detail::builtin_u128>(64U))) < 0};
    const uint128 rhs_u {rhs_bits};
    const uint128 abs_rhs {rhs_negative ? -rhs_u : rhs_u};
    const uint128 res {lhs * abs_rhs};

    return rhs_negative ? -res : res;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator*(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    const detail::builtin_u128 lhs_bits {static_cast<detail::builtin_u128>(lhs)};
    const bool lhs_negative {static_cast<std::int64_t>(static_cast<std::uint64_t>(lhs_bits >> static_cast<detail::builtin_u128>(64U))) < 0};
    const uint128 lhs_u {lhs_bits};
    const uint128 abs_lhs {lhs_negative ? -lhs_u : lhs_u};
    const uint128 res {abs_lhs * rhs};

    return lhs_negative ? -res : res;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator*(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs * static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator*(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) * rhs;
}

#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator*=(const Integer rhs) noexcept
{
    *this = *this * rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator*=(const uint128 rhs) noexcept
{
    *this = *this * rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator*=(const Integer rhs) noexcept
{
    *this = *this * rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Division Operator
//=====================================

// For div we need forward declarations since we mix and match the arguments
BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(uint128 lhs, SignedInteger rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(SignedInteger lhs, uint128 rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(uint128 lhs, UnsignedInteger rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(UnsignedInteger lhs, uint128 rhs) noexcept;

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(uint128 lhs, uint128 rhs) noexcept;

template <BOOST_INT128_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(const uint128 lhs, const SignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;
    return rhs < 0 ? lhs / static_cast<uint128>(rhs) : lhs / static_cast<eval_type>(rhs);
}

template <BOOST_INT128_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(const SignedInteger lhs, const uint128 rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;
    return lhs < 0 ? static_cast<uint128>(lhs) / rhs : static_cast<eval_type>(lhs) / rhs;
}

template <BOOST_INT128_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0U))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    if (lhs < rhs)
    {
        return {0, 0};
    }

    uint128 quotient {};

    detail::one_word_div(lhs, static_cast<eval_type>(rhs), quotient);

    return quotient;
}

template <BOOST_INT128_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0U))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    if (lhs < rhs)
    {
        return {0, 0};
    }

    return {0, static_cast<eval_type>(lhs) / rhs.low};
}

BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(const uint128 lhs, const uint128 rhs) noexcept
{
    if (BOOST_INT128_UNLIKELY(rhs == 0U))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    if (lhs < rhs)
    {
        return {0, 0};
    }

    // A divisor that fits in 64 bits is handled by the hardware-accelerated narrow path. This
    // beats the native 128/128 divide for this common case on every platform (it avoids the
    // out-of-line __udivti3 call on GCC/Clang and uses divq / _udiv128 directly where present).
    if (rhs.high == 0U)
    {
        if (lhs.high == 0U)
        {
            return {0, lhs.low / rhs.low};
        }

        uint128 quotient {};
        detail::one_word_div(lhs, rhs.low, quotient);
        return quotient;
    }

    #if defined(BOOST_INT128_HAS_INT128) && !defined(__s390__) && !defined(__s390x__)

    return static_cast<uint128>(static_cast<detail::builtin_u128>(lhs) / static_cast<detail::builtin_u128>(rhs));

    #else

    return detail::knuth_div(lhs, rhs);

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator/(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs / static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator/(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) / rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator/(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs / static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator/(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) / rhs;
}


#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator/=(const Integer rhs) noexcept
{
    *this = *this / rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator/=(const uint128 rhs) noexcept
{
    *this = *this / rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator/=(const Integer rhs) noexcept
{
    *this = *this / rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

#if defined(__clang__)
#  pragma clang diagnostic pop
#elif defined(__GNUC__)
#  pragma GCC diagnostic pop
#endif

//=====================================
// Modulo Operator
//=====================================

// For div we need forward declarations since we mix and match the arguments
BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(uint128 lhs, SignedInteger rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(SignedInteger lhs, uint128 rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(uint128 lhs, UnsignedInteger rhs) noexcept;

BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(UnsignedInteger lhs, uint128 rhs) noexcept;

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(uint128 lhs, uint128 rhs) noexcept;

template <BOOST_INT128_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(const uint128 lhs, const SignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;
    return rhs < 0 ? lhs % static_cast<uint128>(rhs) : lhs % static_cast<eval_type>(rhs);
}

template <BOOST_INT128_SIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(const SignedInteger lhs, const uint128 rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<SignedInteger>;
    return lhs < 0 ? static_cast<uint128>(lhs) % rhs : static_cast<eval_type>(lhs) % rhs;
}

template <BOOST_INT128_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(const uint128 lhs, const UnsignedInteger rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0U))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }

    if (lhs.high != 0)
    {
        uint128 quotient {};
        uint128 remainder {};

        detail::one_word_div(lhs, static_cast<eval_type>(rhs), quotient, remainder);

        return remainder;
    }
    else
    {
        return {0, lhs.low % rhs};
    }
}

template <BOOST_INT128_UNSIGNED_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(const UnsignedInteger lhs, const uint128 rhs) noexcept
{
    using eval_type = detail::evaluation_type_t<UnsignedInteger>;

    if (BOOST_INT128_UNLIKELY(rhs == 0U))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }
    else if (rhs > lhs)
    {
        return lhs;
    }

    return {0, static_cast<eval_type>(lhs) % rhs.low};
}

BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(const uint128 lhs, const uint128 rhs) noexcept
{
    if (BOOST_INT128_UNLIKELY(rhs == 0U))
    {
        // Division or remainder by zero is undefined behavior for the builtin __int128 types (a hardware trap). We match that: marking it unreachable keeps codegen branch-free and vectorizable.
        BOOST_INT128_UNREACHABLE;
    }
    if (rhs > lhs)
    {
        return lhs;
    }

    // A divisor that fits in 64 bits is handled by the hardware-accelerated narrow path, which
    // beats the native 128/128 divide for this common case on every platform.
    if (rhs.high == 0U)
    {
        if (lhs.high == 0U)
        {
            return {0, lhs.low % rhs.low};
        }

        uint128 quotient {};
        uint128 remainder {};
        detail::one_word_div(lhs, rhs.low, quotient, remainder);
        return remainder;
    }

    #if defined(BOOST_INT128_HAS_INT128) && !defined(__s390__) && !defined(__s390x__)

    return static_cast<uint128>(static_cast<detail::builtin_u128>(lhs) % static_cast<detail::builtin_u128>(rhs));

    #else

    uint128 remainder {};
    detail::knuth_div(lhs, rhs, remainder);
    return remainder;

    #endif
}

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator%(const uint128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return lhs % static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator%(const detail::builtin_u128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) % rhs;
}


BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator%(const uint128 lhs, const detail::builtin_i128 rhs) noexcept
{
    return lhs % static_cast<uint128>(rhs);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator%(const detail::builtin_i128 lhs, const uint128 rhs) noexcept
{
    return static_cast<uint128>(lhs) % rhs;
}


#endif // BOOST_INT128_HAS_INT128

template <BOOST_INT128_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator%=(const Integer rhs) noexcept
{
    *this = *this % rhs;
    return *this;
}

BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator%=(const uint128 rhs) noexcept
{
    *this = *this % rhs;
    return *this;
}

#ifdef BOOST_INT128_HAS_MSVC_INT128

template <BOOST_INT128_128BIT_INTEGER_CONCEPT>
BOOST_INT128_HOST_DEVICE inline uint128& uint128::operator%=(const Integer rhs) noexcept
{
    * this = *this % rhs;
    return *this;
}

#endif // BOOST_INT128_HAS_MSVC_INT128

//=====================================
// Built-in Integer Compound Assignment
//=====================================

// Compound assignment with a built-in integer on the left.
// The builtin applies the operation to the common type of the two operands and converts
// the result back to the type of the left operand, so each of these is the binary operator
// above followed by that conversion, which matches what the builtin 128-bit integer does.
// detail/traits.hpp defines which types Integer may be

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4804) // Unsafe use of type bool in operation
#endif

#define BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(op, compound_op)                                          \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_INTEGER_CONCEPT>                                      \
    BOOST_INT128_HOST_DEVICE constexpr Integer& operator compound_op(Integer& lhs, const uint128 rhs) noexcept \
    {                                                                                                          \
        lhs = static_cast<Integer>(lhs op rhs);                                                                \
        return lhs;                                                                                            \
    }

BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(|, |=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(&, &=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(^, ^=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(+, +=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(-, -=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(*, *=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(/, /=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(%, %=)

// The shifts take the value from the left operand alone, so only the count comes from rhs
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(<<, <<=)
BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP(>>, >>=)

#undef BOOST_INT128_DETAIL_U128_INTEGER_COMPOUND_OP

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

//=====================================
// Floating Point Operators
//=====================================

// The usual arithmetic conversions convert the integer operand to the floating point type
// before the operation is applied, so each of these computes exactly what the builtin
// 128-bit integer computes for the same expression.
// detail/traits.hpp defines which types Float may be

#ifdef __GNUC__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wfloat-equal"
#elif defined(__clang__)
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wfloat-equal"
#endif

#define BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(op, return_type)                                       \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE constexpr return_type operator op(const uint128 lhs, const Float rhs) noexcept \
    {                                                                                                    \
        return static_cast<Float>(lhs) op rhs;                                                           \
    }                                                                                                    \
                                                                                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE constexpr return_type operator op(const Float lhs, const uint128 rhs) noexcept \
    {                                                                                                    \
        return lhs op static_cast<Float>(rhs);                                                           \
    }

BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(+, Float)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(-, Float)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(*, Float)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(/, Float)

BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(==, bool)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(!=, bool)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(<, bool)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(<=, bool)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(>, bool)
BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(>=, bool)

// Mixing an integer and a floating point type yields a partial ordering because of NaN
#ifdef BOOST_INT128_HAS_SPACESHIP_OPERATOR

BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP(<=>, std::partial_ordering)

#endif // BOOST_INT128_HAS_SPACESHIP_OPERATOR

#undef BOOST_INT128_DETAIL_U128_FLOAT_BINARY_OP

// Compound assignment converts the result back to uint128, truncating toward zero.
// A result that is NaN or outside the range of the type saturates as the floating point
// constructor does, rather than being undefined as it is for the builtin

#define BOOST_INT128_DETAIL_U128_FLOAT_COMPOUND_OP(op, compound_op)                                     \
    template <BOOST_INT128_FLOATING_POINT_CONCEPT>                                                       \
    BOOST_INT128_HOST_DEVICE constexpr uint128& uint128::operator compound_op(const Float rhs) noexcept   \
    {                                                                                                    \
        *this = static_cast<uint128>(static_cast<Float>(*this) op rhs);                                   \
        return *this;                                                                                     \
    }                                                                                                    \
                                                                                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE constexpr Float& operator compound_op(Float& lhs, const uint128 rhs) noexcept \
    {                                                                                                    \
        lhs compound_op static_cast<Float>(rhs);                                                          \
        return lhs;                                                                                       \
    }

BOOST_INT128_DETAIL_U128_FLOAT_COMPOUND_OP(+, +=)
BOOST_INT128_DETAIL_U128_FLOAT_COMPOUND_OP(-, -=)
BOOST_INT128_DETAIL_U128_FLOAT_COMPOUND_OP(*, *=)
BOOST_INT128_DETAIL_U128_FLOAT_COMPOUND_OP(/, /=)

#undef BOOST_INT128_DETAIL_U128_FLOAT_COMPOUND_OP

#ifdef __GNUC__
#  pragma GCC diagnostic pop
#elif defined(__clang__)
#  pragma clang diagnostic pop
#endif

// The builtin allows no floating point operand for the modulo, bitwise and shift operators.
// Deleting them keeps that a compile error here, rather than letting the implicit floating
// point constructor silently truncate the operand

#define BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(op)                                                   \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE uint128 operator op(uint128 lhs, Float rhs) = delete;                        \
                                                                                                         \
    BOOST_INT128_EXPORT template <BOOST_INT128_DEFAULTED_FLOATING_POINT_CONCEPT>                         \
    BOOST_INT128_HOST_DEVICE uint128 operator op(Float lhs, uint128 rhs) = delete;

BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(%)
BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(&)
BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(|)
BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(^)
BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(<<)
BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP(>>)

#undef BOOST_INT128_DETAIL_U128_FLOAT_DELETED_OP

namespace detail {

template <bool>
class numeric_limits_impl_u128
{
public:

        // Member constants
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = false;
    static constexpr bool is_integer = true;
    static constexpr bool is_exact = true;
    static constexpr bool has_infinity = false;
    static constexpr bool has_quiet_NaN = false;
    static constexpr bool has_signaling_NaN = false;

    // C++23 deprecated the following two members
    #if defined(__GNUC__) && __cplusplus > 202002L
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    #elif defined(_MSC_VER)
    #  pragma warning(push)
    #  pragma warning(disable:4996)
    #endif

    static constexpr std::float_denorm_style has_denorm = std::denorm_absent;
    static constexpr bool has_denorm_loss = false;

    #if defined(__GNUC__) && __cplusplus > 202002L
    #  pragma GCC diagnostic pop
    #elif defined(_MSC_VER)
    #  pragma warning(pop)
    #endif

    static constexpr std::float_round_style round_style = std::round_toward_zero;
    static constexpr bool is_iec559 = false;
    static constexpr bool is_bounded = true;
    static constexpr bool is_modulo = true;
    static constexpr int digits = 128;
    static constexpr int digits10 = 38;
    static constexpr int max_digits10 = 0;
    static constexpr int radix = 2;
    static constexpr int min_exponent = 0;
    static constexpr int min_exponent10 = 0;
    static constexpr int max_exponent = 0;
    static constexpr int max_exponent10 = 0;
    static constexpr bool traps = std::numeric_limits<std::uint64_t>::traps;
    static constexpr bool tinyness_before = false;

    // Member functions
    BOOST_INT128_HOST_DEVICE static constexpr auto (min)        () -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto lowest       () -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto (max)        () -> boost::int128::uint128 { return {UINT64_MAX, UINT64_MAX}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto epsilon      () -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto round_error  () -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto infinity     () -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto quiet_NaN    () -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto signaling_NaN() -> boost::int128::uint128 { return {0, 0}; }
    BOOST_INT128_HOST_DEVICE static constexpr auto denorm_min   () -> boost::int128::uint128 { return {0, 0}; }
};

#if !defined(__cpp_inline_variables) || __cpp_inline_variables < 201606L

template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_specialized;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_signed;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_integer;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_exact;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::has_infinity;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::has_quiet_NaN;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::has_signaling_NaN;

// These members were deprecated in C++23; suppress the deprecation warning rather
// than dropping the definitions.
#if defined(__GNUC__) && __cplusplus > 202002L
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#  pragma warning(push)
#  pragma warning(disable:4996)
#endif

template <bool b> constexpr std::float_denorm_style numeric_limits_impl_u128<b>::has_denorm;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::has_denorm_loss;

#if defined(__GNUC__) && __cplusplus > 202002L
#  pragma GCC diagnostic pop
#elif defined(_MSC_VER)
#  pragma warning(pop)
#endif

template <bool b> constexpr std::float_round_style numeric_limits_impl_u128<b>::round_style;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_iec559;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_bounded;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::is_modulo;
template <bool b> constexpr int numeric_limits_impl_u128<b>::digits;
template <bool b> constexpr int numeric_limits_impl_u128<b>::digits10;
template <bool b> constexpr int numeric_limits_impl_u128<b>::max_digits10;
template <bool b> constexpr int numeric_limits_impl_u128<b>::radix;
template <bool b> constexpr int numeric_limits_impl_u128<b>::min_exponent;
template <bool b> constexpr int numeric_limits_impl_u128<b>::min_exponent10;
template <bool b> constexpr int numeric_limits_impl_u128<b>::max_exponent;
template <bool b> constexpr int numeric_limits_impl_u128<b>::max_exponent10;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::traps;
template <bool b> constexpr bool numeric_limits_impl_u128<b>::tinyness_before;

#endif // !defined(__cpp_inline_variables) || __cpp_inline_variables < 201606L


} // namespace detail

} // namespace int128
} // namespace boost

namespace std {

#ifdef __clang__
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wmismatched-tags"
#endif

template <>
class numeric_limits<boost::int128::uint128> :
    public boost::int128::detail::numeric_limits_impl_u128<true> {};

#ifdef __clang__
#  pragma clang diagnostic pop
#endif

} // namespace std

#endif //BOOST_INT128_DETAIL_UINT128_IMP_HPP
// ===== END boost/int128/detail/uint128_imp.hpp =====
// ===== BEGIN boost/int128/detail/conversions.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_CONVERSIONS_HPP
#define BOOST_INT128_CONVERSIONS_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/int128_imp.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/uint128_imp.hpp

namespace boost {
namespace int128 {

namespace detail {

template <typename T>
struct valid_overload
{
    static constexpr bool value = std::is_same<T, uint128>::value || std::is_same<T, int128>::value;
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_valid_overload_v = valid_overload<T>::value;

} // namespace detail

#if BOOST_INT128_ENDIAN_LITTLE_BYTE

BOOST_INT128_HOST_DEVICE constexpr int128::int128(const uint128& v) noexcept : low {v.low}, high {v.high} {}

BOOST_INT128_HOST_DEVICE constexpr uint128::uint128(const int128& v) noexcept : low {v.low}, high {v.high} {}

#else

BOOST_INT128_HOST_DEVICE constexpr int128::int128(const uint128& v) noexcept : high {v.high}, low {v.low} {}

BOOST_INT128_HOST_DEVICE constexpr uint128::uint128(const int128& v) noexcept : high {v.high}, low {v.low} {}

#endif // BOOST_INT128_ENDIAN_LITTLE_BYTE

//=====================================
// Comparison Operators
//=====================================

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4127)
#endif

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) == static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) != static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool operator<(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) < static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool operator<=(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) <= static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool operator>(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) > static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool operator>=(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) >= static_cast<uint128>(rhs);
}

#ifdef BOOST_INT128_HAS_SPACESHIP_OPERATOR

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) <=> static_cast<uint128>(rhs);
}

#endif

//=====================================
// Arithmetic Operators
//=====================================

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator+(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) + static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator-(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) - static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator*(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) * static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator/(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) / static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator%(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) % static_cast<uint128>(rhs);
}

//=====================================
// Cross-type Bitwise Operators
//=====================================

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator|(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) | static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator&(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) & static_cast<uint128>(rhs);
}

template <typename T, typename U, std::enable_if_t<detail::is_valid_overload_v<T> && detail::is_valid_overload_v<U> && !std::is_same<T, U>::value, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr uint128 operator^(const T lhs, const U rhs) noexcept
{
    return static_cast<uint128>(lhs) ^ static_cast<uint128>(rhs);
}

//=====================================
// Cross-type Shift Operators
//=====================================

BOOST_INT128_HOST_DEVICE constexpr int128 operator<<(const int128 lhs, const uint128 rhs) noexcept
{
    return lhs << static_cast<int128>(rhs);
}

BOOST_INT128_HOST_DEVICE constexpr uint128 operator<<(const uint128 lhs, const int128 rhs) noexcept
{
    return lhs << static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE constexpr int128 operator>>(const int128 lhs, const uint128 rhs) noexcept
{
    return lhs >> static_cast<int128>(rhs);
}

BOOST_INT128_HOST_DEVICE constexpr uint128 operator>>(const uint128 lhs, const int128 rhs) noexcept
{
    return lhs >> static_cast<uint128>(rhs);
}

//=====================================
// int128 with builtin unsigned __int128 comparison operators
//
// These live here (not in int128_imp.hpp)
// to avoid C++20 rewritten-candidate ambiguity on MSVC
//=====================================

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) == rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator==(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs == static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) != rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator!=(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs != static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) < rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs < static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) <= rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator<=(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs <= static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) > rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs > static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) >= rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR bool operator>=(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs >= static_cast<uint128>(rhs);
}

#endif // BOOST_INT128_HAS_INT128

//=====================================
// int128 with builtin unsigned __int128 binary operators
//=====================================

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator|(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) | rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator|(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs | static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator&(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) & rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator&(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs & static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator^(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) ^ rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator^(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs ^ static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator+(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) + rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator+(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs + static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator-(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) - rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator-(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs - static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator*(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) * rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator*(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs * static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator/(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) / rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator/(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs / static_cast<uint128>(rhs);
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator%(const int128 lhs, const detail::builtin_u128 rhs) noexcept
{
    return static_cast<uint128>(lhs) % rhs;
}

BOOST_INT128_HOST_DEVICE BOOST_INT128_BUILTIN_CONSTEXPR uint128 operator%(const detail::builtin_u128 lhs, const int128 rhs) noexcept
{
    return lhs % static_cast<uint128>(rhs);
}

#endif // BOOST_INT128_HAS_INT128

#ifdef _MSC_VER
#pragma warning(pop)
#endif

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_CONVERSIONS_HPP
// ===== END boost/int128/detail/conversions.hpp =====

#endif // BOOST_INT128_INT128_HPP
// ===== END boost/int128/int128.hpp =====
// ===== BEGIN boost/int128/bit.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_BIT_HPP
#define BOOST_INT128_BIT_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/clz.hpp
// ===== BEGIN boost/int128/detail/ctz.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_CTZ_HPP
#define BOOST_INT128_DETAIL_CTZ_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <limits>
#include <cstdint>

#endif

namespace boost {
namespace int128 {
namespace detail {

namespace impl {

#if BOOST_INT128_HAS_BUILTIN(__builtin_ctz) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

constexpr int countr_impl(unsigned int x) noexcept
{
    return x ? __builtin_ctz(x) : std::numeric_limits<unsigned int>::digits;
}

constexpr int countr_impl(unsigned long x) noexcept
{
    return x ? __builtin_ctzl(x) : std::numeric_limits<unsigned long>::digits;
}

constexpr int countr_impl(unsigned long long x) noexcept
{
    return x ? __builtin_ctzll(x) : std::numeric_limits<unsigned long long>::digits;
}

#endif

#if !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

BOOST_INT128_INLINE_CONSTEXPR int countr_mod37[37] = {
    32, 0, 1, 26, 2, 23, 27, 0,
    3, 16, 24, 30, 28, 11, 0, 13,
    4, 7, 17, 0, 25, 22, 31, 15,
    29, 10, 12, 6, 0, 21, 14, 9,
    5, 20, 8, 19, 18
};

#endif

#if defined(_MSC_VER) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && !BOOST_INT128_HAS_BUILTIN(__builtin_ctz)

#pragma warning(push)
#pragma warning(disable : 4146) // unary minus operator applied to unsigned type, result still unsigned

constexpr int countr_impl(std::uint32_t x) noexcept
{
    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return countr_mod37[(-x & x) % 37]; // LCOV_EXCL_LINE
    }
    else
    {
        unsigned long r {};

        if(_BitScanForward(&r, x))
        {
            return static_cast<int>(r);
        }
        else
        {
            return 32;
        }
    }
}

#pragma warning(pop)

#elif !BOOST_INT128_HAS_BUILTIN(__builtin_ctz) || (defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4146) // unary minus operator applied to unsigned type, result still unsigned
#endif

BOOST_INT128_HOST_DEVICE constexpr int countr_impl(std::uint32_t x) noexcept
{
    #if defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)

    constexpr int countr_mod37[37] = {
        32, 0, 1, 26, 2, 23, 27, 0,
        3, 16, 24, 30, 28, 11, 0, 13,
        4, 7, 17, 0, 25, 22, 31, 15,
        29, 10, 12, 6, 0, 21, 14, 9,
        5, 20, 8, 19, 18
    };

    #endif

    return countr_mod37[(-x & x) % 37];
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif

#if (defined(_M_AMD64) || defined(_M_ARM64)) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION) && !BOOST_INT128_HAS_BUILTIN(__builtin_ctz) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

constexpr int countr_impl(std::uint64_t x) noexcept
{
    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return static_cast<std::uint32_t>(x) != 0 ? countr_impl(static_cast<std::uint32_t>(x)) : countr_impl(static_cast<std::uint32_t>(x >> 32)) + 32; // LCOV_EXCL_LINE
    }
    else
    {
        unsigned long r {};

        if(_BitScanForward64(&r, x))
        {
            return static_cast<int>(r);
        }
        else
        {
            return 64;
        }
    }
}

#elif !BOOST_INT128_HAS_BUILTIN(__builtin_ctz) || (defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

BOOST_INT128_HOST_DEVICE constexpr int countr_impl(std::uint64_t x) noexcept
{
    return static_cast<std::uint32_t>(x) != 0 ? countr_impl(static_cast<std::uint32_t>(x)) :
                                                countr_impl(static_cast<std::uint32_t>(x >> 32)) + 32;
}

#endif

} // namespace impl

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr int countr_zero(T x) noexcept
{
    static_assert(std::numeric_limits<T>::is_integer && !std::numeric_limits<T>::is_signed,
                  "Can only count with unsigned integers");

    return impl::countr_impl(x);
}

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_CTZ_HPP
// ===== END boost/int128/detail/ctz.hpp =====

namespace boost {
namespace int128 {

namespace impl {

BOOST_INT128_HOST_DEVICE constexpr int countl_zero_impl(const uint128 x) noexcept
{
    return x.high == 0 ? 64 + detail::countl_zero(x.low) : detail::countl_zero(x.high);
}

} // namespace impl

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int countl_zero(const uint128 x) noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)) && BOOST_INT128_HAS_BUILTIN(__builtin_clzg) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::countl_zero_impl(x);
    }

    // The second argument is the result for x == 0, which is undefined without it
    return __builtin_clzg(static_cast<detail::builtin_u128>(x), 128);

    #else

    return impl::countl_zero_impl(x);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int countl_one(const uint128 x) noexcept
{
    return countl_zero(~x);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int bit_width(const uint128 x) noexcept
{
    return x ? 128 - countl_zero(x) : 0;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 bit_ceil(const uint128 x) noexcept
{
    // __builtin_stdc_bit_ceil not available, but this is equivalent
    return x <= 1U ? static_cast<uint128>(1) : static_cast<uint128>(2) << (127 - countl_zero(x - 1));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 bit_floor(const uint128 x) noexcept
{
    // __builtin_stdc_bit_floor not available, but this is equivalent
    return x == 0U ? static_cast<uint128>(0) : static_cast<uint128>(1) << (127 - countl_zero(x));
}

namespace impl {

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int countr_zero_impl(const uint128 x) noexcept
{
    return x.low == 0 ? 64 + detail::countr_zero(x.high) : detail::countr_zero(x.low);
}

} // namespace impl

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int countr_zero(const uint128 x) noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)) && BOOST_INT128_HAS_BUILTIN(__builtin_ctzg) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::countr_zero_impl(x);
    }

    // The second argument is the result for x == 0, which is undefined without it
    return __builtin_ctzg(static_cast<detail::builtin_u128>(x), 128);

    #else

    return impl::countr_zero_impl(x);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int countr_one(const uint128 x) noexcept
{
    return countr_zero(~x);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 rotl(const uint128 x, const int s) noexcept
{
    // __builtin_stdc_rotate_left not available
    constexpr auto mask {127U};
    return x << (static_cast<unsigned>(s) & mask) | x >> (static_cast<unsigned>(-s) & mask);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 rotr(const uint128 x, const int s) noexcept
{
    // __builtin_stdc_rotate_right not available
    constexpr auto mask {127U};
    return x >> (static_cast<unsigned>(s) & mask) | x << (static_cast<unsigned>(-s) & mask);
}

namespace impl {

BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr int popcount_impl(std::uint64_t x) noexcept
{
    x = x - ((x >> 1U) & UINT64_C(0x5555555555555555));
    x = (x & UINT64_C(0x3333333333333333)) + ((x >> 2U) & UINT64_C(0x3333333333333333));
    x = (x + (x >> 4U)) & UINT64_C(0x0F0F0F0F0F0F0F0F);

    return static_cast<int>((x * UINT64_C(0x0101010101010101)) >> 56U);
}

// The exact-match overload above is selected for the 64-bit halves
BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr int popcount_impl(const uint128 x) noexcept
{
    return popcount_impl(x.high) + popcount_impl(x.low);
}

} // namespace impl

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int popcount(const uint128 x) noexcept
{
    #if defined(BOOST_INT128_HAS_INT128) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)) && BOOST_INT128_HAS_BUILTIN(__builtin_popcountg) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::popcount_impl(x);
    }

    return __builtin_popcountg(static_cast<detail::builtin_u128>(x));

    #elif BOOST_INT128_HAS_BUILTIN(__builtin_popcountll) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

    return __builtin_popcountll(x.high) + __builtin_popcountll(x.low);

    #elif defined(_M_AMD64) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::popcount_impl(x); // LCOV_EXCL_LINE
    }
    else
    {
        #ifdef __AVX__

        return static_cast<int>(_mm_popcnt_u64(x.high) +  _mm_popcnt_u64(x.low));

        #else

        return static_cast<int>(__popcnt64(x.high) + __popcnt64(x.low));

        #endif
    }

    #elif defined(_M_IX86) && !defined(__GNUC__) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::popcount_impl(x); // LCOV_EXCL_LINE
    }
    else
    {
        #ifdef __AVX__

        return static_cast<int>(
               _mm_popcnt_u32(static_cast<unsigned>(x.high)) +
               _mm_popcnt_u32(static_cast<unsigned>(x.high >> 32U)) +
               _mm_popcnt_u32(static_cast<unsigned>(x.low)) +
               _mm_popcnt_u32(static_cast<unsigned>(x.low >> 32U)));

        #else

        return static_cast<int>(
               __popcnt(static_cast<unsigned>(x.high)) +
               __popcnt(static_cast<unsigned>(x.high >> 32U)) +
               __popcnt(static_cast<unsigned>(x.low)) +
               __popcnt(static_cast<unsigned>(x.low >> 32U)));

        #endif
    }

    #else

    return impl::popcount_impl(x);

    #endif
}

namespace impl {

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr std::uint64_t byteswap_impl(const std::uint64_t x) noexcept
{
    const auto step32 {x << 32U | x >> 32U};
    const auto step16 {(step32 & UINT64_C(0x0000FFFF0000FFFF)) << 16U | (step32 & UINT64_C(0xFFFF0000FFFF0000)) >> 16U};
    return (step16 & UINT64_C(0x00FF00FF00FF00FF)) << 8U | (step16 & UINT64_C(0xFF00FF00FF00FF00)) >> 8U;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 byteswap_impl(const uint128 x) noexcept
{
    return {byteswap_impl(x.low), byteswap_impl(x.high)};
}

} // namespace impl

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 byteswap(const uint128 x) noexcept
{
    // The whole-width builtins are deliberately ranked below the paired 64-bit form.
    // Measured today (7/29/2026) they are a regression: the 128-bit value blocks the loop vectorization
    // the paired __builtin_bswap64 receives, costing up to 1.5x on arm64, and on x86-64 both
    // forms emit identical code. Revisit if the codegen improves.
    #if BOOST_INT128_HAS_BUILTIN(__builtin_bswap64) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA))

    return {__builtin_bswap64(x.low), __builtin_bswap64(x.high)};

    // __builtin_bswapg is clang-only (LLVM 22.1) and __builtin_bswap128 is GCC-only (GCC 11),
    // so at most one of the two whole-width branches is ever live
    #elif defined(BOOST_INT128_HAS_INT128) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)) && BOOST_INT128_HAS_BUILTIN(__builtin_bswapg) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::byteswap_impl(x);
    }

    return static_cast<uint128>(__builtin_bswapg(static_cast<detail::builtin_u128>(x)));

    #elif defined(BOOST_INT128_HAS_INT128) && !(defined(__CUDACC__) && defined(BOOST_INT128_ENABLE_CUDA)) && BOOST_INT128_HAS_BUILTIN(__builtin_bswap128) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::byteswap_impl(x);
    }

    return static_cast<uint128>(__builtin_bswap128(static_cast<detail::builtin_u128>(x)));

    #elif defined(_MSC_VER) && !defined(BOOST_INT128_NO_CONSTEVAL_DETECTION)

    if (BOOST_INT128_IS_CONSTANT_EVALUATED(x))
    {
        return impl::byteswap_impl(x); // LCOV_EXCL_LINE
    }
    else
    {
        return {_byteswap_uint64(x.low), _byteswap_uint64(x.high)};
    }

    #else

    return impl::byteswap_impl(x);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr bool has_single_bit(const uint128 x) noexcept
{
    return popcount(x) == 1;
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_BIT_HPP
// ===== END boost/int128/bit.hpp =====
// ===== BEGIN boost/int128/byte_conversions.hpp =====
// Copyright 2026 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_BYTE_CONVERSIONS_HPP
#define BOOST_INT128_BYTE_CONVERSIONS_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp
// [amalgamate] skipped duplicate include of boost/int128/bit.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#endif

namespace boost {
namespace int128 {

//=====================================
// Whole value byte order conversions
//=====================================

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 to_be(const uint128 value) noexcept
{
    #if BOOST_INT128_ENDIAN_BIG_BYTE

    return value;

    #else

    return byteswap(value);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 to_be(const int128 value) noexcept
{
    #if BOOST_INT128_ENDIAN_BIG_BYTE

    return value;

    #else

    // Reversing the two's complement bit pattern is the same operation for both signs
    return static_cast<int128>(byteswap(static_cast<uint128>(value)));

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 from_be(const uint128 value) noexcept
{
    // Self-inverse
    return to_be(value);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 from_be(const int128 value) noexcept
{
    // Self-inverse
    return to_be(value);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 to_le(const uint128 value) noexcept
{
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    return value;

    #else

    return byteswap(value);

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 to_le(const int128 value) noexcept
{
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    return value;

    #else

    // Reversing the two's complement bit pattern is the same operation for both signs
    return static_cast<int128>(byteswap(static_cast<uint128>(value)));

    #endif
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 from_le(const uint128 value) noexcept
{
    // Self-inverse
    return to_le(value);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 from_le(const int128 value) noexcept
{
    // Self-inverse
    return to_le(value);
}

namespace detail {

// The byte-like element types that the byte array functions operate on.
// std::byte only exists from C++17, and the library is usable from C++14.
template <typename T>
struct byte_like
{
    static constexpr bool value = std::is_same<T, char>::value ||
                                  std::is_same<T, signed char>::value ||
                                  std::is_same<T, unsigned char>::value
                                  #if defined(__cpp_lib_byte) && __cpp_lib_byte >= 201603L
                                  || std::is_same<T, std::byte>::value
                                  #endif
                                  ;
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_byte_like_v = byte_like<T>::value;

// Reads the byte sitting at bit offset shift of a 64-bit word
template <typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr ByteType extract_byte(const std::uint64_t word, const unsigned shift) noexcept
{
    return static_cast<ByteType>(static_cast<unsigned char>((word >> shift) & UINT64_C(0xFF)));
}

// Places a byte at bit offset shift of a 64-bit word
template <typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr std::uint64_t insert_byte(const ByteType value, const unsigned shift) noexcept
{
    return static_cast<std::uint64_t>(static_cast<unsigned char>(value)) << shift;
}

// Rebuilds either library type from the raw two's complement words
template <typename T>
struct word_builder;

template <>
struct word_builder<uint128>
{
    static BOOST_INT128_HOST_DEVICE constexpr uint128 build(const std::uint64_t hi, const std::uint64_t lo) noexcept
    {
        return uint128{hi, lo};
    }
};

template <>
struct word_builder<int128>
{
    static BOOST_INT128_HOST_DEVICE constexpr int128 build(const std::uint64_t hi, const std::uint64_t lo) noexcept
    {
        return from_bits(hi, lo);
    }
};

// The byte arrays are built from shifts rather than from the object representation,
// so all four functions below are exact on either endianness. Bytes is anything
// that can be indexed with the subscript operator: a std::array or a pointer.

template <typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(uint128)> to_be_bytes_impl(const std::uint64_t hi, const std::uint64_t lo) noexcept
{
    return {{extract_byte<ByteType>(hi, 56U), extract_byte<ByteType>(hi, 48U),
             extract_byte<ByteType>(hi, 40U), extract_byte<ByteType>(hi, 32U),
             extract_byte<ByteType>(hi, 24U), extract_byte<ByteType>(hi, 16U),
             extract_byte<ByteType>(hi, 8U),  extract_byte<ByteType>(hi, 0U),
             extract_byte<ByteType>(lo, 56U), extract_byte<ByteType>(lo, 48U),
             extract_byte<ByteType>(lo, 40U), extract_byte<ByteType>(lo, 32U),
             extract_byte<ByteType>(lo, 24U), extract_byte<ByteType>(lo, 16U),
             extract_byte<ByteType>(lo, 8U),  extract_byte<ByteType>(lo, 0U)}};
}

template <typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(uint128)> to_le_bytes_impl(const std::uint64_t hi, const std::uint64_t lo) noexcept
{
    return {{extract_byte<ByteType>(lo, 0U),  extract_byte<ByteType>(lo, 8U),
             extract_byte<ByteType>(lo, 16U), extract_byte<ByteType>(lo, 24U),
             extract_byte<ByteType>(lo, 32U), extract_byte<ByteType>(lo, 40U),
             extract_byte<ByteType>(lo, 48U), extract_byte<ByteType>(lo, 56U),
             extract_byte<ByteType>(hi, 0U),  extract_byte<ByteType>(hi, 8U),
             extract_byte<ByteType>(hi, 16U), extract_byte<ByteType>(hi, 24U),
             extract_byte<ByteType>(hi, 32U), extract_byte<ByteType>(hi, 40U),
             extract_byte<ByteType>(hi, 48U), extract_byte<ByteType>(hi, 56U)}};
}

template <typename T, typename Bytes>
BOOST_INT128_HOST_DEVICE constexpr T from_be_bytes_impl(const Bytes& bytes) noexcept
{
    return word_builder<T>::build(insert_byte(bytes[0], 56U) | insert_byte(bytes[1], 48U) |
                                  insert_byte(bytes[2], 40U) | insert_byte(bytes[3], 32U) |
                                  insert_byte(bytes[4], 24U) | insert_byte(bytes[5], 16U) |
                                  insert_byte(bytes[6], 8U)  | insert_byte(bytes[7], 0U),
                                  insert_byte(bytes[8], 56U) | insert_byte(bytes[9], 48U) |
                                  insert_byte(bytes[10], 40U) | insert_byte(bytes[11], 32U) |
                                  insert_byte(bytes[12], 24U) | insert_byte(bytes[13], 16U) |
                                  insert_byte(bytes[14], 8U)  | insert_byte(bytes[15], 0U));
}

template <typename T, typename Bytes>
BOOST_INT128_HOST_DEVICE constexpr T from_le_bytes_impl(const Bytes& bytes) noexcept
{
    return word_builder<T>::build(insert_byte(bytes[8], 0U)  | insert_byte(bytes[9], 8U) |
                                  insert_byte(bytes[10], 16U) | insert_byte(bytes[11], 24U) |
                                  insert_byte(bytes[12], 32U) | insert_byte(bytes[13], 40U) |
                                  insert_byte(bytes[14], 48U) | insert_byte(bytes[15], 56U),
                                  insert_byte(bytes[0], 0U)  | insert_byte(bytes[1], 8U) |
                                  insert_byte(bytes[2], 16U) | insert_byte(bytes[3], 24U) |
                                  insert_byte(bytes[4], 32U) | insert_byte(bytes[5], 40U) |
                                  insert_byte(bytes[6], 48U) | insert_byte(bytes[7], 56U));
}

} // namespace detail

//=====================================
// Byte array conversions
//=====================================

BOOST_INT128_EXPORT template <typename ByteType = std::uint8_t, std::enable_if_t<detail::is_byte_like_v<ByteType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(uint128)> to_be_bytes(const uint128 value) noexcept
{
    return detail::to_be_bytes_impl<ByteType>(value.high, value.low);
}

BOOST_INT128_EXPORT template <typename ByteType = std::uint8_t, std::enable_if_t<detail::is_byte_like_v<ByteType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(int128)> to_be_bytes(const int128 value) noexcept
{
    return detail::to_be_bytes_impl<ByteType>(value.high, value.low);
}

BOOST_INT128_EXPORT template <typename T, typename ByteType, std::size_t N>
BOOST_INT128_HOST_DEVICE constexpr T from_be_bytes(const std::array<ByteType, N>& bytes) noexcept
{
    static_assert(detail::is_valid_overload_v<T>,
                  "The target type must be boost::int128::uint128 or boost::int128::int128");
    static_assert(detail::is_byte_like_v<ByteType>,
                  "The source bytes must be char, signed char, unsigned char, or std::byte");
    static_assert(N == sizeof(T), "The number of bytes provided, and the target type number of bytes do not match");

    return detail::from_be_bytes_impl<T>(bytes);
}

// Reads sizeof(T) bytes starting at bytes
BOOST_INT128_EXPORT template <typename T, typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr T from_be_bytes(const ByteType* bytes) noexcept
{
    static_assert(detail::is_valid_overload_v<T>,
                  "The target type must be boost::int128::uint128 or boost::int128::int128");
    static_assert(detail::is_byte_like_v<ByteType>,
                  "The source bytes must be char, signed char, unsigned char, or std::byte");

    return detail::from_be_bytes_impl<T>(bytes);
}

BOOST_INT128_EXPORT template <typename ByteType = std::uint8_t, std::enable_if_t<detail::is_byte_like_v<ByteType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(uint128)> to_le_bytes(const uint128 value) noexcept
{
    return detail::to_le_bytes_impl<ByteType>(value.high, value.low);
}

BOOST_INT128_EXPORT template <typename ByteType = std::uint8_t, std::enable_if_t<detail::is_byte_like_v<ByteType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(int128)> to_le_bytes(const int128 value) noexcept
{
    return detail::to_le_bytes_impl<ByteType>(value.high, value.low);
}

BOOST_INT128_EXPORT template <typename T, typename ByteType, std::size_t N>
BOOST_INT128_HOST_DEVICE constexpr T from_le_bytes(const std::array<ByteType, N>& bytes) noexcept
{
    static_assert(detail::is_valid_overload_v<T>,
                  "The target type must be boost::int128::uint128 or boost::int128::int128");
    static_assert(detail::is_byte_like_v<ByteType>,
                  "The source bytes must be char, signed char, unsigned char, or std::byte");
    static_assert(N == sizeof(T), "The number of bytes provided, and the target type number of bytes do not match");

    return detail::from_le_bytes_impl<T>(bytes);
}

// Reads sizeof(T) bytes starting at bytes
BOOST_INT128_EXPORT template <typename T, typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr T from_le_bytes(const ByteType* bytes) noexcept
{
    static_assert(detail::is_valid_overload_v<T>,
                  "The target type must be boost::int128::uint128 or boost::int128::int128");
    static_assert(detail::is_byte_like_v<ByteType>,
                  "The source bytes must be char, signed char, unsigned char, or std::byte");

    return detail::from_le_bytes_impl<T>(bytes);
}

BOOST_INT128_EXPORT template <typename ByteType = std::uint8_t, std::enable_if_t<detail::is_byte_like_v<ByteType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(uint128)> to_ne_bytes(const uint128 value) noexcept
{
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    return detail::to_le_bytes_impl<ByteType>(value.high, value.low);

    #else

    return detail::to_be_bytes_impl<ByteType>(value.high, value.low);

    #endif
}

BOOST_INT128_EXPORT template <typename ByteType = std::uint8_t, std::enable_if_t<detail::is_byte_like_v<ByteType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr std::array<ByteType, sizeof(int128)> to_ne_bytes(const int128 value) noexcept
{
    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    return detail::to_le_bytes_impl<ByteType>(value.high, value.low);

    #else

    return detail::to_be_bytes_impl<ByteType>(value.high, value.low);

    #endif
}

BOOST_INT128_EXPORT template <typename T, typename ByteType, std::size_t N>
BOOST_INT128_HOST_DEVICE constexpr T from_ne_bytes(const std::array<ByteType, N>& bytes) noexcept
{
    static_assert(detail::is_valid_overload_v<T>,
                  "The target type must be boost::int128::uint128 or boost::int128::int128");
    static_assert(detail::is_byte_like_v<ByteType>,
                  "The source bytes must be char, signed char, unsigned char, or std::byte");
    static_assert(N == sizeof(T), "The number of bytes provided, and the target type number of bytes do not match");

    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    return detail::from_le_bytes_impl<T>(bytes);

    #else

    return detail::from_be_bytes_impl<T>(bytes);

    #endif
}

// Reads sizeof(T) bytes starting at bytes
BOOST_INT128_EXPORT template <typename T, typename ByteType>
BOOST_INT128_HOST_DEVICE constexpr T from_ne_bytes(const ByteType* bytes) noexcept
{
    static_assert(detail::is_valid_overload_v<T>,
                  "The target type must be boost::int128::uint128 or boost::int128::int128");
    static_assert(detail::is_byte_like_v<ByteType>,
                  "The source bytes must be char, signed char, unsigned char, or std::byte");

    #if BOOST_INT128_ENDIAN_LITTLE_BYTE

    return detail::from_le_bytes_impl<T>(bytes);

    #else

    return detail::from_be_bytes_impl<T>(bytes);

    #endif
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_BYTE_CONVERSIONS_HPP
// ===== END boost/int128/byte_conversions.hpp =====
// ===== BEGIN boost/int128/iostream.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_IOSTREAM_HPP
#define BOOST_INT128_IOSTREAM_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp
// ===== BEGIN boost/int128/detail/mini_from_chars.hpp =====
// Copyright 2022 Peter Dimov
// Copyright 2023 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef MINI_FROM_CHARS_HPP
#define MINI_FROM_CHARS_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/uint128_imp.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/int128_imp.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cerrno>
#include <limits>
#include <cstddef>

#if !(defined(BOOST_INT128_HAS_GPU_SUPPORT) || defined(BOOST_INT128_DISABLE_EXCEPTIONS))
#include <stdexcept>
#endif

#endif

namespace boost {
namespace int128 {
namespace detail {

namespace impl {

#if !defined(BOOST_INT128_HAS_GPU_SUPPORT)

BOOST_INT128_INLINE_CONSTEXPR unsigned char uchar_values[] =
     {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 254, 255, 255, 255, 255, 255, 255, 255, 255,
        0,   1,   2,   3,   4,   5,   6,   7,   8,   9, 255, 255, 255, 255, 255, 255,
      255,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
       25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35, 255, 255, 255, 255, 255,
      255,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
       25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255};

static_assert(sizeof(uchar_values) == 256, "uchar_values should represent all 256 values of unsigned char");

#endif // __NVCC__

// Convert characters for 0-9, A-Z, a-z to 0-35. The digit separator ' is 254. Anything else is 255
BOOST_INT128_HOST_DEVICE BOOST_INT128_FORCE_INLINE constexpr auto digit_from_char(char val) noexcept -> unsigned char
{
    #if defined(BOOST_INT128_HAS_GPU_SUPPORT)

    constexpr unsigned char uchar_values[] =
    {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 254, 255, 255, 255, 255, 255, 255, 255, 255,
       0,   1,   2,   3,   4,   5,   6,   7,   8,   9, 255, 255, 255, 255, 255, 255,
     255,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
      25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35, 255, 255, 255, 255, 255,
     255,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
      25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255};

    static_assert(sizeof(uchar_values) == 256, "uchar_values should represent all 256 values of unsigned char");

    #endif // __NVCC__

    return uchar_values[static_cast<unsigned char>(val)];
}

template <typename Integer, typename Unsigned_Integer, bool is_literal_parse = false>
BOOST_INT128_HOST_DEVICE constexpr int from_chars_integer_impl(const char* first, const char* last, Integer& value, int base) noexcept
{
    if (last - first <= 0)
    {
        return EINVAL;
    }

    // A base outside 2..36 has no digit set; base 0 would divide by zero below
    if (base < 2 || base > 36)
    {
        return EINVAL;
    }

    Unsigned_Integer result {};
    Unsigned_Integer overflow_value {};
    Unsigned_Integer max_digit {};

    const auto unsigned_base = static_cast<Unsigned_Integer>(base);

    // Strip sign if the type is signed
    // Negative sign will be appended at the end of parsing
    bool is_negative = false;
    static_cast<void>(is_negative);
    auto next = first;

    BOOST_INT128_IF_CONSTEXPR (std::numeric_limits<Integer>::is_signed)
    {
        if (*next == '-')
        {
            is_negative = true;
            ++next;
        }

        overflow_value = static_cast<Unsigned_Integer>((std::numeric_limits<Integer>::max)());
        max_digit = static_cast<Unsigned_Integer>((std::numeric_limits<Integer>::max)());

        if (is_negative)
        {
            ++overflow_value;
            ++max_digit;
        }
    }
    else
    {
        if (*next == '-' || *next == '+')
        {
            return EINVAL;
        }

        overflow_value = (std::numeric_limits<Unsigned_Integer>::max)();
        max_digit = (std::numeric_limits<Unsigned_Integer>::max)();
    }


    overflow_value /= unsigned_base;
    max_digit %= unsigned_base;

    // If the only character was a sign abort now
    if (next == last)
    {
        return EINVAL;
    }

    bool overflowed = false;

    const std::ptrdiff_t nc = last - next;

    // For bases 2..10 the first digits10 characters always fit in the unsigned
    // For bases above 10, the safe window is shorter, so we must check with each iteration
    const std::ptrdiff_t nd {
        base <= 10
            ? static_cast<std::ptrdiff_t>(std::numeric_limits<Integer>::digits10)
            : std::ptrdiff_t{0}
    };

    const std::ptrdiff_t fast_limit {nd < nc ? nd : nc};
    std::ptrdiff_t i = 0;

    for (; i < fast_limit; ++i)
    {
        const auto raw_digit = digit_from_char(*next);

        // When parsing a user-defined literal skip the digit separator ' (marked as 254)
        BOOST_INT128_IF_CONSTEXPR (is_literal_parse)
        {
            if (raw_digit == 254)
            {
                ++next;
                continue;
            }
        }

        const auto current_digit = static_cast<Unsigned_Integer>(raw_digit);

        if (current_digit >= unsigned_base)
        {
            break;
        }

        result = static_cast<Unsigned_Integer>(result * unsigned_base + current_digit);
        ++next;
    }

    for (; i < nc; ++i)
    {
        const auto raw_digit = digit_from_char(*next);

        // When parsing a user-defined literal skip the digit separator ' (marked as 254)
        BOOST_INT128_IF_CONSTEXPR (is_literal_parse)
        {
            if (raw_digit == 254)
            {
                ++next;
                continue;
            }
        }

        const auto current_digit = static_cast<Unsigned_Integer>(raw_digit);

        if (current_digit >= unsigned_base)
        {
            break;
        }

        if (result < overflow_value || (result == overflow_value && current_digit <= max_digit))
        {
            result = static_cast<Unsigned_Integer>(result * unsigned_base + current_digit);
        }
        else
        {
            overflowed = true;
            break;
        }

        ++next;
    }

    // Return the parsed value, adding the sign back if applicable
    // If we have overflowed, then we do not return the result
    if (overflowed)
    {
        return EDOM;
    }

    // Nothing consumed means the first character was not a digit in this base. The
    // output is left untouched, as std::from_chars specifies, and 0 is returned.
    if (next == first || (is_negative && next == first + 1))
    {
        return 0;
    }

    value = static_cast<Integer>(result);

    BOOST_INT128_IF_CONSTEXPR (std::numeric_limits<Integer>::is_signed)
    {
        if (is_negative)
        {
            value = static_cast<Integer>(-(static_cast<Unsigned_Integer>(value)));
        }
    }

    // This value will be negative to differentiate from errno values
    // since they are in the range of acceptable distances

    // This cast is useless on 32-bit platforms
    #if defined(__GNUC__) && !defined(__clang__)
    #  pragma GCC diagnostic push
    #  pragma GCC diagnostic ignored "-Wuseless-cast"
    #endif

    return static_cast<int>(first - next);

    #if defined(__GNUC__) && !defined(__clang__)
    #  pragma GCC diagnostic pop
    #endif
}
} // namespace impl

BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr int from_chars(const char* first, const char* last, uint128& value, int base = 10) noexcept
{
    return impl::from_chars_integer_impl<uint128, uint128>(first, last, value, base);
}

BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr int from_chars(const char* first, const char* last, int128& value, int base = 10) noexcept
{
    return impl::from_chars_integer_impl<int128, uint128>(first, last, value, base);
}

// Parsing entry points for the user-defined literals. Unlike from_chars these skip the
// C++ digit separator ' so that literals such as 1'234'567_u128 are accepted.
BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr int from_chars_literal(const char* first, const char* last, uint128& value, int base = 10) noexcept
{
    return impl::from_chars_integer_impl<uint128, uint128, true>(first, last, value, base);
}

BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr int from_chars_literal(const char* first, const char* last, int128& value, int base = 10) noexcept
{
    return impl::from_chars_integer_impl<int128, uint128, true>(first, last, value, base);
}

// Rejects an out of range literal
[[noreturn]] BOOST_INT128_HOST_DEVICE inline void parse_literal_out_of_range()
{
    #if defined(BOOST_INT128_HAS_GPU_SUPPORT) || defined(BOOST_INT128_DISABLE_EXCEPTIONS)
    BOOST_INT128_UNREACHABLE;
    #else
    BOOST_INT128_THROW_EXCEPTION(std::out_of_range("Literal is out of range of the target type"));
    #endif
}

// Rejects an invalid literal
[[noreturn]] BOOST_INT128_HOST_DEVICE inline void parse_invalid_literal()
{
    #if defined(BOOST_INT128_HAS_GPU_SUPPORT) || defined(BOOST_INT128_DISABLE_EXCEPTIONS)
    BOOST_INT128_UNREACHABLE;
    #else
    BOOST_INT128_THROW_EXCEPTION(std::invalid_argument("Literal is not a valid integer"));
    #endif
}

// GCC before 6 rejects a constexpr function that contains a throw-expression or a
// call to a non-constexpr function anywhere in its body so we need to use unreachable in that case
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ < 6
#  define BOOST_INT128_REJECT_LITERAL(reporter) BOOST_INT128_UNREACHABLE
#else
#  define BOOST_INT128_REJECT_LITERAL(reporter) reporter()
#endif

// Parse a user-defined literal, hard-failing on any malformed or out-of-range input.
// A C++ base prefix (0x/0X hex, 0b/0B binary, or a leading 0 for octal) is stripped and
// the digits parsed in that base, otherwise handled as base 10
// A malformed or out-of-range literal is a compile-time error in a constant expression and
// terminates the program at run time (the reporters throw out of this noexcept function).
template <typename Integer>
BOOST_INT128_HOST_DEVICE constexpr Integer parse_literal(const char* first, const char* last) noexcept
{
    Integer parse_value {};

    // A leading sign stays with the digits; a base prefix, if present, follows it.
    auto next = first;
    const bool negative {next != last && *next == '-'};
    if (negative)
    {
        ++next;
    }

    int base {10};
    bool prefixed {false};

    if (last - next >= 2 && *next == '0')
    {
        const char marker {next[1]};
        if (marker == 'x' || marker == 'X')
        {
            base = 16;
            next += 2;
            prefixed = true;
        }
        else if (marker == 'b' || marker == 'B')
        {
            base = 2;
            next += 2;
            prefixed = true;
        }
        else
        {
            base = 8;
            next += 1;
            prefixed = true;
        }
    }

    // With no prefix, from_chars_literal handles the sign and the full decimal range.
    // Overflow is reported as EDOM; anything else short of full consumption is malformed.
    if (!prefixed)
    {
        const auto status = from_chars_literal(first, last, parse_value);
        if (status == EDOM)
        {
            BOOST_INT128_REJECT_LITERAL(parse_literal_out_of_range);
        }
        else if (status != first - last)
        {
            BOOST_INT128_REJECT_LITERAL(parse_invalid_literal);
        }

        return parse_value;
    }

    // Prefixed: parse the magnitude in the detected base, then reapply the sign.
    const auto status = from_chars_literal(next, last, parse_value, base);
    if (status == EDOM)
    {
        BOOST_INT128_REJECT_LITERAL(parse_literal_out_of_range);
    }
    else if (status != next - last)
    {
        BOOST_INT128_REJECT_LITERAL(parse_invalid_literal);
    }

    if (negative)
    {
        BOOST_INT128_IF_CONSTEXPR (std::numeric_limits<Integer>::is_signed)
        {
            parse_value = static_cast<Integer>(-parse_value);
        }
        else
        {
            BOOST_INT128_UNREACHABLE;
        }
    }

    return parse_value;
}

} // namespace detail
} // namespace int128
} // namespace boost

#undef BOOST_INT128_REJECT_LITERAL

#endif //MINI_FROM_CHARS_HPP
// ===== END boost/int128/detail/mini_from_chars.hpp =====
// ===== BEGIN boost/int128/detail/mini_to_chars.hpp =====
// Copyright 2022 Peter Dimov
// Copyright 2023 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_MINI_TO_CHARS_HPP
#define BOOST_INT128_DETAIL_MINI_TO_CHARS_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp

namespace boost {
namespace int128 {
namespace detail {

// A 128-bit integer needs up to 128 binary digits (base 2); allow for a leading sign and a
// null terminator so mini_to_chars is safe for every supported base (2, 8, 10, 16).
constexpr std::size_t mini_to_chars_buffer_size = 130;

#if !defined(BOOST_INT128_HAS_GPU_SUPPORT)

BOOST_INT128_INLINE_CONSTEXPR char lower_case_digit_table[] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'a', 'b', 'c', 'd', 'e', 'f'
};

static_assert(sizeof(lower_case_digit_table) == sizeof(char) * 16, "10 numbers, and 6 letters");

BOOST_INT128_INLINE_CONSTEXPR char upper_case_digit_table[] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
};

static_assert(sizeof(upper_case_digit_table) == sizeof(char) * 16, "10 numbers, and 6 letters");

#endif // !__NVCC__

BOOST_INT128_HOST_DEVICE constexpr char* mini_to_chars(char (&buffer)[mini_to_chars_buffer_size], uint128 v, const int base, const bool uppercase) noexcept
{
    #if defined(BOOST_INT128_HAS_GPU_SUPPORT)
    constexpr char lower_case_digit_table[] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'a', 'b', 'c', 'd', 'e', 'f'
    };

    constexpr char upper_case_digit_table[] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F'
    };
    #endif

    char* last {buffer + sizeof(buffer)};
    *--last = '\0';

    if (v == 0U)
    {
        *--last = '0';
        return last;
    }

    const auto digit_table {uppercase ? upper_case_digit_table : lower_case_digit_table};

    switch (base)
    {
        case 2:
            while (v != 0U)
            {
                *--last = v.low & 1U ? '1' : '0';
                v >>= 1U;
            }
            break;

        case 8:
            while (v != 0U)
            {
                constexpr unsigned zero {48U};
                *--last = static_cast<char>(zero + (v & 7U));
                v >>= 3U;
            }
            break;

        case 10:
            while (v != 0U)
            {
                *--last = digit_table[static_cast<std::size_t>(v % 10U)];
                v /= 10U;
            }
            break;

        case 16:
            while (v != 0U)
            {
                *--last = digit_table[static_cast<std::size_t>(v & 15U)];
                v >>= 4U;
            }
            break;

        default:                        // LCOV_EXCL_LINE
            BOOST_INT128_UNREACHABLE;   // LCOV_EXCL_LINE
    }

    return last;
}

BOOST_INT128_HOST_DEVICE constexpr char* mini_to_chars(char (&buffer)[mini_to_chars_buffer_size], const int128 v, const int base, const bool uppercase) noexcept
{
    char* p {nullptr};

    if (v < 0)
    {
        // We cant negate the min value inside the signed type, but we know what the result will be
        if (v == (std::numeric_limits<int128>::min)())
        {
            p = mini_to_chars(buffer, uint128{UINT64_C(0x8000000000000000), 0}, base, uppercase);
        }
        else
        {
            const auto neg_v {-v};
            p = mini_to_chars(buffer, static_cast<uint128>(neg_v), base, uppercase);
        }

        *--p = '-';
    }
    else
    {
        p = mini_to_chars(buffer, static_cast<uint128>(v), base, uppercase);
    }

    return p;
}

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_MINI_TO_CHARS_HPP
// ===== END boost/int128/detail/mini_to_chars.hpp =====
// ===== BEGIN boost/int128/detail/utilities.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_UTILITIES_HPP
#define BOOST_INT128_DETAIL_UTILITIES_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstddef>

#endif

namespace boost {
namespace int128 {
namespace detail {

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr std::size_t strlen(const T* str) noexcept
{
    std::size_t i {};
    while (*str != '\0')
    {
        ++str;
        ++i;
    }

    return i;
}

} // namespace detail
} // namespace int128
} // namespace boost

#endif // BOOST_INT128_DETAIL_UTILITIES_HPP
// ===== END boost/int128/detail/utilities.hpp =====
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <type_traits>
#include <iostream>
#include <iomanip>
#include <cstring>

#endif

namespace boost {
namespace int128 {

namespace detail {

template <typename T>
struct streamable_overload
{
    static constexpr bool value = std::is_same<T, uint128>::value || std::is_same<T, int128>::value;
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_streamable_overload_v = streamable_overload<T>::value;

} // namespace detail

#if defined(__GNUC__) && __GNUC__ >= 5 && __GNUC__ < 11
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

BOOST_INT128_EXPORT template <typename charT, typename traits, typename LibIntegerType>
auto operator>>(std::basic_istream<charT, traits>& is, LibIntegerType& v)
    -> std::enable_if_t<detail::is_streamable_overload_v<LibIntegerType>, std::basic_istream<charT, traits>&>
{
    charT t_buffer[64] {};
    is >> std::ws >> std::setw(63) >> t_buffer;

    const auto t_buffer_len {std::char_traits<charT>::length(t_buffer)};

    char buffer[64] {};
    auto buffer_start {buffer};

    BOOST_INT128_IF_CONSTEXPR (!std::is_same<charT, char>::value)
    {
        auto first {buffer};
        auto t_first {t_buffer};
        const auto t_buffer_end {t_buffer + detail::strlen(t_buffer)};

        while (t_first != t_buffer_end)
        {
            *first++ = static_cast<char>(*t_first++);
        }
    }
    else
    {
        std::memcpy(buffer, t_buffer, sizeof(t_buffer));
    }

    const auto flags {is.flags()};
    int base {10};
    if (flags & std::ios_base::oct)
    {
        // No prefix is stripped: in base 8 a leading zero is already an ordinary digit,
        // so "017" reads as 15 and "08" reads as 0 leaving the '8' in the stream, which
        // is what num_get does for the builtin types.
        base = 8;
    }
    else if (flags & std::ios_base::hex)
    {
        base = 16;

        // Skip an explicit 0x or 0X prefix, and never a bare leading zero, which
        // would swallow the first digit of a value such as 0f
        if (buffer_start[0] == '0' && (buffer_start[1] == 'x' || buffer_start[1] == 'X'))
        {
            buffer_start += 2;
        }
    }

    const auto prefix_length {static_cast<std::size_t>(buffer_start - buffer)};

    const auto r {detail::from_chars(buffer_start, buffer + detail::strlen(buffer), v, base)};

    // Put back unconsumed characters. Only a strictly negative r means digits were
    // extracted, and then -r digits were consumed on top of any base prefix. Anything
    // else consumed nothing at all, so even the prefix goes back.
    std::size_t consumed {};
    if (r < 0)
    {
        consumed = prefix_length + static_cast<std::size_t>(-r);
    }

    BOOST_INT128_ASSERT(t_buffer_len >= consumed);
    const auto return_chars {static_cast<std::size_t>(t_buffer_len - consumed)};

    for (std::size_t i {}; i < return_chars; ++i)
    {
        is.putback(t_buffer[t_buffer_len - i - 1]);
    }

    // from_chars returns the negated number of characters consumed on success, so
    // anything not negative means no digits were extracted: r == 0 is a first
    // character that is not a digit in the base, and r > 0 is an errno value
    // (EINVAL for an empty input or a sign, EDOM for a value that does not fit).
    // The stream has to report all of those as a failure. This must come after the
    // putback loop: putback fails its own sentry once failbit is set.
    if (r >= 0)
    {
        v = LibIntegerType{};
        is.setstate(std::ios_base::failbit);
    }

    return is;
}

#if defined(__GNUC__) && __GNUC__ >= 5 && __GNUC__ < 11
#  pragma GCC diagnostic pop
#endif

BOOST_INT128_EXPORT template <typename charT, typename traits, typename LibIntegerType>
auto operator<<(std::basic_ostream<charT, traits>& os, const LibIntegerType& v)
    -> std::enable_if_t<detail::is_streamable_overload_v<LibIntegerType>, std::basic_ostream<charT, traits>&>
{
    char buffer[detail::mini_to_chars_buffer_size] {};

    const auto flags {os.flags()};
    int base {10};
    bool uppercase {false};
    if (flags & std::ios_base::oct)
    {
        base = 8;
    }
    else if (flags & std::ios_base::hex)
    {
        base = 16;
    }

    if (flags & std::ios_base::uppercase)
    {
        uppercase = true;
    }

    auto first {detail::mini_to_chars(buffer, v, base, uppercase)};

    // A zero prints as a bare "0" with showbase, the same as the builtin types
    if ((flags & std::ios_base::showbase) && v != 0U)
    {
        if (base == 8)
        {
            *--first = '0';
        }
        else if (base == 16)
        {
            *--first = uppercase ? 'X' : 'x';
            *--first = '0';
        }
    }

    BOOST_INT128_IF_CONSTEXPR (!std::is_same<charT, char>::value)
    {
        charT t_buffer[64U] {};

        auto t_first {t_buffer};
        while (*first != '\0')
        {
            *t_first++ = static_cast<charT>(*first++);
        }

        os << t_buffer;
    }
    else
    {
        os << first;
    }

    return os;
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_IOSTREAM_HPP
// ===== END boost/int128/iostream.hpp =====
// ===== BEGIN boost/int128/literals.hpp =====
// Copyright 2022 Peter Dimov
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_LITERALS_HPP
#define BOOST_INT128_LITERALS_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/mini_from_chars.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/utilities.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

namespace boost {
namespace int128 {
namespace literals {

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator ""_u128(const char* str) noexcept
{
    return detail::parse_literal<uint128>(str, str + detail::strlen(str));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator ""_U128(const char* str) noexcept
{
    return detail::parse_literal<uint128>(str, str + detail::strlen(str));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator ""_u128(const char* str, std::size_t len) noexcept
{
    return detail::parse_literal<uint128>(str, str + len);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 operator ""_U128(const char* str, std::size_t len) noexcept
{
    return detail::parse_literal<uint128>(str, str + len);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator ""_i128(const char* str) noexcept
{
    return detail::parse_literal<int128>(str, str + detail::strlen(str));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator ""_I128(const char* str) noexcept
{
    return detail::parse_literal<int128>(str, str + detail::strlen(str));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator ""_i128(const char* str, std::size_t len) noexcept
{
    return detail::parse_literal<int128>(str, str + len);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 operator ""_I128(const char* str, std::size_t len) noexcept
{
    return detail::parse_literal<int128>(str, str + len);
}

} // namespace literals
} // namespace int128
} // namespace boost

// ===== BEGIN boost/int128/detail/literal_macros.hpp =====
// Copyright 2022 Peter Dimov
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_DETAIL_LITERAL_MACROS_HPP
#define BOOST_INT128_DETAIL_LITERAL_MACROS_HPP

// Convenience macros for the user-defined literals. This header intentionally has
// no includes and declares nothing, so module consumers can pull it in on its own
// to obtain the macros (macros are never part of a module's exported interface).

#define BOOST_INT128_STRINGIFY(x) #x
#define BOOST_INT128_UINT128_C(x) boost::int128::literals::operator""_u128(BOOST_INT128_STRINGIFY(x))
#define BOOST_INT128_INT128_C(x) boost::int128::literals::operator""_i128(BOOST_INT128_STRINGIFY(x))

#endif // BOOST_INT128_DETAIL_LITERAL_MACROS_HPP
// ===== END boost/int128/detail/literal_macros.hpp =====

#endif // BOOST_INT128_LITERALS_HPP
// ===== END boost/int128/literals.hpp =====
// ===== BEGIN boost/int128/numeric.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_NUMERIC_HPP
#define BOOST_INT128_NUMERIC_HPP

// [amalgamate] skipped duplicate include of boost/int128/bit.hpp
// ===== BEGIN boost/int128/cstdlib.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_CSTDLIB_HPP
#define BOOST_INT128_CSTDLIB_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp

namespace boost {
namespace int128 {

BOOST_INT128_EXPORT struct u128div_t
{
    uint128 quot;
    uint128 rem;
};

BOOST_INT128_EXPORT struct i128div_t
{
    int128 quot;
    int128 rem;
};

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr u128div_t div(const uint128 x, const uint128 y) noexcept
{
    if (y == 0U)
    {
        BOOST_INT128_UNREACHABLE;
    }

    if (x < y)
    {
        return u128div_t{0U, x};
    }
    else if (y.high != 0U)
    {
        u128div_t res {};
        res.quot = detail::knuth_div(x, y, res.rem);
        return res;
    }
    else
    {
        if (x.high == 0U)
        {
            return u128div_t{x.low / y.low, x.low % y.low};
        }
        else
        {
            u128div_t res {};
            detail::one_word_div(x, y.low, res.quot, res.rem);
            return res;
        }
    }
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr i128div_t div(const int128 x, const int128 y) noexcept
{
    if (y == 0)
    {
        BOOST_INT128_UNREACHABLE;
    }

    const auto abs_lhs {static_cast<uint128>(abs(x))};
    const auto abs_rhs {static_cast<uint128>(abs(y))};

    if (abs_rhs > abs_lhs)
    {
        return {0, x};
    }

    const auto negative_quot {(x.signed_high() < 0) != (y.signed_high() < 0)};
    const auto negative_rem {x.signed_high() < 0};

    #if defined(BOOST_INT128_HAS_INT128)

    if (abs_rhs.high != 0)
    {
        const auto builtin_x {static_cast<detail::builtin_i128>(x)};
        const auto builtin_y {static_cast<detail::builtin_i128>(y)};
        return i128div_t{static_cast<int128>(builtin_x / builtin_y),
                         static_cast<int128>(builtin_x % builtin_y)};
    }

    #endif

    const auto unsigned_res {div(abs_lhs, abs_rhs)};

    i128div_t res {static_cast<int128>(unsigned_res.quot), static_cast<int128>(unsigned_res.rem)};

    res.quot = negative_quot ? -res.quot : res.quot;
    res.rem = negative_rem ? -res.rem : res.rem;

    return res;
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_CSTDLIB_HPP
// ===== END boost/int128/cstdlib.hpp =====
// [amalgamate] skipped duplicate include of boost/int128/detail/traits.hpp
// ===== BEGIN boost/int128/utilities.hpp =====
// Copyright 2026 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_UTILITIES_HPP
#define BOOST_INT128_UTILITIES_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp
// [amalgamate] skipped duplicate include of boost/int128/bit.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#endif

namespace boost {
namespace int128 {

namespace detail {

// Modular addition for 128-bit operands assuming 0 <= a, b < m
BOOST_INT128_HOST_DEVICE constexpr uint128 addmod(const uint128 a, const uint128 b, const uint128 m) noexcept
{
    const uint128 s {a + b};

    if (s < a || s >= m)
    {
        return s - m;
    }

    return s;
}

// Modular multiplication via shift-and-add for the full 128-bit modulus case
BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 mulmod_shift(uint128 a, uint128 b, const uint128 m) noexcept
{
    uint128 result {0};

    while (b != 0U)
    {
        if (static_cast<bool>(b.low & 1U))
        {
            result = addmod(result, a, m);
        }

        a = addmod(a, a, m);
        b >>= 1;
    }

    return result;
}

// Modular multiplication when the modulus fits in 64 bits
BOOST_INT128_HOST_DEVICE constexpr std::uint64_t mulmod_word(const std::uint64_t a, const std::uint64_t b, const std::uint64_t m) noexcept
{
    return ((uint128{a} * uint128{b}) % uint128{m}).low;
}

} // namespace detail

// Computes (base ^ exp) mod m using fast modular exponentiation with
// optimizations specific to the boost::int128 library types
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 powm(uint128 base, uint128 exp, const uint128 m) noexcept
{
    if (BOOST_INT128_UNLIKELY(m == 0U))
    {
        return uint128{0};
    }

    if (m == 1U)
    {
        return uint128{0};
    }

    if (exp == 0U)
    {
        return uint128{1};
    }

    base %= m;

    if (base == 0U)
    {
        return uint128{0};
    }

    // Power-of-two modulus: reduction is just a bitmask.
    if (has_single_bit(m))
    {
        const uint128 mask {m - 1U};
        uint128 result {1};

        while (exp != 0U)
        {
            if (static_cast<bool>(exp.low & 1U))
            {
                result = (result * base) & mask;
            }

            base = (base * base) & mask;
            exp >>= 1;
        }

        return result;
    }

    // Modulus fits in 64 bits: stay in 64-bit lanes.
    if (m.high == 0U)
    {
        const auto mm {m.low};
        std::uint64_t result {1};
        auto b {base.low};

        while (exp != 0U)
        {
            if (static_cast<bool>(exp.low & 1U))
            {
                result = detail::mulmod_word(result, b, mm);
            }

            b = detail::mulmod_word(b, b, mm);
            exp >>= 1;
        }

        return uint128{result};
    }

    // General 128-bit modulus: shift-and-add for each squaring keeps every
    // intermediate strictly below m without needing a 256-bit product.
    uint128 result {1};

    while (exp != 0U)
    {
        if (static_cast<bool>(exp.low & 1U))
        {
            result = detail::mulmod_shift(result, base, m);
        }

        base = detail::mulmod_shift(base, base, m);
        exp >>= 1;
    }

    return result;
}

// Signed overload. Returns the non-negative residue in [0, m)
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 powm(const int128 base, const int128 exp, const int128 m) noexcept
{
    if (BOOST_INT128_UNLIKELY(m <= 0 || exp < 0))
    {
        return int128{0};
    }

    const uint128 um {static_cast<uint128>(m)};

    uint128 ub {};

    if (base.signed_high() < 0)
    {
        const uint128 magnitude {static_cast<uint128>(abs(base))};
        const uint128 r {magnitude % um};
        ub = r == 0U ? uint128{0} : um - r;
    }
    else
    {
        ub = static_cast<uint128>(base) % um;
    }

    return static_cast<int128>(powm(ub, static_cast<uint128>(exp), um));
}

// Computes base^exp using exponentiation by squaring. The result is reduced
// modulo 2^128, mirroring the wrap-around behavior of operator*.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 ipow(uint128 base, std::uint64_t exp) noexcept
{
    uint128 result {1};

    while (exp != 0U)
    {
        if (static_cast<bool>(exp & 1U))
        {
            result *= base;
        }

        exp >>= 1;

        if (exp != 0U)
        {
            base *= base;
        }
    }

    return result;
}

// Signed overload. Wraps modulo 2^128 on overflow, matching operator*.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 ipow(int128 base, std::uint64_t exp) noexcept
{
    int128 result {1};

    while (exp != 0U)
    {
        if (static_cast<bool>(exp & 1U))
        {
            result *= base;
        }

        exp >>= 1;

        if (exp != 0U)
        {
            base *= base;
        }
    }

    return result;
}

// Integer square root: returns floor(sqrt(n)).
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 isqrt(const uint128 n) noexcept
{
    if (n < 2U)
    {
        return n;
    }

    // 2^ceil(bit_width(n)/2) is the smallest power of two whose square exceeds n.
    uint128 x {uint128{1} << ((bit_width(n) + 1) / 2)};

    while (true)
    {
        const uint128 y {(x + n / x) >> 1};

        if (y >= x)
        {
            return x;
        }

        x = y;
    }
}

// Signed overload. Negative inputs are documented to return 0.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 isqrt(const int128 n) noexcept
{
    if (BOOST_INT128_UNLIKELY(n < 0))
    {
        return int128{0};
    }

    return static_cast<int128>(isqrt(static_cast<uint128>(n)));
}

namespace detail {

// The C23 checked integer macros accept any integer type for their operands
// except bool, plain char, enumerated types, and bit-precise (_BitInt) types.
template <typename T>
struct valid_checked_type : std::integral_constant<bool, std::is_integral<T>::value &&
                                                         !std::is_same<T, bool>::value &&
                                                         !std::is_same<T, char>::value> {};

template <>
struct valid_checked_type<int128> : std::true_type {};

template <>
struct valid_checked_type<uint128> : std::true_type {};

// Widen an integer operand to its 128-bit two's complement bit pattern, returned as a uint128
template <typename T>
BOOST_INT128_HOST_DEVICE constexpr uint128 ckd_widen(const T value) noexcept
{
    BOOST_INT128_IF_CONSTEXPR (std::numeric_limits<T>::is_signed)
    {
        return static_cast<uint128>(static_cast<int128>(value));
    }
    else
    {
        return static_cast<uint128>(value);
    }
}

// Sign and magnitude of an operand together with its 128-bit two's complement
// image. magnitude is the absolute value; negative records the sign.
struct ckd_operand
{
    uint128 raw;
    uint128 magnitude;
    bool negative;
};

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr ckd_operand ckd_decompose(const T value) noexcept
{
    const uint128 raw {ckd_widen(value)};
    const bool negative {std::numeric_limits<T>::is_signed && ((raw >> 127) != 0U)};
    return ckd_operand{raw, negative ? uint128{0} - raw : raw, negative};
}

// Exact signed sum of two operands given as (magnitude, sign). carry marks a
// 129th bit, which no 128-bit or narrower target can represent.
struct ckd_sum_result
{
    uint128 magnitude;
    bool negative;
    bool carry;
};

BOOST_INT128_HOST_DEVICE constexpr ckd_sum_result ckd_signed_sum(const uint128 a_magnitude, const bool a_negative,
                                                                 const uint128 b_magnitude, const bool b_negative) noexcept
{
    if (a_negative == b_negative)
    {
        // Equal signs: magnitudes add and may overflow into a 129th bit.
        const uint128 magnitude {a_magnitude + b_magnitude};
        return ckd_sum_result{magnitude, a_negative, magnitude < a_magnitude};
    }

    // Opposite signs: the smaller magnitude is subtracted and never carries.
    if (a_magnitude >= b_magnitude)
    {
        return ckd_sum_result{a_magnitude - b_magnitude, a_negative, false};
    }

    return ckd_sum_result{b_magnitude - a_magnitude, b_negative, false};
}

// Whether a result of the given sign and magnitude fits in T1. exceeds_width
// forces overflow when the true magnitude does not even fit in 128 bits.
template <typename T1>
BOOST_INT128_HOST_DEVICE constexpr bool ckd_overflows(const uint128 magnitude, const bool negative, const bool exceeds_width) noexcept
{
    if (exceeds_width)
    {
        return true;
    }

    const uint128 max_magnitude {static_cast<uint128>((std::numeric_limits<T1>::max)())};

    if (negative)
    {
        const uint128 min_magnitude {std::numeric_limits<T1>::is_signed ? max_magnitude + uint128{1} : uint128{0}};
        return magnitude > min_magnitude;
    }

    return magnitude > max_magnitude;
}

} // namespace detail

// Checked addition following the C23 <stdckdint.h> ckd_add contract.
//
// Computes a + b as if both operands were represented in a signed integer
// type of infinite range and then converts that exact result to the type
// pointed to by result. *result always receives the exact result wrapped
// around to the width of *result. Returns false when *result represents the
// exact mathematical sum, and true when the sum did not fit and wrap-around
// occurred.
BOOST_INT128_EXPORT template <typename T1, typename T2, typename T3>
BOOST_INT128_HOST_DEVICE constexpr bool ckd_add(T1* result, const T2 a, const T3 b) noexcept
{
    static_assert(detail::valid_checked_type<T1>::value &&
                  detail::valid_checked_type<T2>::value &&
                  detail::valid_checked_type<T3>::value,
                  "ckd_add operands must be integer types other than bool and plain char.");

    const auto op_a {detail::ckd_decompose(a)};
    const auto op_b {detail::ckd_decompose(b)};

    // The modular sum of the widened images is the exact sum mod 2^128, which
    // is all the wrapped result needs for any target no wider than 128 bits.
    *result = static_cast<T1>(op_a.raw + op_b.raw);

    const auto sum {detail::ckd_signed_sum(op_a.magnitude, op_a.negative, op_b.magnitude, op_b.negative)};
    return detail::ckd_overflows<T1>(sum.magnitude, sum.negative, sum.carry);
}

// Checked subtraction following the C23 <stdckdint.h> ckd_sub contract.
//
// Behaves as ckd_add for a - b: *result receives the exact difference wrapped
// to its width, and the return value reports whether that difference did not
// fit.
BOOST_INT128_EXPORT template <typename T1, typename T2, typename T3>
BOOST_INT128_HOST_DEVICE constexpr bool ckd_sub(T1* result, const T2 a, const T3 b) noexcept
{
    static_assert(detail::valid_checked_type<T1>::value &&
                  detail::valid_checked_type<T2>::value &&
                  detail::valid_checked_type<T3>::value,
                  "ckd_sub operands must be integer types other than bool and plain char.");

    const auto op_a {detail::ckd_decompose(a)};
    const auto op_b {detail::ckd_decompose(b)};

    *result = static_cast<T1>(op_a.raw - op_b.raw);

    // a - b is a + (-b): negating b flips its sign while keeping its magnitude.
    const auto difference {detail::ckd_signed_sum(op_a.magnitude, op_a.negative, op_b.magnitude, !op_b.negative)};
    return detail::ckd_overflows<T1>(difference.magnitude, difference.negative, difference.carry);
}

// Checked multiplication following the C23 <stdckdint.h> ckd_mul contract.
//
// Computes a * b as if both operands had infinite range, stores the result
// wrapped to the width of *result, and returns true when the exact product did
// not fit.
BOOST_INT128_EXPORT template <typename T1, typename T2, typename T3>
BOOST_INT128_HOST_DEVICE constexpr bool ckd_mul(T1* result, const T2 a, const T3 b) noexcept
{
    static_assert(detail::valid_checked_type<T1>::value &&
                  detail::valid_checked_type<T2>::value &&
                  detail::valid_checked_type<T3>::value,
                  "ckd_mul operands must be integer types other than bool and plain char.");

    const auto op_a {detail::ckd_decompose(a)};
    const auto op_b {detail::ckd_decompose(b)};

    *result = static_cast<T1>(op_a.raw * op_b.raw);

    // The product magnitude needs more than 128 bits exactly when it exceeds
    // UINT128_MAX. Dividing the maximum by one magnitude tests that without
    // forming a 256-bit product.
    const bool exceeds_width {op_a.magnitude != 0U &&
                              op_b.magnitude > ((std::numeric_limits<uint128>::max)() / op_a.magnitude)};

    const uint128 product_magnitude {op_a.magnitude * op_b.magnitude};
    const bool product_negative {op_a.negative != op_b.negative};

    return detail::ckd_overflows<T1>(product_magnitude, product_negative, exceeds_width);
}

namespace detail {

// See: https://eel.is/c++draft/utility.intcmp
// [Note 1: These function templates cannot be used to compare byte, char, char8_t, char16_t, char32_t, wchar_t, and bool. end note]
template <typename T>
struct valid_comparison_type
{
    static constexpr bool value = std::is_integral<T>::value &&
                                  !std::is_same<T, char>::value &&
                                      !std::is_same<T, char16_t>::value &&
                                          !std::is_same<T, char32_t>::value &&
                                              !std::is_same<T, wchar_t>::value &&
                                                  !std::is_same<T, bool>::value
                                                    #if defined(__cpp_char8_t)
                                                    && !std::is_same<T, char8_t>::value
                                                    #endif
                                                    #if defined(__cpp_lib_byte) && __cpp_lib_byte >= 201603L
                                                    && !std::is_same<T, std::byte>::value
                                                    #endif
    ;
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_valid_comparison_type_v = valid_comparison_type<T>::value;

// Allow the builtins to be used when available
template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_int128_type_v = std::is_same<T, int128>::value ||
                                                      std::is_same<T, uint128>::value
    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)
                                                      || std::is_same<T, builtin_i128>::value
                                                      || std::is_same<T, builtin_u128>::value
    #endif
                                                      ;

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_valid_comparison_operand_v = is_valid_comparison_type_v<T> ||
                                                                   is_int128_type_v<T>;

// Maps the builtin 128-bit types onto the library equivalents
template <typename T>
struct comparison_canonical
{
    using type = T;
};

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

template <>
struct comparison_canonical<builtin_i128>
{
    using type = int128;
};

template <>
struct comparison_canonical<builtin_u128>
{
    using type = uint128;
};

#endif

template <typename T>
using comparison_canonical_t = typename comparison_canonical<T>::type;

template <typename T>
BOOST_INT128_HOST_DEVICE constexpr comparison_canonical_t<T> canonical_comparison_operand(const T value) noexcept
{
    return static_cast<comparison_canonical_t<T>>(value);
}

// Mathematical equality of two integers regardless of their signedness, via the
// same (sign, magnitude) decomposition.
template <typename T, typename U>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_equal_impl(const T lhs, const U rhs) noexcept
{
    const auto a {ckd_decompose(canonical_comparison_operand(lhs))};
    const auto b {ckd_decompose(canonical_comparison_operand(rhs))};

    return (a.negative == b.negative) && (a.magnitude == b.magnitude);
}

// Mathematical less-than of two integers regardless of their signedness, via the
// same (sign, magnitude) decomposition.
template <typename T, typename U>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_less_impl(const T lhs, const U rhs) noexcept
{
    const auto a {ckd_decompose(canonical_comparison_operand(lhs))};
    const auto b {ckd_decompose(canonical_comparison_operand(rhs))};

    if (a.negative != b.negative)
    {
        return a.negative;
    }

    return a.negative ? (a.magnitude > b.magnitude) : (a.magnitude < b.magnitude);
}

template <typename T, typename U>
BOOST_INT128_INLINE_CONSTEXPR bool enable_comparison_v = is_valid_comparison_operand_v<T> &&
                                                        is_valid_comparison_operand_v<U> &&
                                                        (is_int128_type_v<T> || is_int128_type_v<U>);

} // namespace detail

// C++26 integer comparison functions (https://eel.is/c++draft/utility.intcmp)
// extended to the library and builtin 128-bit types and available from C++14.

BOOST_INT128_EXPORT template <typename T, typename U, std::enable_if_t<detail::enable_comparison_v<T, U>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_equal(const T lhs, const U rhs) noexcept
{
    return detail::cmp_equal_impl(lhs, rhs);
}

BOOST_INT128_EXPORT template <typename T, typename U, std::enable_if_t<detail::enable_comparison_v<T, U>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_not_equal(const T lhs, const U rhs) noexcept
{
    return !detail::cmp_equal_impl(lhs, rhs);
}

BOOST_INT128_EXPORT template <typename T, typename U, std::enable_if_t<detail::enable_comparison_v<T, U>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_less(const T lhs, const U rhs) noexcept
{
    return detail::cmp_less_impl(lhs, rhs);
}

BOOST_INT128_EXPORT template <typename T, typename U, std::enable_if_t<detail::enable_comparison_v<T, U>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_greater(const T lhs, const U rhs) noexcept
{
    return detail::cmp_less_impl(rhs, lhs);
}

BOOST_INT128_EXPORT template <typename T, typename U, std::enable_if_t<detail::enable_comparison_v<T, U>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_less_equal(const T lhs, const U rhs) noexcept
{
    return !detail::cmp_less_impl(rhs, lhs);
}

BOOST_INT128_EXPORT template <typename T, typename U, std::enable_if_t<detail::enable_comparison_v<T, U>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool cmp_greater_equal(const T lhs, const U rhs) noexcept
{
    return !detail::cmp_less_impl(lhs, rhs);
}

// Whether t is representable in the target type R.
BOOST_INT128_EXPORT template <typename R, typename T, std::enable_if_t<detail::enable_comparison_v<R, T>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr bool in_range(const T t) noexcept
{
    using limits = std::numeric_limits<detail::comparison_canonical_t<R>>;

    return !detail::cmp_less_impl(t, (limits::min)()) &&
           !detail::cmp_less_impl((limits::max)(), t);
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_UTILITIES_HPP
// ===== END boost/int128/utilities.hpp =====

#ifndef BOOST_INT128_BUILD_MODULE

#include <limits>
#include <utility>

#endif

namespace boost {
namespace int128 {

namespace detail {

template <typename IntegerType>
struct reduced_integers
{
    static constexpr bool value {std::is_same<IntegerType, signed char>::value ||
                                 std::is_same<IntegerType, unsigned char>::value ||
                                 std::is_same<IntegerType, signed short>::value ||
                                 std::is_same<IntegerType, unsigned short>::value ||
                                 std::is_same<IntegerType, signed int>::value ||
                                 std::is_same<IntegerType, unsigned int>::value ||
                                 std::is_same<IntegerType, signed long>::value ||
                                 std::is_same<IntegerType, unsigned long>::value ||
                                 std::is_same<IntegerType, signed long long>::value ||
                                 std::is_same<IntegerType, unsigned long long>::value ||
                                 std::is_same<IntegerType, int128>::value ||
                                 std::is_same<IntegerType, uint128>::value};
};

#if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)

template <typename IntegerType>
BOOST_INT128_INLINE_CONSTEXPR bool is_reduced_integer_v {reduced_integers<IntegerType>::value ||
                                              std::is_same<IntegerType, detail::builtin_i128>::value ||
                                              std::is_same<IntegerType, detail::builtin_u128>::value};

#else

template <typename IntegerType>
BOOST_INT128_INLINE_CONSTEXPR bool is_reduced_integer_v {reduced_integers<IntegerType>::value};

#endif // 128-bit

} // namespace detail

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 saturating_add(const uint128 x, const uint128 y) noexcept
{
    const auto z {x + y};

    if (z < x)
    {
        return (std::numeric_limits<uint128>::max)();
    }

    return z;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 saturating_sub(const uint128 x, const uint128 y) noexcept
{
    const auto z {x - y};

    if (z > x)
    {
        return (std::numeric_limits<uint128>::min)();
    }

    return z;
}

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4307) // Addition Overflow
#  pragma warning(disable : 4146) // Unary minus applied to unsigned type
#endif

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 saturating_add(const int128 x, const int128 y) noexcept
{
    // Detect overflow BEFORE the addition to avoid signed overflow UB.
    // When both are non-negative: overflow iff x > max - y (subtraction safe: max - non_negative >= 0)
    // When both are negative: overflow iff x < min - y (subtraction safe: min - negative > min)
    // Mixed signs: overflow is impossible.

    if (x.signed_high() >= 0 && y.signed_high() >= 0)
    {
        if (x > (std::numeric_limits<int128>::max)() - y)
        {
            return (std::numeric_limits<int128>::max)();
        }
    }
    else if (x.signed_high() < 0 && y.signed_high() < 0)
    {
        if (x < (std::numeric_limits<int128>::min)() - y)
        {
            return (std::numeric_limits<int128>::min)();
        }
    }

    return x + y;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 saturating_sub(const int128 x, const int128 y) noexcept
{
    // Detect overflow BEFORE the subtraction to avoid signed overflow UB.
    // Positive overflow: x >= 0 and y < 0 and x > max + y (safe: max + negative < max)
    // Negative overflow: x < 0 and y >= 0 and x < min + y (safe: min + non_negative > min)
    // Same signs: overflow is impossible.

    if (x.signed_high() >= 0 && y.signed_high() < 0)
    {
        if (x > (std::numeric_limits<int128>::max)() + y)
        {
            return (std::numeric_limits<int128>::max)();
        }
    }
    else if (x.signed_high() < 0 && y.signed_high() >= 0)
    {
        if (x < (std::numeric_limits<int128>::min)() + y)
        {
            return (std::numeric_limits<int128>::min)();
        }
    }

    return x - y;
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 saturating_mul(const uint128 x, const uint128 y) noexcept
{
    uint128 res {};
    return ckd_mul(&res, x, y) ? (std::numeric_limits<uint128>::max)() : res;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 saturating_mul(const int128 x, const int128 y) noexcept
{
    int128 res {};
    const auto overflowed {ckd_mul(&res, x, y)};

    if (overflowed)
    {
        return (x < 0) != (y < 0) ? (std::numeric_limits<int128>::min)() : (std::numeric_limits<int128>::max)();
    }

    return res;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 saturating_div(const uint128 x, const uint128 y) noexcept
{
    return x / y;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 saturating_div(const int128 x, const int128 y) noexcept
{
    if (BOOST_INT128_UNLIKELY(x == (std::numeric_limits<int128>::min)() && y == -1))
    {
        // This is the only possible case of overflow
        return (std::numeric_limits<int128>::max)();
    }

    return x / y;
}

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable: 4267)
#endif

BOOST_INT128_EXPORT template <typename TargetType, std::enable_if_t<detail::is_reduced_integer_v<TargetType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr TargetType saturating_cast(const uint128 value) noexcept
{
    BOOST_INT128_IF_CONSTEXPR (std::is_same<uint128, TargetType>::value)
    {
        return static_cast<TargetType>(value);
    }
    else
    {
        if (value > static_cast<uint128>((std::numeric_limits<TargetType>::max)()))
        {
            return (std::numeric_limits<TargetType>::max)();
        }

        return static_cast<TargetType>(value);
    }
}

#ifdef _MSC_VER
#  pragma warning(pop)
#endif

BOOST_INT128_EXPORT template <typename TargetType, std::enable_if_t<detail::is_reduced_integer_v<TargetType>, bool> = true>
BOOST_INT128_HOST_DEVICE constexpr TargetType saturating_cast(const int128 value) noexcept
{
    BOOST_INT128_IF_CONSTEXPR (std::is_same<int128, TargetType>::value)
    {
        return static_cast<TargetType>(value);
    }
    #if defined(BOOST_INT128_HAS_INT128) || defined(BOOST_INT128_HAS_MSVC_INT128)
    else BOOST_INT128_IF_CONSTEXPR (std::is_same<uint128, TargetType>::value || std::is_same<detail::builtin_u128, TargetType>::value)
    #else
    else BOOST_INT128_IF_CONSTEXPR (std::is_same<uint128, TargetType>::value)
    #endif
    {
        // We can't possibly have overflow in this case
        return value < 0 ? static_cast<TargetType>(0) : static_cast<TargetType>(value);
    }
    else
    {
        if (value > static_cast<int128>((std::numeric_limits<TargetType>::max)()))
        {
            return (std::numeric_limits<TargetType>::max)();
        }
        else if (value < static_cast<int128>((std::numeric_limits<TargetType>::min)()))
        {
            return (std::numeric_limits<TargetType>::min)();
        }

        return static_cast<TargetType>(value);
    }
}

namespace detail {

BOOST_int128EST_EXPORT BOOST_INT128_HOST_DEVICE constexpr std::uint64_t gcd64(std::uint64_t x, std::uint64_t y) noexcept
{
    if (x == 0)
    {
        return y;
    }
    if (y == 0)
    {
        return x;
    }

    const auto s {impl::countr_impl(x | y)};
    x >>= impl::countr_impl(x);

    do
    {
        y >>= impl::countr_impl(y);
        if (x > y)
        {
            const auto temp {x};
            x = y;
            y = temp;
        }

        y -= x;
    } while (y);

    return x << s;
}

} // namespace detail

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 gcd(uint128 a, uint128 b) noexcept
{
    // Base case
    if (a == 0U)
    {
        return b;
    }
    if (b == 0U)
    {
        return a;
    }

    const auto a_zero {countr_zero(a)};
    const auto b_zero {countr_zero(b)};
    const auto shift {b_zero < a_zero ? b_zero : a_zero};
    a >>= shift;
    b >>= shift;

    // The invariant of the loop below is that a is odd
    a >>= countr_zero(a);

    while (true)
    {
        b >>= countr_zero(b);

        if (a > b)
        {
            const uint128 temp {a};
            a = b;
            b = temp;
        }

        b -= a;

        // The result is a itself, whatever its width: dropping to gcd64 here would
        // discard a.high (gcd(2^64 + 1, 2^64 + 1) used to return 1)
        if (b == 0U)
        {
            return a << shift;
        }

        // Stop doing 128-bit math as soon as we can
        if ((a.high | b.high) == UINT64_C(0))
        {
            return uint128{0, detail::gcd64(a.low, b.low)} << shift;
        }
    }
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 gcd(const int128 a, const int128 b) noexcept
{
    return static_cast<int128>(gcd(static_cast<uint128>(abs(a)), static_cast<uint128>(abs(b))));
}

// For unknown reasons this implementation fails for MSVC x86 only in release mode
// Directly calculating leads to the same failures, so unfortunately we have a viable,
// but very slow impl that we know works.
#if !(defined(_M_IX86) && !defined(_NDEBUG))

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 lcm(const uint128 a, const uint128 b) noexcept
{
    if (a == 0U || b == 0U)
    {
        return static_cast<uint128>(0);
    }

    // Calculate GCD first
    const auto g {gcd(a, b)};

    // Compute LCM avoiding overflow: (a/gcd) * b
    return (a / g) * b;
}

#else

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 lcm(uint128 a, uint128 b) noexcept
{
    if (a == 0U || b == 0U)
    {
        return uint128{0};
    }


    unsigned shift{};
    while ((a & 1U) == 0U && (b & 1U) == 0U)
    {
        a >>= 1U;
        b >>= 1U;
        shift++;
    }

    // Ensure a >= b
    if (a < b)
    {
        std::swap(a, b);
    }

    uint128 lcm{a};

    while (lcm % b != 0U)
    {
        lcm += a;
    }

    return lcm << shift;
}

#endif

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 lcm(const int128 a, const int128 b) noexcept
{
    return static_cast<int128>(lcm(static_cast<uint128>(abs(a)), static_cast<uint128>(abs(b))));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 midpoint(const uint128 a, const uint128 b) noexcept
{
    // Bit manipulation formula works for unsigned integers
    auto mid {(a & b) + ((a ^ b) >> 1)};

    // std::midpoint rounds towards the first parameter
    if ((a ^ b) & 1U && a > b)
    {
        ++mid;
    }

    return mid;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 midpoint(const int128 a, const int128 b) noexcept
{
    // For signed integers, we use a + (b - a) / 2 or a - (a - b) / 2
    // The subtraction is done in unsigned arithmetic to handle overflow correctly
    // Integer division automatically rounds toward the first argument
    //
    // Use direct field access for both the uint128 construction and the
    // comparison to avoid NVCC host compiler issues with operator<= and
    // static_cast on int128 for large-magnitude values

    const uint128 ua {a.high, a.low};
    const uint128 ub {b.high, b.low};

    const bool a_le_b {a.high == b.high ? a.low <= b.low : a.signed_high() < b.signed_high()};

    if (a_le_b)
    {
        // diff = b - a (computed in unsigned, handles wrap-around correctly)
        const auto diff {ub - ua};
        return a + static_cast<int128>(diff / 2U);
    }
    else
    {
        // diff = a - b (computed in unsigned, handles wrap-around correctly)
        const auto diff {ua - ub};
        return a - static_cast<int128>(diff / 2U);
    }
}

// Quotient and remainder of a single division, following the div_result<T> proposed for the
// standard library by P3724 (Integer division)
BOOST_INT128_EXPORT template <typename T>
struct div_result
{
    T quotient;
    T remainder;
};

BOOST_INT128_EXPORT template <typename T>
BOOST_INT128_HOST_DEVICE constexpr bool operator==(const div_result<T>& lhs, const div_result<T>& rhs) noexcept
{
    return lhs.quotient == rhs.quotient && lhs.remainder == rhs.remainder;
}

BOOST_INT128_EXPORT template <typename T>
BOOST_INT128_HOST_DEVICE constexpr bool operator!=(const div_result<T>& lhs, const div_result<T>& rhs) noexcept
{
    return !(lhs == rhs);
}

#ifdef BOOST_INT128_HAS_SPACESHIP_OPERATOR

BOOST_INT128_EXPORT template <typename T>
BOOST_INT128_HOST_DEVICE constexpr std::strong_ordering operator<=>(const div_result<T>& lhs, const div_result<T>& rhs) noexcept
{
    const auto quotient_order {lhs.quotient <=> rhs.quotient};
    return quotient_order != std::strong_ordering::equal ? quotient_order : lhs.remainder <=> rhs.remainder;
}

#endif // BOOST_INT128_HAS_SPACESHIP_OPERATOR

namespace detail {

// -1 when the exact quotient of x / y is negative, and 1 otherwise
BOOST_INT128_HOST_DEVICE constexpr int quotient_sign(const int128 x, const int128 y) noexcept
{
    return (x < 0) != (y < 0) ? -1 : 1;
}

// Applies the quotient offset d (-1, 0, or 1) to a truncated division result, and returns the
// remainder matching the adjusted quotient. The remainder is evaluated in unsigned arithmetic
// so that the d * y term cannot overflow when y is INT128_MIN.
BOOST_INT128_HOST_DEVICE constexpr div_result<int128> offset_quotient(const i128div_t truncated, const int128 y, const int d) noexcept
{
    const uint128 unsigned_rem {truncated.rem.high, truncated.rem.low};
    const uint128 unsigned_y {y.high, y.low};

    uint128 rem {unsigned_rem};

    if (d > 0)
    {
        rem = unsigned_rem - unsigned_y;
    }
    else if (d < 0)
    {
        rem = unsigned_rem + unsigned_y;
    }

    return div_result<int128>{truncated.quot + d, static_cast<int128>(rem)};
}

// An unsigned quotient is never rounded down, so the only offsets are 0 and 1. The remainder
// of an incremented quotient is negative, and is returned reduced modulo 2^128.
BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> offset_quotient(const u128div_t truncated, const uint128 y, const bool increment) noexcept
{
    return div_result<uint128>{increment ? truncated.quot + 1U : truncated.quot,
                                 increment ? truncated.rem - y : truncated.rem};
}

// Round-to-nearest comparison shared by the ties functions: the truncated quotient grows in
// magnitude when the remainder is more than half the divisor. truncate_ties selects the strict
// form, which both breaks an exact tie towards zero and recovers the bit that abs(y) / 2 drops
// when y is odd.
BOOST_INT128_HOST_DEVICE constexpr bool nearest_increment(const uint128 abs_rem, const uint128 abs_half_y, const bool truncate_ties) noexcept
{
    return truncate_ties ? abs_rem > abs_half_y : abs_rem >= abs_half_y;
}

// Magnitude of the remainder of a truncated signed division. The magnitude is always less
// than abs(y), so it is representable for every valid divisor.
BOOST_INT128_HOST_DEVICE constexpr uint128 abs_remainder(const i128div_t truncated) noexcept
{
    return static_cast<uint128>(abs(truncated.rem));
}

// floor(abs(y) / 2), exact for every y including INT128_MIN
BOOST_INT128_HOST_DEVICE constexpr uint128 abs_half_divisor(const int128 y) noexcept
{
    return static_cast<uint128>(abs(y)) >> 1U;
}

// An odd divisor cannot produce an exact tie, so every ties function truncates on it
BOOST_INT128_HOST_DEVICE constexpr bool is_odd(const int128 x) noexcept
{
    return (x.low & 1U) != 0U;
}

BOOST_INT128_HOST_DEVICE constexpr bool is_odd(const uint128 x) noexcept
{
    return (x.low & 1U) != 0U;
}

} // namespace detail

// Rounds towards zero, which is what operator/ already does
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_to_zero(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return div_result<uint128>{truncated.quot, truncated.rem};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_to_zero(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    return div_result<int128>{truncated.quot, truncated.rem};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_to_zero(const uint128 x, const uint128 y) noexcept
{
    return x / y;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_to_zero(const int128 x, const int128 y) noexcept
{
    return x / y;
}

// Rounds away from zero, so the quotient grows in magnitude unless the division is exact
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_away_zero(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, truncated.rem != 0U);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_away_zero(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, truncated.rem != 0 ? detail::quotient_sign(x, y) : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_away_zero(const uint128 x, const uint128 y) noexcept
{
    return div_rem_away_zero(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_away_zero(const int128 x, const int128 y) noexcept
{
    return div_rem_away_zero(x, y).quotient;
}

// Rounds towards positive infinity, which for an unsigned quotient is away from zero
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_to_pos_inf(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, truncated.rem != 0U);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_to_pos_inf(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto adjust {truncated.rem != 0 && detail::quotient_sign(x, y) > 0};
    return detail::offset_quotient(truncated, y, adjust ? 1 : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_to_pos_inf(const uint128 x, const uint128 y) noexcept
{
    return div_rem_to_pos_inf(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_to_pos_inf(const int128 x, const int128 y) noexcept
{
    return div_rem_to_pos_inf(x, y).quotient;
}

// Rounds towards negative infinity, which for an unsigned quotient is truncation
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_to_neg_inf(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return div_result<uint128>{truncated.quot, truncated.rem};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_to_neg_inf(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto adjust {truncated.rem != 0 && detail::quotient_sign(x, y) < 0};
    return detail::offset_quotient(truncated, y, adjust ? -1 : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_to_neg_inf(const uint128 x, const uint128 y) noexcept
{
    return x / y;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_to_neg_inf(const int128 x, const int128 y) noexcept
{
    return div_rem_to_neg_inf(x, y).quotient;
}

// Euclidean division, whose remainder is always in [0, abs(y)). Only a negative remainder
// needs fixing, and growing the quotient magnitude by one makes the remainder positive.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_euclid(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return div_result<uint128>{truncated.quot, truncated.rem};
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_euclid(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, truncated.rem < 0 ? detail::quotient_sign(x, y) : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_euclid(const uint128 x, const uint128 y) noexcept
{
    return x / y;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_euclid(const int128 x, const int128 y) noexcept
{
    return div_rem_euclid(x, y).quotient;
}

// Rounds to nearest, breaking an exact tie towards zero
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_ties_to_zero(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, detail::nearest_increment(truncated.rem, y >> 1U, true));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_ties_to_zero(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto increment {detail::nearest_increment(detail::abs_remainder(truncated), detail::abs_half_divisor(y), true)};
    return detail::offset_quotient(truncated, y, increment ? detail::quotient_sign(x, y) : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_ties_to_zero(const uint128 x, const uint128 y) noexcept
{
    return div_rem_ties_to_zero(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_ties_to_zero(const int128 x, const int128 y) noexcept
{
    return div_rem_ties_to_zero(x, y).quotient;
}

// Rounds to nearest, breaking an exact tie away from zero
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_ties_away_zero(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, detail::nearest_increment(truncated.rem, y >> 1U, detail::is_odd(y)));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_ties_away_zero(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto increment {detail::nearest_increment(detail::abs_remainder(truncated), detail::abs_half_divisor(y), detail::is_odd(y))};
    return detail::offset_quotient(truncated, y, increment ? detail::quotient_sign(x, y) : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_ties_away_zero(const uint128 x, const uint128 y) noexcept
{
    return div_rem_ties_away_zero(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_ties_away_zero(const int128 x, const int128 y) noexcept
{
    return div_rem_ties_away_zero(x, y).quotient;
}

// Rounds to nearest, breaking an exact tie towards positive infinity. A tie only grows the
// magnitude when the quotient is positive.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_ties_to_pos_inf(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, detail::nearest_increment(truncated.rem, y >> 1U, detail::is_odd(y)));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_ties_to_pos_inf(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto sign {detail::quotient_sign(x, y)};
    const auto increment {detail::nearest_increment(detail::abs_remainder(truncated), detail::abs_half_divisor(y), detail::is_odd(y) || sign < 0)};
    return detail::offset_quotient(truncated, y, increment ? sign : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_ties_to_pos_inf(const uint128 x, const uint128 y) noexcept
{
    return div_rem_ties_to_pos_inf(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_ties_to_pos_inf(const int128 x, const int128 y) noexcept
{
    return div_rem_ties_to_pos_inf(x, y).quotient;
}

// Rounds to nearest, breaking an exact tie towards negative infinity. A tie only grows the
// magnitude when the quotient is negative, so an unsigned tie always truncates.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_ties_to_neg_inf(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    return detail::offset_quotient(truncated, y, detail::nearest_increment(truncated.rem, y >> 1U, true));
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_ties_to_neg_inf(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto sign {detail::quotient_sign(x, y)};
    const auto increment {detail::nearest_increment(detail::abs_remainder(truncated), detail::abs_half_divisor(y), detail::is_odd(y) || sign > 0)};
    return detail::offset_quotient(truncated, y, increment ? sign : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_ties_to_neg_inf(const uint128 x, const uint128 y) noexcept
{
    return div_rem_ties_to_neg_inf(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_ties_to_neg_inf(const int128 x, const int128 y) noexcept
{
    return div_rem_ties_to_neg_inf(x, y).quotient;
}

// Rounds to nearest, breaking an exact tie to the odd quotient, so a tie only grows the
// magnitude when truncation would have produced an even quotient
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_ties_to_odd(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto increment {detail::nearest_increment(truncated.rem, y >> 1U, detail::is_odd(y) || detail::is_odd(truncated.quot))};
    return detail::offset_quotient(truncated, y, increment);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_ties_to_odd(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto truncate_ties {detail::is_odd(y) || detail::is_odd(truncated.quot)};
    const auto increment {detail::nearest_increment(detail::abs_remainder(truncated), detail::abs_half_divisor(y), truncate_ties)};
    return detail::offset_quotient(truncated, y, increment ? detail::quotient_sign(x, y) : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_ties_to_odd(const uint128 x, const uint128 y) noexcept
{
    return div_rem_ties_to_odd(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_ties_to_odd(const int128 x, const int128 y) noexcept
{
    return div_rem_ties_to_odd(x, y).quotient;
}

// Rounds to nearest, breaking an exact tie to the even quotient, so a tie only grows the
// magnitude when truncation would have produced an odd quotient
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<uint128> div_rem_ties_to_even(const uint128 x, const uint128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto increment {detail::nearest_increment(truncated.rem, y >> 1U, detail::is_odd(y) || !detail::is_odd(truncated.quot))};
    return detail::offset_quotient(truncated, y, increment);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr div_result<int128> div_rem_ties_to_even(const int128 x, const int128 y) noexcept
{
    const auto truncated {div(x, y)};
    const auto truncate_ties {detail::is_odd(y) || !detail::is_odd(truncated.quot)};
    const auto increment {detail::nearest_increment(detail::abs_remainder(truncated), detail::abs_half_divisor(y), truncate_ties)};
    return detail::offset_quotient(truncated, y, increment ? detail::quotient_sign(x, y) : 0);
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 div_ties_to_even(const uint128 x, const uint128 y) noexcept
{
    return div_rem_ties_to_even(x, y).quotient;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 div_ties_to_even(const int128 x, const int128 y) noexcept
{
    return div_rem_ties_to_even(x, y).quotient;
}

// The Euclidean remainder, which is always in [0, abs(y)). Only a negative remainder needs
// fixing, and abs(y) is added in unsigned arithmetic so that INT128_MIN is handled.
BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr uint128 rem_euclid(const uint128 x, const uint128 y) noexcept
{
    return x % y;
}

BOOST_INT128_EXPORT BOOST_INT128_HOST_DEVICE constexpr int128 rem_euclid(const int128 x, const int128 y) noexcept
{
    const auto rem {x % y};

    if (rem < 0)
    {
        const uint128 unsigned_rem {rem.high, rem.low};
        return static_cast<int128>(unsigned_rem + static_cast<uint128>(abs(y)));
    }

    return rem;
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_NUMERIC_HPP
// ===== END boost/int128/numeric.hpp =====
// ===== BEGIN boost/int128/format.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_FORMAT_HPP
#define BOOST_INT128_FORMAT_HPP

// The feature-test macros below are only visible once <version> has been seen.
// The module build gets them from the global module fragment instead.
#ifndef BOOST_INT128_BUILD_MODULE
#  if __has_include(<version>)
#    include <version>
#  endif
#endif

#if __has_include(<format>) && defined(__cpp_lib_format) && __cpp_lib_format >= 201907L

// [amalgamate] skipped duplicate include of boost/int128/detail/mini_to_chars.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/config.hpp
// [amalgamate] skipped duplicate include of boost/int128/int128.hpp

#ifndef BOOST_INT128_BUILD_MODULE
#include <string>
#include <format>
#include <tuple>
#endif

#define BOOST_INT128_HAS_FORMAT

#if defined(__cpp_lib_constexpr_format) && __cpp_lib_constexpr_format >= 202511L
#  define BOOST_INT128_HAS_CONSTEXPR_FORMAT
#  define BOOST_INT128_CONSTEXPR_FORMAT constexpr
#else
#  define BOOST_INT128_CONSTEXPR_FORMAT
#endif

namespace boost::int128::detail {

enum class sign_option
{
    plus,
    negative,
    space
};

enum class alignment
{
    none,
    left,   // <
    right,  // >
    center  // ^
};

template <typename ParseContext>
constexpr auto parse_impl(ParseContext& ctx)
{
    auto it {ctx.begin()};
    int base = 10;
    bool is_upper = false;
    int padding_digits = 0;
    auto sign = sign_option::negative;
    bool prefix = false;
    bool zero_pad = false;
    char fill_char = ' ';
    auto align = alignment::none;

    // Parse fill and alignment: [[fill]align]
    // Alignment characters are: < (left), > (right), ^ (center)
    if (it != ctx.end())
    {
        // Check if we have [fill]align (fill char followed by alignment)
        auto next = it;
        ++next;
        if (next != ctx.end() && (*next == '<' || *next == '>' || *next == '^'))
        {
            fill_char = *it;
            it = next;
            switch (*it)
            {
                case '<':
                    align = alignment::left;
                    break;
                case '>':
                    align = alignment::right;
                    break;
                case '^':
                    align = alignment::center;
                    break;
                default:                        // LCOV_EXCL_LINE
                    BOOST_INT128_UNREACHABLE;   // LCOV_EXCL_LINE
            }
            ++it;
        }
        // Check if we just have align (no fill char)
        else if (*it == '<' || *it == '>' || *it == '^')
        {
            switch (*it)
            {
                case '<':
                    align = alignment::left;
                    break;
                case '>':
                    align = alignment::right;
                    break;
                case '^':
                    align = alignment::center;
                    break;
                default:                        // LCOV_EXCL_LINE
                    BOOST_INT128_UNREACHABLE;   // LCOV_EXCL_LINE
            }
            ++it;
        }
    }

    // Handle sign or space
    if (it != ctx.end())
    {
        switch (*it) {
            case ' ':
                sign = sign_option::space;
                ++it;
                break;
            case '+':
                sign = sign_option::plus;
                ++it;
                break;
            case '-':
                sign = sign_option::negative;
                ++it;
                break;
            default:
                break;
        }
    }

    // Alternate form option
    if (it != ctx.end() && *it == '#')
    {
        prefix = true;
        ++it;
    }

    // Zero-pad flag (std::format places '0' before the width)
    if (it != ctx.end() && *it == '0')
    {
        zero_pad = true;
        ++it;
    }

    // Parse the width
    while (it != ctx.end() && *it >= '0' && *it <= '9')
    {
        padding_digits = padding_digits * 10 + (*it - '0');
        ++it;
    }

    // Integer presentation
    if (it != ctx.end() && *it != '}')
    {
        switch (*it++)
        {
            case 'b':
                base = 2;
                break;
            case 'B':
                base = 2;
                is_upper = true;
                break;

            case 'o':
                base = 8;
                break;

            case 'd':
                base = 10;
                break;

            case 'x':
                base = 16;
                break;
            case 'X':
                base = 16;
                is_upper = true;
                break;
            default:                                                                                // LCOV_EXCL_LINE
                BOOST_INT128_THROW_EXCEPTION(std::format_error("Unsupported format specifier"));    // LCOV_EXCL_LINE
        }
    }

    // Verify we're at the closing brace
    if (it != ctx.end() && *it != '}')
    {
        BOOST_INT128_THROW_EXCEPTION(std::format_error("Expected '}' in format string")); // LCOV_EXCL_LINE
    }

    return std::make_tuple(base, padding_digits, sign, is_upper, prefix, zero_pad, fill_char, align, it);
}

template <typename T>
struct is_library_type_impl
{
    static constexpr bool value {std::is_same_v<T, boost::int128::uint128> || std::is_same_v<T, boost::int128::int128>};
};

template <typename T>
BOOST_INT128_INLINE_CONSTEXPR bool is_library_type_v = is_library_type_impl<T>::value;

template <typename T>
concept is_library_type = is_library_type_v<T>;

} // namespace boost::int128::detail

namespace std {

template <boost::int128::detail::is_library_type T>
struct formatter<T>
{
    int base;
    int padding_digits;
    boost::int128::detail::sign_option sign;
    bool is_upper;
    bool prefix;
    bool zero_pad;
    char fill_char;
    boost::int128::detail::alignment align;

    constexpr formatter() : base {10},
                            padding_digits {0},
                            sign {boost::int128::detail::sign_option::negative},
                            is_upper {false},
                            prefix {false},
                            zero_pad {false},
                            fill_char {' '},
                            align {boost::int128::detail::alignment::none}
    {}

    constexpr auto parse(format_parse_context& ctx)
    {
        const auto res {boost::int128::detail::parse_impl(ctx)};

        base = std::get<0>(res);
        padding_digits = std::get<1>(res);
        sign = std::get<2>(res);
        is_upper = std::get<3>(res);
        prefix = std::get<4>(res);
        zero_pad = std::get<5>(res);
        fill_char = std::get<6>(res);
        align = std::get<7>(res);

        return std::get<8>(res);
    }

    template <typename FormatContext>
    BOOST_INT128_CONSTEXPR_FORMAT auto format(T v, FormatContext& ctx) const
    {
        char buffer[boost::int128::detail::mini_to_chars_buffer_size];
        bool isneg {false};
        boost::int128::uint128 abs_v {};

        if constexpr (std::is_same_v<T, boost::int128::int128>)
        {
            if (v < 0)
            {
                isneg = true;
                // Can't negate int128::min(), handle specially
                if (v == (std::numeric_limits<T>::min)())
                {
                    abs_v = boost::int128::uint128{UINT64_C(0x8000000000000000), 0};
                }
                else
                {
                    abs_v = static_cast<boost::int128::uint128>(-v);
                }
            }
            else
            {
                abs_v = static_cast<boost::int128::uint128>(v);
            }
        }
        else
        {
            abs_v = static_cast<boost::int128::uint128>(v);
        }

        const auto end = boost::int128::detail::mini_to_chars(buffer, abs_v, base, is_upper);
        std::string s(end, buffer + sizeof(buffer));

        // The alternate form never adds an octal prefix to a zero: std::format("{:#o}", 0) is "0"
        const bool add_prefix {prefix && !(base == 8 && abs_v == 0U)};

        // Calculate prefix length that will be added later
        std::size_t prefix_len {0};
        if (add_prefix)
        {
            switch (base)
            {
                case 2:
                case 16:
                    prefix_len = 2;  // "0b", "0B", "0x", or "0X"
                    break;
                case 8:
                    prefix_len = 1;  // "0"
                    break;
                default:
                    break;
            }
        }

        // Calculate sign length that will be added later
        std::size_t sign_len {0};
        if (sign == boost::int128::detail::sign_option::plus || sign == boost::int128::detail::sign_option::space || isneg)
        {
            sign_len = 1;
        }

        // Zero-padding applies only with the '0' flag and no explicit alignment.
        // Account for prefix and sign in the padding calculation.
        if (zero_pad && align == boost::int128::detail::alignment::none && padding_digits > 0)
        {
            auto target_digit_width {static_cast<std::size_t>(padding_digits)};
            if (target_digit_width > prefix_len + sign_len)
            {
                target_digit_width -= prefix_len + sign_len;
            }
            else
            {
                target_digit_width = 0;
            }

            if (s.size() - 1u < target_digit_width)
            {
                s.insert(s.begin(), target_digit_width - s.size() + 1u, '0');
            }
        }

        if (add_prefix)
        {
            switch (base)
            {
                case 2:
                    if (is_upper)
                    {
                        s.insert(s.begin(), 'B');
                    }
                    else
                    {
                        s.insert(s.begin(), 'b');
                    }
                    s.insert(s.begin(), '0');
                    break;
                case 8:
                    s.insert(s.begin(), '0');
                    break;
                case 16:
                    if (is_upper)
                    {
                        s.insert(s.begin(), 'X');
                    }
                    else
                    {
                        s.insert(s.begin(), 'x');
                    }
                    s.insert(s.begin(), '0');
                    break;
                default:
                    // Nothing to do
                    break;
            }
        }

        // Insert our sign
        switch (sign)
        {
            case boost::int128::detail::sign_option::plus:
                if (isneg)
                {
                    s.insert(s.begin(), '-');
                }
                else
                {
                    s.insert(s.begin(), '+');
                }
                break;
            case boost::int128::detail::sign_option::space:
                if (!isneg)
                {
                    s.insert(s.begin(), ' ');
                }
                if constexpr (std::is_same_v<T, boost::int128::int128>)
                {
                    if (isneg)
                    {
                        s.insert(s.begin(), '-');
                    }
                }
                break;
            case boost::int128::detail::sign_option::negative:
                if constexpr (std::is_same_v<T, boost::int128::int128>)
                {
                    if (isneg)
                    {
                        s.insert(s.begin(), '-');
                    }
                }
                break;
            default:                        // LCOV_EXCL_LINE
                BOOST_INT128_UNREACHABLE;   // LCOV_EXCL_LINE
        }

        s.erase(0, s.find_first_not_of('\0'));
        s.erase(s.find_last_not_of('\0') + 1);

        // Apply alignment. An explicit alignment uses fill_char; with no explicit
        // alignment and no zero-padding, integer types default to right alignment
        // with the fill character (matching std::format).
        if (s.size() < static_cast<std::size_t>(padding_digits) &&
            (align != boost::int128::detail::alignment::none || !zero_pad))
        {
            auto fill_count = static_cast<std::size_t>(padding_digits) - s.size();
            const auto effective_align = (align == boost::int128::detail::alignment::none)
                                             ? boost::int128::detail::alignment::right
                                             : align;
            switch (effective_align)
            {
                case boost::int128::detail::alignment::left:
                    s.append(fill_count, fill_char);
                    break;
                case boost::int128::detail::alignment::right:
                    s.insert(s.begin(), fill_count, fill_char);
                    break;
                case boost::int128::detail::alignment::center:
                {
                    auto left_fill = fill_count / 2;
                    auto right_fill = fill_count - left_fill;
                    s.insert(s.begin(), left_fill, fill_char);
                    s.append(right_fill, fill_char);
                    break;
                }
                default:                        // LCOV_EXCL_LINE
                    break;                      // LCOV_EXCL_LINE
            }
        }

        return std::format_to(ctx.out(), "{}", s);
    }
};

} // namespace std

#endif

#endif // BOOST_INT128_FORMAT_HPP
// ===== END boost/int128/format.hpp =====
// ===== BEGIN boost/int128/limits.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_LIMITS_HPP
#define BOOST_INT128_LIMITS_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/int128_imp.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/uint128_imp.hpp

#endif // BOOST_INT128_LIMITS_HPP
// ===== END boost/int128/limits.hpp =====
// ===== BEGIN boost/int128/climits.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_CLIMITS_HPP
#define BOOST_INT128_CLIMITS_HPP

// [amalgamate] skipped duplicate include of boost/int128/detail/int128_imp.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/uint128_imp.hpp
#include <climits>

#define BOOST_INT128_UINT128_MAX boost::int128::uint128{UINT64_MAX, UINT64_MAX}

#define BOOST_INT128_INT128_MIN  boost::int128::int128{INT64_MIN, 0}
#define BOOST_INT128_INT128_MAX  boost::int128::int128{INT64_MAX, UINT64_MAX}

#endif // BOOST_INT128_CLIMITS_HPP
// ===== END boost/int128/climits.hpp =====
// [amalgamate] skipped duplicate include of boost/int128/cstdlib.hpp
// ===== BEGIN boost/int128/string.hpp =====
// Copyright 2025 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_STRING_HPP
#define BOOST_INT128_STRING_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp
// [amalgamate] skipped duplicate include of boost/int128/detail/mini_to_chars.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <string>

#endif

namespace boost {
namespace int128 {

template <typename T>
auto to_string(const T& value) -> std::enable_if_t<(std::is_same<T, int128>::value || std::is_same<T, uint128>::value), std::string>
{
    char buffer[detail::mini_to_chars_buffer_size];
    const auto last {detail::mini_to_chars(buffer, value, 10, false)};
    return std::string{last, buffer + sizeof(buffer) - 1};
}

template <typename T>
auto to_wstring(const T& value) -> std::enable_if_t<(std::is_same<T, int128>::value || std::is_same<T, uint128>::value), std::wstring>
{
    char buffer[detail::mini_to_chars_buffer_size];
    const auto last {detail::mini_to_chars(buffer, value, 10, false)};
    return std::wstring{last, buffer + sizeof(buffer) - 1};
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_STRING_HPP
// ===== END boost/int128/string.hpp =====
// [amalgamate] skipped duplicate include of boost/int128/utilities.hpp
// ===== BEGIN boost/int128/hash.hpp =====
// Copyright 2026 Matt Borland
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_INT128_HASH_HPP
#define BOOST_INT128_HASH_HPP

// [amalgamate] skipped duplicate include of boost/int128/int128.hpp

#ifndef BOOST_INT128_BUILD_MODULE

#include <cstddef>
#include <cstdint>
#include <functional>

#endif

namespace boost {
namespace int128 {
namespace detail {

// The cast is only useless for 64-bit platforms
// Without we get an implicit conversion warning which is arguably worse
#if defined(__GNUC__) && !defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wuseless-cast"
#endif

// splitmix64 finalizer: mixes all 64 input bits into the result before any narrowing to size_t.
// This is required for correctness on platforms where size_t is 32 bits
inline std::size_t hash_finalize_64(std::uint64_t v) noexcept
{
    v ^= v >> 30;
    v *= UINT64_C(0xbf58476d1ce4e5b9);
    v ^= v >> 27;
    v *= UINT64_C(0x94d049bb133111eb);
    v ^= v >> 31;
    return static_cast<std::size_t>(v);
}

#if defined(__GNUC__) && !defined(__clang__)
#  pragma GCC diagnostic pop
#endif

} // namespace detail
} // namespace int128
} // namespace boost

namespace std {

template <>
struct hash<boost::int128::int128>
{
    auto operator()(const boost::int128::int128 v) const noexcept -> std::size_t
    {
        const std::size_t low_hash {boost::int128::detail::hash_finalize_64(v.low)};
        const std::size_t high_hash {boost::int128::detail::hash_finalize_64(v.high)};

        // boost::hash_combine style mixing of the two finalized halves
        constexpr std::size_t golden_ratio {0x9e3779b9U};
        return low_hash ^ (high_hash + golden_ratio + (low_hash << 6) + (low_hash >> 2));
    }
};

template <>
struct hash<boost::int128::uint128>
{
    auto operator()(const boost::int128::uint128 v) const noexcept -> std::size_t
    {
        const std::size_t low_hash {boost::int128::detail::hash_finalize_64(v.low)};
        const std::size_t high_hash {boost::int128::detail::hash_finalize_64(v.high)};

        // boost::hash_combine style mixing of the two finalized halves
        constexpr std::size_t golden_ratio {0x9e3779b9U};
        return low_hash ^ (high_hash + golden_ratio + (low_hash << 6) + (low_hash >> 2));
    }
};

} // namespace std

namespace boost {
namespace int128 {

inline std::size_t hash_value(const uint128 v) noexcept
{
    return std::hash<uint128>{}(v);
}

inline std::size_t hash_value(const int128 v) noexcept
{
    return std::hash<int128>{}(v);
}

} // namespace int128
} // namespace boost

#endif // BOOST_INT128_HASH_HPP
// ===== END boost/int128/hash.hpp =====

#endif // BOOST_INT128_HPP
// ===== END boost/int128.hpp =====
