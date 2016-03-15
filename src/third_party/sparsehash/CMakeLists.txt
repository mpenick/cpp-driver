cmake_minimum_required(VERSION 2.6.4)

include(CheckCXXSourceCompiles)
include(CheckFunctionExists)
include(CheckIncludeFile)
include(CheckTypeSize)

#include(TestCXXAcceptsFlag)
#check_cxx_accepts_flag("-std=c++11" HAVE_CXX11)
#if(HAVE_CXX11)
#  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++11")
#endif()

set(HASH_NAME hash)

foreach(LOCATION unordered_map;tr1/unordered_map)
  foreach(NAMESPACE std;std::tr1)
    unset(HAVE_UNORDERED_MAP CACHE)
    check_cxx_source_compiles(
      "
      #include <${LOCATION}>
      int main() {
        ${NAMESPACE}::unordered_map<int,int> m;
        return m.find(42) == m.end();
      }
      "
      HAVE_UNORDERED_MAP)
    if(${HAVE_UNORDERED_MAP})
      set(HASH_NAMESPACE ${NAMESPACE})
    endif()
  endforeach(NAMESPACE)
endforeach(LOCATION)

foreach(LOCATION hash_map;ext/hash_map)
  foreach(NAMESPACE std;stdext)
    unset(HAVE_HASH_MAP CACHE)
    check_cxx_source_compiles(
      "
      #include <${LOCATION}>
      int main() {
        ${NAMESPACE}::unordered_map<int,int> m;
        return m.find(42) == m.end();
      }
      "
      HAVE_HASH_MAP)
    if(${HAVE_HASH_MAP})
      set(HASH_NAMESPACE ${NAMESPACE})
    endif()
  endforeach(NAMESPACE)
endforeach(LOCATION)

if(NOT HASH_NAMESPACE)
  message(FATAL_ERROR "Unable to determine the standard hash namespace")
endif()

message(STATUS "Namespace: ${HASH_NAMESPACE}")

foreach(LOCATION functional;tr1/functional;ext/hash_fun.h;ext/stl_hash_fun.h;hash_fun.h;stl_hash_fun.h;stl/_hash_fun.h)
  unset(HAVE_HASH_FUNC_HEADER CACHE)
  check_cxx_source_compiles(
    "
    #include <${LOCATION}>
    int main() { int x = ${HASH_NAMESPACE}::hash<int>()(42); }
    "
    HAVE_HASH_FUNC_HEADER)
  if (${HAVE_HASH_FUNC_HEADER})
    set(HASH_FUN_H ${LOCATION})
  endif()
endforeach(LOCATION)

if(NOT HASH_FUN_H)
  message(FATAL_ERROR "Unable to find standarde hash header file")
endif()

message(STATUS "Header: ${HASH_FUN_H}")

check_include_file(inttypes.h HAVE_INTTYPES_H)
check_include_file(stdint.h HAVE_STDINT_H)
check_include_file(sys/types.h HAVE_SYS_TYPES_H)

check_function_exists(memcpy HAVE_MEMCPY)

check_type_size("long long" LONG_LONG)
check_type_size("uint16_t" UINT16_T)
check_type_size("u_int16_t" U_INT16_T)
check_type_size("__uint16_t" __UINT16_T)

configure_file("${PROJECT_SOURCE_DIR}/src/third_party/sparsehash/config.h.cmake"
               "${PROJECT_SOURCE_DIR}/src/third_party/sparsehash/src/sparsehash/internal/sparseconfig.h")
