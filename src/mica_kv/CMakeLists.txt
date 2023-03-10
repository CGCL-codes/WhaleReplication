# Copyright 2014 Carnegie Mellon University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 2.6)

# basic configuration
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)
	message(FATAL_ERROR "Use out-of-source build only!")
endif(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)

# add_subdirectory(perf_count)
# set(LIBRARY_PATH ${LIBRARY_PATH} perf_count/)

# compiler options
add_definitions(-pthread)
add_definitions(-Wconversion)
add_definitions(-O9)
add_definitions(-msse4.2 -march=corei7)

# preprocessor
SET(NDEBUG "$ENV{NDEBUG}" CACHE STRING "define NDEBUG macro")

message(STATUS "NDEBUG (disable all additional checkings; no*, yes) = ${NDEBUG}")
if("${NDEBUG}" STREQUAL "yes")
	add_definitions(-DNDEBUG)
endif()

SET(NLOCAL "$ENV{NLOCAL}" CACHE STRING "no local programs")
SET(NCLIENT "$ENV{NCLIENT}" CACHE STRING "no clients")
SET(NSERVER "$ENV{NSERVER}" CACHE STRING "no servers")

message(STATUS "NLOCAL (do not compile local programs; no*, yes) = ${NLOCAL}")
message(STATUS "NCLIENT (do not compile clients; no*, yes) = ${NCLIENT}")
message(STATUS "NSERVER (do not compile server; no*, yes) = ${NSERVER}")

# common source files
set(SOURCES ${SOURCES} hash.c)
set(SOURCES ${SOURCES} city.c)
# set(SOURCES ${SOURCES} stopwatch.c)
set(SOURCES ${SOURCES} shm.c)

# common libraries
# set(LIBRARIES ${LIBRARIES} rt crypto perf_count m pthread)
# 增加中间件所需的 rdma 通信库
set(LIBRARIES ${LIBRARIES} rt  m pthread middlib xml2 rdmacm ibverbs numa json-c memkind)
# set(LIBRARIES_cli ${LIBRARIES_cli} rt crypto perf_count m pthread middlib_witout_nic xml2 rdmacm ibverbs numa json-c memkind)
# tcmalloc (optional)
#add_definitions(-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free)
#set(LIBRARIES ${LIBRARIES} tcmalloc_minimal)

# DPDK
# set(RTE_SDK ${CMAKE_CURRENT_SOURCE_DIR}/../../DPDK)
# set(RTE_TARGET x86_64-default-linuxapp-gcc)
# add_definitions(-DUSE_DPDK)
# add_definitions(-isystem ${RTE_SDK}/${RTE_TARGET}/include -include rte_config.h)
# link_directories(${LINK_DIRECTORIES} ${RTE_SDK}/${RTE_TARGET}/lib)
# set(LIBRARIES ${LIBRARIES} rte_eal rte_malloc rte_mempool rte_ring)	# for basic features
# set(LIBRARIES ${LIBRARIES} ethdev rte_mbuf rte_pmd_ixgbe librte_pmd_ring.a)	# for network features
# set(LIBRARIES ${LIBRARIES} pthread rt)	# to make sure symbols in pthread and rt are resolved

set (EXECUTABLE_OUTPUT_PATH ${MIDD_MICA_SOURCE_DIR}/bin)
SET (LIBRARY_OUTPUT_PATH ${MIDD_MICA_SOURCE_DIR}/lib)
if(NOT "${NLOCAL}" STREQUAL "yes")
	# # test
	# add_executable(test test.c ${SOURCES})
	# target_link_libraries(test ${LIBRARIES})
	# message(STATUS "hhhhhhhhh")

	# # # load
	# add_executable(load load.c ${SOURCES})
	# target_link_libraries(load ${LIBRARIES})

	# # test
    # add_executable(ibtest ibtest.c ${SOURCES})
	# target_link_libraries(ibtest ${LIBRARIES})
	add_executable(mmap_test test_mmap.c)
	# server
	add_executable(mica midd_mica_server.c ${SOURCES})
	target_link_libraries(mica ${LIBRARIES} )
	target_link_libraries(mica pthread)

	# add_executable(cli_mica midd_mica_client.c ${SOURCES})
	# target_link_libraries(cli_mica ${LIBRARIES})
	# target_link_libraries(cli_mica pthread)

    # target_link_libraries(ibtest middlib)
    # target_link_libraries(ibtest xml2)
    # target_link_libraries(ibtest pthread)
    # target_link_libraries(ibtest rdmacm)
    # target_link_libraries(ibtest ibverbs)
    # target_link_libraries(ibtest numa)
    # target_link_libraries(ibtest rt)
    # target_link_libraries(ibtest m)
    # target_link_libraries(ibtest json-c)
    # target_link_libraries(ibtest memkind)
	# # microbench
	# add_executable(microbench microbench.c ${SOURCES})
	# target_link_libraries(microbench ${LIBRARIES})
	# MESSAGE("MIDD_MICA_LIB_SRCS: ${MIDD_MICA_LIB_SRCS}")




	# add_executable(test_RTT test_RTT.c ${SOURCES})
	# target_link_libraries(test_RTT ${LIBRARIES})




	# target_link_libraries(ibtest xml2)
	# target_link_libraries(ibtest pthread)
	# target_link_libraries(ibtest rdmacm)
	# target_link_libraries(ibtest ibverbs)
	# target_link_libraries(ibtest numa)
	# target_link_libraries(ibtest rt)
	# target_link_libraries(ibtest m)
	# target_link_libraries(ibtest json-c)
	# target_link_libraries(ibtest memkind)
	# # microbench_store
	# add_executable(microbench_store microbench.c ${SOURCES})
	# set_target_properties(microbench_store PROPERTIES COMPILE_FLAGS "-DMEHCACHED_NO_EVICTION")
	# target_link_libraries(microbench_store ${LIBRARIES})

	# # netbench_analysis
	# add_executable(netbench_analysis netbench_config.c netbench_analysis.c ${SOURCES})
	# target_link_libraries(netbench_analysis ${LIBRARIES})
endif()
