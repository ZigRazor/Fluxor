cmake_minimum_required(VERSION 3.20)

project(Fluxor
    VERSION 0.1.0
    DESCRIPTION "In-memory database with persistence, snapshot, and storage engine"
    LANGUAGES CXX
)

# ============================================================
# OPTIONS
# ============================================================
option(FLUXOR_BUILD_SHARED "Build shared library" ON)
option(FLUXOR_BUILD_STATIC "Build static library" ON)

option(FLUXOR_BUILD_TESTS "Build tests" ON)
option(FLUXOR_USE_GTEST "Use GoogleTest (OFF = Catch2)" ON)

option(FLUXOR_ENABLE_CLANG_TIDY "Enable clang-tidy" ON)
option(FLUXOR_ENABLE_SANITIZERS "Enable sanitizers (ASan/UBSan)" OFF)
option(FLUXOR_ENABLE_LTO "Enable LTO" ON)

option(FLUXOR_BUILD_EXAMPLES "Build examples" ON)

# ============================================================
# STANDARD
# ============================================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ============================================================
# SOURCES
# ============================================================
file(GLOB_RECURSE FLUXOR_SOURCES
    CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
)

set(FLUXOR_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

# ============================================================
# COMMON INTERFACE (vcpkg / install safe)
# ============================================================
add_library(fluxor_common INTERFACE)

target_include_directories(fluxor_common INTERFACE
    $<BUILD_INTERFACE:${FLUXOR_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>
)

find_package(Threads REQUIRED)
target_link_libraries(fluxor_common INTERFACE Threads::Threads)

# ============================================================
# WARNINGS
# ============================================================
if (MSVC)
    set(FLUXOR_WARNINGS /W4 /permissive-)
else()
    set(FLUXOR_WARNINGS -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()

# ============================================================
# STATIC LIB
# ============================================================
if (FLUXOR_BUILD_STATIC)
    add_library(fluxor_static STATIC ${FLUXOR_SOURCES})

    target_link_libraries(fluxor_static PUBLIC fluxor_common)
    target_compile_options(fluxor_static PRIVATE ${FLUXOR_WARNINGS})

    set_target_properties(fluxor_static PROPERTIES
        OUTPUT_NAME fluxor
        POSITION_INDEPENDENT_CODE ON
    )
endif()

# ============================================================
# SHARED LIB
# ============================================================
if (FLUXOR_BUILD_SHARED)
    add_library(fluxor_shared SHARED ${FLUXOR_SOURCES})

    target_link_libraries(fluxor_shared PUBLIC fluxor_common)
    target_compile_options(fluxor_shared PRIVATE ${FLUXOR_WARNINGS})

    set_target_properties(fluxor_shared PROPERTIES
        OUTPUT_NAME fluxor
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
    )
endif()

# ============================================================
# 🧹 CLANG-TIDY
# ============================================================
if (FLUXOR_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES clang-tidy)

    if (CLANG_TIDY_EXE)
        message(STATUS "clang-tidy enabled: ${CLANG_TIDY_EXE}")

        if (FLUXOR_BUILD_STATIC)
            set_target_properties(fluxor_static PROPERTIES
                CXX_CLANG_TIDY "${CLANG_TIDY_EXE};--extra-arg=-std=c++20"
            )
        endif()

        if (FLUXOR_BUILD_SHARED)
            set_target_properties(fluxor_shared PROPERTIES
                CXX_CLANG_TIDY "${CLANG_TIDY_EXE};--extra-arg=-std=c++20"
            )
        endif()
    endif()
endif()

# ============================================================
# ⚠️ SANITIZERS
# ============================================================
if (FLUXOR_ENABLE_SANITIZERS)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        message(STATUS "Sanitizers enabled")

        set(SAN_FLAGS -fsanitize=address,undefined -fno-omit-frame-pointer -g)

        if (FLUXOR_BUILD_STATIC)
            target_compile_options(fluxor_static PRIVATE ${SAN_FLAGS})
            target_link_options(fluxor_static PRIVATE ${SAN_FLAGS})
        endif()

        if (FLUXOR_BUILD_SHARED)
            target_compile_options(fluxor_shared PRIVATE ${SAN_FLAGS})
            target_link_options(fluxor_shared PRIVATE ${SAN_FLAGS})
        endif()
    endif()
endif()

# ============================================================
# 🚀 LTO
# ============================================================
if (FLUXOR_ENABLE_LTO)
    include(CheckIPOSupported)

    check_ipo_supported(RESULT LTO_OK OUTPUT LTO_MSG)

    if (LTO_OK)
        message(STATUS "LTO enabled")

        if (FLUXOR_BUILD_STATIC)
            set_target_properties(fluxor_static PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
        endif()

        if (FLUXOR_BUILD_SHARED)
            set_target_properties(fluxor_shared PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
        endif()
    else()
        message(WARNING "LTO not supported: ${LTO_MSG}")
    endif()
endif()

# ============================================================
# EXAMPLES
# ============================================================
if (FLUXOR_BUILD_EXAMPLES)
    file(GLOB_RECURSE EXAMPLE_SOURCES
        CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/examples/*.cpp
    )

    add_executable(fluxor_example ${EXAMPLE_SOURCES})

    if (FLUXOR_BUILD_SHARED)
        target_link_libraries(fluxor_example PRIVATE fluxor_shared)
    else()
        target_link_libraries(fluxor_example PRIVATE fluxor_static)
    endif()
endif()

# ============================================================
# TESTS (GoogleTest / Catch2)
# ============================================================
if (FLUXOR_BUILD_TESTS)
    include(CTest)
    enable_testing()

    include(FetchContent)

    if (FLUXOR_USE_GTEST)

        FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
        )

        FetchContent_MakeAvailable(googletest)

        file(GLOB_RECURSE TEST_SOURCES
            CONFIGURE_DEPENDS
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/gtest/*.cpp
        )

        add_executable(fluxor_tests ${TEST_SOURCES})

        target_link_libraries(fluxor_tests
            PRIVATE fluxor_common
            PRIVATE fluxor_static
            PRIVATE GTest::gtest
            PRIVATE GTest::gtest_main
            PRIVATE Threads::Threads
        )

        include(GoogleTest)
        gtest_discover_tests(fluxor_tests)

    else()

        FetchContent_Declare(
            Catch2
            URL https://github.com/catchorg/Catch2/archive/refs/tags/v3.5.4.zip
        )

        FetchContent_MakeAvailable(Catch2)

        file(GLOB_RECURSE TEST_SOURCES
            CONFIGURE_DEPENDS
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/catch/*.cpp
        )

        add_executable(fluxor_tests ${TEST_SOURCES})

        target_link_libraries(fluxor_tests
            PRIVATE fluxor_common
            PRIVATE Catch2::Catch2WithMain
        )

        include(Catch)
        catch_discover_tests(fluxor_tests)

    endif()
endif()

# ============================================================
# INSTALL (vcpkg-ready)
# ============================================================
include(GNUInstallDirs)

if (FLUXOR_BUILD_STATIC)
install(TARGETS fluxor_static
    EXPORT FluxorTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
endif()

if (FLUXOR_BUILD_SHARED)
install(TARGETS fluxor_shared
    EXPORT FluxorTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
endif()

install(TARGETS fluxor_common
    EXPORT FluxorTargets
)

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT FluxorTargets
    FILE FluxorTargets.cmake
    NAMESPACE Fluxor::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Fluxor
)

# ============================================================
# 📦 CPACK (Debian / RPM / TGZ)
# ============================================================
include(CPack)

set(CPACK_PACKAGE_NAME "fluxor")
set(CPACK_PACKAGE_VENDOR "Fluxor")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "In-memory database engine with snapshot persistence")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})

set(CPACK_GENERATOR "TGZ;DEB;RPM")

# Debian
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Fluxor Team")
set(CPACK_DEBIAN_PACKAGE_SECTION "libs")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

# RPM
set(CPACK_RPM_PACKAGE_LICENSE "MIT")
set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)