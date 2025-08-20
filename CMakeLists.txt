# Minimum required version of CMake.
cmake_minimum_required(VERSION 3.12)

# Project definition
project(consistent_hash LANGUAGES CXX)

# --- Project Settings ---
# Require C++17 for language features.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


# --- Library Target: consistent_hash ---
add_library(consistent_hash
    src/consistent.cpp
    src/hasher.cpp
    src/member.cpp
)

# Expose the 'include' directory as the public interface.
# This allows consumers to use #include <consistent_hash/consistent.h> and
# automatically get the correct include paths when linking.
target_include_directories(consistent_hash PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)


# --- Testing ---
# Configure testing with GoogleTest.
enable_testing()
find_package(GTest REQUIRED)

add_executable(run_tests
    test/main.cpp # Assuming all tests are here or included from here
)

target_link_libraries(run_tests PRIVATE
    consistent_hash
    GTest::GTest
    GTest::Main
)

# Let CTest discover all tests in the executable.
include(GoogleTest)
gtest_discover_tests(run_tests)


# --- Installation ---
# Define rules for 'make install'.
include(GNUInstallDirs)

# Install the library targets (.a/.so/.lib/.dll).
install(TARGETS consistent_hash
    EXPORT consistent_hash_targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Install the public headers.
install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Generate and install the CMake package files.
# This is what allows other CMake projects to use `find_package(consistent_hash)`.
install(EXPORT consistent_hash_targets
    FILE consistent_hash-config.cmake
    NAMESPACE consistent_hash::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/consistent_hash
)