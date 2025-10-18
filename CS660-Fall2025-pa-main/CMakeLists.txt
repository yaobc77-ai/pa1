cmake_minimum_required(VERSION 3.10)
project(cs660-pa LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()
file(GLOB_RECURSE CPP_SOURCES src/*.cpp)

add_library(db ${CPP_SOURCES})

target_include_directories(db PUBLIC include)

include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(googletest)

file(GLOB_RECURSE CPP_TESTS tests/pa1/*)

add_executable(pa_test ${CPP_TESTS})
target_link_libraries(pa_test PRIVATE db GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(pa_test)