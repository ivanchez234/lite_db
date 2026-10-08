include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.12.0
)
FetchContent_MakeAvailable(Catch2)

add_executable(format_tests
    test_storage_format.cpp
    test_wal.cpp
    test_concurrency.cpp
    ${CMAKE_SOURCE_DIR}/storage/storage.cpp
    ${CMAKE_SOURCE_DIR}/database/wal.cpp
)

target_include_directories(format_tests PRIVATE ${CMAKE_SOURCE_DIR})
find_package(Threads REQUIRED)
target_link_libraries(format_tests PRIVATE Catch2::Catch2WithMain z Threads::Threads)

# Тесты пишут каталог data/ в своём рабочем каталоге, а не в корне проекта.
add_test(NAME format_tests
         COMMAND format_tests
         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
