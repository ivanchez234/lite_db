include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.12.0
)
FetchContent_MakeAvailable(Catch2)

add_executable(lite_db_tests
    test_storage_format.cpp
    test_wal.cpp
    test_concurrency.cpp
    test_database.cpp
    test_server.cpp
    test_json.cpp
    test_sql_parser.cpp
)

target_link_libraries(lite_db_tests PRIVATE lite_db_core Catch2::Catch2WithMain)
lite_db_static_runtime(lite_db_tests)

# Тесты пишут каталог data/ и журналы в своём рабочем каталоге, а не в корне проекта.
add_test(NAME lite_db_tests
         COMMAND lite_db_tests
         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# Тесты на сеть и конкурентность при поломке склонны зависать, а не падать.
set_tests_properties(lite_db_tests PROPERTIES TIMEOUT 600)

# --- Сквозной тест: настоящий процесс сервера + Python-драйвер ----------------
# Проверяет то, что не видно изнутри процесса: остановку по сигналу
# и восстановление из журнала после аварийного завершения.
find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
    add_test(NAME driver_e2e
             COMMAND Python3::Interpreter ${CMAKE_CURRENT_SOURCE_DIR}/test_driver_e2e.py
                     $<TARGET_FILE:server_app>
                     ${CMAKE_SOURCE_DIR}/litedb_driver.py)
    set_tests_properties(driver_e2e PROPERTIES TIMEOUT 300)
else()
    message(STATUS "Python 3 не найден: сквозной тест драйвера пропущен")
endif()
