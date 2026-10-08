cmake_minimum_required(VERSION 3.14)
project(LiteDB LANGUAGES CXX)

# Без явного типа сборки генераторы Makefile/Ninja собирают без оптимизаций,
# и любой замер скорости измеряет не ту программу, которую мы публикуем.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Тип сборки" FORCE)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# compile_commands.json нужен clang-tidy.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(LITE_DB_BUILD_TESTS "Собирать тесты" ON)

# Одна опция, а не набор флагов: ASan и TSan несовместимы между собой.
# Допустимо: address, undefined, address+undefined, thread.
set(LITE_DB_SANITIZER "" CACHE STRING "Санитайзер: address, undefined, address+undefined, thread")

if(LITE_DB_SANITIZER)
    if(MSVC)
        message(FATAL_ERROR "LITE_DB_SANITIZER поддерживается только для GCC/Clang")
    endif()
    if(LITE_DB_SANITIZER MATCHES "thread" AND LITE_DB_SANITIZER MATCHES "address")
        message(FATAL_ERROR "ThreadSanitizer нельзя совмещать с AddressSanitizer")
    endif()

    string(REPLACE "+" "," LITE_DB_SANITIZER_FLAGS "${LITE_DB_SANITIZER}")

    # Санитайзер включается для всего дерева сборки, включая тестовый фреймворк:
    # TSan видит гонки только в инструментированном коде.
    add_compile_options(-fsanitize=${LITE_DB_SANITIZER_FLAGS} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${LITE_DB_SANITIZER_FLAGS})
endif()

find_package(Threads REQUIRED)

# В MSYS2 и Linux zlib находится через find_package. Если его поставили
# вручную рядом с компилятором, CMake его не увидит — тогда линкуемся по имени.
find_package(ZLIB QUIET)
if(ZLIB_FOUND)
    set(LITE_DB_ZLIB ZLIB::ZLIB)
else()
    set(LITE_DB_ZLIB z)
endif()

# Стандартная библиотека MinGW вшивается в .exe, чтобы программу можно было
# запустить на машине без MinGW в PATH.
function(lite_db_static_runtime target)
    if(WIN32 AND NOT MSVC)
        target_link_options(${target} PRIVATE -static -static-libgcc -static-libstdc++)
    endif()
endfunction()

# --- ЯДРО: всё, кроме точки входа --------------------------------------------
# Отдельная библиотека нужна, чтобы тесты линковались с тем же кодом,
# что и сервер, а не перечисляли его исходники заново.
add_library(lite_db_core STATIC
    storage/storage.cpp
    storage/file_sync.cpp
    storage/json.cpp
    database/database.cpp
    database/wal.cpp
    Orm/sql_parser.cpp
    server/tcp_server.cpp
    server/shutdown.cpp
)

target_include_directories(lite_db_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(lite_db_core PUBLIC ${LITE_DB_ZLIB} Threads::Threads)

if(WIN32)
    # Сокеты Windows. На Linux этой библиотеки нет, поэтому только здесь.
    target_link_libraries(lite_db_core PUBLIC ws2_32)
    target_compile_definitions(lite_db_core PUBLIC _WIN32_WINNT=0x0601)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(lite_db_core PRIVATE -Wall -Wextra)
endif()

# --- СЕРВЕР ---------------------------------------------------------------
add_executable(server_app main.cpp)
target_link_libraries(server_app PRIVATE lite_db_core)
lite_db_static_runtime(server_app)

# --- КОНСОЛЬНЫЙ КЛИЕНТ -------------------------------------------------------
add_executable(client_app client.cpp)
target_link_libraries(client_app PRIVATE lite_db_core)
lite_db_static_runtime(client_app)

# --- ТЕСТЫ ----------------------------------------------------------------
if(LITE_DB_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
