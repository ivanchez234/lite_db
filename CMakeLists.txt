cmake_minimum_required(VERSION 3.10)
project(LiteDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. Пути к заголовкам
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/external/include
    ${CMAKE_CURRENT_SOURCE_DIR}/database
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/server
)

# 2. Исходники СЕРВЕРА
file(GLOB_RECURSE SERVER_SOURCES 
    "main.cpp"
    "database/*.cpp" 
    "storage/*.cpp" 
    "server/*.cpp"
)

# --- ФЛАГИ ДЛЯ ПОБЕДЫ НАД DLL (Добавь это!) ---
if(WIN32)
    # Эти флаги заставляют компилятор вшить стандартные библиотеки внутрь .exe
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static -static-libgcc -static-libstdc++")
    # Добавляем поддержку многопоточности для Windows
    add_definitions(-D_WIN32_WINNT=0x0601) 
endif()

# --- СБОРКА СЕРВЕРА ---
add_executable(server_app ${SERVER_SOURCES})

# Линкуем библиотеки
# Примечание: Если ты в UCRT64, убедись, что lz4_static совместим с GCC. 
# Обычно для MinGW/UCRT используются файлы .a, но .lib тоже может сработать.
target_link_libraries(server_app "${CMAKE_CURRENT_SOURCE_DIR}/external/lib/liblz4_static.lib" ws2_32)

# --- СБОРКА КЛИЕНТА ---
add_executable(client_app client.cpp)
target_link_libraries(client_app ws2_32)

# Если клиент тоже ругается на DLL, добавим статику и ему (опционально)
if(WIN32)
    set_target_properties(client_app PROPERTIES LINK_FLAGS "-static -static-libgcc -static-libstdc++")
endif()