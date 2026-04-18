cmake_minimum_required(VERSION 3.10)
project(LiteDB)

set(CMAKE_CXX_STANDARD 17)

# 1. Пути к заголовкам
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/external/include
    ${CMAKE_CURRENT_SOURCE_DIR}/database
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/server
)

# 2. Исходники СЕРВЕРА (все файлы, КРОМЕ client.cpp)
file(GLOB_RECURSE SERVER_SOURCES 
    "main.cpp"
    "database/*.cpp" 
    "storage/*.cpp" 
    "server/*.cpp"
)

# --- СБОРКА СЕРВЕРА ---
add_executable(server_app ${SERVER_SOURCES})
# Линкуем статическую библиотеку LZ4 и сокеты Windows
target_link_libraries(server_app "${CMAKE_CURRENT_SOURCE_DIR}/external/lib/liblz4_static.lib" ws2_32)


# --- СБОРКА КЛИЕНТА ---
# Клиенту нужен только один его файл
add_executable(client_app client.cpp)
# Клиенту LZ4 не нужен, только сеть
target_link_libraries(client_app ws2_32)