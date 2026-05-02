#pragma once

#include <string>
#include <iostream>

// --- Сетевые библиотеки ---
#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
#else
    #include <netinet/in.h>
    #include <unistd.h>
    #define SOCKET int
    #define INVALID_SOCKET (SOCKET)(~0)
    #define SOCKET_ERROR (-1)
#endif

// --- Библиотеки для многопоточности ---
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <queue>
#include <atomic>

// Подключаем класс нашей базы данных
#include "../database/database.h"

// 1. Структура задачи для очереди (Команда + Кому отвечать)
struct ClientTask {
    std::string command;
    SOCKET client_socket;
};

class TcpServer {
public:
    // Конструктор (принимает порт и указатель на базу)
    TcpServer(int port, Database* db);
    
    // Деструктор (обязателен для безопасного завершения потоков)
    ~TcpServer();

    // Запуск сервера
    void start();

private:
    // --- Основные переменные сервера ---
    int port;
    SOCKET server_fd;
    Database* database;

    // --- ПЕРЕМЕННЫЕ ДЛЯ THREAD POOL ---
    std::condition_variable producerCV; // Чтобы притормаживать сеть при перегрузке
    std::mutex sendMutex;               // Замок для безопасной отправки ответов
    // Очередь задач, куда сетевой поток скидывает команды
    std::queue<ClientTask> taskQueue;
    
    // Мьютекс для защиты очереди от одновременного доступа потоков
    std::mutex queueMutex;
    
    // Условная переменная (сигнальный колокольчик) для пробуждения потоков
    std::condition_variable queueCV;
    
    // Массив (пул) рабочих потоков
    std::vector<std::thread> workers;
    
    // Флаг остановки сервера (atomic гарантирует безопасное чтение/запись из разных потоков)
    std::atomic<bool> stopFlag{false}; 

    // --- Внутренние методы ---

    // Метод для обработки клиента (теперь он только читает сеть и кидает задачи в очередь)
    void handleClient(SOCKET client_socket);

    // Метод, который работает внутри каждого потока (берет задачи и выполняет их в базе)
    void workerThread(int workerId);
};