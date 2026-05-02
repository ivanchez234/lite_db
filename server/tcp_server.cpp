#include "tcp_server.h"
#include <iostream>
#include <thread>
#include <sstream>
#include <string>  

#ifdef _WIN32
    #pragma comment(lib, "ws2_32.lib")
#endif

// --- КОНСТРУКТОР ---
TcpServer::TcpServer(int p, Database* db) {
    port = p;
    database = db;
}

// --- ДЕСТРУКТОР (ПРАВИЛЬНОЕ ЗАКРЫТИЕ ПОТОКОВ) ---
TcpServer::~TcpServer() {
    stopFlag = true;       // Поднимаем красный флаг для потоков
    queueCV.notify_all();  // Будим всех спящих, чтобы они увидели флаг и завершились

    // Дожидаемся, пока каждый рабочий поток закончит свои дела
    for (std::thread &worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

// --- МЕТОД РАБОЧЕГО ПОТОКА (CONSUMER) ---
// Эти потоки спят в фоне и просыпаются только когда есть работа.
void TcpServer::workerThread(int workerId) {
    while (true) {
        ClientTask task;
        
        { // Критическая секция очереди
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCV.wait(lock, [this]() { 
                return !taskQueue.empty() || stopFlag.load(); 
            });
            
            if (stopFlag.load() && taskQueue.empty()) return; 
            
            task = std::move(taskQueue.front());
            taskQueue.pop();
            producerCV.notify_one(); 
        } 
        
        std::string response;
        
        // --- БРОНЯ ПОТОКА ---
        try {
            // Пытаемся выполнить команду
            response = database->execute(task.command);
        } catch (const std::exception& e) {
            // Если база выкинула стандартную ошибку C++
            std::cerr << "\n[Worker " << workerId << "] CRITICAL ERROR: " << e.what() << std::endl;
            std::cerr << "Command that killed it: " << task.command << std::endl;
            response = "ERR_INTERNAL_EXCEPTION";
        } catch (...) {
            // Если произошла вообще неведомая дичь
            std::cerr << "\n[Worker " << workerId << "] UNKNOWN CRASH!" << std::endl;
            std::cerr << "Command that killed it: " << task.command << std::endl;
            response = "ERR_UNKNOWN_CRASH";
        }
        
        response += "\n";
        
        {
            // Отправляем ответ
            std::lock_guard<std::mutex> sendLock(sendMutex);
            send(task.client_socket, response.c_str(), response.length(), 0);
        }
    }
}
// --- ЗАПУСК СЕРВЕРА ---
void TcpServer::start() {
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    bind(server_fd, (sockaddr*)&address, sizeof(address));
    listen(server_fd, 5);

    // --- ИНИЦИАЛИЗАЦИЯ THREAD POOL ---
    // Узнаем, сколько логических ядер на ПК (у тебя их 8)
    unsigned int worker_count = 5; // Принудительно ставим 5 рабочих потоков

    std::cout << "[ThreadPool] Starting " << worker_count << " worker threads..." << std::endl;

    // Создаем и запускаем рабочие потоки
    for (unsigned int i = 0; i < worker_count; ++i) {
        workers.emplace_back(&TcpServer::workerThread, this, i);
    }

    std::cout << "Server started on port " << port << std::endl;

    // --- ПРИЕМ КЛИЕНТОВ ---
    // --- ПРИЕМ КЛИЕНТОВ (БЕЗОПАСНЫЙ ЦИКЛ) ---
    while (true) {
        int client_socket = accept(server_fd, nullptr, nullptr);
        
        // 1. Защита от "цикла смерти"
        if (client_socket < 0) {
            std::cerr << "[Network] Accept failed. Retrying..." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Даем системе передохнуть
            continue; 
        }

        // 2. Бронируем фоновый сетевой поток
        std::thread([this, client_socket]() {
            try {
                handleClient(client_socket);
            } catch (const std::exception& e) {
                std::cerr << "\n[Network Thread] FATAL ERROR: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "\n[Network Thread] UNKNOWN CRASH!" << std::endl;
            }
        }).detach();
    }
}

// --- ОБРАБОТКА СЕТИ (PRODUCER) ---
void TcpServer::handleClient(SOCKET clientSocket) {
    char buffer[4096];
    std::string clientBuffer = ""; 

    while (true) {
        int bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (bytesRead <= 0) break; // Клиент отключился

        buffer[bytesRead] = '\0';
        clientBuffer += buffer; 

        size_t pos;
        while ((pos = clientBuffer.find('\n')) != std::string::npos) {
            
            std::string singleCommand = clientBuffer.substr(0, pos);
            clientBuffer.erase(0, pos + 1); 

            size_t first = singleCommand.find_first_not_of(" \n\r\t;");
            if (first == std::string::npos) continue; 
            size_t last = singleCommand.find_last_not_of(" \n\r\t;");
            singleCommand = singleCommand.substr(first, (last - first + 1));

            if (singleCommand.empty()) continue;

            // --- КИДАЕМ ЗАДАЧУ В ОЧЕРЕДЬ ВМЕСТО ПРЯМОГО ВЫЗОВА БАЗЫ ---
            ClientTask newTask;
            newTask.command = singleCommand;
            newTask.client_socket = clientSocket;

            {
                std::unique_lock<std::mutex> lock(queueMutex);
                // ЛИМИТАТОР: Если в очереди > 10000 задач, ждем!
                producerCV.wait(lock, [this]() { 
                    return taskQueue.size() < 10000; 
                });
                
                taskQueue.push(std::move(newTask));
            }
            
            queueCV.notify_one();
        }
    }

#ifdef _WIN32
    closesocket(clientSocket);
#else
    close(clientSocket);
#endif
}