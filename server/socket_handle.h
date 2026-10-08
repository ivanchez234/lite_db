#pragma once

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
#else
    #include <netinet/in.h>
    #include <sys/socket.h>
    #include <unistd.h>
    #include <cerrno>
    #define SOCKET int
    #define INVALID_SOCKET (SOCKET)(~0)
    #define SOCKET_ERROR (-1)
#endif

#include <algorithm>
#include <cstddef>
#include <string>

namespace net {

inline void close_socket(SOCKET sock) noexcept {
#ifdef _WIN32
    ::closesocket(sock);
#else
    ::close(sock);
#endif
}

// Владеющая обёртка над сокетом.
//
// Закрывает дескриптор при выходе из области видимости, поэтому он не утекает
// ни на путях с ошибкой, ни при исключении. Копирование запрещено: два
// владельца закрыли бы один дескриптор дважды, а освободившийся номер
// система к тому моменту могла выдать другому соединению.
class SocketHandle {
public:
    SocketHandle() noexcept = default;
    explicit SocketHandle(SOCKET sock) noexcept : sock_(sock) {}

    ~SocketHandle() { reset(); }

    SocketHandle(SocketHandle&& other) noexcept : sock_(other.release()) {}

    SocketHandle& operator=(SocketHandle&& other) noexcept {
        if (this != &other) {
            reset();
            sock_ = other.release();
        }
        return *this;
    }

    SocketHandle(const SocketHandle&) = delete;
    SocketHandle& operator=(const SocketHandle&) = delete;

    bool   valid() const noexcept { return sock_ != INVALID_SOCKET; }
    SOCKET get()   const noexcept { return sock_; }

    SOCKET release() noexcept {
        const SOCKET sock = sock_;
        sock_ = INVALID_SOCKET;
        return sock;
    }

    void reset() noexcept {
        if (valid()) {
            close_socket(sock_);
            sock_ = INVALID_SOCKET;
        }
    }

private:
    SOCKET sock_ = INVALID_SOCKET;
};

// send() не обязан отправить всё за один вызов: он возвращает, сколько байт
// действительно ушло. Без цикла длинный ответ молча обрезается на середине.
inline bool send_all(SOCKET sock, const char* data, std::size_t size) noexcept {
    std::size_t sent = 0;

    while (sent < size) {
        const std::size_t left = size - sent;

#ifdef _WIN32
        const int chunk_size = static_cast<int>(std::min<std::size_t>(left, 1u << 20));
#else
        const std::size_t chunk_size = std::min<std::size_t>(left, 1u << 20);
#endif
        const auto written = ::send(sock, data + sent, chunk_size, 0);

        if (written > 0) {
            sent += static_cast<std::size_t>(written);
            continue;
        }

#ifndef _WIN32
        // Прервано сигналом — это не ошибка, продолжаем с того же места.
        if (written < 0 && errno == EINTR) continue;
#endif
        return false;
    }

    return true;
}

inline bool send_all(SOCKET sock, const std::string& data) noexcept {
    return send_all(sock, data.data(), data.size());
}

} // namespace net
