#pragma once
// Тонкий слой над сокетами Windows и POSIX.
// Всё платформенное собрано здесь, чтобы сервер и тесты писались один раз.

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
#else
    #include <arpa/inet.h>
    #include <netinet/in.h>
    #include <netinet/tcp.h>
    #include <poll.h>
    #include <sys/socket.h>
    #include <unistd.h>
    #include <cerrno>

    using SOCKET = int;
    constexpr SOCKET INVALID_SOCKET = -1;
    constexpr int    SOCKET_ERROR   = -1;
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

// Последний системный вызов прерван сигналом — это не ошибка, его надо повторить.
inline bool interrupted() noexcept {
#ifdef _WIN32
    return ::WSAGetLastError() == WSAEINTR;
#else
    return errno == EINTR;
#endif
}

// Владеющая обёртка над сокетом
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

    // Сравнение именно с INVALID_SOCKET: на Windows SOCKET беззнаковый,
    // и проверка "< 0" там всегда ложна.
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

// wait_readable() стоит на входе
[[nodiscard]] inline int wait_readable(SOCKET sock, int timeout_ms) noexcept {
#ifdef _WIN32
    // На Windows select() не ограничен значением дескриптора, а в набор
    // кладётся ровно один сокет.
    fd_set read_set;
    FD_ZERO(&read_set);
    FD_SET(sock, &read_set);

    timeval timeout{};
    timeout.tv_sec  = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;

    const int ready = ::select(0, &read_set, nullptr, nullptr, &timeout);
    if (ready == SOCKET_ERROR) return -1;
    return ready > 0 ? 1 : 0;
#else
    // poll, а не select: select на POSIX ломается на дескрипторах >= FD_SETSIZE (1024).
    pollfd entry{};
    entry.fd     = sock;
    entry.events = POLLIN;

    const int ready = ::poll(&entry, 1, timeout_ms);
    if (ready < 0) return -1;
    return ready > 0 ? 1 : 0;
#endif
}

// send_all() стоит на выходе
[[nodiscard]] inline bool send_all(SOCKET sock, const char* data, std::size_t size) noexcept {
#if defined(MSG_NOSIGNAL)
    
    constexpr int kFlags = MSG_NOSIGNAL;
#else
    constexpr int kFlags = 0;
#endif

    std::size_t sent = 0;

    while (sent < size) {
        const std::size_t left = size - sent;

#ifdef _WIN32
        const int chunk_size = static_cast<int>(std::min<std::size_t>(left, 1u << 20));
#else
        const std::size_t chunk_size = std::min<std::size_t>(left, 1u << 20);
#endif
        const auto written = ::send(sock, data + sent, chunk_size, kFlags);

        if (written > 0) {
            sent += static_cast<std::size_t>(written);
            continue;
        }

        if (written < 0 && interrupted()) continue;
        return false;
    }

    return true;
}

[[nodiscard]] inline bool send_all(SOCKET sock, const std::string& data) noexcept {
    return send_all(sock, data.data(), data.size());
}

// флаг для алгоритма нейгла на отправителе
inline void set_no_delay(SOCKET sock) noexcept {
    int flag = 1;
    ::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&flag), sizeof(flag));
}

// Сообщает собеседнику, что мы больше ничего не отправим, но читать продолжаем.
inline void shutdown_send(SOCKET sock) noexcept {
#ifdef _WIN32
    ::shutdown(sock, SD_SEND);
#else
    ::shutdown(sock, SHUT_WR);
#endif
}

} // namespace net
