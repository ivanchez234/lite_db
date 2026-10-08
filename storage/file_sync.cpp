#include "file_sync.h"

#ifdef _WIN32
    #include <fcntl.h>
    #include <io.h>
#else
    #include <cerrno>
    #include <fcntl.h>
    #include <unistd.h>
#endif

namespace lite_db {

#ifdef _WIN32

bool sync_file(const std::string& path) noexcept {
    // _commit — это FlushFileBuffers поверх CRT-дескриптора. Нужен доступ на запись.
    const int fd = ::_open(path.c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) return false;
    const bool ok = ::_commit(fd) == 0;
    ::_close(fd);
    return ok;
}

bool sync_directory(const std::string&) noexcept {
    // NTFS журналирует метаданные сам, отдельный сброс каталога не нужен.
    return true;
}

#else

namespace {

int open_retrying(const char* path, int flags) noexcept {
    int fd = -1;
    do {
        fd = ::open(path, flags | O_CLOEXEC);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

bool fsync_and_close(int fd) noexcept {
    if (fd < 0) return false;
    // Ошибку fsync нельзя «повторить»: ядро после неё помечает страницы
    // чистыми, и второй fsync вернёт успех, хотя данные потеряны.
    // Поэтому результат первой попытки и есть ответ.
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

} // namespace

bool sync_file(const std::string& path) noexcept {
    return fsync_and_close(open_retrying(path.c_str(), O_RDONLY));
}

bool sync_directory(const std::string& path) noexcept {
    return fsync_and_close(open_retrying(path.c_str(), O_RDONLY | O_DIRECTORY));
}

#endif

} // namespace lite_db
