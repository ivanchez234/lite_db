#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

class ByteReader {
public:
    ByteReader(const char* data, std::size_t size) noexcept
        : data_(data), size_(size) {}

    std::size_t offset() const noexcept { return off_; }
    std::size_t remaining() const noexcept { return size_ - off_; }

    bool has(std::size_t n) const noexcept { return size_ - off_ >= n; }

    template <class T>
    bool read(T& out) noexcept {
        static_assert(std::is_trivially_copyable<T>::value,
                      "ByteReader::read копирует байты, тип должен это допускать");
        if (!has(sizeof(T))) return false;
        std::memcpy(&out, data_ + off_, sizeof(T));
        off_ += sizeof(T);
        return true;
    }

    const char* take(std::size_t n) noexcept {
        if (!has(n)) return nullptr;
        const char* p = data_ + off_;
        off_ += n;
        return p;
    }

private:
    const char* data_;
    std::size_t size_;
    std::size_t off_ = 0;  
};
