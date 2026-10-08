#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

// Последовательное чтение из буфера с проверкой границ.
//
// Нужен потому, что длины, количества и смещения берутся из самих данных:
// обрезанный или повреждённый файл не должен уводить чтение за пределы буфера.
class ByteReader {
public:
    ByteReader(const char* data, std::size_t size) noexcept
        : data_(data), size_(size) {}

    std::size_t offset() const noexcept { return off_; }
    std::size_t remaining() const noexcept { return size_ - off_; }

    // Именно разность, а не off_ + n <= size_: сложение переполнится
    // на большом n, и проверка пропустит заведомо битую длину.
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

    // Отдаёт указатель на n байт и сдвигает позицию. nullptr, если их нет.
    const char* take(std::size_t n) noexcept {
        if (!has(n)) return nullptr;
        const char* p = data_ + off_;
        off_ += n;
        return p;
    }

private:
    const char* data_;
    std::size_t size_;
    std::size_t off_ = 0;   // инвариант: off_ <= size_
};
