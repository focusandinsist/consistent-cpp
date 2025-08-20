#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>

namespace consistent {

class Hasher {
public:
    virtual ~Hasher() = default;
    virtual uint64_t Sum64(const std::vector<uint8_t>& data) const = 0;
    virtual uint64_t Sum64(const std::string& data) const = 0;
};

// CRC64Hasher .
class CRC64Hasher : public Hasher {
private:
    static const uint64_t CRC64_ISO_POLY = 0xD800000000000000ULL;
    static std::vector<uint64_t> crc64_table_;
    static std::once_flag table_init_flag_;

    static void InitializeTable();
    static uint64_t CalculateCRC64(const uint8_t* data, size_t length);

public:
    CRC64Hasher();
    
    uint64_t Sum64(const std::vector<uint8_t>& data) const override;
    uint64_t Sum64(const std::string& data) const override;
};

// FNVHasher .
class FNVHasher : public Hasher {
private:
    static const uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
    static const uint64_t FNV_PRIME = 1099511628211ULL;

public:
    FNVHasher() = default;
    
    uint64_t Sum64(const std::vector<uint8_t>& data) const override;
    uint64_t Sum64(const std::string& data) const override;
};

std::unique_ptr<Hasher> CreateCRC64Hasher();
std::unique_ptr<Hasher> CreateFNVHasher();

} // namespace consistent
