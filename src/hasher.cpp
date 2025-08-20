#include "hasher.h"
#include <mutex>

namespace consistent {

// CRC64Hasher static members
std::vector<uint64_t> CRC64Hasher::crc64_table_(256);
std::once_flag CRC64Hasher::table_init_flag_;

void CRC64Hasher::InitializeTable() {
    std::call_once(table_init_flag_, []() {
        for (uint32_t i = 0; i < 256; ++i) {
            uint64_t crc = i;
            for (int j = 0; j < 8; ++j) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ CRC64_ISO_POLY;
                } else {
                    crc >>= 1;
                }
            }
            crc64_table_[i] = crc;
        }
    });
}

uint64_t CRC64Hasher::CalculateCRC64(const uint8_t* data, size_t length) {
    uint64_t crc = 0xFFFFFFFFFFFFFFFFULL;
    
    for (size_t i = 0; i < length; ++i) {
        uint8_t byte = data[i];
        crc = crc64_table_[(crc ^ byte) & 0xFF] ^ (crc >> 8);
    }
    
    return crc ^ 0xFFFFFFFFFFFFFFFFULL;
}

CRC64Hasher::CRC64Hasher() {
    InitializeTable();
}

uint64_t CRC64Hasher::Sum64(const std::vector<uint8_t>& data) const {
    return CalculateCRC64(data.data(), data.size());
}

uint64_t CRC64Hasher::Sum64(const std::string& data) const {
    return CalculateCRC64(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

// FNVHasher .
uint64_t FNVHasher::Sum64(const std::vector<uint8_t>& data) const {
    uint64_t hash = FNV_OFFSET_BASIS;
    
    for (uint8_t byte : data) {
        hash ^= byte;
        hash *= FNV_PRIME;
    }
    
    return hash;
}

uint64_t FNVHasher::Sum64(const std::string& data) const {
    uint64_t hash = FNV_OFFSET_BASIS;
    
    for (char c : data) {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }
    
    return hash;
}

std::unique_ptr<Hasher> CreateCRC64Hasher() {
    return std::make_unique<CRC64Hasher>();
}

std::unique_ptr<Hasher> CreateFNVHasher() {
    return std::make_unique<FNVHasher>();
}

} // namespace consistent
