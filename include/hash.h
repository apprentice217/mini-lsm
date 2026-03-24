#pragma once
#include <cstddef>
#include <cstdint>

namespace minidb {

// LevelDB 经典的非加密极速哈希算法 (类似 MurmurHash2)
uint32_t Hash(const char* data, size_t n, uint32_t seed);

} // namespace minidb