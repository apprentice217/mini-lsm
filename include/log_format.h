#pragma once

namespace minidb {
namespace log {

// 工业级铁律：32KB Block 边界对齐
// 为什么是 32KB？既能包容绝大多数小对象的完整写入，又能保证发生损坏时爆炸半径可控。
constexpr int kBlockSize = 32768;

// Header 内存布局：4 字节 CRC + 2 字节长度 + 1 字节类型
constexpr int kHeaderSize = 4 + 2 + 1;

// 记录的物理分片类型
enum RecordType {
    // 零类型：通常用于预分配空间的填充，或表示 Block 尾部的无用空间
    kZeroType = 0,
    
    // 一条记录恰好能完整放入一个 Chunk
    kFullType = 1,
    
    // 一条记录被切片时的起始、中间、结束部分
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
};

// 预留的最大类型，用于在 CRC 校验中做数组边界
constexpr int kMaxRecordType = kLastType;

} // namespace log
} // namespace minidb