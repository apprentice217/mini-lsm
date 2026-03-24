#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "slice.h"

namespace minidb {

class FilterPolicy;

// 负责在构建 SSTable 时，收集 Key 并生成 .filter 物理块
class FilterBlockBuilder {
public:
    explicit FilterBlockBuilder(const FilterPolicy* policy);
    
    FilterBlockBuilder(const FilterBlockBuilder&) = delete;
    FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

    // 当 TableBuilder 写入的数据超过下一个 2KB 边界时，调用此方法
    void StartBlock(uint64_t block_offset);

    // 将当前遍历到的 Key 塞入车间
    void AddKey(const Slice& key);

    // 封盘：打包所有的布隆过滤器片段和偏移量，返回最终的二进制物理块
    Slice Finish();


private:
    void GenerateFilter();

    const FilterPolicy* policy_;
    
    // 工业级扁平化存储：将多个字符串无缝拼接到一起，省去 vector<string> 的内存开销
    std::string keys_;             
    std::vector<size_t> start_;    // 记录每个 Key 在 keys_ 里的起始位置
    
    std::string result_;           // 最终拼接而成的物理 Block 字节流
    std::vector<uint32_t> filter_offsets_; // 记录每个小 Filter 在 result_ 中的偏移量
    
    // 经典魔数：BaseLg = 11，意味着 2^11 = 2048 字节 (即每 2KB 数据生成一个 Filter)
    enum { kFilterBaseLg = 11 };
};

// =========================================================================
// 读端探测器：负责在读取 SSTable 时，解析 .filter 并拦截无效查询
// =========================================================================
class FilterBlockReader {
public:
    FilterBlockReader(const FilterPolicy* policy,const Slice& contents);

    bool KeyMayMatch(uint64_t block_offset, const Slice& key);

private:
    const FilterPolicy* policy_;
    const char* data_;    
    const char* offset_;  
    size_t num_;          
    size_t base_lg_;
};

} // namespace minidb