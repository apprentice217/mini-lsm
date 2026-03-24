#pragma once

#include <cstdint>
#include <string>
#include "options.h"
#include "slice.h"
#include "status.h"
#include "format.h" // 引入 BlockHandle

namespace minidb {

class BlockBuilder;
class WritableFile;
class FilterBlockBuilder;


// 存储引擎磁盘格式落地的高级协调器
class TableBuilder {
public:
    // 【修改】：接收 Options 和 WritableFile
    TableBuilder(const Options& options, WritableFile* file);
    
    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator=(const TableBuilder&) = delete;

    ~TableBuilder();

    // 核心流式写入接口：接收来自 MemTable 的严格有序数据
    void Add(const Slice& key, const Slice& value);

    // 强制将当前的 Data Block 刷入文件，通常在外部感知到 4KB 边界时被调用
    void Flush();

    // 结束整个 SSTable 的构建，写入 Index Block 和 Footer
    Status Finish();

    // 返回当前已生成的物理文件大小
    uint64_t FileSize() const;

private:
    void WriteBlock(BlockBuilder* block, BlockHandle* handle);
    void WriteRawBlock(const Slice& block_contents, BlockHandle* handle);

    Options options_; // 全局配置
    Options index_block_options_; // 专门给顶层索引块开小灶的配置

    WritableFile* file_;
    uint64_t offset_;               // 当前文件尾部的物理偏移量
    Status status_;                 // 构建过程中的全局错误状态

    BlockBuilder* data_block_;      // 负责构建当前的 Data Block
    BlockBuilder* index_block_;     // 负责构建顶层的 Index Block

    std::string last_key_;          // 暂存刚写入的最后一个 Key
    uint64_t num_entries_;          // 整个 SSTable 中的 KV 总数
    bool closed_;                   // 是否已调用 Finish

    // 状态机精髓：Data Block 落盘后，由于还不知道下一个 Data Block 的起始 Key，
    // 索引记录的写入必须被"挂起 (Pending)"，直到下一次 Add() 触发时再写入 Index Block。
    bool pending_index_entry_;
    BlockHandle pending_handle_;    // 刚刚 Flush 的 Data Block 的句柄

    FilterBlockBuilder* filter_block_;
};

} // namespace minidb