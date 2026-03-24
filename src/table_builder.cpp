#include "table_builder.h"
#include "block_builder.h"
#include "filter_policy.h"
#include "filter_block.h"
#include "db_format.h"
#include "coding.h"
#include "crc32c.h"
#include "env.h"
#include <cassert>

namespace minidb {

// Data Block 的目标大小阈值，超过后触发 Flush 切块。
static const size_t kBlockSizeThreshold = 4096;

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : options_(options),
      index_block_options_(options),
      file_(file),
      offset_(0),
      status_(Status::OK()),
      num_entries_(0),
      closed_(false),
      pending_index_entry_(false),
      filter_block_(options.filter_policy == nullptr
                        ? nullptr
                        : new FilterBlockBuilder(options.filter_policy)) {
    assert(file_ != nullptr);

    // Index Block 不需要前缀压缩（每个 entry 都是一个独立的 separator key），
    // 将重启点间隔设为 1 使每个 key 都是一个完整重启点，支持高效二分查找。
    index_block_options_.block_restart_interval = 1;

    data_block_  = new BlockBuilder(&options_);
    index_block_ = new BlockBuilder(&index_block_options_);
}

TableBuilder::~TableBuilder() {
    // 析构前必须已调用 Finish()，防止文件被截断。
    assert(closed_);
    delete data_block_;
    delete index_block_;
    delete filter_block_;
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
    if (!status_.ok() || closed_) return;

    if (num_entries_ > 0) {
        // SSTable 内 key 必须严格单调递增，由调用方（MemTable Iterator）保证。
        assert(options_.comparator->Compare(key, Slice(last_key_)) > 0);
    }

    if (filter_block_ != nullptr) {
        // Bloom Filter 基于 UserKey（去掉序列号尾缀），查询时同样只用 UserKey 匹配。
        filter_block_->AddKey(ExtractUserKey(key));
    }

    // 延迟索引写入：Index Block 中每个 entry 的 separator key 需要满足
    // "大于上一个 Data Block 的所有 key，且小于等于下一个 Data Block 的第一个 key"。
    // 在下一次 Add 时才能确定 separator，故此处才正式写入。
    if (pending_index_entry_) {
        assert(data_block_->empty());
        std::string handle_encoding;
        pending_handle_.EncodeTo(&handle_encoding);
        index_block_->Add(Slice(last_key_), Slice(handle_encoding));
        pending_index_entry_ = false;
    }

    last_key_.assign(key.data(), key.size());
    num_entries_++;
    data_block_->Add(key, value);

    if (data_block_->CurrentSizeEstimate() >= kBlockSizeThreshold) {
        Flush();
    }
}

void TableBuilder::Flush() {
    if (!status_.ok() || closed_ || data_block_->empty()) return;

    WriteBlock(data_block_, &pending_handle_);
    if (status_.ok()) {
        pending_index_entry_ = true;
        status_ = file_->Flush();
    }
    if (filter_block_ != nullptr) {
        filter_block_->StartBlock(offset_);
    }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
    Slice raw_data = block->Finish();
    WriteRawBlock(raw_data, handle);
    block->Reset();
}

// WriteRawBlock 将一个已序列化的 Block 追加到文件，格式：
// [payload][compression_type: 1B][masked_crc32c: 4B]
void TableBuilder::WriteRawBlock(const Slice& data, BlockHandle* handle) {
    assert(file_ != nullptr);

    handle->set_offset(offset_);
    handle->set_size(data.size());

    status_ = file_->Append(data);
    if (!status_.ok()) return;
    offset_ += data.size();

    // 当前实现不压缩，compression_type = 0x00。
    char type = 0;
    status_ = file_->Append(Slice(&type, 1));
    if (!status_.ok()) return;
    offset_ += 1;

    // CRC 覆盖 payload + compression_type，掩码后存储防止与数据内容碰撞。
    uint32_t crc = crc32c::Value(data.data(), data.size());
    crc = crc32c::Extend(crc, &type, 1);
    char crc_buf[4];
    EncodeFixed32(crc_buf, crc32c::Mask(crc));
    status_ = file_->Append(Slice(crc_buf, 4));
    if (!status_.ok()) return;
    offset_ += 4;
}

// Finish 完成 SSTable 构建，依次写入 Filter Block、MetaIndex Block、Index Block、Footer。
Status TableBuilder::Finish() {
    Flush();
    closed_ = true;

    BlockHandle filter_block_handle, metaindex_handle, index_handle;

    // Filter Block 包含所有 Data Block 的布隆过滤器数据，直接以 raw block 写出。
    if (status_.ok() && filter_block_ != nullptr) {
        Slice filter_block_contents = filter_block_->Finish();
        WriteRawBlock(filter_block_contents, &filter_block_handle);
    }

    // MetaIndex Block：key 为 "filter.<policy_name>"，value 为 Filter Block 的 Handle。
    // 读取时据此定位 Filter Block 的物理位置。
    if (status_.ok()) {
        BlockBuilder meta_block(&options_);
        if (filter_block_ != nullptr) {
            std::string key = "filter.";
            key.append(options_.filter_policy->Name());
            std::string handle_encoding;
            filter_block_handle.EncodeTo(&handle_encoding);
            meta_block.Add(Slice(key), Slice(handle_encoding));
        }
        WriteBlock(&meta_block, &metaindex_handle);
    }

    // Index Block：每个 entry 对应一个 Data Block，value 为该 Data Block 的 Handle。
    if (status_.ok()) {
        if (pending_index_entry_) {
            std::string handle_encoding;
            pending_handle_.EncodeTo(&handle_encoding);
            index_block_->Add(Slice(last_key_), Slice(handle_encoding));
            pending_index_entry_ = false;
        }
        WriteBlock(index_block_, &index_handle);
    }

    // Footer 固定 48 字节，位于文件末尾，存储 MetaIndex Handle、Index Handle 和魔数。
    if (status_.ok()) {
        Footer footer;
        footer.set_metaindex_handle(metaindex_handle);
        footer.set_index_handle(index_handle);
        std::string footer_encoding;
        footer.EncodeTo(&footer_encoding);
        status_ = file_->Append(Slice(footer_encoding));
        if (status_.ok()) {
            offset_ += footer_encoding.size();
        }
    }

    return status_;
}

uint64_t TableBuilder::FileSize() const {
    return offset_;
}

} // namespace minidb
