#include "format.h"
#include "coding.h" // 完美复用底层序列化能力

namespace minidb {

void BlockHandle::EncodeTo(std::string* dst) const {
    // 极致压缩：使用变长整型写入 offset 和 size
    PutVarint64(dst, offset_);
    PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
    const char* p = input->data();
    const char* limit = p + input->size();
    
    // TLV 游走法：解析成功后指针 p 会自动推进
    if (GetVarint64(&p, limit, &offset_) &&
        GetVarint64(&p, limit, &size_)) {
        // 截断已经被解析的部分
        input->remove_prefix(p - input->data());
        return Status::OK();
    }
    return Status::Corruption("bad block handle");
}

void Footer::EncodeTo(std::string* dst) const {
    const size_t original_size = dst->size();
    
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    
    // 工业级补齐：为了保证 Footer 固定为 48 字节，中间的空隙用 0 填充
    // 使得 Magic Number 永远精确落在文件倒数 8 字节的位置
    dst->resize(original_size + 2 * BlockHandle::kMaxEncodedLength);
    
    // 强制小端序追加 8 字节魔数
    PutFixed64(dst, kTableMagicNumber);
}

Status Footer::DecodeFrom(Slice* input) {
    if (input->size() < kEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
    }

    // 提取并校验最后 8 字节的魔数
    const char* magic_ptr = input->data() + kEncodedLength - 8;
    const uint64_t magic = DecodeFixed64(magic_ptr);
    if (magic != kTableMagicNumber) {
        return Status::Corruption("not an sstable (bad magic number)");
    }

    // 依次解析两个 Handle
    Status result = metaindex_handle_.DecodeFrom(input);
    if (result.ok()) {
        result = index_handle_.DecodeFrom(input);
    }
    if (result.ok()) {
        // 跳过填充的废弃字节，直接对齐到 Magic Number 前面
        const char* end = magic_ptr;
        *input = Slice(end, 8); 
    }
    return result;
}

} // namespace minidb