#include "table.h"
#include "format.h"
#include "coding.h"
#include "comparator.h"
#include "options.h"
#include "filter_block.h"
#include "db_format.h"
#include <cassert>
#include <memory>

namespace minidb {

// Rep 持有 SSTable 的所有运行时状态，通过 Pimpl 惯用法对外隐藏细节。
struct Table::Rep {
    Options           options;
    Status            status;
    RandomAccessFile* file;
    uint64_t          cache_id;
    BlockHandle       metaindex_handle;
    Block*            index_block;    // 常驻内存，所有点查的入口。

    FilterBlockReader* filter;
    Slice              filter_data;   // filter_alloc 内存的只读视图，生命周期与 filter_alloc 绑定。

    const char* filter_alloc; // Filter Block 的原始字节，析构时释放。
    const char* index_alloc;  // Index Block 的原始字节，析构时释放。

    Rep() : filter(nullptr), filter_alloc(nullptr), index_alloc(nullptr) {}
    ~Rep() {
        delete filter;
        delete index_block;
        // filter_data 是对 filter_alloc 内存的 Slice 视图，filter 对象析构后方可释放底层内存。
        delete[] filter_alloc;
        // index_alloc 持有从磁盘读入的 Index Block 原始字节，index_block 析构后释放。
        delete[] index_alloc;
    }
};

// ---------------------------------------------------------------------------
// TwoLevelIterator：两层迭代器，Index Block（外层）+ Data Block（内层）。
// 外层迭代器定位 Data Block，内层迭代器遍历具体的 KV 记录。
// ---------------------------------------------------------------------------
class TwoLevelIterator : public Iterator {
public:
    TwoLevelIterator(Iterator* index_iter, RandomAccessFile* file,
                     const ReadOptions& options, const Comparator* cmp)
        : index_iter_(index_iter),
          file_(file),
          options_(options),
          data_iter_(nullptr),
          cmp_(cmp),
          data_block_(nullptr),
          data_block_scratch_(nullptr) {}

    ~TwoLevelIterator() override {
        delete index_iter_;
        SetDataIterator(nullptr);
    }

    bool  Valid()  const override {
        return index_iter_ != nullptr && data_iter_ != nullptr && data_iter_->Valid();
    }
    Slice key()    const override { assert(Valid()); return data_iter_->key(); }
    Slice value()  const override { assert(Valid()); return data_iter_->value(); }

    Status status() const override {
        if (index_iter_ && !index_iter_->status().ok()) return index_iter_->status();
        if (data_iter_ && !data_iter_->status().ok())   return data_iter_->status();
        return status_;
    }

    void Seek(const Slice& target) override {
        if (!index_iter_) return;
        index_iter_->Seek(target);
        InitDataBlock();
        if (data_iter_) data_iter_->Seek(target);
    }

    void SeekToFirst() override {
        if (!index_iter_) return;
        index_iter_->SeekToFirst();
        InitDataBlock();
        if (data_iter_) data_iter_->SeekToFirst();
    }

    void SeekToLast() override {
        if (!index_iter_) return;
        index_iter_->SeekToLast();
        InitDataBlock();
        if (data_iter_) data_iter_->SeekToLast();
    }

    void Next() override {
        assert(Valid());
        data_iter_->Next();
        if (!data_iter_->Valid()) {
            index_iter_->Next();
            InitDataBlock();
            if (data_iter_) data_iter_->SeekToFirst();
        }
    }

    void Prev() override {
        assert(Valid());
        data_iter_->Prev();
        if (!data_iter_->Valid()) {
            index_iter_->Prev();
            InitDataBlock();
            if (data_iter_) data_iter_->SeekToLast();
        }
    }

private:
    // InitDataBlock 根据 index_iter_ 当前指向的 Handle 加载对应的 Data Block。
    // 若 handle 与上次相同（缓存命中），则跳过磁盘读取。
    void InitDataBlock() {
        if (!index_iter_->Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        Slice handle_val = index_iter_->value();
        if (data_iter_ != nullptr && handle_val.compare(data_block_handle_) == 0) {
            return; // 已加载相同 Data Block，无需重新读取。
        }

        SetDataIterator(nullptr);

        BlockHandle handle;
        Status s = handle.DecodeFrom(&handle_val);
        if (s.ok()) {
            char* scratch = new char[handle.size()];
            Slice contents;
            s = file_->Read(handle.offset(), handle.size(), &contents, scratch);
            if (s.ok()) {
                Block* block = new Block(contents);
                data_iter_           = block->NewIterator(cmp_);
                data_block_          = block;
                data_block_scratch_  = scratch;
                data_block_handle_   = handle_val.ToString();
            } else {
                delete[] scratch;
            }
        }
        status_ = s;
    }

    // SetDataIterator 统一管理 Data Block 及其原始内存的生命周期。
    void SetDataIterator(Iterator* data_iter) {
        delete data_iter_;
        data_iter_ = data_iter;
        delete data_block_;
        data_block_ = nullptr;
        delete[] data_block_scratch_;
        data_block_scratch_ = nullptr;
    }

    Iterator*         index_iter_;
    RandomAccessFile* file_;
    ReadOptions       options_;
    Iterator*         data_iter_;
    const Comparator* cmp_;
    Block*            data_block_;
    char*             data_block_scratch_;
    std::string       data_block_handle_;
    Status            status_;
};

// ---------------------------------------------------------------------------
// Table::Open：从文件末尾读取 Footer，加载 Index Block 和（可选）Filter Block。
// ---------------------------------------------------------------------------
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
    *table = nullptr;
    if (size < Footer::kEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
    }

    // 读取固定 48 字节的 Footer，校验魔数，解析 Index Handle 和 MetaIndex Handle。
    char footer_space[Footer::kEncodedLength];
    Slice footer_input;
    Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                          &footer_input, footer_space);
    if (!s.ok()) return s;

    Footer footer;
    s = footer.DecodeFrom(&footer_input);
    if (!s.ok()) return s;

    // 将 Index Block 常驻内存，后续所有点查的入口。
    char* index_space = new char[footer.index_handle().size()];
    Slice index_contents;
    s = file->Read(footer.index_handle().offset(), footer.index_handle().size(),
                   &index_contents, index_space);
    if (!s.ok()) {
        delete[] index_space;
        return s;
    }

    Block* index_block = new Block(index_contents);
    Rep*   rep         = new Table::Rep;
    rep->options     = options;
    rep->file        = file;
    rep->index_block = index_block;
    rep->index_alloc = index_space;

    // 若配置了 FilterPolicy，则从 MetaIndex Block 中定位并加载 Filter Block。
    // Filter Block 按每 2KB 数据生成一个布隆过滤器，用于在读 Data Block 前快速排除不存在的 key。
    if (options.filter_policy != nullptr) {
        uint64_t meta_size   = footer.metaindex_handle().size();
        uint64_t meta_offset = footer.metaindex_handle().offset();
        char*    meta_space  = new char[meta_size];
        Slice    meta_contents;

        if (file->Read(meta_offset, meta_size, &meta_contents, meta_space).ok()) {
            Block meta_block(meta_contents);
            Iterator* meta_iter = meta_block.NewIterator(BytewiseComparator());

            std::string filter_key = "filter.";
            filter_key.append(options.filter_policy->Name());
            meta_iter->Seek(Slice(filter_key));

            if (meta_iter->Valid() && meta_iter->key() == Slice(filter_key)) {
                BlockHandle filter_handle;
                Slice v = meta_iter->value();
                if (filter_handle.DecodeFrom(&v).ok()) {
                    uint64_t filter_size   = filter_handle.size();
                    uint64_t filter_offset = filter_handle.offset();
                    char*    filter_space  = new char[filter_size];
                    Slice    filter_contents;
                    if (file->Read(filter_offset, filter_size,
                                   &filter_contents, filter_space).ok()) {
                        rep->filter_data  = filter_contents;
                        rep->filter       = new FilterBlockReader(options.filter_policy,
                                                                   rep->filter_data);
                        rep->filter_alloc = filter_space;
                    } else {
                        delete[] filter_space;
                    }
                }
            }
            delete meta_iter;
        }
        delete[] meta_space;
    }

    *table = new Table(rep);
    return Status::OK();
}

// Table::Get 执行单 key 点查：
//   1. 在 Index Block 中二分查找包含 key 的 Data Block Handle；
//   2. 若有 Bloom Filter，先做概率性过滤，排除不存在的 key；
//   3. 读取 Data Block，再次二分确认 key 是否存在及其 value。
Status Table::Get(const ReadOptions& /*options*/, const Slice& k, std::string* value) {
    std::unique_ptr<Iterator> iiter(rep_->index_block->NewIterator(rep_->options.comparator));
    iiter->Seek(k);

    if (!iiter->Valid()) {
        return Status::NotFound("Not found in SSTable");
    }

    Slice      handle_val = iiter->value();
    BlockHandle handle;
    if (!handle.DecodeFrom(&handle_val).ok()) {
        return Status::NotFound("Not found in SSTable");
    }

    // Bloom Filter：对 UserKey（去掉序列号尾缀）做概率性匹配，确定不存在则直接返回。
    if (rep_->filter != nullptr) {
        Slice user_key = ExtractUserKey(k);
        if (!rep_->filter->KeyMayMatch(handle.offset(), user_key)) {
            return Status::NotFound("Blocked by Bloom Filter");
        }
    }

    // 加载对应的 Data Block 并做精确的 key 匹配。
    std::unique_ptr<char[]> block_space(new char[handle.size()]);
    Slice block_contents;
    Status s = rep_->file->Read(handle.offset(), handle.size(),
                                &block_contents, block_space.get());
    if (!s.ok()) return s;

    Block data_block(block_contents);
    std::unique_ptr<Iterator> data_iter(
        data_block.NewIterator(rep_->options.comparator));
    data_iter->Seek(k);

    if (!data_iter->Valid()) {
        return Status::NotFound("Not found in Data Block");
    }

    // Seek 返回第一个 >= k 的 key，需校验 UserKey 是否严格相等。
    Slice found_user_key  = ExtractUserKey(data_iter->key());
    Slice target_user_key = ExtractUserKey(k);
    if (found_user_key != target_user_key) {
        return Status::NotFound("Not found in Data Block (key mismatch)");
    }

    *value = data_iter->value().ToString();
    return Status::OK();
}

Table::~Table() {
    delete rep_;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
    return new TwoLevelIterator(
        rep_->index_block->NewIterator(rep_->options.comparator),
        rep_->file, options, rep_->options.comparator);
}

} // namespace minidb
