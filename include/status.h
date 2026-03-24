#pragma once

#include "slice.h"
#include <string>

namespace minidb {

class [[nodiscard]] Status {
public:
    // 错误码枚举
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5
    };

    // 默认构造为OK状态,零成本
    Status() noexcept : state_(nullptr) {}
    ~Status() { delete[] state_; }

    // 拷贝与移动语义,错误状态的拷贝需要深拷贝 state_
    Status(const Status& rhs);
    Status& operator=(const Status& rhs);
    Status(Status&& rhs) noexcept;
    Status& operator=(Status&& rhs) noexcept;

    // 静态工厂方法,方便上层快速构建不同类型的错误
    static Status OK() { return Status(); }
    static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotFound, msg, msg2);
    }
    static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kCorruption, msg, msg2);
    }
    static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotSupported, msg, msg2);
    }
    static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kInvalidArgument, msg, msg2);
    }
    static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kIOError, msg, msg2);
    }

    // 状态查询接口
    bool ok() const { return state_ == nullptr; }
    bool IsNotFound() const { return code() == kNotFound; }
    bool IsCorruption() const { return code() == kCorruption; }
    bool IsIOError() const { return code() == kIOError; }
    bool IsNotSupported() const { return code() == kNotSupported; }
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    // 输出为便于阅读的字符串格式
    std::string ToString() const;

private:
    Code code() const {
        return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
    }

    // 核心构造函数，负责组装底层的内存块
    Status(Code code, const Slice& msg, const Slice& msg2);
    static const char* CopyState(const char* s);

    // state_ 内存布局：
    // state_[0..3] == message length (uint32_t)
    // state_[4] == code (Code enum)
    // state_[5..] == message
    const char* state_;
};

} // namespace minidb