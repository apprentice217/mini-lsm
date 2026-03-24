#include "status.h"
#include <cstdint>
#include <cstring>

namespace minidb {

// 辅助函数：深拷贝 state_ 内存块
const char* Status::CopyState(const char* state) {
    // 从前4个字节读取消息长度
    uint32_t size;
    std::memcpy(&size, state, sizeof(size));
    // 总分配长度=长度(4)+错误码(1)+消息内容(size)
    char* result = new char[5 + size];
    std::memcpy(result, state, 5 + size);
    return result;
}

Status::Status(Code code,const Slice& msg,const Slice& msg2) {
    assert(code != kOk);
    const uint32_t len1 = static_cast<uint32_t>(msg.size());
    const uint32_t len2 = static_cast<uint32_t>(msg2.size());
    // 如果存在第二个信息片段，在中间加一个冒号和空格 ": "
    const uint32_t size = len1 + (len2 ? (2+len2) : 0);

    char* result = new char[5 + size];
    // 写入长度
    std::memcpy(result, &size, sizeof(size));
    // 写入错误码
    result[4] = static_cast<char>(code);
    // 写入错误消息主体
    std::memcpy(result + 5, msg.data(), len1);
    if(len2) {
        result[5 + len1] = ':';
        result[5 + len1 + 1] = ' ';
        std::memcpy(result + 5 + len1 + 2, msg2.data(), len2);
    }
    state_ = result;
}

Status::Status(const Status& rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

Status& Status::operator=(const Status& rhs) {
    if (this != &rhs) {
        // 先拷贝新状态，再释放旧状态，保证异常安全
        const char* new_state = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
        delete[] state_;
        state_ = new_state;
    }
    return *this;
}

Status::Status(Status&& rhs) noexcept : state_(rhs.state_) {
    rhs.state_ = nullptr;
}

Status& Status::operator=(Status&& rhs) noexcept {
    if (this != &rhs) {
        delete[] state_;
        state_ = rhs.state_;
        rhs.state_ = nullptr;
    }
    return *this;
}

std::string Status::ToString() const {
    if (state_ == nullptr) {
        return "OK";
    } else {
        char tmp[30];
        const char* type;
        switch (code()) {
            case kOk:             type = "OK"; break;
            case kNotFound:       type = "NotFound: "; break;
            case kCorruption:     type = "Corruption: "; break;
            case kNotSupported:   type = "Not implemented: "; break;
            case kInvalidArgument:type = "Invalid argument: "; break;
            case kIOError:        type = "IO error: "; break;
            default:
                std::snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
                              static_cast<int>(code()));
                type = tmp;
                break;
        }
        std::string result(type);
        uint32_t length;
        std::memcpy(&length, state_, sizeof(length));
        result.append(state_ + 5, length);
        return result;
    }
}

}// namespace minidb