#include "merging_iterator.h"

namespace minidb {

MergingIterator::MergingIterator(const Comparator* comparator,
                                 std::vector<Iterator*> children)
    : comparator_(comparator),
      children_(std::move(children)),
      current_(nullptr),
      forward_(true) {}

MergingIterator::~MergingIterator() {
    for (Iterator* it : children_) {
        delete it;
    }
}

bool MergingIterator::Valid() const {
    return current_ != nullptr && current_->Valid();
}

void MergingIterator::SeekToFirst() {
    forward_ = true;
    for (Iterator* it : children_) {
        it->SeekToFirst();
    }
    FindSmallest();
}

void MergingIterator::SeekToLast() {
    forward_ = false;
    for (Iterator* it : children_) {
        it->SeekToLast();
    }
    FindLargest();
}

void MergingIterator::Seek(const Slice& target) {
    forward_ = true;
    for (Iterator* it : children_) {
        it->Seek(target);
    }
    FindSmallest();
}

void MergingIterator::Next() {
    // 确保所有迭代器都已定位到 > current_->key() 的位置。
    // 若迭代方向刚从 backward 切换到 forward，需要将非当前迭代器重新定位。
    if (!forward_) {
        forward_ = true;
        for (Iterator* it : children_) {
            if (it != current_) {
                it->Seek(current_->key());
                // 若 Seek 结果等于 current_->key()，需要再 Next 一步。
                if (it->Valid() && comparator_->Compare(it->key(), current_->key()) == 0) {
                    it->Next();
                }
            }
        }
    }
    current_->Next();
    FindSmallest();
}

void MergingIterator::Prev() {
    if (forward_) {
        forward_ = false;
        for (Iterator* it : children_) {
            if (it != current_) {
                it->Seek(current_->key());
                if (it->Valid()) {
                    it->Prev(); // 后退到严格小于 current_->key() 的位置
                } else {
                    it->SeekToLast();
                }
            }
        }
    }
    current_->Prev();
    FindLargest();
}

Slice MergingIterator::key() const {
    return current_->key();
}

Slice MergingIterator::value() const {
    return current_->value();
}

Status MergingIterator::status() const {
    for (Iterator* it : children_) {
        if (!it->status().ok()) return it->status();
    }
    return Status::OK();
}

void MergingIterator::FindSmallest() {
    Iterator* smallest = nullptr;
    for (Iterator* it : children_) {
        if (it->Valid()) {
            if (smallest == nullptr ||
                comparator_->Compare(it->key(), smallest->key()) < 0) {
                smallest = it;
            }
        }
    }
    current_ = smallest;
}

void MergingIterator::FindLargest() {
    Iterator* largest = nullptr;
    for (Iterator* it : children_) {
        if (it->Valid()) {
            if (largest == nullptr ||
                comparator_->Compare(it->key(), largest->key()) > 0) {
                largest = it;
            }
        }
    }
    current_ = largest;
}

} // namespace minidb
