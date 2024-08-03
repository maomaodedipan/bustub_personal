#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {}

void BasicPageGuard::Drop() {
    if (page_ != nullptr){
      bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    }
    page_ = nullptr;
    bpm_ = nullptr;
    //guard_.is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
    Drop(); 
    page_ = that.page_;
    bpm_ = that.bpm_;
    is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
    return *this;
}

BasicPageGuard::~BasicPageGuard(){
    Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
    this->page_->RLatch();
    ReadPageGuard read_page_guard(this->bpm_,this->page_);
    page_ = nullptr;
    bpm_ = nullptr;
    return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { 
    this->page_->WLatch();
    WritePageGuard write_page_guard(this->bpm_,this->page_);
    page_ = nullptr;
    bpm_ = nullptr;
    return  write_page_guard;
 }

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
    guard_ = BasicPageGuard(bpm,page);
} 

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
    guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    Drop(); 
    guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
    auto &basic_guard = this->guard_;
    if (basic_guard.page_ != nullptr) {
      basic_guard.bpm_->UnpinPage(basic_guard.page_->GetPageId(), basic_guard.is_dirty_);
      basic_guard.page_->RUnlatch();
    }
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
    //guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() {
    Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
    guard_ = BasicPageGuard(bpm,page);
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
    guard_ = std::move(that.guard_);
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {
    auto &basic_guard = this->guard_;
    if (basic_guard.page_ != nullptr) {
      basic_guard.bpm_->UnpinPage(basic_guard.page_->GetPageId(), basic_guard.is_dirty_);
      basic_guard.page_->WUnlatch();
    }
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
    //guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() {
    Drop();
}  // NOLINT

}  // namespace bustub
