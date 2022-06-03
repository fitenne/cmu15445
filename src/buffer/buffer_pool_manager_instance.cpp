//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::shared_lock<std::shared_mutex> lock_page{pageframe_table_latch_};
  return FlushPgLgc(page_id);
}

bool BufferPoolManagerInstance::FlushPgLgc(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::shared_lock lock_page{pageframe_table_latch_};
  FlushAllPgsLgc();
}

void BufferPoolManagerInstance::FlushAllPgsLgc() {
  for (auto &[_, frame_id] : page_table_) {
    Page *page = pages_ + frame_id;
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock_page_free{pageframe_table_latch_, free_list_latch_};
  return NewPgLgc(page_id);
}

Page *BufferPoolManagerInstance::NewPgLgc(page_id_t *page_id) {
  frame_id_t free_frame;
  if (!GetFreeFrameLgc(&free_frame)) {
    return nullptr;
  }

  auto disk_page_id = AllocatePage();
  pages_[free_frame].ResetMemory();
  pages_[free_frame].pin_count_ = 1;
  pages_[free_frame].is_dirty_ = true;
  pages_[free_frame].page_id_ = disk_page_id;
  page_table_[disk_page_id] = free_frame;
  frame_table_[free_frame] = disk_page_id;

  *page_id = disk_page_id;
  return pages_ + free_frame;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock_page_free{pageframe_table_latch_, free_list_latch_};
  return FetchPgLgc(page_id);
}

Page *BufferPoolManagerInstance::FetchPgLgc(page_id_t page_id) {
  if (page_table_.count(page_id) != 0) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = pages_ + frame_id;
    page->pin_count_++;

    replacer_->Pin(frame_id);
    return page;
  }

  frame_id_t free_frame;
  if (!GetFreeFrameLgc(&free_frame)) {
    return nullptr;
  }

  Page *page = pages_ + free_frame;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->data_);
  page_table_[page_id] = free_frame;
  frame_table_[free_frame] = page_id;

  replacer_->Pin(free_frame);
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock_page_free{pageframe_table_latch_, free_list_latch_};
  return DeletePgLgc(page_id);
}

bool BufferPoolManagerInstance::DeletePgLgc(page_id_t page_id) {
  if (page_table_.count(page_id) != 0) {
    return true;
  }
  Page *page = pages_ + page_table_[page_id];
  if (page->pin_count_ != 0) {
    return false;
  }

  if (page->is_dirty_) {
    disk_manager_->WritePage(page_id, page->data_);
  }

  auto frame_id = page_table_[page_id];
  free_list_.emplace_back(frame_id);
  page_table_.erase(page_id);
  frame_table_.erase(frame_id);
  new (page) Page();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::shared_lock lock_page{pageframe_table_latch_};
  return UnpinPgLgc(page_id, is_dirty);
}

bool BufferPoolManagerInstance::UnpinPgLgc(page_id_t page_id, bool is_dirty) {
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page->pin_count_ <= 0) {
    return false;
  }

  --page->pin_count_;
  page->is_dirty_ = is_dirty;
  replacer_->Unpin(frame_id);
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::GetFreeFrameLgc(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  if (!replacer_->Victim(frame_id)) {
    return false;
  }
  Page *page = pages_ + *frame_id;
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page_table_.erase(frame_table_[*frame_id]);
  frame_table_.erase(*frame_id);
  return true;
}

}  // namespace bustub
