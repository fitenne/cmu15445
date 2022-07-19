//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lst_.empty()) {
    return false;
  }

  std::scoped_lock lock{latch_};
  if (lst_.empty()) {
    return false;
  }
  *frame_id = lst_.front();
  lst_.pop_front();
  frame_table_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frame_table_.count(frame_id) == 0) {
    return;
  }

  std::scoped_lock lock{latch_};
  if (frame_table_.count(frame_id) == 0) {
    return;
  }
  lst_.erase(frame_table_[frame_id]);
  frame_table_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_table_.count(frame_id) != 0) {
    return;
  }

  std::scoped_lock lock{latch_};
  if (frame_table_.count(frame_id) != 0) {
    return;
  }
  frame_table_[frame_id] = lst_.insert(lst_.end(), frame_id);
}

size_t LRUReplacer::Size() { return lst_.size(); }

}  // namespace bustub
