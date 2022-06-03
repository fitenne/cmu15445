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
  std::lock_guard<std::mutex> lock{latch_};
  if (frame_stroage_.empty()) {
    *frame_id = 0;
    return false;
  }

  *frame_id = frame_stroage_.front();
  frame_stroage_.pop_front();
  index_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock{latch_};
  if (index_.count(frame_id) != 0) {
    frame_stroage_.erase(index_[frame_id]);
    index_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock{latch_};
  if (index_.count(frame_id) == 0) {
    frame_stroage_.emplace_back(frame_id);
    index_[frame_id] = std::prev(frame_stroage_.end());
  }
}

size_t LRUReplacer::Size() { return frame_stroage_.size(); }

}  // namespace bustub
