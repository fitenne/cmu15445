//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  page_id_t dir_page_id{};
  page_id_t bucket_page_id{};
  Page *dir_rpage{};
  if ((dir_rpage = buffer_pool_manager->NewPage(&dir_page_id)) == nullptr ||
      buffer_pool_manager->NewPage(&bucket_page_id) == nullptr) {
    throw Exception("failed to allocate a new page");
  }

  this->directory_page_id_ = dir_page_id;
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_rpage->GetData());
  dir_page->SetPageId(dir_page_id);
  dir_page->SetBucketPageId(0, bucket_page_id);
  dir_page->SetLocalDepth(0, 0);

  buffer_pool_manager->UnpinPage(dir_page_id, true);
  buffer_pool_manager->UnpinPage(bucket_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *dir_rpage = buffer_pool_manager_->FetchPage(directory_page_id_);
  if (dir_rpage == nullptr) {
    return nullptr;
  }
  return reinterpret_cast<HashTableDirectoryPage *>(dir_rpage->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<Page *, HASH_TABLE_BUCKET_TYPE *> HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  // fixme
  // nullptr?
  Page *bucket_rpage = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_rpage->GetData());
  return {bucket_rpage, bucket_page};
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  auto [bucket_rpage, bucket_page] = FetchBucketPage(KeyToPageId(key, dir_page));

  bucket_rpage->RLatch();
  bool success = bucket_page->GetValue(key, comparator_, result);
  bucket_rpage->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  auto [bucket_rpage, bucket_page] = FetchBucketPage(KeyToPageId(key, dir_page));

  bucket_rpage->WLatch();
  bool success = bucket_page->Insert(key, value, comparator_);
  if (!success) {
    if (!bucket_page->IsFull()) {
      bucket_rpage->WUnlatch();
      table_latch_.RUnlock();
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
      return false;
    }
    bucket_rpage->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return SplitInsert(transaction, key, value);
  }

  bucket_rpage->WUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  // status may changed
  // try insert before split
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  auto [bucket_rpage, bucket_page] = FetchBucketPage(KeyToPageId(key, dir_page));

  bucket_rpage->WLatch();
  bool success = bucket_page->Insert(key, value, comparator_);
  if (success) {
    bucket_rpage->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    return true;
  }
  if (!success && !bucket_page->IsFull()) {
    bucket_rpage->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return false;
  }

  // allocate a new bucket
  page_id_t new_bucket_id{};
  Page *new_bucket_rpage = buffer_pool_manager_->NewPage(&new_bucket_id);
  if (new_bucket_rpage == nullptr) {
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return false;
  }
  HASH_TABLE_BUCKET_TYPE *new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_bucket_rpage->GetData());

  if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  uint32_t bucket_ld = dir_page->GetLocalDepth(bucket_idx);
  uint32_t step = (1U << bucket_ld);
  uint32_t mask = step - 1;
  for (size_t i = (bucket_idx & mask); i < dir_page->Size(); i += step) {
    dir_page->IncrLocalDepth(i);
  }
  dir_page->SetBucketPageId(bucket_idx, new_bucket_id);

  bool new_page_dirty{false};
  new_bucket_rpage->WLatch();
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    KeyType key = bucket_page->KeyAt(i);
    if (KeyToDirectoryIndex(key, dir_page) == bucket_idx) {
      new_page_dirty = true;
      new_bucket_page->Insert(key, bucket_page->ValueAt(i), comparator_);
      bucket_page->RemoveAt(i);
    }
  }
  new_bucket_rpage->WUnlatch();
  bucket_rpage->WUnlatch();

  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(new_bucket_id, new_page_dirty);
  buffer_pool_manager_->UnpinPage(bucket_page_id, new_page_dirty);
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  auto [bucket_rpage, bucket_page] = FetchBucketPage(KeyToPageId(key, dir_page));

  bucket_rpage->WLatch();
  bool success = bucket_page->Remove(key, value, comparator_);
  // cautions! it's possbily to call Remove on an empty bucket
  if (bucket_page->IsEmpty()) {
    bucket_rpage->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, success);
    Merge(nullptr, key, value);
  } else {
    bucket_rpage->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  }
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);

  uint32_t bucket_ld = dir_page->GetLocalDepth(bucket_idx);
  uint32_t image_idx = bucket_idx ^ (1U << (bucket_ld - 1));
  if (bucket_ld > 0 && dir_page->GetLocalDepth(image_idx) == bucket_ld) {
    // merge
    uint32_t step = 1U << (bucket_ld - 1);
    uint32_t mask = step - 1;
    page_id_t image_bucket_id = dir_page->GetBucketPageId(image_idx);
    uint32_t sz = dir_page->Size();
    for (size_t i = (bucket_idx & mask); i < sz; i += step) {
      dir_page->DecrLocalDepth(i);
      dir_page->SetBucketPageId(i, image_bucket_id);
    }
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    if (!buffer_pool_manager_->DeletePage(bucket_page_id)) {
      throw Exception("failed to delete a page");
    }

    // should shrink?
    uint32_t max_ld = dir_page->GetLocalDepth(0);
    for (size_t i = 1; i < dir_page->Size(); ++i) {
      max_ld = std::max(max_ld, dir_page->GetLocalDepth(i));
    }
    unsigned delta = dir_page->GetGlobalDepth() - max_ld;
    for (unsigned i = 0; i < delta; ++i) {
      dir_page->DecrGlobalDepth();
    }
  }

  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
