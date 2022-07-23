//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <map>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/config.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ScaleTest) {
  using KeyType = int;
  using ValueType = int;

  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // empty hash table at initial
  std::vector<int> res;
  EXPECT_FALSE(ht.GetValue(nullptr, 0, &res));
  EXPECT_TRUE(res.empty());

  // insert some k-v pair until a bucket split
  size_t i;
  for (i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }
  EXPECT_EQ(ht.GetGlobalDepth(), 0);
  EXPECT_TRUE(ht.Insert(nullptr, BUCKET_ARRAY_SIZE, BUCKET_ARRAY_SIZE));  // should call SplitInsert internally here
  EXPECT_EQ(ht.GetGlobalDepth(), 1);
  for (i = 0; i <= BUCKET_ARRAY_SIZE; ++i) {
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(res.size(), i + 1);
  }

  // insert duplicate k-v pair should fail
  // delete some elements
  for (i = 0; i <= BUCKET_ARRAY_SIZE; i += 2) {
    EXPECT_FALSE(ht.Insert(nullptr, i, i));
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
  }
  // check the other elements and clear the table
  for (i = 1; i <= BUCKET_ARRAY_SIZE; i += 2) {
    res.clear();
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(res.size(), 1);
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    if (i + 2 > BUCKET_ARRAY_SIZE) {
      ht.VerifyIntegrity();
      EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    } else {
      EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    }
  }
  EXPECT_EQ(ht.GetGlobalDepth(), 0);

  ht.VerifyIntegrity();

  // ---------------------------------------------------
  // many insert and remove
  std::default_random_engine engine(time(nullptr));
  std::uniform_int_distribution<size_t> dist(1, 114514);
  std::vector<std::pair<KeyType, ValueType>> inserted{};
  for (i = 0; i < BUCKET_SIZE * 114 + dist(engine); ++i) {
    bool op = (dist(engine) % 10) < 5;
    if (!op) {
      KeyType key = dist(engine);
      ValueType value = dist(engine);
      auto kv = std::make_pair(key, value);
      if (std::find(inserted.begin(), inserted.end(), kv) == inserted.end()) {
        EXPECT_TRUE(ht.Insert(nullptr, key, value));
        inserted.emplace_back(kv);
        EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
      } else {
        EXPECT_FALSE(ht.Insert(nullptr, key, value));
      }
    } else {
      if (inserted.empty()) {
        continue;
      }
      size_t idx = dist(engine) % inserted.size();
      std::swap(inserted[idx], inserted.back());
      ht.Remove(nullptr, inserted.back().first, inserted.back().second);
      inserted.pop_back();
    }
  }
  ht.VerifyIntegrity();
  for (auto &kv : inserted) {
    res.clear();
    EXPECT_TRUE(ht.GetValue(nullptr, kv.first, &res));
    EXPECT_TRUE(ht.Remove(nullptr, kv.first, kv.second));
    ht.VerifyIntegrity();
  }
  // reinterpret_cast<HashTableDirectoryPage*>(bpm->FetchPage(0)->GetData())->PrintDirectory();
  // int id;
  // while (std::cin >> id) {
  //   reinterpret_cast<HashTableBucketPage<KeyType, ValueType,
  //   IntComparator>*>(bpm->FetchPage(id)->GetData())->PrintBucket();
  // }

  EXPECT_LE(ht.GetGlobalDepth(), 0);

  for (auto it = inserted.rbegin(); it != inserted.rend(); ++it) {
    EXPECT_TRUE(ht.Insert(nullptr, it->first, it->second));
  }
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ConcurrentTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // empty hash table at initial
  std::vector<int> res;

  // many insert and remove
  const int n_thread = 20;
  std::thread threads[n_thread];
  std::vector<std::pair<int, int>> inserted;
  std::mutex inserted_latch;
  for (auto &i : threads) {
    i = std::thread([&ht, &inserted_latch, &inserted]() {
      std::default_random_engine engine(time(nullptr));
      std::uniform_int_distribution<size_t> dist(1, 114514);

      for (size_t i = 0; i < BUCKET_SIZE * (dist(engine) % 10 + 1); ++i) {
        bool op_insert = dist(engine) % 10 < 7;
        if (op_insert) {
          int k = dist(engine);
          int v = dist(engine);
          ht.Insert(nullptr, k, v);
          std::scoped_lock lock{inserted_latch};
          inserted.emplace_back(k, v);
          std::vector<int> res;
          ht.GetValue(nullptr, k, &res);
        } else {
          std::scoped_lock lock{inserted_latch};
          if (inserted.empty()) {
            continue;
          }
          size_t p = dist(engine) % inserted.size();
          swap(inserted[p], inserted.back());
          ht.Remove(nullptr, inserted.back().first, inserted.back().second);
          inserted.pop_back();
        }

        ht.VerifyIntegrity();
      }
    });
  }

  for (auto &i : threads) {
    i.join();
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

/**
 * did not find a way to mock hash function
 * manually modify hash function to return the key itself before run this test
 */
TEST(HashTableTest, ScaleTest2) {
  using KeyType = int;
  using ValueType = int;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  std::map<KeyType, ValueType> inserted;

  EXPECT_EQ(ht.GetGlobalDepth(), 0);
  for (int i = 0b000; i < 0b1000; ++i) {
    for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
      EXPECT_TRUE(ht.Insert(nullptr, i, j));
    }
  }
  EXPECT_EQ(ht.GetGlobalDepth(), 3);

  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b001, j));
  }
  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b101, j));
  }
  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b111, j));
  }
  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b011, j));
  }

  EXPECT_EQ(ht.GetGlobalDepth(), 3);
  ht.VerifyIntegrity();

  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b100, j));
  }
  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b110, j));
  }
  EXPECT_EQ(ht.GetGlobalDepth(), 2);
  ht.VerifyIntegrity();

  for (int i = 0b000; i < 0b1000; ++i) {
    std::vector<ValueType> result;
    if (i == 0b000 || i == 0b010) {
      EXPECT_TRUE(ht.GetValue(nullptr, i, &result));
      EXPECT_EQ(result.size(), BUCKET_ARRAY_SIZE);
    } else {
      EXPECT_FALSE(ht.GetValue(nullptr, i, &result));
      EXPECT_EQ(result.size(), 0);
    }
  }

  for (size_t j = 0; j < BUCKET_ARRAY_SIZE; ++j) {
    EXPECT_TRUE(ht.Remove(nullptr, 0b010, j));
  }
  for (int i = 0b001; i < 0b1000; ++i) {
    EXPECT_FALSE(ht.Remove(nullptr, i, -1));
  }
  EXPECT_EQ(ht.GetGlobalDepth(), 0);
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
