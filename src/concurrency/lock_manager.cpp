//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!SelfCheck(txn, LockMode::SHARED)) {
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  LockRequestQueue *lrq = GetLockRequestQueue(rid);
  std::unique_lock lrq_lock{lrq->mut_};

  auto request = lrq->wait_queue_.emplace(lrq->wait_queue_.end(), txn->GetTransactionId(), LockMode::SHARED);
  while (request != lrq->wait_queue_.begin() || !lrq->Compatible()) {
    if (TryWound(txn, lrq) > 0) {
      lrq->cv_.notify_all();
    }

    lrq->cv_.wait(lrq_lock);

    if (txn->GetState() == TransactionState::ABORTED) {
      lrq->wait_queue_.erase(request);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  lrq->granted_queue_.emplace_back(lrq->wait_queue_.back());
  lrq->wait_queue_.pop_back();
  lrq->slock_count_++;
  txn->GetSharedLockSet()->emplace(rid);
  // LOG_DEBUG("[v]slock of %lu to %u", rid.Get(), txn->GetTransactionId());
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!SelfCheck(txn, LockMode::SHARED)) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  LockRequestQueue *lrq = GetLockRequestQueue(rid);
  std::unique_lock lrq_lock{lrq->mut_};

  auto request = lrq->wait_queue_.emplace(lrq->wait_queue_.end(), txn->GetTransactionId(), LockMode::SHARED);
  while (request != lrq->wait_queue_.begin() || !lrq->Compatible()) {
    if (TryWound(txn, lrq) > 0) {
      lrq->cv_.notify_all();
    }

    lrq->cv_.wait(lrq_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lrq->wait_queue_.erase(request);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  lrq->granted_queue_.emplace_back(lrq->wait_queue_.back());
  lrq->wait_queue_.pop_back();
  lrq->xlock_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!SelfCheck(txn, LockMode::SHARED)) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  LockRequestQueue *lrq = GetLockRequestQueue(rid);
  std::unique_lock lrq_lock{lrq->mut_};
  if (lrq->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  lrq->upgrading_ = txn->GetTransactionId();
  auto slock_request =
      std::find_if(lrq->granted_queue_.begin(), lrq->granted_queue_.end(),
                   [txn](const LockRequest &request) -> bool { return request.txn_id_ == txn->GetTransactionId(); });
  BUSTUB_ASSERT(slock_request != lrq->granted_queue_.end(), "slock must exists before upgrade");
  lrq->granted_queue_.erase(slock_request);
  txn->GetSharedLockSet()->erase(rid);
  --lrq->slock_count_;

  auto request = lrq->wait_queue_.emplace(lrq->wait_queue_.end(), txn->GetTransactionId(), LockMode::EXCLUSIVE);
  while (request != lrq->wait_queue_.begin() || !lrq->Compatible()) {
    if (TryWound(txn, lrq) > 0) {
      lrq->cv_.notify_all();
    }

    lrq->cv_.wait(lrq_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lrq->wait_queue_.erase(request);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  lrq->upgrading_ = INVALID_TXN_ID;
  lrq->granted_queue_.emplace_back(lrq->wait_queue_.back());
  lrq->wait_queue_.pop_back();
  lrq->xlock_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  bool success{false};
  bool is_slock{false};
  LockRequestQueue *lrq = GetLockRequestQueue(rid);
  {
    std::scoped_lock lrq_lock{lrq->mut_};
    bool should_notify{false};
    if (txn->GetSharedLockSet()->erase(rid) != 0) {
      BUSTUB_ASSERT(!lrq->xlock_, "slock and xlockmust not exists at the same time");
      is_slock = true;
      if (--lrq->slock_count_ == 0) {
        should_notify |= true;
      }
      success = true;
      // LOG_DEBUG("[x]slock of %lu to %u", rid.Get(), txn->GetTransactionId());
    }
    if (txn->GetExclusiveLockSet()->erase(rid) != 0) {
      BUSTUB_ASSERT(lrq->slock_count_ == 0, "slock and xlockmust not exists at the same time");
      lrq->xlock_ = false;
      should_notify |= true;
      success = true;
      // LOG_DEBUG("[x]xlock of %lu to %u", rid.Get(), txn->GetTransactionId());
    }

    if (success) {
      auto it = std::find_if(lrq->granted_queue_.begin(), lrq->granted_queue_.end(),
                             [txn](const LockRequest &request) { return request.txn_id_ == txn->GetTransactionId(); });
      BUSTUB_ASSERT(it != lrq->granted_queue_.end(), "lock request must exists in the granted queue");
      // if (!it->wouned_) {
      //   if (it->lock_mode_ == LockMode::SHARED) {
      //     if (--lrq->slock_count_ == 0) {
      //       should_notify |= true;
      //     }
      //   } else {
      //     lrq->xlock_ = false;
      //     should_notify |= true;
      //   }
      // }
      lrq->granted_queue_.erase(it);
    }

    if (should_notify) {
      lrq->cv_.notify_all();
    }
  }

  if (txn->GetState() == TransactionState::GROWING) {
    if (!(is_slock && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  if (!success) {
    LOG_WARN("didn't find lock request specified in unlock");
  }
  return success;
}

bool LockManager::SelfCheck(Transaction *txn, LockMode lock_mode) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (lock_mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  return true;
}

LockManager::LockRequestQueue *LockManager::GetLockRequestQueue(const RID &rid) {
  std::scoped_lock lock{latch_};
  return &lock_table_[rid];
}

bool LockManager::LockRequestQueue::Compatible() const {
  if (granted_queue_.empty()) {
    return true;
  }

  const LockRequest &request = wait_queue_.front();

  if (request.lock_mode_ == LockMode::SHARED) {
    return !xlock_;
  }

  return !xlock_ && slock_count_ == 0;
}

size_t LockManager::TryWound(Transaction *txn, LockRequestQueue *lrq) {
  // const auto wound = [txn, lrq](std::list<LockRequest> &requests) -> size_t {
  const auto wound = [txn](std::list<LockRequest> &requests) -> size_t {
    size_t count{0};
    for (auto &request : requests) {
      if (request.txn_id_ > txn->GetTransactionId()) {
        Transaction *young_txn = TransactionManager::GetTransaction(request.txn_id_);
        young_txn->SetState(TransactionState::ABORTED);
        // if (request.lock_mode_ == LockMode::SHARED) {
        //   --lrq->slock_count_;
        // } else {
        //   lrq->xlock_ = false;
        // }
      }
      ++count;
    }
    return count;
  };

  // notify txn only with granted request will make no different anyway
  (void)wound(lrq->granted_queue_);

  return wound(lrq->wait_queue_);

  // size_t count{0};
  // for (auto it = lrq->granted_queue_.begin(); it != lrq->granted_queue_.end(); ++it) {
  //   if (it->txn_id_ > txn->GetTransactionId()) {
  //     Transaction *young_txn = TransactionManager::GetTransaction(it->txn_id_);
  //     young_txn->SetState(TransactionState::ABORTED);
  //     it->wouned_ = true;
  //     LOG_DEBUG("%u wound %u in granted queue", txn->GetTransactionId(), it->txn_id_);
  //   }
  // }
  // for (auto it = lrq->wait_queue_.begin(); it != lrq->wait_queue_.end(); ++it) {
  //   if (it->txn_id_ > txn->GetTransactionId()) {
  //     Transaction *young_txn = TransactionManager::GetTransaction(it->txn_id_);
  //     young_txn->SetState(TransactionState::ABORTED);
  //     LOG_DEBUG("%u wound %u in wait queue", txn->GetTransactionId(), it->txn_id_);
  //   }
  //   ++count;
  // }
  // return count;
}

}  // namespace bustub
