//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      iterator_(nullptr, RID(), nullptr),
      end_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->GetPredicate();
  Tuple cur_tuple{};

  Transaction *txn = exec_ctx_->GetTransaction();
  while (iterator_ != end_) {
    cur_tuple = (*iterator_++);
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      exec_ctx_->GetLockManager()->LockShared(txn, cur_tuple.GetRid());
    }

    if (predicate != nullptr && !predicate->Evaluate(&cur_tuple, plan_->OutputSchema()).GetAs<bool>()) {
      continue;
    }
    std::vector<Value> res;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      res.emplace_back(col.GetExpr()->Evaluate(&cur_tuple, &table_info_->schema_));
    }
    *tuple = Tuple(res, plan_->OutputSchema());
    *rid = cur_tuple.GetRid();
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->Unlock(txn, cur_tuple.GetRid());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
