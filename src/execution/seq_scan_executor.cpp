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
#include "common/logger.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iterator_(nullptr, RID(), nullptr), end_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iterator_ = table_info->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->GetPredicate();
  while (iterator_ != end_) {
    *tuple = (*iterator_++);
    if (predicate != nullptr && !predicate->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      continue;
    }
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
