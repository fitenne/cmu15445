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
#include "common/logger.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      iterator_(nullptr, RID(), nullptr),
      end_(nullptr, RID(), nullptr) {
  // remember that the Init() may be called multiple times
  const Schema *output_schema = plan_->OutputSchema();
  for (size_t i = 0; i < output_schema->GetColumnCount(); ++i) {
    output_col_idx_.emplace_back(table_info_->schema_.GetColIdx(output_schema->GetColumn(i).GetName()));
  }
}

void SeqScanExecutor::Init() {
  iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->GetPredicate();
  Tuple cur_tuple{};

  while (iterator_ != end_) {
    cur_tuple = (*iterator_++);
    if (predicate != nullptr && !predicate->Evaluate(&cur_tuple, plan_->OutputSchema()).GetAs<bool>()) {
      continue;
    }
    std::vector<Value> res;
    for (auto idx : output_col_idx_) {
      res.emplace_back(cur_tuple.GetValue(&table_info_->schema_, idx));
    }
    if (res.size() != plan_->OutputSchema()->GetColumnCount()) {
      LOG_DEBUG("wtf");
    }
    *tuple = Tuple(res, plan_->OutputSchema());
    *rid = cur_tuple.GetRid();
    // LOG_DEBUG("table: %s, schema: %s, tuple: %s",
    //           exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->name_.c_str(),
    //           plan_->OutputSchema()->ToString().c_str(), tuple->ToString(plan_->OutputSchema()).c_str());
    return true;
  }
  return false;
}

}  // namespace bustub
