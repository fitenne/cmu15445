//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();

  Tuple tuple{};
  RID rid{};
  const Schema *child_chema = child_executor_->GetOutputSchema();
  const uint32_t child_col_cnt = child_chema->GetColumnCount();
  while (child_executor_->Next(&tuple, &rid)) {
    DistinctHashTupleWarpper tuple_warpper;
    for (size_t i = 0; i < child_col_cnt; ++i) {
      tuple_warpper.tuple_.emplace_back(tuple.GetValue(child_chema, i));
    }
    ht_.insert(tuple_warpper);
  }
  iter_ = ht_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != ht_.end()) {
    auto values = iter_++->tuple_;
    *tuple = Tuple(values, GetOutputSchema());
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
