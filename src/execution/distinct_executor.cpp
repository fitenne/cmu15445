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
  ht_.clear();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple{};
  RID cur_rid{};
  while (child_executor_->Next(&cur_tuple, &cur_rid)) {
    DistinctHashTupleWarpper tuple_warpper;
    tuple_warpper.tuple_.reserve(plan_->OutputSchema()->GetColumnCount());
    const Schema *child_schema = child_executor_->GetOutputSchema();
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      uint32_t idx = child_schema->GetColIdx(col.GetName());
      tuple_warpper.tuple_.emplace_back(cur_tuple.GetValue(child_schema, idx));
    }
    if (ht_.insert(std::move(tuple_warpper)).second) {
      *tuple = cur_tuple;
      *rid = cur_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
