//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>
#include "catalog/schema.h"
#include "common/logger.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  left_schema_ = left_executor_->GetOutputSchema();
  right_schema_ = right_executor_->GetOutputSchema();
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  uint32_t left_col_cnt = left_schema_->GetColumnCount();
  cur_left_tuple_values_.reserve(left_col_cnt);
  Tuple tuple;
  RID rid;
  if (left_executor_->Next(&tuple, &rid)) {
    cur_left_tuple_ = tuple;
    cur_left_tuple_values_.clear();
    for (size_t i = 0; i < left_col_cnt; ++i) {
      cur_left_tuple_values_.emplace_back(cur_left_tuple_.value().GetValue(left_schema_, i));
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  RID cur_rid;

  while (true) {
    if (!cur_left_tuple_.has_value()) {
      // no more tuple
      break;
    }

    while (right_executor_->Next(&cur_tuple, &cur_rid)) {
      auto pred = plan_->Predicate()->EvaluateJoin(&cur_left_tuple_.value(), left_schema_, &cur_tuple, right_schema_);
      if (pred.GetAs<bool>()) {
        std::vector<Value> values(cur_left_tuple_values_);
        uint32_t right_col_cnt = right_schema_->GetColumnCount();
        values.reserve(values.size() + right_col_cnt);
        for (size_t i = 0; i < right_col_cnt; ++i) {
          values.emplace_back(cur_left_tuple_.value().GetValue(right_schema_, i));
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        *rid = RID();  // always invalid rid?
        return true;
      }
    }

    // next left tuple
    if (left_executor_->Next(&cur_tuple, &cur_rid)) {
      cur_left_tuple_ = cur_tuple;
      cur_left_tuple_values_.clear();
      uint32_t left_col_cnt = left_schema_->GetColumnCount();
      for (size_t i = 0; i < left_col_cnt; ++i) {
        cur_left_tuple_values_.emplace_back(cur_left_tuple_.value().GetValue(left_schema_, i));
      }
    } else {
      cur_left_tuple_.reset();
    }
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
