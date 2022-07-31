//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <vector>
#include "common/logger.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple tuple{};
  RID rid{};
  while (right_child_->Next(&tuple, &rid)) {
    Value value = plan_->RightJoinKeyExpression()->Evaluate(&tuple, right_child_->GetOutputSchema());
    ht_[HashJoinKey(std::move(value))].emplace_back(tuple);
  }

  if (left_child_->Next(&tuple, &rid)) {
    cur_left_tuple_ = tuple;
    HashJoinKey key = GetLeftKey(tuple);
    if (ht_.count(key) != 0) {
      cur_right_tuple_iter_ = ht_[key].begin();
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple{};
  RID cur_rid{};

  while (true) {
    if (!cur_left_tuple_.has_value()) {
      break;
    }

    while (cur_right_tuple_iter_.has_value() &&
           cur_right_tuple_iter_ != ht_[GetLeftKey(cur_left_tuple_.value())].end()) {
      Tuple right_tuple = *cur_right_tuple_iter_.value()++;

      std::vector<Value> values;
      uint32_t left_col_cnt = plan_->GetLeftPlan()->OutputSchema()->GetColumnCount();
      uint32_t right_col_cnt = plan_->GetRightPlan()->OutputSchema()->GetColumnCount();
      for (size_t i = 0; i < left_col_cnt; ++i) {
        values.emplace_back(cur_left_tuple_->GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
      }
      for (size_t i = 0; i < right_col_cnt; ++i) {
        values.emplace_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), i));
      }

      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = RID();
      return true;
    }

    if (left_child_->Next(&cur_tuple, &cur_rid)) {
      cur_left_tuple_ = cur_tuple;
      HashJoinKey key = GetLeftKey(cur_tuple);
      if (ht_.count(key) != 0) {
        cur_right_tuple_iter_ = ht_[key].begin();
      } else {
        cur_right_tuple_iter_.reset();
      }
    } else {
      cur_left_tuple_.reset();
    }
  }
  return false;
}

HashJoinKey HashJoinExecutor::GetLeftKey(const Tuple &tuple) const {
  return HashJoinKey(plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema()));
};

}  // namespace bustub
