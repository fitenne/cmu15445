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
#include "execution/expressions/column_value_expression.h"
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
  right_child_->Init();

  Tuple tuple{};
  RID rid{};
  if (ht_.empty()) {
    left_child_->Init();
    while (left_child_->Next(&tuple, &rid)) {
      Value value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema());
      ht_[HashJoinKey(value)].emplace_back(tuple);
    }
  }

  cur_right_tuple_.reset();
  while (right_child_->Next(&tuple, &rid)) {
    HashJoinKey key(plan_->RightJoinKeyExpression()->Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema()));
    if (ht_.count(key) != 0) {
      cur_right_tuple_ = tuple;
      cur_left_tuple_iter_ = ht_[key].begin();
      break;
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!cur_right_tuple_.has_value()) {
      break;
    }

    HashJoinKey right_key(
        plan_->RightJoinKeyExpression()->Evaluate(&cur_right_tuple_.value(), plan_->GetRightPlan()->OutputSchema()));
    while (cur_left_tuple_iter_ != ht_[right_key].end()) {
      Tuple left_tuple = *cur_left_tuple_iter_++;

      std::vector<Value> values;
      values.reserve(plan_->OutputSchema()->GetColumnCount());
      for (const auto &col : plan_->OutputSchema()->GetColumns()) {
        const auto col_expr = dynamic_cast<const ColumnValueExpression *>(col.GetExpr());
        if (col_expr->GetTupleIdx() == 0) {
          values.emplace_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), col_expr->GetColIdx()));
        } else {
          values.emplace_back(cur_right_tuple_->GetValue(plan_->GetRightPlan()->OutputSchema(), col_expr->GetColIdx()));
        }
      }

      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = RID();
      return true;
    }

    Tuple cur_tuple{};
    RID cur_rid{};
    cur_right_tuple_.reset();
    while (right_child_->Next(&cur_tuple, &cur_rid)) {
      HashJoinKey key(plan_->RightJoinKeyExpression()->Evaluate(&cur_tuple, plan_->GetRightPlan()->OutputSchema()));
      if (ht_.count(key) != 0) {
        cur_right_tuple_ = cur_tuple;
        cur_left_tuple_iter_ = ht_[key].begin();
        break;
      }
    }
  }
  return false;
}

// HashJoinKey HashJoinExecutor::GetLeftKey(const Tuple &tuple) const {
//   return HashJoinKey(plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema()));
// }

}  // namespace bustub
