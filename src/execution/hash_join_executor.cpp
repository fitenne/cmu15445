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
  left_child_->Init();

  Tuple tuple{};
  RID rid{};
  if (ht_.empty()) {
    right_child_->Init();
    while (right_child_->Next(&tuple, &rid)) {
      Value value = plan_->RightJoinKeyExpression()->Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
      ht_[HashJoinKey(std::move(value))].emplace_back(tuple);
    }
  }

  cur_left_tuple_.reset();
  while (left_child_->Next(&tuple, &rid)) {
    HashJoinKey key(plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema()));
    if (ht_.count(key) != 0) {
      cur_left_tuple_ = tuple;
      cur_right_tuple_iter_ = ht_[key].begin();
      break;
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!cur_left_tuple_.has_value()) {
      break;
    }

    HashJoinKey left_key(
        plan_->LeftJoinKeyExpression()->Evaluate(&cur_left_tuple_.value(), plan_->GetLeftPlan()->OutputSchema()));
    while (cur_right_tuple_iter_ != ht_[left_key].end()) {
      Tuple right_tuple = *cur_right_tuple_iter_++;

      std::vector<Value> values;
      values.reserve(plan_->OutputSchema()->GetColumnCount());
      for (const auto &col : plan_->OutputSchema()->GetColumns()) {
        const auto col_expr = dynamic_cast<const ColumnValueExpression *>(col.GetExpr());
        if (col_expr->GetTupleIdx() == 0) {
          values.emplace_back(col_expr->Evaluate(&cur_left_tuple_.value(), plan_->GetLeftPlan()->OutputSchema()));
          // values.emplace_back(cur_left_tuple_->GetValue(plan_->GetLeftPlan()->OutputSchema(),
          // col_expr->GetColIdx()));
        } else {
          values.emplace_back(col_expr->Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema()));
        }
      }

      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = RID();
      return true;
    }

    Tuple cur_tuple{};
    RID cur_rid{};
    cur_left_tuple_.reset();
    while (left_child_->Next(&cur_tuple, &cur_rid)) {
      HashJoinKey key(plan_->LeftJoinKeyExpression()->Evaluate(&cur_tuple, plan_->GetLeftPlan()->OutputSchema()));
      if (ht_.count(key) != 0) {
        cur_left_tuple_ = cur_tuple;
        cur_right_tuple_iter_ = ht_[key].begin();
        break;
      }
    }
  }
  return false;
}

}  // namespace bustub
