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
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  if (left_executor_->Next(&tuple, &rid)) {
    cur_left_tuple_ = tuple;
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
      auto pred = plan_->Predicate()->EvaluateJoin(&cur_left_tuple_.value(), plan_->GetLeftPlan()->OutputSchema(),
                                                   &cur_tuple, plan_->GetRightPlan()->OutputSchema());
      if (pred.GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(plan_->OutputSchema()->GetColumnCount());
        for (const auto &col : plan_->OutputSchema()->GetColumns()) {
          auto col_expr = dynamic_cast<const ColumnValueExpression *>(col.GetExpr());
          if (col_expr->GetTupleIdx() == 0) {
            values.emplace_back(col_expr->Evaluate(&cur_left_tuple_.value(), plan_->GetLeftPlan()->OutputSchema()));
          } else {
            values.emplace_back(col_expr->Evaluate(&cur_tuple, plan_->GetRightPlan()->OutputSchema()));
          }
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        *rid = RID();  // always invalid rid
        return true;
      }
    }

    if (left_executor_->Next(&cur_tuple, &cur_rid)) {
      cur_left_tuple_ = cur_tuple;
      right_executor_->Init();
    } else {
      cur_left_tuple_.reset();
    }
  }
  return false;
}

}  // namespace bustub
