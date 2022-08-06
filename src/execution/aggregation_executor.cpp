//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple cur_tuple{};
  RID cur_rid{};
  if (aht_.Begin() == aht_.End()) {
    while (child_->Next(&cur_tuple, &cur_rid)) {
      AggregateKey key = MakeAggregateKey(&cur_tuple);
      AggregateValue val = MakeAggregateValue(&cur_tuple);
      aht_.InsertCombine(key, val);
    }
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    const auto &k = aht_iterator_.Key();
    const auto &v = aht_iterator_.Val();

    if (plan_->GetHaving() != nullptr &&
        !plan_->GetHaving()->EvaluateAggregate(k.group_bys_, v.aggregates_).GetAs<bool>()) {
      ++aht_iterator_;
      continue;
    }

    const Schema *output_schema = plan_->OutputSchema();
    std::vector<Value> res;
    res.reserve(output_schema->GetColumnCount());
    for (const auto &e : output_schema->GetColumns()) {
      const auto ae = dynamic_cast<const AggregateValueExpression *>(e.GetExpr());
      res.emplace_back(ae->EvaluateAggregate(k.group_bys_, v.aggregates_));
    }
    *tuple = Tuple(res, output_schema);
    *rid = RID();
    ++aht_iterator_;
    return true;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
