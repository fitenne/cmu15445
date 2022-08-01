//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <string_view>
#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Distinct; }

  /** @return The child plan node */
  const AbstractPlanNode *GetChildPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

struct DistinctHashTupleWarpper {
  std::vector<Value> tuple_{};

  bool operator==(const DistinctHashTupleWarpper &rhs) const {
    if (tuple_.size() != rhs.tuple_.size()) {
      return false;
    }
    for (size_t i = 0; i < tuple_.size(); ++i) {
      if (tuple_[i].CompareEquals(rhs.tuple_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

template <>
struct hash<bustub::DistinctHashTupleWarpper> {
  std::size_t operator()(const bustub::DistinctHashTupleWarpper &tuple_warpper) const {
    size_t curr_hash = 0;
    for (const auto &v : tuple_warpper.tuple_) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&v));
    }
    return curr_hash;
  }
};

}  // namespace std