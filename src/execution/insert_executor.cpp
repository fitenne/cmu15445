//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/exception.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  /** Insert dont produce tuple. always return false */
  Tuple cur_tuple{};
  RID cur_rid{};

  try {
    auto txn = exec_ctx_->GetTransaction();
    if (plan_->IsRawInsert()) {
      for (auto &t : plan_->RawValues()) {
        cur_tuple = Tuple(t, &table_info_->schema_);
        table_info_->table_->InsertTuple(cur_tuple, &cur_rid, txn);
        for (IndexInfo *index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
          auto key = cur_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
          index_info->index_->InsertEntry(key, cur_rid, txn);
        }
      }
    } else {
      while (child_executor_->Next(&cur_tuple, &cur_rid)) {
        table_info_->table_->InsertTuple(cur_tuple, &cur_rid, txn);
        for (IndexInfo *index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
          auto key = cur_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
          index_info->index_->InsertEntry(key, cur_rid, txn);
        }
      }
    }
  } catch (Exception &e) {
    LOG_ERROR("%s", e.what());
  }

  return false;
}

}  // namespace bustub
