//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/exception.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple cur_tuple{};
  RID cur_rid{};

  auto txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&cur_tuple, &cur_rid)) {
    if (table_info_->table_->MarkDelete(cur_rid, txn)) {
      for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        Tuple key = cur_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                           index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key, cur_rid, txn);
      }
    }
  }
  return false;
}

}  // namespace bustub
