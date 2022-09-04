//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "catalog/catalog.h"
#include "common/exception.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple cur_tuple{};
  RID cur_rid{};

  auto txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&cur_tuple, &cur_rid)) {
    auto updated_tuple = GenerateUpdatedTuple(cur_tuple);
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      exec_ctx_->GetLockManager()->LockUpgrade(txn, cur_rid);
    } else {
      exec_ctx_->GetLockManager()->LockExclusive(txn, cur_rid);
    }
    if (table_info_->table_->UpdateTuple(updated_tuple, cur_rid, txn)) {
      exec_ctx_->GetLockManager()->LockExclusive(txn, updated_tuple.GetRid());
      for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        Tuple cur_key =
            cur_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        Tuple updated_key = updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                       index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(cur_key, cur_rid, txn);
        index_info->index_->InsertEntry(updated_key, cur_rid, txn);
        txn->GetIndexWriteSet()->emplace_back(updated_tuple.GetRid(), table_info_->oid_, WType::UPDATE, updated_tuple,
                                              index_info->index_oid_, exec_ctx_->GetCatalog());
        txn->GetIndexWriteSet()->back().old_tuple_ = cur_tuple;
      }
    } else {
      throw Exception("failed to update a tuple");
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
