//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  Transaction *txn = GetTransaction();
  Catalog *catalog = GetCatalog();
  // table staff
  const Schema &table_schema = table_info_->schema_;
  const table_oid_t &table_id = table_info_->oid_;
  // index staff
  const auto &indexes = catalog->GetTableIndexes(table_info_->name_);
  const auto &index_records = txn->GetIndexWriteSet();
  while (child_executor_->Next(tuple, rid)) {
    Lock(*rid);
    Tuple updated = GenerateUpdatedTuple(*tuple);
    table_info_->table_->UpdateTuple(updated, *rid, txn);
    for (const auto &index_info : indexes) {
      const index_oid_t &index_id = index_info->index_oid_;
      const Schema *key_schema = index_info->index_->GetKeySchema();
      const auto &key_attrs = index_info->index_->GetKeyAttrs();
      const auto &old_key = tuple->KeyFromTuple(table_schema, *key_schema, key_attrs);
      const auto &new_key = updated.KeyFromTuple(table_schema, *key_schema, key_attrs);
      index_info->index_->DeleteEntry(old_key, *rid, txn);
      index_info->index_->InsertEntry(new_key, *rid, txn);
      index_records->emplace_back(*rid, table_id, WType::UPDATE, updated, index_id, catalog);
      index_records->back().old_tuple_ = *tuple;
    }
  }
  return false;
}
}  // namespace bustub
