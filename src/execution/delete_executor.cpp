//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  Transaction *txn = GetTransaction();
  Catalog *catalog = GetCatalog();
  TableMetadata *table_info = catalog->GetTable(plan_->TableOid());
  // table staff
  const Schema &table_schema = table_info->schema_;
  const table_oid_t &table_id = table_info->oid_;
  // index staff
  const auto &index_infos = catalog->GetTableIndexes(table_info->name_);
  const auto &index_records = txn->GetIndexWriteSet();
  // Start deleting
  while (child_executor_->Next(tuple, rid)) {
    Lock(*rid);
    table_info->table_->MarkDelete(*rid, txn);
    for (const auto &index_info : index_infos) {
      const index_oid_t &index_id = index_info->index_oid_;
      const Schema *key_schema = index_info->index_->GetKeySchema();
      const auto &key_attrs = index_info->index_->GetKeyAttrs();
      const Tuple &key = tuple->KeyFromTuple(table_schema, *key_schema, key_attrs);
      index_info->index_->DeleteEntry(key, *rid, txn);
      index_records->emplace_back(*rid, table_id, WType::DELETE, *tuple, index_id, catalog);
    }
  }
  return false;
}

}  // namespace bustub
