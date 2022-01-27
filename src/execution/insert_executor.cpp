//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {
/*
Hint: You will want to look up table and index information during executor construction, and insert into index(es) if
necessary during execution.
*/

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  if (plan_->IsRawInsert()) {
    RawInsert(rid);
  } else {
    NonRawInsert(tuple, rid);
  }
  return false;
}

void InsertExecutor::RawInsert(RID *rid) {
  Transaction *txn = GetTransaction();
  Catalog *catalog = GetCatalog();
  const auto &raw_tuples = plan_->RawValues();
  const TableMetadata *metadata = catalog->GetTable(plan_->TableOid());
  // table staff
  const Schema *table_schema = &metadata->schema_;
  const table_oid_t &table_id = metadata->oid_;
  // index staff
  const auto &index_infos = catalog->GetTableIndexes(metadata->name_);
  const auto &index_records = txn->GetIndexWriteSet();

  for (const std::vector<Value> &raw_tuple : raw_tuples) {
    Tuple inserted(raw_tuple, table_schema);
    metadata->table_->InsertTuple(inserted, rid, txn);
    for (const auto &index_info : index_infos) {
      const index_oid_t &index_id = index_info->index_oid_;
      const Schema *key_schema = index_info->index_->GetKeySchema();
      const auto &key_attrs = index_info->index_->GetKeyAttrs();
      const Tuple &key = inserted.KeyFromTuple(*table_schema, *key_schema, key_attrs);
      index_info->index_->InsertEntry(key, *rid, txn);
      txn->GetIndexWriteSet()->emplace_back(*rid, table_id, WType::INSERT, inserted, index_id, catalog);
    }
  }
}

void InsertExecutor::NonRawInsert(Tuple *tuple, RID *rid) {
  Transaction *txn = GetTransaction();
  Catalog *catalog = GetCatalog();
  const TableMetadata *table_info = catalog->GetTable(plan_->TableOid());
  // table staff
  const Schema *table_schema = &table_info->schema_;
  const table_oid_t &table_id = table_info->oid_;
  // index staff
  const auto &index_infos = catalog->GetTableIndexes(table_info->name_);
  const auto &index_records = txn->GetIndexWriteSet();

  while (child_executor_->Next(tuple, rid)) {
    table_info->table_->InsertTuple(*tuple, rid, txn);
    for (const auto &index_info : index_infos) {
      const index_oid_t &index_id = index_info->index_oid_;
      const Schema *key_schema = index_info->index_->GetKeySchema();
      const auto &key_attrs = index_info->index_->GetKeyAttrs();
      const Tuple &key = tuple->KeyFromTuple(*table_schema, *key_schema, key_attrs);
      index_info->index_->InsertEntry(key, *rid, txn);
      index_records->emplace_back(*rid, table_id, WType::INSERT, *tuple, index_id, catalog);
    }
  }
}

}  // namespace bustub
