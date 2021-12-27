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
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  auto indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  Transaction *txn = GetExecutorContext()->GetTransaction();
  const Schema &table_schema = table_info_->schema_;
  while (child_executor_->Next(tuple, rid)) {
    Tuple updated = GenerateUpdatedTuple(*tuple);
    table_info_->table_->UpdateTuple(updated, *rid, txn);
    for (const auto &index_info : indexes) {
      Index *index = index_info->index_.get();
      const auto &old_key = tuple->KeyFromTuple(table_schema, *index->GetKeySchema(), index->GetKeyAttrs());
      const auto &new_key = updated.KeyFromTuple(table_schema, *index->GetKeySchema(), index->GetKeyAttrs());
      index->DeleteEntry(old_key, *rid, txn);
      index->InsertEntry(new_key, *rid, txn);
    }
  }
  return false;
}
}  // namespace bustub
