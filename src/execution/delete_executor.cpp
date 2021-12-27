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
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  // lookup table schema and indexes need deleted
  Transaction *txn = GetExecutorContext()->GetTransaction();
  TableHeap *table = table_info_->table_.get();
  const Schema &schema = table_info_->schema_;
  const auto &indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  // Start deleting
  while (child_executor_->Next(tuple, rid)) {
    table->MarkDelete(*rid, txn);
    for (const auto &index_info : indexes) {
      Index *index = index_info->index_.get();
      const auto &deleted_key = tuple->KeyFromTuple(schema, *index->GetKeySchema(), index->GetKeyAttrs());
      index->DeleteEntry(deleted_key, *rid, txn);
    }
  }
  return false;
}

}  // namespace bustub
