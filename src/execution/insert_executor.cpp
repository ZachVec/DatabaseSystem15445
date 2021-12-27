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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_(nullptr),
      table_schema_(nullptr) {}

void InsertExecutor::Init() {
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  TableMetadata *table_metadata = catalog->GetTable(plan_->TableOid());
  table_ = table_metadata->table_.get();
  table_schema_ = &table_metadata->schema_;
  const auto &index_infos = catalog->GetTableIndexes(table_metadata->name_);
  indexes.reserve(index_infos.size());
  for (const auto &index_info : index_infos) {
    indexes.emplace_back(index_info->index_.get());
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (plan_->IsRawInsert()) {
    auto &raw_values = plan_->RawValues();
    for (const auto &raw_value : raw_values) {
      InsertAndUpdateIndexes(Tuple(raw_value, table_schema_), rid, txn);
    }
  } else {
    child_executor_->Init();
    while (child_executor_->Next(tuple, rid)) {
      InsertAndUpdateIndexes(*tuple, rid, txn);
    }
  }
  return false;
}

void InsertExecutor::InsertAndUpdateIndexes(Tuple tuple, RID *rid, Transaction *txn) {
  if (!table_->InsertTuple(tuple, rid, txn)) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out Of Memory!");
  }
  for (Index *index : indexes) {
    const Tuple &key = tuple.KeyFromTuple(*table_schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    index->InsertEntry(key, *rid, txn);
  }
}
}  // namespace bustub
