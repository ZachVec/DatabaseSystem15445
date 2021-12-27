//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
/*
Hint: Make sure to retrieve the tuple from the table once the RID is retrieved from the index. Remember to use the
catalog to search up the table.

Hint: You will want to make use of the iterator of your B+ tree to support both point query and range scan.

Hint: For this project, you can safely assume the key value type for index is <GenericKey<8>, RID, GenericComparator<8>>
to not worry about template, though a more general solution is also welcomed
*/

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_(nullptr), table_schema_(nullptr) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  IndexInfo *index_info = catalog->GetIndex(plan_->GetIndexOid());

  // Cast the index to B+ Tree index
  BPLUSTREE_INDEX_TYPE *index = dynamic_cast<BPLUSTREE_INDEX_TYPE *>(index_info->index_.get());
  itr_ = index->GetBeginIterator();
  end_ = index->GetEndIterator();

  // Get Table and it's schema
  TableMetadata *table_metadata_ = catalog->GetTable(index_info->table_name_);
  table_ = table_metadata_->table_.get();
  table_schema_ = &table_metadata_->schema_;
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");
  const AbstractExpression *predicate = plan_->GetPredicate();
  const Schema *output_schema = GetOutputSchema();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  Tuple tmp_tuple;
  while (itr_ != end_ && table_->GetTuple((*itr_).second, &tmp_tuple, txn)) {
    if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, table_schema_).GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(output_schema->GetColumnCount());
      for (const auto &col : output_schema->GetColumns()) {
        values.emplace_back(col.GetExpr()->Evaluate(&tmp_tuple, table_schema_));
      }
      *tuple = Tuple(values, output_schema);
      *rid = (*itr_).second;
      ++itr_;
      return true;
    }
    ++itr_;
  }
  return false;
}

}  // namespace bustub
