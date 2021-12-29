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
    : AbstractExecutor(exec_ctx), plan_(plan), table_(nullptr), key_schema_(nullptr) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  IndexInfo *index_info = catalog->GetIndex(plan_->GetIndexOid());

  // Cast the index to B+ Tree index
  Index *index = index_info->index_.get();
  key_schema_ = index->GetKeySchema();
  itr_ = dynamic_cast<BPLUSTREE_INDEX_TYPE *>(index)->GetBeginIterator();

  // Get Table and it's schema
  table_ = catalog->GetTable(index_info->table_name_)->table_.get();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");
  const AbstractExpression *predicate = plan_->GetPredicate();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  for (Tuple key; !itr_.isEnd(); ++itr_) {
    key = Tuple({(*itr_).first.ToValue(key_schema_, 0)}, key_schema_);
    if (predicate == nullptr || predicate->Evaluate(&key, key_schema_).GetAs<bool>()) {
      *rid = (*itr_).second;
      BUSTUB_ASSERT(table_->GetTuple(*rid, tuple, txn), "Inconsistence!");
      ++itr_;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
