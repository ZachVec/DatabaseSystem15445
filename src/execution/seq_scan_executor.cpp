//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {
/*
Hint: Be careful when using the TableIterator object. Make sure that you understand the difference between the
pre-increment and post-increment operators. You may find yourself getting strange output by switching between ++iter and
iter++.

Hint: You will want to make use of the predicate in the sequential scan plan node. In particular, take a look at
AbstractExpression::Evaluate. Note that this returns a Value, which you can GetAs<bool>.

Hint: The ouput of sequential scan should be a value copy of matched tuple and its oringal record ID
*/

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_metadata_(GetCatalog()->GetTable(plan->GetTableOid())),
      predicate_(plan->GetPredicate()),
      itr_(nullptr, RID(), nullptr),
      end_(nullptr, RID(), nullptr),
      start_(false) {}

void SeqScanExecutor::Init() { start_ = true; }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");
  const Schema *output_schema = GetOutputSchema();
  const Schema *table_schema = GetTableSchema();
  if (start_) {
    itr_ = table_metadata_->table_->Begin(GetTransaction());
    end_ = table_metadata_->table_->End();
    start_ = false;
  } else {
    ++itr_;
  }

  for (; itr_ != end_; ++itr_) {
    Lock(itr_->GetRid());
    if (predicateTrue(*itr_, table_schema)) {
      *tuple = OutputFromTuple(*itr_, table_schema, output_schema);
      *rid = itr_->GetRid();
      Unlock(itr_->GetRid());
      return true;
    }
    Unlock(itr_->GetRid());
  }
  return false;
}

}  // namespace bustub
