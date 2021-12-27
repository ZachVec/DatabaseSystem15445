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
      table_schema_(nullptr),
      itr_(nullptr, RID(), nullptr),
      end_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  TableMetadata *metadata = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  TableHeap *table_ = metadata->table_.get();
  table_schema_ = &(metadata->schema_);
  itr_ = table_->Begin(GetExecutorContext()->GetTransaction());
  end_ = table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  const AbstractExpression *predicate = plan_->GetPredicate();
  const Schema *output_schema = GetOutputSchema();
  while (itr_ != end_) {
    // if query has predicate, and predicate is true for this tuple, then materialize
    if (predicate == nullptr || predicate->Evaluate(&(*itr_), table_schema_).GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(output_schema->GetColumnCount());
      for (const auto &col : output_schema->GetColumns()) {
        values.emplace_back(col.GetExpr()->Evaluate(&(*itr_), table_schema_));
      }
      *tuple = Tuple(values, output_schema);
      *rid = itr_->GetRid();
      ++itr_;
      return true;
    }
    ++itr_;
  }
  return false;
}

}  // namespace bustub
