//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  TableMetadata *table_info = catalog->GetTable(plan_->GetInnerTableOid());
  table_ = table_info->table_.get();
  index_ = catalog->GetIndex(plan_->GetIndexName(), table_info->name_)->index_.get();
  ltuple = Tuple();
  rtuples.clear();
  it = rtuples.begin();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  Transaction *txn = GetExecutorContext()->GetTransaction();
  const Schema *outer_schema = plan_->OuterTableSchema();
  const Schema *inner_schema = plan_->InnerTableSchema();
  std::vector<Value> values;
  Tuple rtuple;
generation:
  if (it == rtuples.end()) {
    goto allocation;
  }
  *rid = *it++;
  BUSTUB_ASSERT(table_->GetTuple(*rid, &rtuple, txn), "Inconsistence!");
  values.reserve(GetOutputSchema()->GetColumnCount());
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    values.emplace_back(col.GetExpr()->EvaluateJoin(&ltuple, outer_schema, &rtuple, inner_schema));
  }
  *tuple = Tuple(values, GetOutputSchema());
  *rid = RID();
  return true;
allocation:
  if (child_executor_->Next(&ltuple, rid)) {
    rtuples.clear();
    const AbstractExpression *lcol = plan_->Predicate()->GetChildAt(0);
    const Schema *key_schema = index_->GetKeySchema();
    const Schema *chd_schema = child_executor_->GetOutputSchema();
    Tuple key(std::vector<Value>{lcol->Evaluate(&ltuple, chd_schema)}, key_schema);
    index_->ScanKey(key, &rtuples, txn);
    it = rtuples.begin();
    goto generation;
  }
  return false;
}

}  // namespace bustub
