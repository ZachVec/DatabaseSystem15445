//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  const AbstractExpression *having = plan_->GetHaving();
  std::vector<Value> values;
  values.reserve(GetOutputSchema()->GetColumnCount());
  while (aht_iterator_ != aht_.End()) {
    const std::vector<Value> &gby = aht_iterator_.Key().group_bys_;
    const std::vector<Value> &agg = aht_iterator_.Val().aggregates_;
    if (having == nullptr || having->EvaluateAggregate(gby, agg).GetAs<bool>()) {
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(gby, agg));
      }
      *tuple = Tuple(values, GetOutputSchema());
      *rid = RID();
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

}  // namespace bustub
