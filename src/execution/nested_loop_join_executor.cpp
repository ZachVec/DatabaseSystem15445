//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_(std::move(left_executor)), right_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_->Init();
  right_->Init();
  lschema = left_->GetOutputSchema();
  rschema = right_->GetOutputSchema();
  predicate = plan_->Predicate();
  ltuple = Tuple();
  rtuple = Tuple();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  BUSTUB_ASSERT(tuple != nullptr, "Tuple have invalid address 'nullptr'!");
  BUSTUB_ASSERT(rid != nullptr, "RID have invalid address 'nullptr'!");

  while (ltuple.IsAllocated() || left_->Next(&ltuple, rid)) {
    while (right_->Next(&rtuple, rid)) {
      if (predicate == nullptr || predicate->EvaluateJoin(&ltuple, lschema, &rtuple, rschema).GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema()->GetColumnCount());
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          values.emplace_back(col.GetExpr()->EvaluateJoin(&ltuple, lschema, &rtuple, rschema));
        }
        *tuple = Tuple(values, GetOutputSchema());
        *rid = RID();
        return true;
      }
    }
    right_->Init();
    ltuple = Tuple();
  }
  return false;
}

}  // namespace bustub
