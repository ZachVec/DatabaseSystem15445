//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  size_t offset = plan_->GetOffset();
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  for (size_t i = 0; i < offset; i++) {
    if (!child_executor_->Next(&tuple, &rid)) {
      break;
    }
  }
  remain_cnt = plan_->GetLimit();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (remain_cnt > 0 && child_executor_->Next(tuple, rid)) {
    --remain_cnt;
    return true;
  }
  return false;
}

}  // namespace bustub
