//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
  bool tryLock(Transaction *txn, const RID &rid) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      return GetLockManager()->LockShared(txn, rid);
    }
    return true;
  }
  bool tryUnlock(Transaction *txn, const RID &rid) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        return true;
      case IsolationLevel::READ_COMMITTED:
        return GetLockManager()->Unlock(txn, rid);
      default:
        return true;
    }
  }
  Catalog *GetCatalog() { return GetExecutorContext()->GetCatalog(); }
  Transaction *GetTransaction() { return GetExecutorContext()->GetTransaction(); }
  LockManager *GetLockManager() { return GetExecutorContext()->GetLockManager(); }
  bool isTrue(const AbstractExpression *predicate, const Tuple *tuple, const Schema *schema) {
    if (predicate == nullptr) {
      return true;
    }
    return predicate->Evaluate(tuple, schema).GetAs<bool>();
  }
  Tuple OutputFromTuple(const Tuple *tuple, const Schema *output, const Schema *table) {
    std::vector<Value> values;
    values.reserve(output->GetColumnCount());
    for (const auto &col : output->GetColumns()) {
      values.emplace_back(col.GetExpr()->Evaluate(tuple, table));
    }
    return Tuple(values, output);
  }

 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;  // query plan
  TableIterator itr_;            // itr iterator
  TableIterator end_;            // end iterator
  bool iterator_init_;
};
}  // namespace bustub
