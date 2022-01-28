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
  /** Helper Functions */
  Catalog *GetCatalog() { return GetExecutorContext()->GetCatalog(); }
  Transaction *GetTransaction() { return GetExecutorContext()->GetTransaction(); }
  LockManager *GetLockManager() { return GetExecutorContext()->GetLockManager(); }
  const Schema *GetTableSchema() { return &table_metadata_->schema_; }
  bool predicateTrue(const Tuple &tuple, const Schema *schema) {
    return predicate_ == nullptr || predicate_->Evaluate(&tuple, schema).GetAs<bool>();
  }
  Tuple OutputFromTuple(const Tuple &tuple, const Schema *table, const Schema *output) {
    std::vector<Value> values;
    values.reserve(output->GetColumnCount());
    for (const auto &col : output->GetColumns()) {
      values.emplace_back(col.GetExpr()->Evaluate(&tuple, table));
    }
    return Tuple(values, output);
  }
  bool Lock(const RID &rid) {
    LockManager *lmgr = GetLockManager();
    Transaction *txn = GetTransaction();
    if (txn->IsExclusiveLocked(rid) || txn->IsSharedLocked(rid)) {
      return true;
    }
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED:
        return lmgr->LockShared(txn, rid);
      default:
        return true;
    }
  }
  bool Unlock(const RID &rid) {
    Transaction *txn = GetTransaction();
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_COMMITTED:
        return GetLockManager()->Unlock(txn, rid);
      default:
        return true;
    }
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
  const SeqScanPlanNode *plan_;          // query plan
  TableMetadata *table_metadata_;        // table metadata
  const AbstractExpression *predicate_;  // predicate
  TableIterator itr_;                    // itr iterator
  TableIterator end_;                    // end iterator
  bool start_;
};
}  // namespace bustub
