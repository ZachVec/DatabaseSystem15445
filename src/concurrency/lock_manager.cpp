//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!canLockShared(txn, rid)) {
    return false;
  }
  /** 1. Acquiring the latch on tuple */
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &q = lock_table_[rid];
  { // Acquire latch on tuple
    std::unique_lock<std::mutex> latch_on_tuple(q.latch_);
    latch.swap(latch_on_tuple);
  } // And release latch on whole LockManager

  /** 2. Check to see if it's time to grant the lock */
  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  LockRequest &req = q.request_queue_.back();
  while (!shouldGrantLock(q.request_queue_, req)) {
    q.cv_.wait(latch);
  }
  
  /** 3. If txn has been aborted for deadlock during waiting */
  if (isTxnInState(txn, TransactionState::ABORTED)) {
    // clean up request and weak up other threads waiting on this tuple
    q.request_queue_.remove_if([&req](const LockRequest &r) { return req.txn_id_ == r.txn_id_; });
    q.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  /** 4. Grant the lock and track the locks that txn has hold */
  req.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!canLockExclusive(txn, rid)) {
    return false;
  }
  /** 1. Acquiring the latch on tuple */
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &q = lock_table_[rid];
  { // Acquire latch on tuple
    std::unique_lock<std::mutex> latch_on_tuple(q.latch_);
    latch.swap(latch_on_tuple);
  } // And release latch on whole LockManager

  /** 2. Check to see if it's time to grant the lock */
  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequest &req = q.request_queue_.back();
  while (!shouldGrantLock(q.request_queue_, req)) {
    q.cv_.wait(latch);
  }
  
  /** 3. If txn has been aborted for deadlock during waiting */
  if (isTxnInState(txn, TransactionState::ABORTED)) {
    // clean up request and weak up other threads waiting on this tuple
    q.request_queue_.remove_if([&req](const LockRequest &r) { return req.txn_id_ == r.txn_id_; });
    q.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  /** 4. Grant the lock and track the locks that txn has hold */
  req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!canUpgradeLock(txn, rid)) {
    return false;
  }
  /** 1. Acquiring the latch on tuple if no txn is upgrading */
  std::unique_lock<std::mutex> latch(latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  auto &[queue, cv, upgrading, q_latch] = lock_table_[rid];
  if (upgrading) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  } else {
    std::unique_lock<std::mutex> latch_on_tuple(q_latch);
    latch.swap(latch_on_tuple);
  }

  /** 2. clear former request and insert new request */
  queue.remove_if([&txn_id](const LockRequest &r){ return r.txn_id_ == txn_id; });
  auto it = std::find_if_not(queue.begin(), queue.end(), [](const LockRequest &req) {
    return !req.granted_;
  });
  LockRequest &req = *queue.emplace(it, txn_id, LockMode::EXCLUSIVE);

  /** 3. Check to see if it's time to upgrade */
  while (!shouldGrantLock(queue, req)) {
    cv.wait(latch);
  }

  /** 4. If the txn has been aborted for deadlock during waiting */
  if (isTxnInState(txn, TransactionState::ABORTED)) {
    queue.remove_if([&req](const LockRequest &r){ return r.txn_id_ == req.txn_id_; });
    cv.notify_all();
    upgrading = false;
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  /** 5. Grant the Lock and tract the locks that txn has hold */
  req.granted_ = true;
  upgrading = false;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // READ_UNCOMMITTED doesn't have SHRINKING stage
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
  isTxnInState(txn, TransactionState::SHRINKING)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UNLOCK_ON_SHRINKING);
  }
  /** 1. Acquiring the latch on tuple */
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &q = lock_table_[rid];
  { // Acquire latch on tuple
    std::unique_lock<std::mutex> latch_on_tuple(q.latch_);
    latch.swap(latch_on_tuple);
  } // And release latch on whole LockManager

  /** 2. Clear the request that has been issued by this txn on rid */
  txn_id_t txn_id = txn->GetTransactionId();
  q.request_queue_.remove_if([&txn_id](const LockRequest &req) {
    return req.txn_id_ == txn_id;
  });

  if (transitToShrink(txn, rid)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  /** 3. Clear the trace in txn */
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
