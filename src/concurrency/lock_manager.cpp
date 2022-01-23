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

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  BUSTUB_ASSERT(!txn->IsSharedLocked(rid), "Undefined Behavior: try to get shared lock while holding shared");
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Undefined Behavior: try to get shared lock while holding exclusive");
  if (isTxnInState(txn, TransactionState::ABORTED)) {
    return false;
  }
  AssertNotInLevel(txn, IsolationLevel::READ_UNCOMMITTED, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  AssertNotInState(txn, TransactionState::SHRINKING, AbortReason::LOCK_ON_SHRINKING);

  /** 1. Acquiring the latch on tuple */
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &q = lock_table_[rid];
  {  // Acquire latch on tuple
    std::unique_lock<std::mutex> latch_on_tuple(q.latch_);
    latch.swap(latch_on_tuple);
  }  // And release latch on whole LockManager

  /** 2. Check to see if it's time to grant the lock */
  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  LockRequest &req = q.request_queue_.back();
  while (!shouldGrantLock(q.request_queue_, req)) {
    q.cv_.wait(latch);
    // throw exception when txn is aborted while waiting due to deadlock
    if (isTxnInState(txn, TransactionState::ABORTED)) {
      q.request_queue_.remove_if([&req](const LockRequest &r) { return req.txn_id_ == r.txn_id_; });
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  /** 3. Grant the lock and track the locks that txn has hold */
  req.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  BUSTUB_ASSERT(!txn->IsSharedLocked(rid), "Undefined Behavior: try to get exclusive lock while holding shared");
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Undefined Behavior: try to get exclusive lock while holding exclusive");
  if (isTxnInState(txn, TransactionState::ABORTED)) {
    return false;
  }
  AssertNotInState(txn, TransactionState::SHRINKING, AbortReason::LOCK_ON_SHRINKING);

  /** 1. Acquiring the latch on tuple */
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &q = lock_table_[rid];
  {  // Acquire latch on tuple
    std::unique_lock<std::mutex> latch_on_tuple(q.latch_);
    latch.swap(latch_on_tuple);
  }  // And release latch on whole LockManager

  /** 2. Check to see if it's time to grant the lock */
  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequest &req = q.request_queue_.back();
  while (!shouldGrantLock(q.request_queue_, req)) {
    q.cv_.wait(latch);
    // throw exception when txn is aborted while waiting due to deadlock
    if (isTxnInState(txn, TransactionState::ABORTED)) {
      q.request_queue_.remove_if([&req](const LockRequest &r) { return req.txn_id_ == r.txn_id_; });
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  /** 3. Grant the lock and track the locks that txn has hold */
  req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  BUSTUB_ASSERT(txn->IsSharedLocked(rid), "Txn must hold shared lock when upgrading");
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Undefined Behavior: try to get exclusive lock while holding exclusive");
  if (isTxnInState(txn, TransactionState::ABORTED)) {
    return false;
  }
  AssertNotInState(txn, TransactionState::SHRINKING, AbortReason::LOCK_ON_SHRINKING);
  /** 1. Acquiring the latch on tuple if no txn is upgrading */
  std::unique_lock<std::mutex> latch(latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  auto &[queue, cv, upgrading, q_latch] = lock_table_[rid];
  {
    std::unique_lock<std::mutex> latch_on_tuple(q_latch);
    latch.swap(latch_on_tuple);
  }
  if (upgrading) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }

  /** 2. clear former request and insert new request */
  queue.remove_if([&txn_id](const LockRequest &r) { return r.txn_id_ == txn_id; });
  auto it = std::find_if_not(queue.begin(), queue.end(), [](const LockRequest &req) { return !req.granted_; });
  LockRequest &req = *queue.emplace(it, txn_id, LockMode::EXCLUSIVE);

  /** 3. Check to see if it's time to upgrade */
  while (!shouldGrantLock(queue, req)) {
    cv.wait(latch);
    // throw exception when txn is aborted while waiting due to deadlock
    if (isTxnInState(txn, TransactionState::ABORTED)) {
      queue.remove_if([&req](const LockRequest &r) { return r.txn_id_ == req.txn_id_; });
      upgrading = false;
      throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
    }
  }

  /** 4. Grant the Lock and tract the locks that txn has hold */
  req.granted_ = true;
  upgrading = false;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // READ_UNCOMMITTED doesn't have SHRINKING stage
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && isTxnInState(txn, TransactionState::SHRINKING)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UNLOCK_ON_SHRINKING);
  }
  /** 1. Acquiring the latch on tuple */
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &q = lock_table_[rid];
  {  // Acquire latch on tuple
    std::unique_lock<std::mutex> latch_on_tuple(q.latch_);
    latch.swap(latch_on_tuple);
  }  // And release latch on whole LockManager

  /** 2. Clear the request that has been issued by this txn and notify others */
  txn_id_t txn_id = txn->GetTransactionId();
  q.request_queue_.remove_if([&txn_id](const LockRequest &req) { return req.txn_id_ == txn_id; });
  q.cv_.notify_all();

  if (transitToShrink(txn, rid)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  /** 3. Clear the trace in txn */
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t2].emplace(t1); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t2].erase(t1); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::set<txn_id_t> visit;
  std::set<txn_id_t> route;

  for (const auto &kv : waits_for_) {
    if (visit.find(kv.first) != visit.end()) {
      continue;
    }
    route.clear();
    if (hasCycle(&visit, &route, kv.first)) {
      *txn_id = *route.rbegin();
      return true;
    }
  }
  return false;
}

bool LockManager::hasCycle(std::set<txn_id_t> *visit, std::set<txn_id_t> *route, const txn_id_t &src) {
  if (route->find(src) != route->end()) {
    return true;
  }
  if (visit->find(src) != visit->end()) {
    return false;
  }
  visit->emplace(src);
  route->emplace(src);
  for (const txn_id_t &txn_id : waits_for_[src]) {
    if (hasCycle(visit, route, txn_id)) {
      return true;
    }
  }
  route->erase(src);
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> ret;
  for (const auto &[dst, srcs] : waits_for_) {
    for (const auto &src : srcs) {
      ret.emplace_back(src, dst);
    }
  }
  return ret;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    txn_id_t txn_id;
    buildGraph();
    while (HasCycle(&txn_id)) {
      TransactionManager::GetTransaction(txn_id)->SetState(TransactionState::ABORTED);
      clearGraph(txn_id);
    }
    clearGraph();
  }
}

void LockManager::buildGraph() {
  std::unique_lock<std::mutex> latch(latch_);
  for (auto &kv : lock_table_) {
    std::unique_lock<std::mutex> tuple_latch(kv.second.latch_);
    buildGraph(kv.second.request_queue_);
  }
}

void LockManager::buildGraph(const std::list<LockRequest> &queue) {
  std::vector<txn_id_t> granteds;
  std::vector<txn_id_t> blockeds;
  for (const LockRequest &req : queue) {
    if (!isTxnInState(req.txn_id_, TransactionState::ABORTED)) {
      if (req.granted_) {
        granteds.emplace_back(req.txn_id_);
      } else {
        blockeds.emplace_back(req.txn_id_);
      }
    }
  }
  for (const txn_id_t &granted : granteds) {
    for (const txn_id_t &blocked : blockeds) {
      AddEdge(blocked, granted);
    }
  }
}

bool LockManager::isTxnInState(Transaction *txn, const TransactionState &state) { return txn->GetState() == state; }

bool LockManager::isTxnInState(const txn_id_t &txn_id, const TransactionState &state) {
  return isTxnInState(TransactionManager::GetTransaction(txn_id), state);
}

void LockManager::clearGraph(const txn_id_t &txn_id) {
  waits_for_.erase(txn_id);
  for (auto &kv : waits_for_) {
    kv.second.erase(txn_id);
  }
  std::unique_lock<std::mutex> latch(latch_);
  for (auto &kv : lock_table_) {
    std::unique_lock<std::mutex> q_latch(kv.second.latch_);
    const std::list<LockRequest> &queue = kv.second.request_queue_;
    auto it =
        std::find_if(queue.begin(), queue.end(), [&txn_id](const LockRequest &req) { return req.txn_id_ == txn_id; });
    if (it != queue.end()) {
      kv.second.cv_.notify_all();
    }
  }
}

}  // namespace bustub
