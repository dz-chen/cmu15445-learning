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


/**
 * Any failed lock operation should lead to an ABORTED transaction state (implicit abort) and throw an exception.
 */ 
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // txn 已经 aborted
  if(txn->GetState() == TransactionState::ABORTED){
    return false;
  }
  if(txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }

  // 已经获得 rid 上的 s-lock
  if(txn->IsSharedLocked(rid)){
    return true;
  }

  // 先加入请求队列
  LockRequestQueue& req_queue = lock_table_[rid];
  latch_.lock();
  req_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(),LockMode::SHARED));
  auto req = req_queue.request_queue_.end();
  latch_.unlock();

  /**
   * 以下情况需要阻塞:
   * 1.当前rid上已经有互斥锁;
   */
  while (IsExclusiveGranted(rid)){
    std::unique_lock<std::mutex> ul(req_queue.mtx_);
    req_queue.cv_.wait(ul);
  }
  
  // 授予锁
  req->granted_ = true;
  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // txn 已经 aborted
  if(txn->GetState() == TransactionState::ABORTED){
    return false;
  }

  // 已经获得 rid 上的 x-lock
  if(txn->IsExclusiveLocked(rid)){
    return true;
  }

  // 先加入请求队列
  LockRequestQueue& req_queue = lock_table_[rid];
  latch_.lock();
  req_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(),LockMode::EXCLUSIVE));
  auto req = req_queue.request_queue_.end();
  latch_.unlock();

  /**
   * 以下情况需要阻塞:
   * 1.当前rid上已经有锁;
   */
  while (IsShareOrExclusiveGranted(rid)){
    std::unique_lock<std::mutex> ul(req_queue.mtx_);
    req_queue.cv_.wait(ul);
  }
  
  // 授予锁
  req->granted_ = true;
  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

/**
 * this should also abort the transaction and return false 
 *  if another transaction is already waiting to upgrade their lock => 为什么?
 */ 
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  LockRequestQueue& req_queue = lock_table_[rid];
  /**
   * 为什么? 多个事务在一个对象上最多只有一个X锁,已经有txn打算升级,则当前txn不必升级了...
   * 但是感觉不要这个判断似乎也没有影响? TODO
   */ 
  if(req_queue.upgrading_){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }
  // 已经有了互斥锁
  if(txn->IsExclusiveLocked(rid)){
    return true;
  }

  /**
   * 阻塞等待升级时机
   */
  req_queue.upgrading_ = true;
  while(IsUpgradable(txn->GetTransactionId(),rid)){
    std::unique_lock<std::mutex> ul(req_queue.mtx_);
    req_queue.cv_.wait(ul);
  }

  // 升级锁
  req_queue.upgrading_ = false;
  auto req = std::find_if(req_queue.request_queue_.begin(),req_queue.request_queue_.end(),
                        [&txn](auto &req){ return req.txn_id_==txn->GetTransactionId();} );
  latch_.lock();
  req->lock_mode_ = LockMode::EXCLUSIVE;
  latch_.unlock();

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 要释放锁的 req
  LockRequestQueue& req_queue = lock_table_[rid];
  auto req = std::find_if(req_queue.request_queue_.begin(),req_queue.request_queue_.end(),
                        [&txn](auto &req){ return req.txn_id_==txn->GetTransactionId();} );
  if(req->lock_mode_==LockMode::SHARED) txn->GetSharedLockSet()->erase(rid);  // 释放共享锁
  else txn->GetExclusiveLockSet()->erase(rid);                                // 释放排它锁

  latch_.lock();
  req_queue.request_queue_.erase(req);
  latch_.unlock();
  req_queue.cv_.notify_all();

  // 对于回滚的事务(状态为 ABORTED),txnMgr最终要unlock所有锁,必须有此判断,防止状态备重新设置为shrinking...
  if(txn->GetState()==TransactionState::GROWING){
    txn->SetState(TransactionState::SHRINKING);
  }
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

  /*******functions add by cdz *********/
  /**
   * rid上是否已经有 exclusive lock 被授予
   * 注意与 txn 的 IsExclusiveLocked()对比
   */ 
bool LockManager::IsExclusiveGranted(const RID &rid){
  LockRequestQueue& req_queue = lock_table_[rid];
  for(auto& req : req_queue.request_queue_){
    if(req.lock_mode_==LockMode::EXCLUSIVE && req.granted_){
      return true;
    }
  }
  return false;
}

/**
 * rid 上是否已经有锁了
 */
bool LockManager::IsShareOrExclusiveGranted(const RID& rid){
  LockRequestQueue& req_queue = lock_table_[rid];
  for(auto& req : req_queue.request_queue_){
    if(req.granted_){
      return true;
    }
  }
  return false;
}

/**
 * txn 是否可升级rid上的锁
 * 条件:当前rid上有且仅有 txn 的一个共享锁
 */ 
bool LockManager::IsUpgradable(txn_id_t txn_id, const RID& rid){
  LockRequestQueue& req_queue = lock_table_[rid];
  bool is_txn_slocked=false;  // 确保rid上确实有txn的共享锁
  for(auto& req : req_queue.request_queue_){
    if(req.granted_){     // 已经被授予了锁
      if(req.lock_mode_!=LockMode::SHARED) return false; // 已经有了互斥锁
      if(req.txn_id_!=txn_id) return false;              // rid上的共享锁不是txn_id的
      else is_txn_slocked=true;
    }
  }
  return is_txn_slocked;
}


}  // namespace bustub
