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
  /**
   * 判断事务隔离级别
   */
  if(txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED){
     // 1.READ_UNCOMMITTED: 只有写锁,没有读锁,在数据读上没有限制,不必也不能上读锁
     // 2.但是注意,READ_UNCOMMITTED 写锁是必须的,否则连事务的原子性都无法保证
     // 3.如何保证T1能读到未提交的数据? => 因为没有加读锁(T2加写锁本质是阻止T1务施加读锁,而不能直接禁止T1读数据)
     // 4.为何T1还可能出现脏读? => 因为读取的数据可能是T2未提交的,若T2回滚,则T1读的是脏数据
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED){
    // 1.READ_COMMITTED : 写锁一直保持到事务结束,但是读锁在SELECT操作完成后马上释放
    // 2.由于读锁需要立即释放,所以不可能遵守2PL协议,这种情况下没有 growing,shrinking的概念,
    //    无需像 REPEATABLE_READ一样判断 shrinking 状态 
    // 3.如何保证T1读的是已经提交的数据? => 因为T2的写锁是在事务结束时才释放,rid上有写锁,则T1不能读...
    // 4.为何T1还可能出现不可重复读? => 因为T1事务的生命周期内,一旦释放读锁,T2可能修改之前的内容
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ){
    // 1.REPEATABLE_READ:对象的读锁和写锁一直保持到事务结束
    // 2.遵守 2PL(只是没有范围锁),有growing,shrinking概念,shrinking阶段不能上锁
    // 3.如何保证T1可重复读?  => 因为T1的读写锁都是在事务结束后才释放,T2无法修改T1事务内的数据,因此T1重复读的数据是一致的
    // 4.为何还可能出现幻读? => 因为没有范围锁,T1的读锁只能保证读的数据不被修改,而无法阻止T2插入新的数据...
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 已经获得rid 上的 s-lock 或者 x-lock(x-lock包含了读的含义)
  if(txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)){
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

  // 可能因为死锁,当前线程已经被选作 victim
  if(txn->GetState() == TransactionState::ABORTED){
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  // 授予锁
  req->granted_ = true;
  txn->SetState(TransactionState::GROWING);   // 仅对 REPEATABLE_READ 有用
  txn->GetSharedLockSet()->emplace(rid);      // 对 REPEATABLE_READ,READ_COMMITTED 有用
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  /**
   * 判断事务隔离级别
   */ 
  if(txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED){
    // 正常上写锁即可,没有growing,shrinking概念
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED){
     // 正常上写锁即可,没有growing,shrinking概念
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ){
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
    }
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
  
  // 可能因为死锁,当前线程已经被选作 victim
  if(txn->GetState() == TransactionState::ABORTED){
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  // 授予锁
  req->granted_ = true;
  txn->SetState(TransactionState::GROWING);   // 仅对 REPEATABLE_READ 有用
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

/**
 * this should also abort the transaction and return false 
 *  if another transaction is already waiting to upgrade their lock => 为什么?
 */ 
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  /**
   * 判断事务隔离级别
   */ 
  if(txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED){
    // READ_UNCOMMITTED 没有读锁,不应该有升级的概念
    LOG_ERROR("READ_UNCOMMITTED isolation level should not upgrade lock");
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED){
     // READ_COMMITTED 需要立即释放读锁,应该没有升级的概念 (个人观点)
     LOG_ERROR("READ_COMMITTED isolation level should not upgrade lock");
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ){
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
    }
  }

  LockRequestQueue& req_queue = lock_table_[rid];
  /**
   * 为什么? 多个事务在一个对象上最多只有一个X锁,已经有txn打算升级,则当前txn不必升级了...
   * 但是感觉不要这个判断似乎也没有影响? TODO
   */ 
  if(req_queue.upgrading_){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
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
  /**
   * 判断事务隔离级别
   */ 
  if(txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED){
    // 正常释放锁即可,没有growing,shrinking概念
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED){
     // 正常释放锁即可,没有growing,shrinking概念
  }
  else if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ){
    // 修改txn状态为 shrinking
    // 注意:对于回滚的事务(状态为 ABORTED),txnMgr最终要unlock所有锁,
    //     必须有GROWING这一判断,防止ABORTED 状态被重新设置为shrinking...
    if(txn->GetState()==TransactionState::GROWING){
      txn->SetState(TransactionState::SHRINKING);
    }
  }

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
