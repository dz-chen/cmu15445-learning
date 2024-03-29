//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"
// #include "concurrency/transaction_manager.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 * 注意:
 *    1.事务 向 lockmanager 请求锁;
 *    2.
 */
class LockManager {
  // 共享锁,排它锁
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  /**
   * 每个 rid 上一个 LockRequestQueue
   * request_queue_ 存放等待获取锁的txn
   */ 
  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;  // 存放两类txn: 未获得锁而阻塞的txn 以及  获得锁但是尚未释放的txn !
    std::condition_variable cv_;  // for notifying blocked transactions on this rid
    std::mutex mtx_;               // add by cdz, 与条件变量搭配使用,每个mutex对应后一个共享对象(rid)...
    bool upgrading_ = false;      // request_queue_ 中是否有请求升级锁的txn
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    // enable_cycle_detection_ = false;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    LOG_INFO("Cycle detection thread launched");
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
    LOG_INFO("Cycle detection thread stopped");
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we: => 重要,重点理解...
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
   *    is responsible for keeping track of its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the requesting transaction
   * @return true if the upgrade is successful, false otherwise
   * this should also abort the transaction and return false 
   * if another transaction is already waiting to upgrade their lock
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /*** Graph API ***/
  /**
   * Adds edge t1->t2
   */

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

  /*******functions add by cdz *********/
  /**
   * rid上是否已经有 exclusive lock 被授予
   * 注意与 txn 的 IsExclusiveLocked()对比
   */ 
  bool IsExclusiveGranted(const RID &rid);

  bool IsShareOrExclusiveGranted(const RID& rid);
  
  bool IsUpgradable(txn_id_t txn_id, const RID& rid);

  void BuildWaitsFor();

  bool DFS(txn_id_t start,txn_id_t* youngest);

  /* 以下为封装后供 executor 调用, by cdz */
  bool tryLockShared(Transaction *txn, const RID &rid);
  
  bool tryUnlockShared(Transaction *txn, const RID &rid);

  bool tryLockExclusive(Transaction *txn, const RID &rid);
  
  // bool tryUnlockExclusive(Transaction *txn, const RID &rid); // exclusive不需要手动释放,commit时会释放

 private:
  std::mutex latch_;                            // 保护lockmanager中的共享变量(lock_table_)
  std::atomic<bool> enable_cycle_detection_;    // 是否开启死检测(需要一个后台线程进行检测)
  std::thread *cycle_detection_thread_;         // 进行死锁检测的后台线程

  /** 
   * Lock table for lock requests. 
   * 每个 rid 上有一个 队列 
   */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
};

}  // namespace bustub
