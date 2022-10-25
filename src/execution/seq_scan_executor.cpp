//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
  :AbstractExecutor(exec_ctx),
  plan_(plan),
  table_(nullptr),
  iter_(nullptr, RID(), nullptr),
  schema_({}){
    // 初始化列表多数为参数为空, 在Init中设置...
}

void SeqScanExecutor::Init() {
  Catalog* catalog = GetExecutorContext()-> GetCatalog();
  Transaction* txn = GetExecutorContext()-> GetTransaction();
  table_oid_t toid = plan_->GetTableOid();
  table_ = catalog->GetTable(toid)->table_.get();
  iter_ = table_->Begin(txn);
  schema_ = catalog->GetTable(toid)->schema_;
}

/**
 * 每调用一次Next,通过tuple,rid 返回一条结果 
 *  @return true if a tuple was produced, false if there are no more tuples 
 */
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

  // *iter_ 即 Tuple
  while(iter_ != table_->End()){
    RID currRid = iter_->GetRid();

    // 读前先加锁,scan只需读锁,根据隔离级别决定是否释放读锁
    LockManager* lock_mgr = GetExecutorContext()->GetLockManager();
    if(lock_mgr) lock_mgr->tryLockShared(GetExecutorContext()-> GetTransaction(),currRid);
    
    // 获取当前tuple
    Tuple currTuple = *iter_;
    iter_++;

    // 检验当前tuple是否满足条件:满足则返回,不满足则继续遍历下一个tuple...
    bool ok =true;
    const AbstractExpression *predicate = plan_->GetPredicate();  // ComparisonExpression 可能没有(nullptr)...
    if(predicate){
      Value evalResult = predicate->Evaluate(&currTuple, &schema_);
      ok = evalResult.GetAs<bool>();
    }
    
    // 读完,释放读锁(tryUnlockShared 会自动判断隔离级别)
    if(lock_mgr) lock_mgr->tryUnlockShared(GetExecutorContext()-> GetTransaction(),currRid);

    if(ok) {
      *tuple = currTuple;
      *rid = currRid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
