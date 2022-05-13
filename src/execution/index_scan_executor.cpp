//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),index_(nullptr),iter_(),table_(nullptr),schema_({}),txn_(nullptr) {
      // nop
}

void IndexScanExecutor::Init() {
  Catalog* catalog = GetExecutorContext()-> GetCatalog();
  IndexInfo* index_info = catalog->GetIndex(plan_->GetIndexOid());
  index_ = dynamic_cast<IndexScanBPTreeIdxType*>(index_info->index_.get());
  iter_ = index_->GetBeginIterator();
  table_ = catalog->GetTable(index_info->table_name_)->table_.get();
  schema_ = catalog->GetTable(index_info->table_name_)->schema_;
  txn_ = GetExecutorContext()-> GetTransaction();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while(iter_!=index_->GetEndIterator()){
    // 获取当前tuple
    RID currRid = (*iter_).second;
    Tuple currTuple;
    table_->GetTuple(currRid, &currTuple, txn_);
    ++iter_;

    // 判断tuple是否满足条件
    bool ok =true;
    const AbstractExpression *predicate = plan_->GetPredicate();  // ComparisonExpression
    if(predicate){
      Value evalResult = predicate->Evaluate(&currTuple, &schema_);
      ok = evalResult.GetAs<bool>();
    }
    if(ok) {
      *tuple = currTuple;
      *rid = currRid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
