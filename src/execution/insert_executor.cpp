//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),child_executor_(std::move(child_executor)),tuples_({}),iter_(),
    table_info_(nullptr),txn_(nullptr),index_infos_({}){

}

void InsertExecutor::Init() {
  Catalog* catalog = GetExecutorContext()->GetCatalog();
  table_oid_t toid = plan_->TableOid();
  table_info_ = catalog->GetTable(toid);
  txn_ = GetExecutorContext()->GetTransaction();
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);

  if(plan_->IsRawInsert()){     // plan 中有要插入的数据
    tuples_ = plan_->RawValues();
    iter_ = tuples_.begin();
  }else{                        // select insert, 保证提前初始化child_executor_
    child_executor_->Init();
  }
}

/*
 * @return true if a tuple was produced, false if there are no more tuples
 *  => 注:
 * 1.insert不会返回tuple,但是会返回rid,Next执行成功则返回true...
 * 2.插入没办法在插入之前tryExclusiveLock，因为此时该tuple根本没创建.
 *   而如果插入后再tryExclusiveLock，则其他事务可能在插入后，上锁前访问该tuple
 *   具体解决方法在 TableHeap::InsertTuple 中 !!
 */ 
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    // 如果plan_中直接包含了所有要插入的元组
    if(plan_->IsRawInsert()){
      Schema schema = table_info_->schema_;
      while(iter_!=tuples_.end()){
        std::vector<Value>& vals = *iter_;
        Tuple currTuple(vals,&schema); RID currRid;
        insert_tuple_and_index(currTuple,&currRid);
        *tuple = currTuple;
        *rid = currRid;
        iter_++;
        return true;
      }
      return false;
    }

    // 否则,是select insert,需要先执行 plan_的child...
    else{
      Tuple currTuple;
      RID currRid;
      while(child_executor_->Next(&currTuple,&currRid)){
        insert_tuple_and_index(currTuple,&currRid);
        *tuple = currTuple;
        *rid = currRid;
        return true;
      }
      return false;
    }
}

// 插入元组的同时,更新表上的索引
void InsertExecutor::insert_tuple_and_index(Tuple& tuple,RID* rid){
  Schema schema = table_info_->schema_;
  TableHeap* table = table_info_->table_.get();

  // 插入table
  RID currRid;
  table->InsertTuple(tuple,&currRid,txn_);
  *rid = currRid;

  // 插入index
  for(size_t i=0;i<index_infos_.size();i++){
    Index* bptIndex = index_infos_[i]->index_.get();
    Schema key_schema = index_infos_[i]->key_schema_;
    std::vector<uint32_t> key_attrs = bptIndex->GetMetadata()->GetKeyAttrs();
    // b+树插入...
    Tuple key = tuple.KeyFromTuple(schema, key_schema, key_attrs);
    bptIndex->InsertEntry(key, currRid, txn_);
  }
}
}  // namespace bustub
