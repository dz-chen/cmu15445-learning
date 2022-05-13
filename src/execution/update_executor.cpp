//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),table_info_(nullptr),child_executor_(std::move(child_executor)),
   index_infos_({}),txn_(nullptr){
    
}

void UpdateExecutor::Init() {
  txn_ = GetExecutorContext()-> GetTransaction();
  Catalog* catalog = GetExecutorContext()-> GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();      // 保证child_executor_在调用前完成初始化...
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 通过子 executor 获取需要更改的数据
  Tuple currTuple; RID currRid;
  while(child_executor_->Next(&currTuple,&currRid)){
    // update table data
    Tuple newTuple = GenerateUpdatedTuple(currTuple);
    TableHeap* table_ = table_info_->table_.get();
    table_->UpdateTuple(newTuple, currRid,txn_);

    // update table index
    updataIndexes(currTuple,newTuple,currRid);
    
    *tuple = newTuple;
    *rid = currRid;
    return true;
  }
  return false;
}


/*
 * TODO: 这样更新索引的前提是元组计算出的key不会重复; 否则需要修改B+树满足非唯一键...
 */
void UpdateExecutor::updataIndexes( Tuple& old_tup, Tuple& new_tup, const RID& rid){
  for(size_t i=0;i<index_infos_.size();i++){                               // 当前索引(B+树)
    Index* bptIndex = index_infos_[i]->index_.get();
    Schema schema = table_info_->schema_;
    Schema key_schema = index_infos_[i]->key_schema_;
    std::vector<uint32_t> key_attrs = bptIndex->GetMetadata()->GetKeyAttrs();

    Tuple old_key = old_tup.KeyFromTuple(schema,key_schema,key_attrs);
    Tuple new_key = new_tup.KeyFromTuple(schema,key_schema,key_attrs);
    bptIndex->DeleteEntry(old_key, rid, txn_);
    bptIndex->InsertEntry(new_key, rid, txn_);
  }
}



}  // namespace bustub
