//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)),
    table_info_(nullptr),index_infos_({}),txn_(nullptr) {

}

void DeleteExecutor::Init() {
  txn_ = GetExecutorContext()-> GetTransaction();
  Catalog* catalog = GetExecutorContext()-> GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();      // 保证child_executor_在调用前完成初始化...
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple currTuple; RID currRid;
  while(child_executor_->Next(&currTuple,&currRid)){
    // 删除元组
    TableHeap* table_ = table_info_->table_.get();
    // only need to mark. The deletes will be applied when transaction commits
    table_->MarkDelete(currRid,txn_);  

    // 删除索引
    for(size_t i=0;i<index_infos_.size();i++){
      Index* bptIndex = index_infos_[i]->index_.get();
      Schema schema = table_info_->schema_;
      Schema key_schema = index_infos_[i]->key_schema_;
      std::vector<uint32_t> key_attrs = bptIndex->GetMetadata()->GetKeyAttrs();
      Tuple key = currTuple.KeyFromTuple(schema,key_schema,key_attrs);
      bptIndex->DeleteEntry(key, currRid, txn_);
    }
  }
  return false;
}

}  // namespace bustub
