//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)),
    inner_table_metadata(),inner_index_info(),txn() {
}

void NestIndexJoinExecutor::Init() {
  Catalog* catalog = GetExecutorContext()->GetCatalog();
  inner_table_metadata = catalog->GetTable(plan_->GetInnerTableOid());
  inner_index_info = catalog->GetIndex(plan_->GetIndexName(),inner_table_metadata->name_);
  txn = GetExecutorContext()->GetTransaction();
  child_executor_->Init();
}

/**
 * 对于 NestIndexJoinExecutor,plan_仅一个子结点,用于获取外表数据;
 * 对于外表中的每条记录,通过索引在内表中查找合适的元组;
 * 查找内表,主要通过 NestedIndexJoinPlanNode 的 inner_table_oid_、index_name_两个成员;
 * 注:连接查询是带有条件的,需要使用plan_中的predicate_
 */ 
bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outer_tuple;
  RID outer_rid;
  while(child_executor_->Next(&outer_tuple,&outer_rid)){
   // 1.查找内表tuple
    std::vector<RID> inner_rid;
    // 比较只使用一个列,所以ScanKey()的参数不能是整个outer_tuple
    // inner_index_info->index_->ScanKey(outer_tuple,&inner_rid,txn);
     // plan_->Predicate()->GetChildAt(0) 即外表用于连接的列 的 ColumnValueExpression,它含有参数schema中某列的index !!!
    Value colval = plan_->Predicate()->GetChildAt(0)->Evaluate(&outer_tuple,child_executor_->GetOutputSchema());
    Tuple colkey(std::vector<Value>{colval},&inner_index_info->key_schema_);
    inner_index_info->index_->ScanKey(colkey,&inner_rid,txn);
    if (inner_rid.empty()) {    // 如果内表没有找到...
      return Next(tuple, rid);
    }
    Tuple inner_tuple;
    inner_table_metadata->table_->GetTuple(inner_rid[0],&inner_tuple,txn);


    // 2.判断是否满足条件. 输入的tuple都是完整的tuple,而schema只是一个列的schema...
    Value res = plan_->Predicate()->EvaluateJoin(&inner_tuple,plan_->InnerTableSchema(),
                                     &outer_tuple,plan_->OuterTableSchema());
    bool match = res.GetAs<bool>();
    if(!match) return Next(tuple,rid);

    // 3.构造返回结果(思路就找到原内表,外表的schema,分别与要输出的schema比较列名...)
    // const Schema* out_schema = plan_->OutputSchema();
    // std::vector<Value> out_vals;
    // const Schema* outer_tbl_schema = dynamic_cast<SeqScanExecutor*>(child_executor_.get())->GetTableSchema();
    //  std::vector<Column> outer_columns = outer_tbl_schema ->GetColumns();
    // for(size_t i=0;i<outer_columns.size();i++){
    //   Column out_col = outer_columns[i];
    //   for(const auto col: out_schema->GetColumns()){
    //     if(out_col.GetName() == col.GetName()){
    //       Value val = outer_tuple.GetValue(plan_->OuterTableSchema(),i);
    //       out_vals.push_back(val);
    //     }
    //   }
    // }
    // std::vector<Column> inner_columns = inner_table_metadata->schema_.GetColumns();
    // for(size_t i=0;i<inner_columns.size();i++){
    //   const Column in_col = inner_columns[i];
    //   for(const auto col: out_schema->GetColumns()){
    //     if(in_col.GetName() == col.GetName()){
    //       Value val = inner_tuple.GetValue(&inner_table_metadata->schema_,i);
    //       out_vals.push_back(val);
    //     }
    //   }
    // }
    // *tuple = Tuple(out_vals,out_schema);
    // return true;     

    /**
     * 注:获取返回结果另有一种写法如下,参考自:https://github.com/yzhao001/15445-2020FALL/blob/master/src/execution/nested_index_join_executor.cpp
     * expr_就是创建当前Column的expression,可以方便地获取想要的expr(详细见 executor_test.cpp中的 `MakeOutputSchema()`函数;)
     */
    std::vector<Value> output_row;
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      std::cout<<typeid(col).name()<<std::endl;
      output_row.push_back(col.GetExpr()->EvaluateJoin(&outer_tuple, plan_->OuterTableSchema(), &inner_tuple,
                                                       &inner_table_metadata->schema_));
    }
    *tuple = Tuple(output_row, GetOutputSchema());
    return true;


  }

   return false; 
}

}  // namespace bustub
