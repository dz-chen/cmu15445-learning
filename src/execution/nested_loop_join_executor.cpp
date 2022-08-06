//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),
    left_executor_(std::move(left_executor)),right_executor_(std::move(right_executor)),
    left_tuple_(),left_rid_(),
    left_schema_(nullptr),right_schema_(nullptr){

}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  // 左表的第一个元素
  left_executor_->Next(&left_tuple_,&left_rid_);
  left_schema_ = const_cast<Schema*>(plan_->GetLeftPlan()->OutputSchema());
  right_schema_ = const_cast<Schema*>(plan_->GetRightPlan()->OutputSchema());
}


bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  while(true){
    // 右表遍历下一个tuple;
    // 如果返回false说明右表遍历完一轮,左表需进入下一个tuple,右表则从头开始
    if(!right_executor_->Next(&right_tuple,&right_rid)){
      // 如果左表遍历下一个tuple返回false,则所有数据访问完成...
      if(!left_executor_->Next(&left_tuple_,&left_rid_)) return false;
      right_executor_->Init();    // 右表从头开始
      right_executor_->Next(&right_tuple,&right_rid);
    }
    
    // 判断tuples是否满足条件
    const AbstractExpression *predicate = plan_->Predicate();  // ComparisonExpression
    if(predicate){
      Value evalResult = predicate->EvaluateJoin(&left_tuple_,left_schema_,&right_tuple,right_schema_);
      // 如果等值连接成功
      if(evalResult.GetAs<bool>()){
        std::vector<Value> vals;
        for(size_t i=0; i<left_schema_->GetColumnCount(); i++){
          vals.emplace_back(left_tuple_.GetValue(left_schema_,i));
        }
        for(size_t i=0; i<right_schema_->GetColumnCount(); i++){
          vals.emplace_back(right_tuple.GetValue(right_schema_,i));
        }

        *tuple = Tuple(vals,plan_->OutputSchema());
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
