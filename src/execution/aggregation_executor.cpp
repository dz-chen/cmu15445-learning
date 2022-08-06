//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),plan_(plan),child_(std::move(child)),
    aht_(plan_->GetAggregates(),plan_->GetAggregateTypes()),aht_iterator_(aht_.Begin()){
        child_->Init();
    }

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  Tuple currTuple;
  RID currRid;
  while (child_->Next(&currTuple,&currRid)){
    AggregateKey agg_key = MakeKey(&currTuple);
    AggregateValue agg_val = MakeVal(&currTuple);
    /**
     * 重要,理解此函数是如何统计 聚合结果的...
     * 1.对于没有group by 的聚合
     * 2.对于有group by 的聚合
     * .....
     */ 
    aht_.InsertCombine(agg_key,agg_val);
  }
  aht_iterator_ = aht_.Begin();
}


/**
 * 由于有 group by 子句,所以可能返回多条结果;
 * child 最好在 init()中执行完毕,此处Next()只负责返回数据的逻辑,每次返回一条
 *   => 从而省去判断何时child_该何时暂停的麻烦...
 */ 
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if(aht_iterator_ == aht_.End()){
    return false;
  }

  AggregateKey group_bys = aht_iterator_.Key();
  AggregateValue aggregates = aht_iterator_.Val();
  ++aht_iterator_;

  // 判断是否满足having条件
  if(plan_->GetHaving()){
    // plan_->GetHaving() 是 ComparisonExpression
    Value res = plan_->GetHaving()->EvaluateAggregate(group_bys.group_bys_,aggregates.aggregates_);
    if(!res.GetAs<bool>()) return Next(tuple,rid);
  }

  // 获取返回结果
  std::vector<Value> output_row;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    /**
     * col.GetExpr() 是 AggregateValueExpression,为countA/sumA/minA/maxA;
     * EvaluateAggregate()直接返回 aggregates_ 中特定的列的统计结果;
     */ 
    output_row.push_back(col.GetExpr()->EvaluateAggregate(group_bys.group_bys_,aggregates.aggregates_));
  }
  *tuple = Tuple(output_row, GetOutputSchema());

  return true; 
}

}  // namespace bustub
