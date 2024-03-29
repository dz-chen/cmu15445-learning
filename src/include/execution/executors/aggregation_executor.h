//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 * AggregateKey就是 SimpleAggregationHashTable 中的key,用于确定group by分类后的分组;
 * 不过这个key可能是由多个属性组合计算后所得(或者说这些属性共同构成key);
 * 最终hashtable中的一个bucket,存放的AggregateValue就是当前分组内的count,sum,max,min统计结果; 
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Create a new simplified aggregation hash table.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<const AbstractExpression *> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** 
   * @return the initial aggregrate value for this aggregation executor 
   * 初始化聚合结果 => count,min,max,sum(他们是在同一趟扫描中完成)
   */
  AggregateValue GenerateInitialAggregateValue() {
    std::vector<Value> values;
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountAggregate:
          // Count starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::SumAggregate:
          // Sum starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::MinAggregate:
          // Min starts at INT_MAX.
          values.emplace_back(ValueFactory::GetIntegerValue(BUSTUB_INT32_MAX));
          break;
        case AggregationType::MaxAggregate:
          // Max starts at INT_MIN.
          values.emplace_back(ValueFactory::GetIntegerValue(BUSTUB_INT32_MIN));
          break;
      }
    }
    return {values};
  }

  /** 
   * Combines the input into the aggregation result. 
   * 即在一趟扫描的每一步中, 统计当前聚合结果: count,sum,min,max 
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountAggregate:
          // Count increases by one.
          result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::SumAggregate:
          // Sum increases by addition.
          result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
          break;
        case AggregationType::MinAggregate:
          // Min is just the min.
          result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
          break;
        case AggregationType::MaxAggregate:
          // Max is just the max.
          result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
          break;
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   * 将当前轮次遍历 添加到结果中(HASH表)
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht.count(agg_key) == 0) {   // count()计算当前map中的元素个数
      ht.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht[agg_key], agg_val);
  }

  /**
   * An iterator through the simplified aggregation hash table.
   */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_(iter) {}

    /** @return the key of the iterator */
    const AggregateKey &Key() { return iter_->first; }

    /** @return the value of the iterator */
    const AggregateValue &Val() { return iter_->second; }

    /** @return the iterator before it is incremented */
    Iterator &operator++() {
      ++iter_;
      return *this;
    }

    /** @return true if both iterators are identical */
    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

    /** @return true if both iterators are different */
    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map. */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return iterator to the start of the hash table */
  Iterator Begin() { return Iterator{ht.cbegin()}; }

  /** @return iterator to the end of the hash table */
  Iterator End() { return Iterator{ht.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values. */
  std::unordered_map<AggregateKey, AggregateValue> ht{};
  /** The aggregate expressions that we have. */
  const std::vector<const AbstractExpression *> &agg_exprs_;
  /** The types of aggregations that we have. */
  const std::vector<AggregationType> &agg_types_;
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) on the tuples of a child executor.
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new aggregation executor.
   * @param exec_ctx the context that the aggregation should be performed in
   * @param plan the aggregation plan node
   * @param child the child executor
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child);

  /** Do not use or remove this function, otherwise you will get zero points. */
  const AbstractExecutor *GetChildExecutor() const;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  /**
   *  @return the tuple as an AggregateKey 
   * 返回的 AggregateKey 即 group by 子句中所有列的取值
   * 对于不含group by 子句的聚合,每次总是返回同一个key
   */
  AggregateKey MakeKey(const Tuple *tuple) {
    std::vector<Value> keys;
    // plan_->GetGroupBys() 返回的是group by 子句对应的列
    // expr 是 ColumnValueExpression, 故keys 就是 group by 子句中那些列的取值
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
    }
    // 对于 不含 group by的 聚合,plan_->GetGroupBys() size为0,不会进入for循环
    //最终hash表中只有一个 AggregateKey, 故所有聚合结果都在该key对应的 val 中
    return {keys};
  }

  /**
   *  @return the tuple as an AggregateValue
   *  返回参与聚合的列的取值
   */
  AggregateValue MakeVal(const Tuple *tuple) {
    std::vector<Value> vals;
    // 每个expr都是 AggregateValueExpression
    // vals 即 要统计聚合结果的列的 取值
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node. */
  const AggregationPlanNode *plan_;
  /** The child executor whose tuples we are aggregating. */
  std::unique_ptr<AbstractExecutor> child_;     // 用于查询做聚合的数据

  /** Simple aggregation hash table. */
  // Uncomment me! SimpleAggregationHashTable aht_;
  SimpleAggregationHashTable aht_;

  /** Simple aggregation hash table iterator. */
  // Uncomment me! SimpleAggregationHashTable::Iterator aht_iterator_;
  SimpleAggregationHashTable::Iterator aht_iterator_;
};
}  // namespace bustub
