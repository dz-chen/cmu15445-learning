//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// comparison_expression.h
//
// Identification: src/include/expression/comparison_expression.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/** ComparisonType represents the type of comparison that we want to perform. */
enum class ComparisonType { Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual };

/**
 * ComparisonExpression represents two expressions being compared.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /** Creates a new comparison expression representing (left comp_type right). */
  ComparisonExpression(const AbstractExpression *left, const AbstractExpression *right, ComparisonType comp_type)
      : AbstractExpression({left, right}, TypeId::BOOLEAN), comp_type_{comp_type} {
        // 注意: {left, right} 构成 AbstractExpression(children,ret_type) 的参数 children !!!
      }

  /* 简单比较 */
  Value Evaluate(const Tuple *tuple, const Schema *schema) const override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema);   // 返回比较的左边元素
    Value rhs = GetChildAt(1)->Evaluate(tuple, schema);   // 返回比较的右边元素
    return ValueFactory::GetBooleanValue(PerformComparison(lhs, rhs));
  }

  /**
   * 连接
   * 这里的tuple是完整的tuple,而schema是内表/外表查询结果的schema(不一定是包含所有列,也不一定只有用于比较的那个列)
   */
  Value EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple,
                     const Schema *right_schema) const override {
    // GetChildAt(0) 得到的是外表用于比较的那个列对应的 ColumnValueExpression(但是参数传入的schema可能不止一个列!!!)
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    // GetChildAt(1) 得到的是内表用于比较的那个列对应的 ColumnValueExpression(但是参数传入的schema可能不止一个列!!!)
    Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return ValueFactory::GetBooleanValue(PerformComparison(lhs, rhs));
  }

  /* 聚合 */
  Value EvaluateAggregate(const std::vector<Value> &group_bys, const std::vector<Value> &aggregates) const override {
    Value lhs = GetChildAt(0)->EvaluateAggregate(group_bys, aggregates);
    Value rhs = GetChildAt(1)->EvaluateAggregate(group_bys, aggregates);
    return ValueFactory::GetBooleanValue(PerformComparison(lhs, rhs));
  }

 private:
  CmpBool PerformComparison(const Value &lhs, const Value &rhs) const {
    switch (comp_type_) {
      case ComparisonType::Equal:
        return lhs.CompareEquals(rhs);
      case ComparisonType::NotEqual:
        return lhs.CompareNotEquals(rhs);
      case ComparisonType::LessThan:
        return lhs.CompareLessThan(rhs);
      case ComparisonType::LessThanOrEqual:
        return lhs.CompareLessThanEquals(rhs);
      case ComparisonType::GreaterThan:
        return lhs.CompareGreaterThan(rhs);
      case ComparisonType::GreaterThanOrEqual:
        return lhs.CompareGreaterThanEquals(rhs);
      default:
        BUSTUB_ASSERT(false, "Unsupported comparison type.");
    }
  }

  std::vector<const AbstractExpression *> children_;
  ComparisonType comp_type_;
};
}  // namespace bustub
