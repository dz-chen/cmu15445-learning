//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_plan.h
//
// Identification: src/include/execution/plans/seq_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {
/**
 * SeqScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class SeqScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new sequential scan plan node.
   * @param output the output format of this scan plan node
   * @param predicate the predicate to scan with, tuples are returned if predicate(tuple) = true or predicate = nullptr
   * @param table_oid the identifier of table to be scanned
   */
  SeqScanPlanNode(const Schema *output, const AbstractExpression *predicate, table_oid_t table_oid)
      : AbstractPlanNode(output, {}), predicate_{predicate}, table_oid_(table_oid) {
        // 注意: AbstractPlanNode(output, {}) 的children参数为空,说明 SeqScanPlanNode没有子结点
  }

  PlanType GetType() const override { return PlanType::SeqScan; }

  /** @return the predicate to test tuples against; tuples should only be returned if they evaluate to true */
  const AbstractExpression *GetPredicate() const { return predicate_; }

  /** @return the identifier of the table that should be scanned */
  table_oid_t GetTableOid() const { return table_oid_; }

 private:
  /** 
   * The predicate that all returned tuples must satisfy. 即 断言/约束,它是SQL的where子句 
   * 被组织为一颗表达式树
   */
  const AbstractExpression *predicate_;
  /** The table whose tuples should be scanned. */
  table_oid_t table_oid_;

  /* 以下为父类 AbstractPlanNode 的成员 */
  /**
   * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
   * and this tells you what schema this plan node's tuples will have.
   */
  // const Schema *output_schema_;
  // /** The children of this plan node. */
  // std::vector<const AbstractPlanNode *> children_;

};

}  // namespace bustub
