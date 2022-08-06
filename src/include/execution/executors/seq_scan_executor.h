//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 * 一个executor 即一个 
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  void Init() override;

  /* 每调用一次Next,通过tuple,rid 返回一条结果 */
  bool Next(Tuple *tuple, RID *rid) override;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  /**
   * add by cdz, 方便 nested_index_join_executor.cpp 中获取整张外表的schema
   * 注:需要保证先初始化了这个 executor 才能获取schema,某个返回的schema无效
   */ 
  const Schema *GetTableSchema() {return &schema_; }
 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;

  /* add by cdz */
  TableHeap* table_;            //  physical table on disk
  TableIterator iter_;          // 用于遍历表中的所有tuple
  Schema schema_;               // 整张表的schema

  // 下为 父类中包含的成员
  // ExecutorContext *exec_ctx_;
};
}  // namespace bustub
