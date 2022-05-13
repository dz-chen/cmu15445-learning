//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
  // TODO: 这是课程官方提示可直接默认为此类型(<GenericKey<8>, RID, GenericComparator<8>>), 如何写得更泛化?
  using IndexScanIterType = IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
  using IndexScanBPTreeIdxType = BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;

 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  
  // add by cdz
  IndexScanBPTreeIdxType* index_;
  IndexScanIterType iter_;
  TableHeap* table_;
  Schema schema_;
  Transaction *txn_;
};
}  // namespace bustub
