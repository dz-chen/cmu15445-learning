//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next([[maybe_unused]] Tuple *tuple, RID *rid) override;

 private:
  /* this func add by cdz */
  void insert_tuple_and_index(Tuple& tuple,RID* rid);
  
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  
  /* below all added by cdz */
  
  // child_executor_ 可能为nullptr
  std::unique_ptr<AbstractExecutor> child_executor_;

  // tuples_也可能没有(取决于plan_类型)
  std::vector<std::vector<Value>> tuples_;
  std::vector<std::vector<Value>>::iterator iter_;

  // 其他成员
  TableMetadata* table_info_;
  Transaction *txn_;
  std::vector<IndexInfo*> index_infos_;  // B+树索引信息,一个表上可能有多个索引!
};
}  // namespace bustub
