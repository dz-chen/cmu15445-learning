//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_test.cpp
//
// Identification: test/execution/executor_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "execution/plans/delete_plan.h"
#include "execution/plans/limit_plan.h"

#include "buffer/buffer_pool_manager.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"  // NOLINT
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

class ExecutorTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
    disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
    bpm_ = std::make_unique<BufferPoolManager>(32, disk_manager_.get());
    page_id_t page_id;
    bpm_->NewPage(&page_id);
    lock_manager_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
    // Begin a new transaction, along with its executor context.
    txn_ = txn_mgr_->Begin();
    exec_ctx_ =
        std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get(), txn_mgr_.get(), lock_manager_.get());
    // Generate some test tables.
    TableGenerator gen{exec_ctx_.get()};
    gen.GenerateTestTables();

    execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());
  }

  // This function is called after every test.
  void TearDown() override {
    // Commit our transaction.
    txn_mgr_->Commit(txn_);
    // Shut down the disk manager and clean up the transaction.
    disk_manager_->ShutDown();
    remove("executor_test.db");
    delete txn_;
  };

  /** @return the executor context in our test class */
  ExecutorContext *GetExecutorContext() { return exec_ctx_.get(); }
  ExecutionEngine *GetExecutionEngine() { return execution_engine_.get(); }
  Transaction *GetTxn() { return txn_; }
  TransactionManager *GetTxnManager() { return txn_mgr_.get(); }
  Catalog *GetCatalog() { return catalog_.get(); }
  BufferPoolManager *GetBPM() { return bpm_.get(); }
  LockManager *GetLockManager() { return lock_manager_.get(); }

  // The below helper functions are useful for testing.

  const AbstractExpression *MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx,
                                                      const std::string &col_name) {
    uint32_t col_idx = schema.GetColIdx(col_name);
    auto col_type = schema.GetColumn(col_idx).GetType();
    allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeConstantValueExpression(const Value &val) {
    allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs,
                                                     ComparisonType comp_type) {
    allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) {
    allocated_exprs_.emplace_back(
        std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
    return allocated_exprs_.back().get();
  }

  const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs) {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }

 private:
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;                         // executor_test.db
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;                         // 包括: txn_, catalog_, bpm_, txn_mgr_, lock_manager_
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;  // 即 expression
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
};

// 多个测试场景需要相同数据配置的情况,用TEST_F
// NOLINTNEXTLINE
TEST_F(ExecutorTest, SimpleSeqScanTest) {
  // SELECT colA, colB FROM test_1 WHERE colA < 500

  // Construct query plan
  TableMetadata *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  Schema &schema = table_info->schema_;
  auto *colA = MakeColumnValueExpression(schema, 0, "colA");  // 这个表达式 Evaluate() 将直接返回指定列的元素值; ColumnValueExpression内部包含了 col_idx_
  auto *colB = MakeColumnValueExpression(schema, 0, "colB");
  auto *const500 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(500));     // Evaluate() 将返回常数 500 对应的 Value
  auto *predicate = MakeComparisonExpression(colA, const500, ComparisonType::LessThan); // colA, const500 构成了 ComparisonExpression 的两个子结点
  auto *out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  SeqScanPlanNode plan{out_schema, predicate, table_info->oid_};    // 注: SeqScanPlanNode 没有子PlanNode结点

  // Execute
  // 会根据plan类型创建相应的executor
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  std::cout << "ColA, ColB" << std::endl;
  for (const auto &tuple : result_set) {
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() < 500);
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() < 10);
    std::cout << tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
              << tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  }
  ASSERT_EQ(result_set.size(), 500);
}


// NOLINTNEXTLINE
// add by cdz
TEST_F(ExecutorTest, SimpleIndexScanTest) {
  // SELECT colA, colB FROM test_1 WHERE colA > 500

  // Construct query plan
  TableMetadata *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  Schema &schema = table_info->schema_;

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "test_1", table_info->schema_, *key_schema, {0}, 8);

  auto *colA = MakeColumnValueExpression(schema, 0, "colA");
  auto *colB = MakeColumnValueExpression(schema, 0, "colB");
  auto *const600 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(600));
  auto *predicate = MakeComparisonExpression(colA, const600, ComparisonType::GreaterThan);
  auto *out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  IndexScanPlanNode plan{out_schema, predicate, index_info->index_oid_};

  // Execute
  // 根据 plan 类型
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  for (const auto &tuple : result_set) {
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() > 600);
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() < 10);
  }
  ASSERT_EQ(result_set.size(), 399);

  delete key_schema;
}


// NOLINTNEXTLINE
TEST_F(ExecutorTest, SimpleRawInsertTest) {
  // INSERT INTO empty_table2 VALUES (100, 10), (101, 11), (102, 12)
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(10)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(101), ValueFactory::GetIntegerValue(11)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(102), ValueFactory::GetIntegerValue(12)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  // std::move(raw_vals) 避免了不必要的拷贝
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  // 插入
  GetExecutionEngine()->Execute(&insert_plan, nullptr, GetTxn(), GetExecutorContext());

  // Iterate through table make sure that values were inserted.
  // SELECT * FROM empty_table2;
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");   // 用于获取colA属性值
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  // seq_scan 检查...
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, GetTxn(), GetExecutorContext());

  std::cout << "ColA, ColB" << std::endl;
  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 100);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 10);
  std::cout << result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
            << result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 101);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 11);
  std::cout << result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
            << result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 102);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 12);
  std::cout << result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
            << result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  // Size
  ASSERT_EQ(result_set.size(), 3);
}

// NOLINTNEXTLINE
TEST_F(ExecutorTest, SimpleSelectInsertTest) {
  // INSERT INTO empty_table2 SELECT colA, colB FROM test_1 WHERE colA < 500
  std::unique_ptr<AbstractPlanNode> scan_plan1;
  const Schema *out_schema1;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto colB = MakeColumnValueExpression(schema, 0, "colB");
    auto const500 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(500));
    auto predicate = MakeComparisonExpression(colA, const500, ComparisonType::LessThan);
    out_schema1 = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, predicate, table_info->oid_);
  }
  std::unique_ptr<AbstractPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    // 注意这里: insert_plan有一个child(scan_plan1),用于获取要插入的数据
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }
  // test by cdz
  ExecutionEngine* engine = GetExecutionEngine();
  AbstractPlanNode* plan = insert_plan.get();
  Transaction* txn = GetTxn();
  ExecutorContext* ctx = GetExecutorContext();
  engine ->Execute(plan,nullptr,txn,ctx);
  // GetExecutionEngine()->Execute(insert_plan.get(), nullptr, GetTxn(), GetExecutorContext());

  // Now iterate through both tables, and make sure they have the same data
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  const Schema *out_schema2;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto colB = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }
  std::vector<Tuple> result_set1;
  std::vector<Tuple> result_set2;
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set1, GetTxn(), GetExecutorContext());
  GetExecutionEngine()->Execute(scan_plan2.get(), &result_set2, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set1.size(), result_set2.size());
  for (size_t i = 0; i < result_set1.size(); ++i) {
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int32_t>());
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colB")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>());
    std::cout << result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>() << ", "
              << result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colB")).GetAs<int32_t>() << ", "
              << result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int32_t>() << ", "
              << result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  }
  ASSERT_EQ(result_set1.size(), 500);
}

// NOLINTNEXTLINE
TEST_F(ExecutorTest, SimpleRawInsertWithIndexTest) {
  // INSERT INTO empty_table2 VALUES (100, 10), (101, 11), (102, 12)
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(10)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(101), ValueFactory::GetIntegerValue(11)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(102), ValueFactory::GetIntegerValue(12)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  GetExecutionEngine()->Execute(&insert_plan, nullptr, GetTxn(), GetExecutorContext());

  // Iterate through table make sure that values were inserted.
  // SELECT * FROM empty_table2;
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  // test by cdz
  ExecutionEngine* engine = GetExecutionEngine();
  AbstractPlanNode* plan = &scan_plan;
  Transaction* txn = GetTxn();
  ExecutorContext* ctx = GetExecutorContext();
  engine ->Execute(plan,&result_set,txn,ctx);
  // GetExecutionEngine()->Execute(&scan_plan, &result_set, GetTxn(), GetExecutorContext());

  std::cout << "ColA, ColB" << std::endl;
  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 100);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 10);
  std::cout << result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
            << result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 101);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 11);
  std::cout << result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
            << result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 102);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 12);
  std::cout << result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
            << result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  // Size
  ASSERT_EQ(result_set.size(), 3);
  std::vector<RID> rids;

  // Get RID from index, fetch tuple and then compare
  for (auto &table_tuple : result_set) {
    rids.clear();
    // test by cdz-> table_tuple : {allocated_ = true, rid_ = {page_id_ = 10, slot_num_ = 0}, size_ = 8, data_ = 0x602000008830 "d"}
    auto index_key = table_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    // test by cdz-> index_key : {allocated_ = true, rid_ = {page_id_ = -1, slot_num_ = 0}, size_ = 8, data_ = 0x602000008870 "d"}
    index_info->index_->ScanKey(index_key, &rids, GetTxn());
    Tuple indexed_tuple;
    auto fetch_tuple = table_info->table_->GetTuple(rids[0], &indexed_tuple, GetTxn());

    ASSERT_TRUE(fetch_tuple);
    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(),
              table_tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>());
    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(),
              table_tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>());

    std::cout << indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() << ", "
              << indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() << std::endl;
  }
  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(ExecutorTest, SimpleUpdateTest) {
  // INSERT INTO empty_table2 SELECT colA, colA FROM test_1 WHERE colA < 50   // 注意是两个colA
  // UPDATE empty_table2 SET colA = colA+10 WHERE colA < 50
  //////////////////////////////////////// select insert (into empty_table2)
  std::unique_ptr<AbstractPlanNode> scan_plan1;
  const Schema *out_schema1;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
    auto predicate = MakeComparisonExpression(colA, const50, ComparisonType::LessThan);
    out_schema1 = MakeOutputSchema({{"colA", colA}, {"colA", colA}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, predicate, table_info->oid_);
  }
  std::unique_ptr<AbstractPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }
  // INSERT INTO empty_table2 SELECT colA, colA FROM test_1 WHERE colA < 50
  std::vector<Tuple> insert_result_set;
  GetExecutionEngine()->Execute(insert_plan.get(), &insert_result_set, GetTxn(), GetExecutorContext());
  // test by cdz
  // {
  //   auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  //   for(size_t i=0;i<insert_result_set.size();i++){
  //     auto tuple = insert_result_set[i];
  //     auto col0 = tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
  //     auto col1 = tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
  //     std::cout<<col0<<","<<col1<<std::endl;
  //   }
  //   std::cout<<"\n\n"<<std::endl;
  // }
  // end test by cdz

  ////////////////////////////////////////  Create Indexes and scan 
  // Create Indexes for col1 and col2 on table empty_table2
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  Schema *key_schema = ParseCreateStatement("a int");
  GenericComparator<8> comparator(key_schema);
  [[maybe_unused]] auto index_info_1 = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "empty_table2", GetExecutorContext()->GetCatalog()->GetTable("empty_table2")->schema_,
      *key_schema, {0}, 8);     // 第一列上的索引
  [[maybe_unused]] auto index_info_2 = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index2", "empty_table2", GetExecutorContext()->GetCatalog()->GetTable("empty_table2")->schema_,
      *key_schema, {1}, 8);     // 第二列上的索引

  std::unique_ptr<AbstractPlanNode> scan_plan2;
  const Schema *out_schema2;
  {
    out_schema2 = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
    // scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
    scan_plan2 = std::make_unique<IndexScanPlanNode>(out_schema2, nullptr, index_info_1->index_oid_);
  }

  std::vector<Tuple> result_set2;
  GetExecutionEngine()->Execute(scan_plan2.get(), &result_set2, GetTxn(), GetExecutorContext());
  // test by cdz
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    for(size_t i=0;i<result_set2.size();i++){
      auto orig_tuple = insert_result_set[i];
      auto orig_col0 = orig_tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto orig_col1 = orig_tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      auto tuple = result_set2[i];
      auto col0 = tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto col1 = tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      ASSERT_EQ(orig_col0,col0);
      ASSERT_EQ(orig_col1,col1);
      // std::cout<<col0<<","<<col1<<std::endl;
    }
    // std::cout<<"total size:"<<result_set2.size()<<"\n\n"<<std::endl;
  }
  // end test by cdz


  ////////////////////////////////////////  update
  // Construct query plan
  auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
  auto predicate = MakeComparisonExpression(colA, const50, ComparisonType::LessThan);
  auto out_empty_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  auto scan_empty_plan = std::make_unique<SeqScanPlanNode>(out_empty_schema, predicate, table_info->oid_);
  std::unordered_map<uint32_t, UpdateInfo> update_attrs;
  update_attrs.insert(std::make_pair(0, UpdateInfo(UpdateType::Add, 10))); // update_plan 要更新的属性,以及如何更新(Add) => 第0列属性(colA)+10
  // update_attrs.insert(std::make_pair(1, UpdateInfo(UpdateType::Add, 10)));   // test by cdz
  std::unique_ptr<AbstractPlanNode> update_plan;
  // scan_empty_plan 是 UpdatePlanNode 的子结点,用于获取要更新的数据...
  { update_plan = std::make_unique<UpdatePlanNode>(scan_empty_plan.get(), table_info->oid_, update_attrs); }
  // 更新...
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(update_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  // test by cdz
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    for(size_t i=0;i<result_set.size();i++){
      auto orig_tuple = insert_result_set[i];
      auto orig_col0 = orig_tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto orig_col1 = orig_tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      auto tuple = result_set[i];
      auto col0 = tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto col1 = tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      ASSERT_EQ(orig_col0+10,col0);
      ASSERT_EQ(orig_col1,col1);
      // std::cout<<col0<<","<<col1<<std::endl;
    }
    // std::cout<<"total size:"<<result_set.size()<<"\n\n"<<std::endl;
  }
  // end test by cdz


  ////////////////////////////////////////  check updated tuples
  std::unique_ptr<AbstractPlanNode> check_plan_seq;
  check_plan_seq = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  std::vector<Tuple> check_set;
  GetExecutionEngine()->Execute(check_plan_seq.get(), &check_set, GetTxn(), GetExecutorContext());
  // test by cdz
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    for(size_t i=0;i<check_set.size();i++){
      auto orig_tuple = insert_result_set[i];
      auto orig_col0 = orig_tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto orig_col1 = orig_tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      auto tuple = check_set[i];
      auto col0 = tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto col1 = tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      ASSERT_EQ(orig_col0+10,col0);
      ASSERT_EQ(orig_col1,col1);
      // std::cout<<col0<<","<<col1<<std::endl;
    }
    std::cout<<"seq_scan_check_set size:"<<check_set.size()<<"\n\n"<<std::endl;
  }
  // end test by cdz

  ////////////////////////////////////////  check updated index
  std::unique_ptr<AbstractPlanNode> check_plan_idx;
  // check_plan_idx = std::make_unique<IndexScanPlanNode>(out_schema2, nullptr, index_info_1->index_oid_);
  check_plan_idx = std::make_unique<IndexScanPlanNode>(out_schema2, nullptr, index_info_2->index_oid_);
  check_set.clear();
  GetExecutionEngine()->Execute(check_plan_idx.get(), &check_set, GetTxn(), GetExecutorContext());
  // test by cdz
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    for(size_t i=0;i<check_set.size();i++){
      // auto orig_tuple = insert_result_set[i];
      // auto orig_col0 = orig_tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      // auto orig_col1 = orig_tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      auto tuple = check_set[i];
      auto col0 = tuple.GetValue(&(table_info->schema_), 0).GetAs<uint32_t>();
      auto col1 = tuple.GetValue(&(table_info->schema_), 1).GetAs<uint32_t>();
      // ASSERT_EQ(orig_col0+10,col0);
      // ASSERT_EQ(orig_col1,col1);
      std::cout<<col0<<","<<col1<<std::endl;
    }
    std::cout<<"index_scan_check_set size:"<<check_set.size()<<"\n\n"<<std::endl;
  }
  // end test by cdz

  // std::vector<RID> rids;
  // for (int32_t i = 0; i < 50; ++i) {
  //   Tuple key = Tuple({Value(TypeId::INTEGER, i)}, key_schema);
  //   index_info_2->index_->ScanKey(key, &rids, GetTxn());    // 找到key 对应的 rid
  //   // index_info_1->index_->ScanKey(key, &rids, GetTxn());    // 找到key 对应的 rid
  //   Tuple indexed_tuple;
  //   auto fetch_tuple = table_info->table_->GetTuple(rids[0], &indexed_tuple, GetTxn()); // 读取 rids[0]对应的tuple
  //   ASSERT_TRUE(fetch_tuple);
  //   auto cola_val = indexed_tuple.GetValue(&schema, 0).GetAs<uint32_t>();
  //   auto colb_val = indexed_tuple.GetValue(&schema, 1).GetAs<uint32_t>();
  //   std::cout<<cola_val<<","<<colb_val<<std::endl;
  //   // ASSERT_TRUE(cola_val == colb_val + 10);
  // }
  delete key_schema;
}


// NOLINTNEXTLINE
TEST_F(ExecutorTest, SimpleDeleteTest) {
  // SELECT colA FROM test_1 WHERE colA == 50
  // DELETE FROM test_1 WHERE colA == 50
  // SELECT colA FROM test_1 WHERE colA == 50

  // Construct query plan
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
  auto predicate = MakeComparisonExpression(colA, const50, ComparisonType::Equal);
  auto out_schema1 = MakeOutputSchema({{"colA", colA}});
  auto scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, predicate, table_info->oid_);
  // index
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "test_1", GetExecutorContext()->GetCatalog()->GetTable("test_1")->schema_, *key_schema, {0},
      8);

  // Execute : SELECT colA FROM test_1 WHERE colA == 50
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify
  std::cout << "colA" << std::endl;
  for (const auto &tuple : result_set) {
    std::cout << tuple.GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>() << std::endl;
    ASSERT_TRUE(tuple.GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>() == 50);
  }
  ASSERT_EQ(result_set.size(), 1);
  Tuple index_key = Tuple(result_set[0]);

  // scan_plan1 是 delete_plan 的 child PlanNode; child PlanNode 用于获取当前 plan 需要的数据...
  // Execute: DELETE FROM test_1 WHERE colA == 50
  std::unique_ptr<AbstractPlanNode> delete_plan;
  { delete_plan = std::make_unique<DeletePlanNode>(scan_plan1.get(), table_info->oid_); }
  GetExecutionEngine()->Execute(delete_plan.get(), nullptr, GetTxn(), GetExecutorContext());

  // Execute:  SELECT colA FROM test_1 WHERE colA == 50
  result_set.clear();
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_TRUE(result_set.empty());

  std::vector<RID> rids;

  // 注: index_key的size 超过了 B+树设置的KeySize, 部分情况下可能出问题...
  index_info->index_->ScanKey(index_key, &rids, GetTxn());
  ASSERT_TRUE(rids.empty());

  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(ExecutorTest, DISABLED_SimpleNestedLoopJoinTest) {
  // SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col3 FROM test_1 JOIN test_2 ON test_1.colA = test_2.col1
  std::unique_ptr<AbstractPlanNode> scan_plan1;
  const Schema *out_schema1;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto colB = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  const Schema *out_schema2;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_2");
    auto &schema = table_info->schema_;
    auto col1 = MakeColumnValueExpression(schema, 0, "col1");
    auto col3 = MakeColumnValueExpression(schema, 0, "col3");
    out_schema2 = MakeOutputSchema({{"col1", col1}, {"col3", col3}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan;
  const Schema *out_final;
  {
    // colA and colB have a tuple index of 0 because they are the left side of the join
    auto colA = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto colB = MakeColumnValueExpression(*out_schema1, 0, "colB");
    // col1 and col2 have a tuple index of 1 because they are the right side of the join
    auto col1 = MakeColumnValueExpression(*out_schema2, 1, "col1");
    auto col3 = MakeColumnValueExpression(*out_schema2, 1, "col3");
    auto predicate = MakeComparisonExpression(colA, col1, ComparisonType::Equal);
    out_final = MakeOutputSchema({{"colA", colA}, {"colB", colB}, {"col1", col1}, {"col3", col3}});
    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_final, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), 100);
  std::cout << "ColA, ColB, Col1, Col3" << std::endl;
  for (const auto &tuple : result_set) {
    std::cout << tuple.GetValue(out_final, out_final->GetColIdx("colA")).GetAs<int32_t>() << ", "
              << tuple.GetValue(out_final, out_final->GetColIdx("colB")).GetAs<int32_t>() << ", "
              << tuple.GetValue(out_final, out_final->GetColIdx("col1")).GetAs<int16_t>() << ", "
              << tuple.GetValue(out_final, out_final->GetColIdx("col3")).GetAs<int32_t>() << ", " << std::endl;
  }
}

// NOLINTNEXTLINE
TEST_F(ExecutorTest, DISABLED_SimpleAggregationTest) {
  // SELECT COUNT(colA), SUM(colA), min(colA), max(colA) from test_1;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  const Schema *scan_schema;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    scan_schema = MakeOutputSchema({{"colA", colA}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  std::unique_ptr<AbstractPlanNode> agg_plan;
  const Schema *agg_schema;
  {
    const AbstractExpression *colA = MakeColumnValueExpression(*scan_schema, 0, "colA");
    const AbstractExpression *countA = MakeAggregateValueExpression(false, 0);
    const AbstractExpression *sumA = MakeAggregateValueExpression(false, 1);
    const AbstractExpression *minA = MakeAggregateValueExpression(false, 2);
    const AbstractExpression *maxA = MakeAggregateValueExpression(false, 3);

    agg_schema = MakeOutputSchema({{"countA", countA}, {"sumA", sumA}, {"minA", minA}, {"maxA", maxA}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{colA, colA, colA, colA},
        std::vector<AggregationType>{AggregationType::CountAggregate, AggregationType::SumAggregate,
                                     AggregationType::MinAggregate, AggregationType::MaxAggregate});
  }
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  auto countA_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>();
  auto sumA_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("sumA")).GetAs<int32_t>();
  auto minA_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("minA")).GetAs<int32_t>();
  auto maxA_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("maxA")).GetAs<int32_t>();
  // Should count all tuples
  ASSERT_EQ(countA_val, TEST1_SIZE);
  // Should sum from 0 to TEST1_SIZE
  ASSERT_EQ(sumA_val, TEST1_SIZE * (TEST1_SIZE - 1) / 2);
  // Minimum should be 0
  ASSERT_EQ(minA_val, 0);
  // Maximum should be TEST1_SIZE - 1
  ASSERT_EQ(maxA_val, TEST1_SIZE - 1);
  std::cout << countA_val << std::endl;
  std::cout << sumA_val << std::endl;
  std::cout << minA_val << std::endl;
  std::cout << maxA_val << std::endl;
  ASSERT_EQ(result_set.size(), 1);
}

// NOLINTNEXTLINE
TEST_F(ExecutorTest, DISABLED_SimpleGroupByAggregation) {
  // SELECT count(colA), colB, sum(colC) FROM test_1 Group By colB HAVING count(colA) > 100
  std::unique_ptr<AbstractPlanNode> scan_plan;
  const Schema *scan_schema;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto colB = MakeColumnValueExpression(schema, 0, "colB");
    auto colC = MakeColumnValueExpression(schema, 0, "colC");
    scan_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}, {"colC", colC}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  std::unique_ptr<AbstractPlanNode> agg_plan;
  const Schema *agg_schema;
  {
    const AbstractExpression *colA = MakeColumnValueExpression(*scan_schema, 0, "colA");
    const AbstractExpression *colB = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *colC = MakeColumnValueExpression(*scan_schema, 0, "colC");
    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{colB};
    const AbstractExpression *groupbyB = MakeAggregateValueExpression(true, 0);
    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{colA, colC};
    std::vector<AggregationType> agg_types{AggregationType::CountAggregate, AggregationType::SumAggregate};
    const AbstractExpression *countA = MakeAggregateValueExpression(false, 0);
    const AbstractExpression *sumC = MakeAggregateValueExpression(false, 1);
    // Make having clause
    const AbstractExpression *having = MakeComparisonExpression(
        countA, MakeConstantValueExpression(ValueFactory::GetIntegerValue(100)), ComparisonType::GreaterThan);

    // Create plan
    agg_schema = MakeOutputSchema({{"countA", countA}, {"colB", groupbyB}, {"sumC", sumC}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), having, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  std::unordered_set<int32_t> encountered;
  std::cout << "countA, colB, sumC" << std::endl;
  for (const auto &tuple : result_set) {
    // Should have countA > 100
    ASSERT_GT(tuple.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>(), 100);
    // Should have unique colBs.
    auto colB = tuple.GetValue(agg_schema, agg_schema->GetColIdx("colB")).GetAs<int32_t>();
    ASSERT_EQ(encountered.count(colB), 0);
    encountered.insert(colB);
    // Sanity check: ColB should also be within [0, 10).
    ASSERT_TRUE(0 <= colB && colB < 10);

    std::cout << tuple.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>() << ", "
              << tuple.GetValue(agg_schema, agg_schema->GetColIdx("colB")).GetAs<int32_t>() << ", "
              << tuple.GetValue(agg_schema, agg_schema->GetColIdx("sumC")).GetAs<int32_t>() << std::endl;
  }
}

}  // namespace bustub
