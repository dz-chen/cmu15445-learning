[toc]

# 实现细节注意
## SYSTEM CATALOG
- 
## executor
- executorEngine,executorContext,executorFactory,plan,executor的关系
```
executor => plan => expression
```
- 每个`XxxPlanNode`最重要的就是其中的`chidren`以及`predicate_`.
```
1.chidren 用于获取当前plan需要的输入数据(比如对于join,就需要两个child,一个返回左表的数据,一个返回右表的数据);
2.predicate_代表XxxPlanNode返回的数据需要满足的条件(即SQL的where子句),predicate_是一颗表达式树;
```
- 满足`XxxPlanNode->predicate_`的数据,才会通过`XxxExecutor->Next()`返回;
- `XxxExecutor->Next()应该是一个递归的调用`,当前层Next()的数据,依赖于`XxxPlanNode->children->predicate_`返回的数据;
- Each executor is responsible for processing a single plan node type. They accept tuples from their children and give tuples to their parent. They may rely on conventions about the ordering of their children.
- We also assume executors are single-threaded throughout the entire project.

## expression
- ColumnValueExpression->Evaluate(tuple,schema),求值即`返回tuple中某个属性的值`(该属性的col_index是ColumnValueExpression的成员变量!)
- 

# 知识点积累

# TODO
## Catalog.CreateTable()时,如何解决TableHeap的初始化问题(fixed)?
见代码...

## Catalog.CreateIndex()时,Index是抽象类,如何创建IndexInfo的实例(fixed)?
见代码...

## TableHeap源码待学习(TODO)

## executor的children_,plan_的children_,expression的children_有何区别与联系(TODO)?

## index_scan_executor中IndexScanBPTreeIdxType修改(TODO)?

## 在部分属性列上存在取值相同的可能,因此B+树应该支持一个key对应多个val,待修改(TODO)?

## b_plus_tree_index源码待学习(TODO)

## update_executor时,索引更新部分未调试成功(TODO)?


# tmp
## 复制粘贴常用
```
// cmake 生成makefile(debug版)
cmake -DCMAKE_BUILD_TYPE=DEBUG ..

// catalog_test 执行及调试
make -j4 catalog_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/catalog_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/catalog_test --gtest_filter=CatalogTest.CreateTableTest
b catalog_test.cpp:68


// executor_test 执行及调试
make -j4 executor_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/executor_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/executor_test --gtest_filter=ExecutorTest.SimpleUpdateTest
b executor_test.cpp:621
```