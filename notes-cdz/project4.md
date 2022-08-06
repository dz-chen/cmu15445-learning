[toc]

# 实现细节注意
## lock manager
- The basic idea of a LM is that it maintains an internal data structure about the locks currently held by active transactions
- The `TableHeap` and `Executor` classes will use your LM to acquire locks on tuple records (by record id RID) whenever a transaction wants to access/modify a tuple
- This task requires you to implement a tuple-level LM that supports the three common isolation levels : `READ_UNCOMMITED, READ_COMMITTED, and REPEATABLE_READ` => 重点关注lecture18的事务隔离性
- 

# 知识点积累

# TODO


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