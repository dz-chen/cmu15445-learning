[toc]

# 实现细节注意
## SYSTEM CATALOG
- 
## b


# 知识点积累


# TODO
## Catalog.CreateTable()时,如何解决TableHeap的初始化问题?
## Catalog.CreateIndex()时,Index是抽象类,如何创建IndexInfo的实例?




# tmp
## 复制粘贴常用
```
// cmake 生成makefile(debug版)
cmake -DCMAKE_BUILD_TYPE=DEBUG ..

// b_plus_tree_print_test 执行及调试
make -j4 catalog_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/catalog_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/catalog_test --gtest_filter=CatalogTest.CreateTableTest
b catalog_test.cpp:68
```