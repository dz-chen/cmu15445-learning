[toc]

# 实现细节注意
## B+树数据结构
- 从测试代码递归往下看,更容易理解;  
- bustub中B+树的叶子结点中的val只是RecordId,是`非聚簇索引`;  
- bustub不支持重复键;  
- 在bustub中,对于内部结点,第一个key是落单的;对于叶子结点,所有kv都是成对的;详见b_plus_tree_internal_page.h、详见b_plus_tree_leaf_page.h;  
- 内部结点的每个key,都对应了一个叶子结点中的最小值;
- 内部结点的第一个kv对中只存储了val,没有key,`但本质上第一个kv对的key是父结点中指向当前内部结点的kv对中的那个key`;
- b_plus_tree_page的`GetSize()函数,返回的是除PageHdr外的所有kv对个数`,对于内部结点,第一个key无效<=>对于叶子结点,第一个key有效;  
- 为方便B+树的插入,b_plus_tree_page的`GetMaxSize()函数应该返回page实际可存储的KV对数-1`,从而能够先插入,再分裂,方便编码;  
- B+树叶子结点和内部结点分裂时,到底多少个kv对(或者说key)留在原结点,多少个kv对放到右边兄弟结点,似乎说法不一(主要是相差1) => 本人做法如下:
```
对于叶子结点,假设分裂前有n个kv对(有效key有n个)       => 前(n+1)/2个kv对保留在原结点,剩余的放在右兄弟结点;即保证原结点的kv对不少于右兄弟.
对于内部结点,假设分裂前有n个kv对(有效key只有n-1个)   => 有效key里边的中间那个key需放到父结点,剩余的才是原结点与右兄弟分,同时也要保证原结点的key不少于右兄弟;
```
- 判断B+树是否满足最少key、val时,本文的规则为:
```
当size < (max_size_+1)/2 时,则需要借取或者合并
```
- 删除时涉及判断兄弟结点的问题,需注意不管内部结点还是叶子结点,`不是同一个父结点不能算是兄弟`;  

## B+树latch
- 上锁过程按照slides09中最原始的方式进行,即:
```
Find:Start at root and go down; repeatedly,
    => Acquire R latch on child,Then unlatch parent(no need to check safe)

Insert/Delete: Start at root and go down,obtaining W latches as needed. Once child is latched, check if it is safe:
    => If child is safe, release all latches on ancestors.(safe means:won't split for insert,won't merge for delete)
```
- `只能对Page进行latch`,不能对BPlusTreePage进行latch;  
- B+树并发控制中transaction的作用:  
```
1.store the page on which you have acquired latch while traversing through B+ tree;
2.store the page which you have deleted during Remove operation;
注意:都是在当前事务中的,操作结束需要清空.
```
- 对于INSERT导致分裂、DELETE导致合并的情况,因为FindLeafPage()时已经保证了在某个合适的祖先结点已经latch,故:`无需latch分裂出的新结点,合并后的结点如未latch也无需latch`;
- 部分结点可能已经latch,但是后续因为DELETE合并而被删除,最终释放latch时注意将结点对应的Page一并删除;
- 


# 知识点积累
## ReaderWriterLatch的实现(include/common/rwlatch.h) TODO
## Lock crabbing protocol
参考:[Lock crabbing protocol - how to make indexes concurrent](https://duynguyen-ori75.github.io/lock-crabbing/)

# TODO
## project1中lru、buffer_pool_manager需重新检查(关于pin)
**问题描述**  
buffer_pool_manager中的 page_table中存放的映射是否能在lru中,目前比较混乱,导致目前关于pin的时刻很不清晰,需重新界定...=> 目前个人的想法是:page_table应该包含lru中的,这样fetch时遇到lru中的page,可以减少一次换出!  

## b_plus_tree.cpp中template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>的作用?

## B+树的度/阶是指key的个数,还是val的个数?
暂时认为是val的个数  
TODO:...待确认...  

## Index_iterator中遍历的过程需要加锁吗?
暂时未加  
TODO...  

## B+树中结点内部的record查找全部改为二分
TODO...

## 进一步调试(重要)
虽然通过了课程提供的简单测试,但实际上bug极多,特比是与buff_pool_manager_交互的部分,还需进一步debug...



# tmp
## 复制粘贴常用
```
// cmake 生成makefile(debug版)
cmake -DCMAKE_BUILD_TYPE=DEBUG ..

// b_plus_tree_print_test 执行及调试
make -j4 b_plus_tree_print_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/b_plus_tree_print_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/b_plus_tree_print_test --gtest_filter=BptTreeTest.UnitTest
b b_plus_tree_print_test.cpp:68

// b_plus_tree_insert_test 执行及调试
make -j4 b_plus_tree_insert_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/b_plus_tree_insert_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/b_plus_tree_insert_test --gtest_filter=BPlusTreeTests.InsertTest2
b b_plus_tree_insert_test.cpp:18


// b_plus_tree_insert_test 执行及调试
make -j4 b_plus_tree_delete_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/b_plus_tree_delete_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/b_plus_tree_delete_test --gtest_filter=BPlusTreeTests.DeleteTest1
b b_plus_tree_delete_test.cpp:18


// b_plus_tree_concurrent_test 执行及调试
make -j4 b_plus_tree_concurrent_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/b_plus_tree_concurrent_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/b_plus_tree_concurrent_test --gtest_filter=BPlusTreeConcurrentTest.InsertTest2
b b_plus_tree_concurrent_test.cpp:98
```