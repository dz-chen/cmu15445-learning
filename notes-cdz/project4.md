[toc]

# 实现细节注意
## lock manager
### 官方建议/重点
- The basic idea of a LM is that it maintains an internal data structure about the locks currently held by active transactions
- The `TableHeap` and `Executor` classes will use your LM to acquire locks on tuple records (by record id RID) whenever a transaction wants to access/modify a tuple => record级别的锁
- This task requires you to implement a tuple-level LM that supports the three common isolation levels : `READ_UNCOMMITED, READ_COMMITTED, and REPEATABLE_READ` => 重点关注lecture18的事务隔离性
- Any failed lock operation should lead to an `ABORTED` transaction state (implicit abort) and throw an exception. The transaction manager would further catch this exception and rollback write operations executed by the transaction.=> 必须abort;  
- The only file you need to modify for this task is the LockManager class (concurrency/`lock_manager.cpp` and `concurrency/lock_manager.h`),此外`transaction.h`是很重要的参考.  
- You will need some way to keep track of which transactions are waiting on a lock. Take a look at `LockRequestQueue` class in `lock_manager.h`.  
- You will need some way to notify transactions that are waiting when they may be up to grab the lock. We recommend using `std::condition_variable`.  
- `Although some isolation levels are achieved by ensuring the properties of strict two phase locking, your implementation of lock manager is only required to ensure properties of two phase locking. The concept of strict 2PL will be achieved through the logic in your executors and transaction manager`. Take a look at `Commit` and `Abort` methods there. => 重点理解,todo 
- You should also `maintain state of transaction`. For example, the states of transaction may be changed from GROWING phase to SHRINKING phase due to unlock operation (Hint: Look at the methods in transaction.h)  
- You should also `keep track of the shared/exclusive lock acquired by a transaction` using `shared_lock_set_`  and `exclusive_lock_set_` so that when the TransactionManager wants to commit/abort a transaction, the LM can release them properly.  
- `Setting a transaction's state to ABORTED implicitly aborts it, but it is not explicitly aborted` until TransactionManager::Abort is called. You should read through this function to understand what it does, and how your lock manager is used in the abort process.

### Transaction的实现(重点)
**事务状态**  
```
/**
 * Transaction states for 2PL:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 * Transaction states for Non-2PL:
 *     __________
 *    |          v
 * GROWING  -> COMMITTED     ABORTED
 *    |_________________________^
 *
 **/
```

**transaction类的成员理解(重点)**  
```
class Transaction {
 // ......
 private:
  /** The current transaction state. */
  TransactionState state_;
  /** The isolation level of the transaction. */
  IsolationLevel isolation_level_;
  /** The thread ID, used in single-threaded transactions. */
  std::thread::id thread_id_;
  /** The ID of this transaction. */
  txn_id_t txn_id_;

  /** The undo set of table tuples. */
  std::shared_ptr<std::deque<TableWriteRecord>> table_write_set_;
  /** The undo set of indexes. */
  std::shared_ptr<std::deque<IndexWriteRecord>> index_write_set_;
  /** The LSN of the last record written by the transaction. */
  lsn_t prev_lsn_;

  /** Concurrent index: the pages that were latched during index operation. 已加latch的Page! */
  std::shared_ptr<std::deque<Page *>> page_set_;
  /** Concurrent index: the page IDs that were deleted during index operation.*/
  std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;

  /** LockManager: the set of shared-locked tuples held by this transaction. */
  std::shared_ptr<std::unordered_set<RID>> shared_lock_set_;
  /** LockManager: the set of exclusive-locked tuples held by this transaction. */
  std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set_;
};
```




### LockManager的实现

### 隔离级别的实现(非常重要)
重点参考维基百科:[事务隔离](https://zh.m.wikipedia.org/zh-hans/%E4%BA%8B%E5%8B%99%E9%9A%94%E9%9B%A2)  
**1.基础知识点**  
`事务隔离`（英语：Transaction Isolation）定义了数据库系统中一个事务中操作的结果在何时以何种方式对其他并发事务操作可见;  
`并发控制`描述了数据库`事务隔离`以保证数据正确性的机制;  
`两阶段锁(2PL)`是关系数据库中最常见的提供了`可序列化`(即保证可串行调度)和`可恢复性`（英语：recoverability）的并发控制机制;  
并发控制,2PL,隔离性是紧密相关的!!  

**2.脏读,不可重复读,幻读**  
ANSI/ISO SQL 92标准描述了三种不同的`一个事务读取另外一个事务可能修改的数据的“读现象”`.  
注意,此处只是讨论读现象,实际上并发事务/隔离性涉及四个问题:脏读,不可重复读,幻读,以及`脏写`(详见cmu15445 lecture16);  
`脏读`:T1读取T2修改但未提交的数据 => 问题:如果T2回滚则T1读取的数据便是不正确的;  
`不可重复读`:事务T两次读取同一行数据 => 问题:如果在T两次读取之间,T2修改了该行数据,则T两次读取的数据不一致;  
`幻读`:事务T两次读取同一部分数据 => 问题:如果在T两次读取之间,T2插入/删除了该部分数据的某行,则T两次读取的数据不一致;  
 
**3. 2PL中隔离级别的实现**  
四个事务隔离级别:`读未提交,读已提交,可重复读,可串行化`.  
三个读现象:`脏读,不可重复读,幻读`;  
|隔离级别	  | 脏读	  | 不可重复读	|幻读 |
| -------- | -------| ---------  | ---- | 
| 读未提交	|     √  |   √       | √   |
|	读已提交  |    -	 |   √       |  √   |
| 可重复读	|    -	 |   -       |  √   |
| 可串行化  |   -	   |    -      | 	-   |
`2PL保证了可串行化调度(最高的隔离级别)`(注:这里的2PL指SS2PL,slide17中普通的2PL仍然可能导致脏读!!);  
但是`2PL会限制并发的性能,很多数据库系统中,事务大都避免高等级的隔离级别从而减少锁的开销`;  
`在2PL协议的实现过程中,特别判断事务的隔离级别,低级别隔离性不会严格执行2PL,从而对隔离性做一定的松绑,获得更多的读/写范围`;  
- `可串行化`:在选定`对象上的读锁和写锁直到事务结束后才能释放`。在SELECT的查询中使用一个WHERE子句来描述一个范围时应该获得一个`范围锁`(range-locks).这种机制可以避免“幻影读”现象.  
- `可重复读`:对`选定对象的读锁和写锁一直保持到事务结束`,但`不要求"范围锁"`,因此可能会发生“幻影读”;  
- `读已提交`:选定对象的`写锁一直保持到事务结束`,但是`读锁在SELECT操作完成后马上释放`,`不要求范围锁`,可能发生"不可重复读";  
- `读未提交`:允许"脏读"(dirty read),`只有写锁,没有读锁`,写锁无法阻塞其他事务读数据,其他事务能看到“尚未提交”的修改.  


**4.隔离级别与读现象的理解 & 疑问解答**  
[在隔离级别为读未提交的情况下，为什么一个事务在修改数据时已经加了写锁，还是会被另一个事务读取到未提交的数据?](https://blog.csdn.net/qq_42961707/article/details/125967197)  
`读未提交`  
如何保证T1能读到未提交的数据? => 因为T2没有加读锁(T2加写锁本质是阻止T1施加读锁,而不能直接禁止T1读数据)  
为何T1还可能出现脏读? => 因为读取的数据可能是T2未提交的,若T2回滚,则T1读的是脏数据
`读已提交`  
如何保证T1读的是已经提交的数据? => 因为T2的写锁是在事务结束时才释放,rid上有写锁,则T1不能读...  
为何T1还可能出现不可重复读? => 因为T1事务的生命周期内,一旦释放读锁,T2可能修改之前的内容  
`可重复读`  
如何保证T1可重复读?  => 因为T1的读写锁都是在事务结束后才释放,T2无法修改T1事务内的数据,因此T1重复读的数据是一致的  
为何还可能出现幻读? => 因为没有范围锁,T1的读锁只能保证读的数据不被修改,而无法阻止T2插入新的数据...  
更多内容理解见 lock_manager.cpp中代码...  



## Deadlock Detection

## Concurrent Query Execution
### TransactionManager的实现

# 知识点积累
## 理解条件变量(重要)
如果说`互斥锁是用于同步线程对共享数据的访问`的话,那么`条件变量则是用于在线程之间同步共享数据的值`;  
当某个共享数据达到某个值的时候,唤醒等待这个共享数据的线程;  

## std::condition_variable 只可与 std::unique_lock<std::mutex>一同使用
https://blog.csdn.net/princeteng/article/details/103945610  

## std::condition_variable wait的两种用法
官方文档:https://www.cplusplus.com/reference/condition_variable/condition_variable/wait/  

## 条件变量配合while使用的必要性(todo)
是否存在一种情况:某个线程while检查过程中,另一个线程修改了共享变量?
https://www.cnblogs.com/lxy-xf/p/11172912.html

## 2PL一个事务在一个对象上始终只有一把锁
在同一个对象rid上,同一个事务txn最多只有一把锁,不存在先请求一个S,再请求一个X的情况 => 这样应该使用 upgrading,锁升级为X锁,但锁的数量还是一把;  
`2PL的growing阶段是在不同对象上不断获得锁的过程`...  

## 2PL多个事务在同一个对象上最多只有一个X锁

## 每个LockManger启动时会自动启动cycleDectection的线程
详见 LockManager 类的构造函数...  

## LockManager中只需要实现2PL,SS2PL的实现依赖于executor以及transactionManager(重要,未理解)
Although some isolation levels are achieved by ensuring the properties of strict two phase locking, your implementation of lock manager is only required to ensure properties of two phase locking. The concept of strict 2PL will be achieved through the logic in your executors and transaction manager. Take a look at `Commit` and `Abort` methods there.

## 为何可重复读会有幻读,读已提交会有不可重复读...(TODO)




# TODO
## 2PL从growing阶段转换到shrinking阶段的时刻是什么时候?
## TableWriteRecord与IndexWriteRecord的作用?
## transcation为何在commit时才执行delete(transaction_manager.cpp)?
## condition_variable使用时传入的mutex的作用?
## LockRequestQueue.request_queue_代表的是什么?(重要,fixed)
存放两类txn: `未获得锁而阻塞的txn` 以及  `获得锁但是尚未释放的txn` !  
事务释放锁时将从队列中移除.  

## LockRequestQueue.request_queue_中txn获得锁的时间是与队列顺序有关吗?
个人理解:对于`队列中被阻塞而尚未获得锁的txn线程而言,它们应该是竞争关系,竞争获胜者获得锁`;  
## lockManager中读lock_table_时是否有必要上latch_?
## transactionManager 的 Abort()函数理解(重要,fixed)
三件事:  
1.恢复(撤销)所有对表的操作;  
2.恢复(撤销)所有对索引的操作;  
3.释放所有已经持有的锁;  
`这里的恢复是使用事务对象自身的临时数据(内存),而不是用日志`(这个理解是否正确?)  

## transactionManager的 Commit()函数理解(重要)
不理解为何delete 要放到commit中???? TODO


# tmp
## 复制粘贴常用
```
// cmake 生成makefile(debug版)
cmake -DCMAKE_BUILD_TYPE=DEBUG ..

// executor_test 执行及调试
make -j4 lock_manager_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/lock_manager_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/lock_manager_test --gtest_filter=LockManagerTest.TwoPLTest
b lock_manager_test.cpp:123


// executor_test 执行及调试
make -j4 grading_lock_manager_2test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/grading_lock_manager_1test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/grading_lock_manager_1test --gtest_filter=LockManagerTest.TwoPLTest
b lock_manager_test.cpp:123
```
