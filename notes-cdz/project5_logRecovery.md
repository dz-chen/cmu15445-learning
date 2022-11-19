[toc]
注:此为2019年的作业要求(非本2020年的作业要求),由个人补充完成


# 实现细节注意
## LOG MANAGER
### 官方建议/重点
- The `TablePage class will explicitly create a log record (before any update operations) and invoke the AppendLogRecord method` of LogManager to `write it into log buffer` when the global variable enable_logging (include/common/config.h) flag is set to be true
- There is a ReaderWriterLatch instance in the TransactionManager called `global_txn_latch_`. This latch is used in the two helper functions for stopping the execution of transactions (`BlockAllTransactions, ResumeTransactions`).
- There is a global variable called `enable_logging` within include/common/config.h
- `for every log_timeout seconds, your LogManager implementation needs to execute a flush operation`(按时刷新数据)
- Disk Manager:The `DiskManager` creates the database file as well as the log file for its constructor. We have also `provided separate helper functions WriteLog and ReadLog to support accessing the log file`.
- HashTableHeaderPage: There is a member variable `lsn_` to record the page's log sequence number. You do not need to update lsn_ within your index implementation, we only test logging and recovery functionalities on table page level.
- Transaction:The Transaction object has the GetPrevLSN and SetPrevLSN helper methods. `Each transaction is responsible for maintaining the previous log sequence number to be used in the undo phase`(详见代码prev_lsn_成员,用于undo)
__________________ 以下是task1需要做的工作
- `RunFlushThread`:需要开启后台线程刷新日志缓冲区的数据到磁盘.维护两个日志缓冲区`flush_buffer`和`log_buffer`.刷新日志/交换buffer的三个时机:`log_timeout时间到`、`log_buffer满`,`缓冲区满导致有page被驱逐时`;  
- `LogManager`:需要实现 `group commit`.(使用了group commit,每次commit时则不能强制刷新日志,比如等待log_timeout或者buffer pool manager刷新日志) 
- `BufferPoolManager`: In your BufferPoolManager, when a new page is created, if there is already an entry in the page_table_ mapping for the given page id, you should make sure you explicitly overwrite it with the frame id of the new page that was just created. This is a minor behavioral change in the buffer pool manager, but it is a requirement to pass the test cases.(重要,待理解)
- buffer pool manager `驱逐脏页时,需刷新pageLSN前的所有日志`(需要比较LogManager中的persistent_lsn_与Page中的lsn).`buffer pool manager可以强制让log manger刷新日志`(注:由于使用了group commit,每次commit时则不能强制刷新日志,比如等待log_timeout或者buffer pool manager刷新日志!!)  
- TablePage类的方法为WAL做了很多工作(不用自己写但需了解):a.创建LogRecord(见Init函数); b.将record写到buffer中(见Init函数); c.为当前事务更新prevLSN(见MarkDelete,InsertTuple等函数); d.为当前Page更新LSN(见UpdateTuple,InsertTuple等函数);  
- implement AppendLogRecord in the LogManager class so that the log records are properly serialized to the log buffer


### log_buffer_尾部无法写入完整的Record时导致碎片说明
log_buffer_ 尾部可能没有足够空间写入整个Record,为图简便,目前的实现中直接不使用这部分剩余空间;  
然后将log_buffer_整个写入磁盘,因此log_file中可能存在碎片;  
但读取时仍是以log_buffer_为单位,因此可以感知到末尾的碎片(只要写入时将碎片全部置为0);  
记于此,特别注意!!!  
实现太不优雅,后续可修改...  

## SYSTEM RECOVERY
### 官方建议/重点
- The only file you need to modify for this task is the `LogRecovery class`
- `DeserializeLogRecord`:从log buffer中反序列化日志;
- `Redo`:Redo pass on the TABLE PAGE level (include/storage/page/table_page.h). Read the log file from the beginning to end (you must prefetch log records into the buffer to reduce unnecessary I/O operations), and `for each log record, redo the action unless the page is already more up-to-date than this record`. Also make sure to maintain the `active_txn_ table` and `lsn_mapping_ table` along the way.
- `Undo`: Undo pass on the TABLE PAGE level (include/table/table_page.h). `Iterate through the active_txn_ table and undo operations within each transaction`. You do not need to worry about a crash during the recovery process, meaning no complementary logging is needed.


## CHECKPOINTS
### 官方建议/重点
- CheckpointManager:用于进行checkpoint.它会阻塞所有新txn,属性WAL日志到磁盘,刷新所有脏页到磁盘.
- `BeginCheckpoint`:a.调用TransactionManager阻止启动任何新txn; b.调用LogManager刷新日志到磁盘; c.调用BufferPoolManager刷新所有脏页到磁盘;Note that the DBMS must keep all transactions blocked when this method returns (this is necessary for grading).
- `EndCheckpoint`: Use the TransactionManager to unblock transactions and allow new transactions to begin.


# 知识点积累
## LSN的作用(UNDO阶段使用) TODO
Transaction: The Transaction object has the GetPrevLSN and SetPrevLSN helper methods. Each transaction is responsible for `maintaining the previous log sequence number to be used in the undo phase`. Please consult Chapter 16.8 in the textbook for more detailed information.

pageLSN,pwersistentLSN,prevLSN等的存储地方,作用?  
待梳理清楚...  

**************************** 要点 
- `log_manager 维护全局唯一的lsn`
- `每个Transaction都维护了自己的lsn,表示该txn最新的log的lsn`;由于lsn全局唯一,txn的lsn是非连续的,见table_page.cpp  
- `每个page的header处维护了自己的lsn,表示该page中最新log的lsn`;  
- 

## GROUP Commit,TODO
https://wiki.postgresql.org/wiki/Group_commit  

a group commit feature enables PostgreSQL to commit a group of transactions in batch, amortizing the cost of flushing WAL

## ARIES protocol TODO

## Redo与Undo

## ATT(active transaction table)
即日志中没有commit/abort的事务.说明这部分事务尚未提交就发生了异常,灾难恢复时需要Undo这部分数据  

## 对tuple的增删查改是通过TableHeap进行; recover则是通过TablePage进行的! 原因 TODO
https://15445.courses.cs.cmu.edu/fall2019/project4/#recovery  
```
Redo pass on the TABLE PAGE level
```
正常增删查改时,TableHeap 内部再调用 TablePage 的相关函数;  
而recover时跳过了TableHeap层,直接在TableHeap层面进行!!!  

原因:TODO

# TODO
## 重要紧急:group commit => ARIES protocol => 

## 应该考虑 log_buffer_ 存在没有写满导致存在碎片的问题 todo

## recover时执行insert,delete等操作,是否还需正常的增删查改那样写日志呢?是否应该传入txn呢?

## MARKDELETE,APPLYDELETE,ROLLBACKDELETE的区别/细节?


# tmp
## 复制粘贴常用
```
cmake -DCMAKE_BUILD_TYPE=DEBUG ..
make -j4 recovery_test
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  &  ./test/recovery_test
或者 LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/recovery_test

gdb --args ./test/recovery_test --gtest_filter=RecoveryTest.RedoTest
b recovery_test.cpp:43
```