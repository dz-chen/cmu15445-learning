[toc]


# 实现细节注意
## LRU
- `LRU的pin与unpin指的是当page被pin/unpin时LRU_Replacer的行为`:
```
当某个Page被pin时,它不能移出内存,因而LRU.pin()函数需要将该Page从LRU链表中剔除;
当某个Page被unpin时,它可以被移出内存,因而LRU.unpin()函数需要将该Page加入LRU链表;
```

## 缓冲区管理
- 对于page.h中Page类的理解:
```
1.Page 是RAM中缓冲区的基本单位;
2.一个Page实体中,在不同时刻存放的Disk Page可能是不同的;Disk Page 即成员data_[];
3.成员page_id_描述的是Disk Page的编号,如果当前Page实例中没有Disk Page,则page_id_设置为INVALID_PAGE_ID;
4.成员pin_count_代表同时访问该页面的线程/进程数;
```
- 对于buffer_pool_manager.h中BufferPoolManager类的理解:
```

```

## 磁盘管理
- 磁盘管理位于 `src/storage/disk` 需要仔细阅读其实现  
- 由于目前尚未真正往磁盘写数据,测试时disk_manager会打印一些IO错误,此为正常现象  
- 



# 知识点积累

# 杂七杂八
## ASAN报错(ASan runtime does not come first in initial library list...) => fixed
**问题描述**  
当 cmake -DCMAKE_BUILD_TYPE=DEBUG .. ,则运行时报错
```
==26846==ASan runtime does not come first in initial library list; you should either link runtime to your application or manually preload it with LD_PRELOAD
```
若只是 cmake ../ 则正常

**解决办法**  
经参考:  
```
http://blog.sina.com.cn/s/blog_5423c45a0102xmvi.html
https://blog.csdn.net/lishun1422840684/article/details/118728954
```
知:gcc编译需要link asan statically (which is default with clang),否则运行时会报错. 由于使用gcc编译,需要静态链接,根据提示可手动操作如下:  
```
1. ldd ./test/buffer_pool_manager_test          => 找到依赖的 libasan.so 动态库路径
2. LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/buffer_pool_manager_test   => 手动传入asan的路径,进行链接并执行程序
(当然,2中的环境变量也可指当前shell中设置... => 注意要先编译,再设置环境变量)
```

## gtest无法使用GDB调试 => fixed
参考:[GTest GDB调试方法](https://www.jianshu.com/p/43ca81a2df17)
```
gdb --args ./test/buffer_pool_manager_test --gtest_filter=BufferPoolManagerTest.SampleTest
b buffer_pool_manager_test.cpp:90
```

# TODO
## LRU算法中的上锁方式是否正确,对锁还需进一步研究?
- 如果一个线程访问pin、一个线程访问unpin,目前的上锁方式是否能保证正确?

## LRU内部真的需要上锁吗?
- 直接在buffermanager中上锁不就可以了?


# tmp
## 复制粘贴常用
```
cmake -DCMAKE_BUILD_TYPE=DEBUG ..
make -j4 buffer_pool_manager_test
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  &  ./test/buffer_pool_manager_test
或者 LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/buffer_pool_manager_test

gdb --args ./test/buffer_pool_manager_test --gtest_filter=BufferPoolManagerTest.SampleTest
b buffer_pool_manager_test.cpp:90
```