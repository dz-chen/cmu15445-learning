[toc]


# 注意
- 磁盘管理位于 `src/storage/disk` 需要仔细阅读其实现  




# TODO
## ASAN保存
当 cmake -DCMAKE_BUILD_TYPE=DEBUG .. 时报错
```
==26846==ASan runtime does not come first in initial library list; you should either link runtime to your application or manually preload it with LD_PRELOAD
```
若只是 cmake ../ 则正常

## LRU算法中的上锁方式是否正确,对锁还需进一步研究?