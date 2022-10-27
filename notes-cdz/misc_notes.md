[toc]
# cpp
## std::condition_variable
https://blog.csdn.net/wxj1992/article/details/116888582  
理解外部传入 mutex是作用!!!  

其实可根据 pthread_mutex_t 的使用来理解!!!  
wait前要lock,wait后要unlock.  
只是对于本项目,unique_lock自动完成了lock与unlock  
