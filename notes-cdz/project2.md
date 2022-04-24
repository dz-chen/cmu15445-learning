[toc]

# 注意
- 从测试代码递归往下看,更容易理解;  
- bustub中B+树的叶子结点中的val只是RecordId,是`非聚簇索引`;  
- bustub不支持重复键;  
- 在bustub中,对于内部结点,第一个key是落单的;对于叶子结点,所有kv都是成对的;详见b_plus_tree_internal_page.h、详见b_plus_tree_leaf_page.h;  
- b_plus_tree_page的`GetSize()函数,返回的是除PageHdr外的所有kv对个数`,对于内部结点,第一个key无效<=>对于叶子结点,第一个key有效;  
- 为方便B+树的插入,b_plus_tree_page的`GetMaxSize()函数应该返回page实际可存储的KV对数-1`,从而能够先插入,再分裂,方便编码;  
- B+树叶子结点和内部结点分裂时,到底多少个kv对(或者说key)留在原结点,多少个kv对放到右边兄弟结点,似乎说法不一(主要是相差1) => 本人做法如下:
```
对于叶子结点,假设分裂前有n个kv对(有效key有n个)       => 前(n+1)/2个kv对保留在原结点,剩余的放在右兄弟结点;即保证原结点的kv对不少于右兄弟.
对于内部结点,假设分裂前有n个kv对(有效key只有n-1个)   => 有效key里边的中间那个key需放到父结点,剩余的才是原结点与右兄弟分,同时也要保证原结点的key不少于右兄弟;
```




# TODO
## project1中lru、buffer_pool_manager需重新检查(关于pin)
**问题描述**  
buffer_pool_manager中的 page_table中存放的映射是否能在lru中,目前比较混乱,导致目前关于pin的时刻很不清晰,需重新界定...=> 目前个人的想法是:page_table应该包含lru中的,这样fetch时遇到lru中的page,可以减少一次换出!  
