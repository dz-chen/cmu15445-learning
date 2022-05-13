/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node,BufferPoolManager* buffer_pool_manager){
    leaf_node_ = leaf_node;
    index_ = 0;
    buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node,int index, BufferPoolManager* buffer_pool_manager){
    leaf_node_ = leaf_node;
    index_ = index;
    buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

/*
 * return whether this iterator is pointing at the last key/value pair. 
 * 准确的说:如果真的指向最后一个record应该返回false, 只有当遍历完最后一个record后才返回true
 */ 
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    return leaf_node_ ==nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
    return leaf_node_->GetItem(index_);
}

/*
 * 注意:operator++()是前置自增 => ++iter
 *     operator++(int)是后置自增 => iter++
 */ 
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    index_++;
    // 确定是否需要进入右兄弟结点
    if(index_>=leaf_node_->GetSize()){
        page_id_t next_page_id = leaf_node_->GetNextPageId();
        if(next_page_id == INVALID_PAGE_ID){    // 已经遍历完所有叶子结点中的记录
            leaf_node_ = nullptr;
            index_ = 0; 
        }
        else{                                   // 否则进入下一个结点
            Page* next_page = buffer_pool_manager_->FetchPage(next_page_id);
            leaf_node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(next_page->GetData());
            index_ = 0;
        }
    }
    return *this;
}

/*
 * Return whether two iterators are equal
 */ 
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const{
    return buffer_pool_manager_ == itr.buffer_pool_manager_
            && leaf_node_ == itr.leaf_node_
            && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const{
    return buffer_pool_manager_ != itr.buffer_pool_manager_
            || leaf_node_ != itr.leaf_node_
            || index_ != itr.index_;
}


template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
