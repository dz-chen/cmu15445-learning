//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
  // TODO : LSN = ?
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { 
  return next_page_id_; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * 找到从左往右数第一个大于等于key的 array下标; 这也就是参数key最终可以存放的下标位置;
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int i=0;
  int size=GetSize();
  while (i<size && comparator(key,array[i].first)>0){
    i++;
  }
  return i;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >=0 && index<GetSize());
  return array[index].first;
}


/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  assert(index >=0 && index<GetSize());
  return array[index];
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 * TODO:改为二分查找
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int i=GetSize()-1;
  // 找到kv对应该存放的位置(i+1)  => 也可改用上面的 KeyIndex()函数
  while(i>=0 && comparator(key,array[i].first)<0 ){
    array[i+1]=array[i];
    i--;
  }
  array[i+1].first=key;
  array[i+1].second=value;

  SetSize(GetSize()+1);

  return GetSize();
}


/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 由个人添加,只是为了在Split()时避免模板静态多态的问题,不会被使用
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,BufferPoolManager *buffer_pool_manager) {
  assert(recipient!=nullptr);

  int size = GetSize();
  int start = (size+1)/2;       // 要移动的第一个kv对下标
  for(int i=start;i<size;i++){
    // recipient->array[i-start]=array[i];
    recipient->array[i-start].first=array[i].first;
    recipient->array[i-start].second=array[i].second;
  }
  recipient->SetSize(size-start);
  SetSize(start);
  
  // 对于叶子结点,需要更新兄弟结点指针
  page_id_t next_page_id = GetNextPageId();
  recipient->SetNextPageId(next_page_id);
  SetNextPageId(recipient->GetPageId());

  // CopyNFrom(...);     // PASS
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // PASS,本身就是叶子结点,无需像内部结点一样修改子结点的父指针
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 * 注意:
 *   1.当key存在时,需要通过参数value返回对应的值;
 *   2.leaf_page,不需要舍弃第一个kv对;
 * TODO:改为二分查找
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  for(int i=0;i<GetSize();i++){
    if(comparator(key,array[i].first)==0){
      *value=array[i].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 * TODO:改为二分查找
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  ValueType tmp_val;
  if(!Lookup(key,&tmp_val,comparator)) return GetSize();
  int idx = KeyIndex(key,comparator);
  int size=GetSize();
  // 元素往前移
  for(int i=idx;i+1<size;i++){
    array[i]=array[i+1];
  }
  SetSize(size-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE         => 与兄弟结点合并
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 * 理解:
 *   1.删除后,若当前结点不满足最小要求,且相邻兄弟结点也无法借取kv,则将当前结点合并到兄弟结点;
 *   2.注意这是叶子结点;
 *   3.为了方便更新 next_page_id_,所以应该将当前page视作要删除的,recipient是其左侧的兄弟!
 *   4.参数 buffer_pool_manager 由个人添加,与InternalPage保持一致,避免b_plus_tree.cpp中编译出错
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,BufferPoolManager *buffer_pool_manager) {
  int start=recipient->GetSize();
  int size=GetSize();
  for(int i=0;i<size;i++){
    recipient->array[start+i]=array[i];
  }
  recipient->SetSize(start+size);
  recipient->SetNextPageId(GetNextPageId());
}


/*****************************************************************************
 * REDISTRIBUTE     => 向兄弟结点借kv
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 * recipient结点删除后kv不够,MoveFirstToEndOf意味着当前结点是recipient的右兄弟
 * 注:
 *   1.参数 buffer_pool_manager 由个人添加,与InternalPage保持一致,避免b_plus_tree.cpp中编译出错
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,BufferPoolManager *buffer_pool_manager) {
  // 将第一个kv复制给recipient
  recipient->CopyLastFrom(array[0]);
  
  // 当前page向前移动元素并更新size
  int size=GetSize(); 
  for(int i=0;i+1<size;i++){
    array[i]=array[i+1];
  }
  SetSize(size-1);

  // TODO:应该需要更新parent
  // TODO:但看书更新parent应该不是必须的...
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int size=GetSize();
  array[size]=item;
  SetSize(size+1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 * recipient结点删除后kv不够,MoveLastToFrontOf意味着当前结点是recipient的左兄弟
 * 注:
 *   1.参数 buffer_pool_manager 由个人添加,与InternalPage保持一致,避免b_plus_tree.cpp中编译出错
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,BufferPoolManager *buffer_pool_manager) {
  int size=GetSize();
  // recipient 复制元素
  recipient->CopyFirstFrom(array[size-1]);

  // 更新当前page大小
  SetSize(size-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int size=GetSize();
  // 元素后移
  for(int i=size;i>0;i--){
    array[i]=array[i-1];
  }
  array[0]=item;
  SetSize(size+1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
