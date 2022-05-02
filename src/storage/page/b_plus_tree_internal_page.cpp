//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 * 注意这不是构造函数...
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  // lsn_ = ?
}


/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  assert(index >=0 && index<GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index>=0 && index<GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
  for(int i=0;i<GetSize();i++){
    if(array[i].second == value) return i;
  }
  return -1; 
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index>=0 && index<GetSize());
  return array[index].second;
}


/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 注: 1.lookup 并不是找等于key的, 而是找包含 key 的下层结点;
 *     2.结点内 key 是有序的,故可以二分查找;
 *     3. val(i)指向的下层所有结点: k(i)<= k <k(i+1);
 *     4.默认: 若comparator 返回值<0, 则o1排在o2前面(即认为o1 < 02);  TODO:暂时这样理解,不确定正确与否...
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // assert(GetSize()>1);
  // int start=1; int end=GetSize()-1;
  // // 二分查找,找到第一个 >= 参数key 的 key,但是返回其前一个!!!
  // while(start <= end){
  //   int mid = start + (end-start)/2;
  //   if(comparator(array[mid].first, key) == 0) return array[mid].second;
  //   else if(comparator(array[mid].first, key) < 0 ) start = mid+1;
  //   else end = mid -1;
  // }
  // return array[start-1].second;

  // TODO:改为二分查找
  // 找到第一个比key大的 kv 对
  int i=1;
  while(i<GetSize() && comparator(key,KeyAt(i))>=0){
    i++;
  }
  return ValueAt(i-1);
}




/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * 理解: 之前的root page被填满,当前的page实例是新建的root page,此函数用于填充当前
 *       page实例(新root page)的key及两个指针
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  // array[1].first = new_key;
  // array[1].second = new_value;
  // SetSize(2);
  SetSize(1);
  // 注: 4.24 目前改为了仅插入第一个val,方便与BPlusTree.Split()中的代码保持一致
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  assert(GetSize() < GetMaxSize());
  int i=GetSize()-1;
  // 元素后移
  while(i>=0 && array[i].second != old_value){
    array[i+1] = array[i];    
    i--;
  }
  array[i+1].first = new_key;
  array[i+1].second = new_value;
  
  IncreaseSize(1);
  return GetSize();
}



/*****************************************************************************
 * SPLIT
 * 注意与leaf_page中的split操作对比
 *****************************************************************************/
/**
 * Remove half of key & value pairs from this page to "recipient" page
 * 理解:1.由于下层结点的分裂导致增加了一个key,且当前结点没有多余空间,需分裂当前结点(不过在实现时是先插入再分裂);
 *      2.此时的策略是为当前结点创建一个兄弟结点(即此函数中的recipient),将当前结点的后
 *          一半key放到兄弟结点,此时需注意更新移动后的子结点的父指针;
 *      3.插入数据后的内部结点分裂时,中间那个key是要放到父结点(且不能处出现在之前的内部结点,这点不同于叶子结点)
 *      4.假设插入数据后的内部结点,总kv对数为n(则有效key个数为n-1),那么中间那个有效key的下标计算:
 *          low=1; high=GetSize()-1; mid=(low+high+1)/2;
 *        从而能够保证最后分到原结点的key不少于右兄弟;
 * TODO:需要改动较多
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient!=nullptr);
  int size = GetSize();
  int low=1; int high=size-1;
  int mid=(low+high+1)/2;

  // 后一半数据移动到右兄弟,注意从右兄弟第二个kv对开始存放; TODO:似乎不对吧? --4.27
  int start = mid+1;       // 要移动的第一个kv对下标
  for(int i=start;i<size;i++){
    recipient->array[i-start+1]=array[i];
  }
  recipient->SetSize(size-start+1);
  // 特别注意:右兄弟的第一个val指针,之前分裂前mid指向的结点!
  recipient->array[0].second=array[mid].second;
  SetSize(mid);

  // 右兄弟结点接收拷贝过来的kv对
  int size_rb=size-mid;               // 右兄弟结点中的kv对数(包含第一个key无效的kv对)
  recipient->CopyNFrom(array,size_rb,buffer_pool_manager);
}

/**
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 * 理解: 此时当前结点被作为新创建的一个兄弟结点,MoveHalfTo()函数向当前结点加入了若干孩子结点;
 *       此函数的作用就是,修改这些孩子的父指针为当前这个新的结点;       
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int start = GetSize();
  for(int i=0;i<size;i++){
    // 拷贝
    array[start+i] = items[i];
    // 更新被移动的子结点的父指针(注意fetch时进行了pin, 后面需要unpin)
    page_id_t child_page_id = reinterpret_cast<page_id_t>(items[i].second);
    Page* child_page = buffer_pool_manager->FetchPage(child_page_id);
    char* child_page_data = child_page->GetData();
    BPlusTreePage*  child_hdr =  reinterpret_cast<BPlusTreePage*>(child_page_data);
    child_hdr->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id,true);
  }
  IncreaseSize(size);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 * 理解:删除当前结点中的index对应的kv对
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index>=0 && index<GetSize());
  int size=GetSize();
  for(int i=index;i<size;i++){
    array[i] = array[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType val = array[1].second;
  IncreaseSize(-1);
  return val;
}



/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * by cdz: const KeyType &middle_key 不再作为传入参数,而是直接在函数内部计算出
 * 注意:
 *   1.输入必须满足:recipient 是currnode的左兄弟,将currnode合并到recipient; <recipient,currnode>;
 *   2.middle_key需要先放到右侧子结点(currnode)的第一个kv对中,然后再将currnode全部复制到recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient,
                                               BufferPoolManager *buffer_pool_manager) {
  Page* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage* parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  int middle_idx = parent_node->ValueIndex(GetPageId());      // currnode 在父结点中的idx
  KeyType middle_key = parent_node->KeyAt(middle_idx);        // 中间键,虽然位于父结点,但它应该被视作右边子结点(即currnode)第一个kv对的key

  // 复制 currnode 到 recipient
  array[0].first = middle_key;
  int size = GetSize();
  recipient->CopyNFrom(array,size,buffer_pool_manager);
  IncreaseSize(-size);

  // 删除父结点中middle_key对应的kv对(即currnode对应的kv对)
  parent_node->Remove(middle_idx);
}





/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * by cdz: const KeyType &middle_key 不再作为传入参数,而是直接在函数内部计算出
 * recipient结点删除后kv不够,MoveFirstToEndOf意味着当前结点是recipient的右兄弟, <recipient, currnode>
 * 注意:
 *   1.currnode的first record其实只存了一个val,这个record的key应该是父结点指向currnode的那个kv对中的key,
 *      也就是上面提到的middle_key;
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager) {
  Page* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage* parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  int middle_idx = parent_node->ValueIndex(GetPageId());      // currnode 在父结点中的idx
  KeyType middle_key = parent_node->KeyAt(middle_idx);        // 中间键,虽然位于父结点,但它应该被视作右边子结点(即currnode)第一个kv对的key

  // 复制第一个kv对到recipient的末尾
  MappingType pair(middle_key,array[0].second);
  recipient->CopyLastFrom(pair,buffer_pool_manager);
  

  // 更新:此时父结点中 middle_key 的位置应该是当前结点第一个kv对的key!
  Remove(0);  // 当前结点中删除第一个record
  parent_node->SetKeyAt(middle_idx,array[0].first);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 插入末尾
  int size = GetSize();
  array[size] = pair;
  IncreaseSize(1);
  // 更新插入kv对所对应结点的父指针
  page_id_t child_pgid = pair.second;
  Page* child_page = buffer_pool_manager->FetchPage(child_pgid);
  BPlusTreePage* child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 * by cdz: const KeyType &middle_key 不再作为传入参数,而是直接在函数内部计算出
 * recipient结点删除后kv不够,MoveLastToFrontOf意味着当前结点是recipient的左兄弟, <currnode,recipient>
 * 注意:
 *   1.这个情况下,currnode最后一个kv对的key,需要作为中间key放到父结点中,而原来的中间key则需要放到右边的子结点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                                                       BufferPoolManager *buffer_pool_manager) {
  Page* parent_page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());
  BPlusTreeInternalPage* parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  int middle_idx = parent_node->ValueIndex(recipient->GetPageId());      // recipient 在父结点中的idx
  KeyType middle_key = parent_node->KeyAt(middle_idx);                   // 中间键,虽然位于父结点,但它应该被视作右边子结点(即recipient)第一个kv对的key

  // 特别注意:需将原来的middle_key放到recipient的第一个kv对后,再复制currnode最后一个kv对到recipient开始处;
  recipient->array[0].first = middle_key;
  int size = GetSize();
  recipient->CopyFirstFrom(array[size-1],buffer_pool_manager);

  // 更新:currnode最后一个kv对的key需要作为中间键放到父结点对应位置
  parent_node->SetKeyAt(middle_idx,array[size-1].first);
  Remove(size-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 插入最开始处
  int size = GetSize();
  for(int i=size;i-1>=0;i--){
    array[i]=array[i-1];
  }
  array[0]=pair;
  IncreaseSize(1);

  // 更新插入kv对所对应结点的父指针
  page_id_t child_pgid = pair.second;
  Page* child_page = buffer_pool_manager->FetchPage(child_pgid);
  BPlusTreePage* child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());  
}


// TODO:这部分作用?
// valuetype for internalNode should be page_id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
