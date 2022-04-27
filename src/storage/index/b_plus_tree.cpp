//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

// B+树,模板类
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query => 点查询
 * @return : true means key exists
 * result 为何搞个双指针这么麻烦...
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page* leaf_page = FindLeafPage(key,false);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page);
  ValueType val;
  bool exist = leaf_node->Lookup(key,&val,comparator_);
  if(!exist) return false;

  result[0].clear();
  result[0][0]=val;
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;    
  }
  return InsertIntoLeaf(key,value,transaction); 
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id;
  Page* new_page=buffer_pool_manager_->NewPage(&new_page_id);
  if(new_page==nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,"b_plus_tree.cpp,StartNewTree");
  }
  // 新创建的root page,其内容被视为一个叶子结点; ValueType 类型为RID
  B_PLUS_TREE_LEAF_PAGE_TYPE* root_page=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_page);
  root_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);

  // 插入kv
  root_page->Insert(key,value,comparator_);
  // 在索引文件中新增一棵B+树
  root_page_id_ = new_page_id;
  UpdateRootPageId(true);   
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page=FindLeafPage(key,false);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  ValueType tmp_val;
  bool exist=leaf_page->Lookup(key,&tmp_val,comparator_);
  if(exist) return false;
  
  // 先插入,再判断是否需要分裂
  leaf_page->Insert(key,value,comparator_);
  if(leaf_page->GetSize() > leaf_page->GetMaxSize()){  // 分裂
    Split(leaf_page);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 * 注:
 *   1.插入算法最麻烦的就是分裂,特别重要...
 *   2.分裂是递归进行的;
 * TODO:待简化
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page* new_page=buffer_pool_manager_->NewPage(&new_page_id);
  if(new_page==nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,"b_plus_tree.cpp,Split");
  }
  N* r_brother = reinterpret_cast<N*>(new_page->GetData());

  /////////////////////////////// x. 特别判断要分裂的结点是否为根结点(可能是叶子也可能是内部结点)
  if(node->IsRootPage()){
    // 需要创建新的根结点
    page_id_t new_root_pgid;
    Page* new_root_page=buffer_pool_manager_->NewPage(&new_root_pgid);
    if(new_root_page==nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,"b_plus_tree.cpp,Split");
    }

    // 更新相关信息
    // 注: 对于BPlusTreeInternalPage,它的ValueType为page_id_t, 不能沿用 b_plus_tree传入的ValueType
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* root_node = 
                          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(new_root_page->GetData());
    root_node->Init(new_root_pgid,INVALID_PAGE_ID,internal_max_size_);
    KeyType null_key; page_id_t null_val;   // 仅设置新root的第一个val指针
    root_node->PopulateNewRoot(node->GetPageId(),null_key,null_val);
    root_page_id_ = new_root_pgid;
    UpdateRootPageId(false);
    node->SetParentPageId(new_root_pgid);
  }

  /////////////////////////////// a.分裂
  KeyType add_key;      // 分裂时需要向parent新插入的key
  if(node->IsLeafPage()){                                              //// 1.如果分裂的是叶子结点
    r_brother->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    // 1.1 将叶子结点 node 的后一半数据拷贝到右兄弟
    node->MoveHalfTo(r_brother,nullptr);    // 内部更新了右指针nextPageId

    // 1.2 r_brother最小key以及r_bother的页号作为kv对插入父结点
    add_key = r_brother->KeyAt(0);
  }
  else{                                                                //// 2.如果分裂的是内部结点
    r_brother->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    // 2.1 将内部结点 node 的后一半数据拷贝到右兄弟
    // 注意:对于内部结点的分裂,中间那个key是要放到父结点(且不能处出现在之前的内部结点,这点不同于叶子结点)
    node->MoveHalfTo(r_brother,buffer_pool_manager_);

    // 2.2 将中间键与 r_bother 的页号作为kv对插入父结点
    int low=1; int high=node->GetSize()-1;
    int mid=(low+high+1)/2;
    add_key=node->KeyAt(mid);
  }

  /////////////////////////////// b.分裂后更新父结点(即插入因分裂新增的kv)
  page_id_t parent_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent_node = 
                        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  parent_node->InsertNodeAfter(node->GetPageId(),add_key,r_brother->GetPageId());
  if(parent_node->GetSize() > parent_node->GetMaxSize()){     // 父结点需要分裂
    Split(parent_node);
  }

  return r_brother;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 直接在 Split()中实现了
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()) return;
  Page* leaf_page = FindLeafPage(key,false);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page);
  leaf_node->RemoveAndDeleteRecord(key,comparator_);    // 内部会自动判断是否包含要删除的key

  // 需要合并 或者 重构, 具体哪种操作由CoalesceOrRedistribute()内部决定
  if(leaf_node->GetSize() < leaf_node->GetMinSize()){
    bool coalesced = CoalesceOrRedistribute(leaf_node,nullptr);
  }
}

/*
 * 完成 node 与兄弟结点的合并 或者 重构
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 注: 
 *   1.不是同一个父结点不能算是兄弟(即使叶子结点有nextPageId也不能保证下一个一定是右兄弟!);
 *   2.重构时,优先从左兄弟借还是从右兄弟借都可(本文优先从左兄弟借);
 *   3.合并时,优先合并到左兄弟;如果没有左兄弟,则将右兄弟合并到当前结点(主要是为了方便叶子结点调整nextPageId指针)!
 *   4.对于返回值,true代表node需要被删除(与兄弟合并、或者node作为根结点被删除); false代表node没有被删除(从兄弟结点借到了kv);
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if(node->IsRootPage()){
    return AdjustRoot(node);
    // 即使root page的 GetSize()<GetMinSize(),也无需/无法做合并、重构,因而直接退出
  }

  // 找到父结点
  page_id_t parent_page_id=node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent_node=
                                                  reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  // 找到左右兄弟
  int idx = parent_node->ValueIndex(node->GetPageId());
  N *l_brother=nullptr,*r_brother=nullptr;
  if(idx>0){                          // 存在左兄弟
    page_id_t page_id = parent_node->ValueAt(idx-1);
    Page* l_page = buffer_pool_manager_->FetchPage(page_id);
    l_brother = reinterpret_cast<N*>(l_page->GetData());
    if(l_brother->GetSize()>l_brother->GetMinSize()){
      // 借左兄弟的最后一个kv对
      Redistribute(l_brother,node,l_brother->GetSize()-1);
      return false;
    }
  }
  if(idx<parent_node->GetSize()-1){  // 存在右兄弟
    page_id_t page_id = parent_node->ValueAt(idx+1);
    Page* r_page = buffer_pool_manager_->FetchPage(page_id);
    r_brother = reinterpret_cast<N*>(r_page->GetData());
    if(r_brother->GetSize()>r_brother->GetMinSize()){
      // 借右兄弟的第一个kv对
      Redistribute(r_brother,node,0);
      return false;
    }
  }

  // 到此,说明左右兄弟都不能借,则需合并,然后更改父结点
  if(l_brother){
    Coalesce(l_brother,node,parent_node,idx,nullptr);
    return true;
  }
  else if(r_brother){
    Coalesce(node,r_brother,parent_node,idx+1,nullptr);
    return false;
  }
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              node在父结点中对应的kv下标,node是要删除的结点(它将被合并到左兄弟)
 * @return  true means parent node should be deleted, false means no deletion happend ?
 * 注意:
 *   1.将 node 合并到 neighbor_node, 需保证 neighbor_node 是node的左兄弟!
 *   2.需要更改父结点中信息;
 *   3.本文实现时,返回值的含义:true代表父结点有递归合并或者重构,false则没有递归处理...
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  if(node->IsLeafPage()) node->MoveAllTo(neighbor_node);
  else node->MoveAllTo(neighbor_node,buffer_pool_manager_);

  // 在父结点中删除node的kv对,判断是否需要递归合并/重构
  parent->Remove(index);
  if(parent->GetSize() < parent->GetMinSize()){
    CoalesceOrRedistribute(parent,nullptr);
    return true;
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * 重构无需更改父结点
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   index              从兄弟结点借取的kv对的下标,据此可判断是左兄弟还是右兄弟
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if(index == 0){         // neighbor是右兄弟
    if(node->IsLeafPage()) neighbor_node->MoveFirstToEndOf(node);
    else neighbor_node->MoveFirstToEndOf(node,buffer_pool_manager_);
  }
  else{                   // neighbor是左兄弟
    if(node->IsLeafPage()) neighbor_node->MoveLastToFrontOf(node);
    else neighbor_node->MoveLastToFrontOf(node,buffer_pool_manager_);
  }
}


/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // TODO:待完善
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize()==1){ // case1: 根结点是内部结点,只有一个指针,没有key了
    // 删除当前根结点,直接将叶子结点作为根结点
    page_id_t child_page_id = old_root_node->RemoveAndReturnOnlyChild();
    Page* child_page = buffer_pool_manager_->FetchPage(child_page);
    B_PLUS_TREE_LEAF_PAGE_TYPE* child_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(child_page->GetData());
    child_node->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    return true;
  }


  if(old_root_node->IsLeafPage() && old_root_node->GetSize()==0){  // case2: 根节点是叶子结点,已删完所有元素
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
  }

  // 其他情况不做任何处理,即使 GetSize()<GetMinSize()
  return false; 
}


/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType tmp_key;
  Page* leaf_page = FindLeafPage(tmp_key,true);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(leaf_node,buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  Page* leaf_page = FindLeafPage(key,true);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page->GetData());
  int idx = leaf_node->KeyIndex(key,comparator_);
  return INDEXITERATOR_TYPE(leaf_node,idx,buffer_pool_manager_); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  return INDEXITERATOR_TYPE(nullptr,buffer_pool_manager_); 
}



/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * 理解:两个功能
 *   1.如果leftMost为true,则直接返回最左叶子结点,不用考虑Key;
 *   2.leftMost为false,则返回key所在的叶子结点;
 * TODO:改为二分查找
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  Page* page=buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node=reinterpret_cast<BPlusTreePage*>(page->GetData());
  
  if(leftMost){
    while (!node->IsLeafPage()){
      // ValueAt(0) 是第一个左子结点的指针
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal_node = 
            reinterpret_cast< BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(node);
      page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(0));
      node=reinterpret_cast<BPlusTreePage*>(page->GetData());
    }
    return page;
  }

  // 查找key所在的叶子结点
  while(!node->IsLeafPage()){
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal_node = 
            reinterpret_cast< BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(node);
    page_id_t child_page_id=internal_node->Lookup(key,comparator_);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    node=reinterpret_cast<BPlusTreePage*>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 * 理解:
 *   1.一个索引文件中可能包含多棵B+树,索引文件的header_page中存放了这些B+树的根结点信息;
 *   2.一棵B+树的root结点也可能不断更新...
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
