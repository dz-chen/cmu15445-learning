//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
// B+树共两类结点:LEAF_PAGE、INTERNAL_PAGE, 根结点既可能是LEAF_PAGE,也可能是INTERNAL_PAGE
// 如何识别根结点 => parent_page_id_ == INVALID_PAGE_ID 则为根结点
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE};

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // member variable, attributes that both internal and leaf page share
  // by cdz:
  //      1.本人实现时,max_size_ 为实际可存储的kv对数-1(或者更小),从而方便插入及分裂 => 即每个page总有一个kv对的空间不会真正存储数据,当这个空间有数据时将会分裂...
  //      2.本人将max_size_ 理解为所有kv对数(包括第一个无效的key),也就是最大子结点数 <=> 而不是仅指有效key的个数; TODO:这种方式是否正确 ?
  IndexPageType page_type_ __attribute__((__unused__));       // internal or leaf
  lsn_t lsn_ __attribute__((__unused__));                     // Log sequence number
  int size_ __attribute__((__unused__));                      // Number of Key & Value pairs in page
  int max_size_ __attribute__((__unused__));                  // Max number of Key & Value pairs in page
  page_id_t parent_page_id_ __attribute__((__unused__));      // Parent Page Id
  page_id_t page_id_ __attribute__((__unused__));             // Self Page Id
};

}  // namespace bustub
