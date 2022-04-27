//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>


// IndexIterator 仅在叶子结点上滑动
INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node,BufferPoolManager* buffer_pool_manager);  // add by cdz
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node,int index, BufferPoolManager* buffer_pool_manager);  // add by cdz
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const;

  bool operator!=(const IndexIterator &itr) const;

 private:
  // add your own private member variables here
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node_;
  BufferPoolManager* buffer_pool_manager_;
};

}  // namespace bustub
