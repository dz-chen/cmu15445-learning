//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 * The LRUReplacer is initialized to have no frame in it. Then, only the newly unpinned ones will be
 *  considered in the LRUReplacer
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // 基本思路: 双线链表+hash实现LRU , 链表头部的是最近最久未使用的
  // TODO(student): implement me!
  typedef struct Node{
    frame_id_t frame_id;
    Node* next;
    Node* prev;
    Node(){
      frame_id=-1; next=nullptr; prev=nullptr;
    }
    Node(frame_id_t fid){
      // 参数和成员名不能相同!
      frame_id=fid; next=nullptr; prev=nullptr;
    }
  }Node;

  // head、tail不实际存储 frame_id
  Node* head_;
  Node* tail_;
  size_t max_pages_;
  size_t used_pages_;
  std::unordered_map<int,Node*> id2ptr_;
  // 用于保护 lru_replacer 中所有共享变量; 这里上锁粒度较粗,基本都是锁整个函数...
  std::mutex latch_;
};

}  // namespace bustub
