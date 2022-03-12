//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_pages_ = num_pages;
  head_ = new Node();
  tail_ = new Node();
  head_->next = tail_;  head_->prev = tail_;
  tail_->next = head_;  tail_->prev = head_;
  unpin_pages_ = 0;
}

LRUReplacer::~LRUReplacer(){
  Node* node = head_;
  while(node != tail_){
    head_ = node->next;
    delete node;
    node = head_;
  }
  delete tail_;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if(head_->next == tail_){
    latch_.unlock();
    return false;
  }
  Node* vict = head_->next;
  *frame_id = vict->frame_id;
  head_->next = vict->next;
  vict->next->prev = head_;
  id2ptr_.erase(*frame_id);
  delete vict;
  unpin_pages_--;
  latch_.unlock();
  return true;
 }

void LRUReplacer::Pin(frame_id_t frame_id) {
  // pin 的frame需要从LRU链表删除(它们不能被置换出内存)
  latch_.lock();
  if(id2ptr_.find(frame_id) == id2ptr_.end()){
    latch_.unlock();
    return;
  }

  Node* node = id2ptr_[frame_id];
  Node* prev = node->prev; 
  Node* next = node->next;
  prev->next=next; next->prev=prev;
  delete node;
  id2ptr_.erase(frame_id);
  unpin_pages_--;
  latch_.unlock();
}

/**
 * pin_count_ 为0的 page,才可以将其 unpin 到 lru 中
 */ 
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // unpined 的 frame 可以被加入LRU链表末尾
  // 注: project1中没有提及LRU缓存满的情况,这里暂时Log下来
  latch_.lock();
  if(unpin_pages_ >= max_pages_){
    LOG_WARN("cached pages in LRU buff have exceed, where max_pages=%d, used_pages=%d",\
            (int)max_pages_,(int)unpin_pages_);
    latch_.unlock();
    return;
  }
  
  // 若 frame 已经在LRU链表中, 根据提供的测试代码:不需要将其放到链表末尾...
  if(id2ptr_.find(frame_id) != id2ptr_.end()){
    latch_.unlock();
    return;
  }

  // 添加新链表节点
  Node* node = new Node(frame_id);
  Node* prev = tail_->prev;
  prev->next = node;
  node->prev = prev; node->next = tail_;
  tail_->prev = node;
  id2ptr_[frame_id] = node;
  unpin_pages_++;
  latch_.unlock();
}

size_t LRUReplacer::Size() {
  // TODO: 这里需要上锁吗 ?
  return unpin_pages_; 
}

}  // namespace bustub
