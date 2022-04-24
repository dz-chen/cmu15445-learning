//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"
#include "common/config.h"


namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}



Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  
  // 1.1 查找buffer pool, 确定需要的page 是否已经在 buffer pool 中, 有则直接返回
  frame_id_t frame_id;
  if(page_table_.find(page_id)!=page_table_.end()){
    frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  // 1.2 没有找到,说明不在buffer pool,得从磁盘读取 => 需要先在buffer pool 中找到一个空frame 来给调入的 page 腾出空间
  if(!free_list_.empty()){                // 空闲链表中找到空frame
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else{                                   // 否则需要通过 LRU 置换,找到一个 frame
    bool find_victim = replacer_->Victim(&frame_id);
    if(!find_victim) return nullptr;
  }

  // 2. 判断1.2中找到的 frame 中数据是否 dirty, 是则写回(本质上就是将内存page置换回磁盘)
  Page* page = &pages_[frame_id];
  if(page -> is_dirty_){
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  // 3.从页表中删除被替换出去的 page 信息; 把将要新置换进入 buffer 的 page 信息写入页表
  page_table_.erase(page->page_id_);
  page_table_[page_id] = frame_id;

  // 4.从磁盘读取数据到 buffer 中(当前frame/page), 更新 page 的元数据
  page->page_id_ = page_id;
  page->pin_count_ = 1;         // TODO: ==1 or +1 ?
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page->page_id_, page->data_);
  
  return page;
}


/**
 * unpin时标注page是否为dirty
 * 将dirty写回磁盘是在: 执行页面置换算法时(FecthPage)、刷新page时(FlushPage)
 * 当 pin_count_ 为0时则可以加入LRU
 */ 
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  if(page_table_.find(page_id)==page_table_.end()){
    LOG_WARN("the page(page_id = %d ) want to unpin is not in the buffer pool",page_id);
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];
  page->is_dirty_ = is_dirty;
  if(page->pin_count_ <= 0) return false;
  page->pin_count_--;

  if(page->pin_count_ ==0 ){
    replacer_->Unpin(frame_id);
  }
  
  return true; 
}

/**
 * FlushPageImpl should flush a page regardless of its pin status
 */ 
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if(page_table_.find(page_id)==page_table_.end() || page_id == INVALID_PAGE_ID){
    LOG_INFO("the page want to flush is not in the buffer pool");
    return false;
  }
  Page* page = &pages_[page_table_[page_id]];
  disk_manager_->WritePage(page_id,page->data_);

  return true;
}

/**
 * 在缓冲区分配一个空闲page
 */ 
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // 0. 调用diskmanager分配一个page
  *page_id = disk_manager_->AllocatePage();

  // 1. 检查磁盘上新分配的page能否调入内存; 若空闲链表 和 LRU链表都空,则buffer中没有多余空间
  if(free_list_.empty() && replacer_->Size()==0){
    return nullptr;
  }
  
  // 2. 从空闲链表 或者 LRU中找到一个存放新page的物理页
  Page* page;
  frame_id_t frame_id;
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  }
  else{
    replacer_->Victim(&frame_id);
    page = &pages_[frame_id];
    FlushPageImpl(page->page_id_);   // 置换到磁盘,需要修改页表...
    page_table_.erase(page->page_id_);
  }

  // 3. 更新page的元数据
  page->page_id_ = *page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;           // 根据测试代码,pin_count_ 不应该设置为0
  disk_manager_->ReadPage(page->page_id_,page->data_);
  page_table_[page->page_id_] = frame_id;

  // 4.返回
  return page;
}


/**
 * 根据源码,目前 disk_manager_->DeallocatePage(page_id) 没有做任何事情
 */ 
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  // 0. 调用diskmanager释放磁盘上的page => 根据源码,这个函数目前没有做任何事情...
  disk_manager_->DeallocatePage(page_id);

  // 1. 找出内存中的page
  if(page_table_.find(page_id)==page_table_.end()){
    return true;
  }
  Page* page = &pages_[page_table_[page_id]];

  // 2. 判断pin_count_
  if(page->pin_count_ > 0){
    return false;
  }

  // 3. 清空内存中的page
  free_list_.emplace_back(static_cast<int>(page_table_[page_id]));
  page_table_.erase(page_id);
  page->page_id_ = 0;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  memset(page->data_,0,PAGE_SIZE);

  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for(auto it=page_table_.begin(); it!=page_table_.end(); it++){
    FlushPageImpl(it->first);
  }
}

}  // namespace bustub
