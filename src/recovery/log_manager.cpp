//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  enable_logging = true;
  flush_thread_ = new std::thread(&LogManager::FlushLog,this,false);
  LOG_INFO("log flush thread started");
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  enable_logging = false;
  timer_thread_->join();
  flush_thread_->join();
  delete timer_thread_;
  delete flush_thread_;
  LOG_INFO("log flush thread stopped");
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 * 
 * 在 InsertTuple/UpdateTuple 等函数中被调用
 * 需立即 LogRecord的格式
 * TODO:此线程可能被多个线程同时调用,需上锁...
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  latch_.lock();
  log_record->lsn_ = next_lsn_++;
  size_t need_length;
  switch(log_record->log_record_type_){
    case LogRecordType::INVALID:          // invalid
      LOG_WARN("invalid log record type");
      latch_.unlock();
      return -1;
    case LogRecordType::INSERT:           // insert
      // 检查log_buffer_是否足够
      need_length = 20 + sizeof(RID) + log_record->insert_tuple_.GetLength();
      trySwapBuffer(need_length);
      // 拷贝数据
      memcpy(log_buffer_ + log_offset_, &log_record, 20);   // 公共部分
      log_offset_ += 20;
      memcpy(log_buffer_ + log_offset_, &(log_record->insert_rid_), sizeof(RID)); // 特定内容
      log_offset_+=sizeof(RID);
      log_record->insert_tuple_.SerializeTo(log_buffer_ + log_offset_);
      log_offset_+=log_record->insert_tuple_.GetLength();
      break;
    case LogRecordType::MARKDELETE:       // delete
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      // 检查log_buffer_是否足够
      need_length = 20 + sizeof(RID) + log_record->delete_tuple_.GetLength();
      trySwapBuffer(need_length);
      // 拷贝数据
      memcpy(log_buffer_ + log_offset_, &log_record, 20);   // 公共部分
      log_offset_ += 20;
      memcpy(log_buffer_ + log_offset_, &(log_record->delete_rid_), sizeof(RID)); // 特定内容
      log_offset_+=sizeof(RID);
      log_record->delete_tuple_.SerializeTo(log_buffer_ + log_offset_);
      log_offset_+=log_record->delete_tuple_.GetLength();
      break;
    case LogRecordType::UPDATE:           // update
      // 检查log_buffer_是否足够
      need_length = 20 + sizeof(RID) + log_record->old_tuple_.GetLength() + log_record->new_tuple_.GetLength();
      trySwapBuffer(need_length);
      // 拷贝数据
      memcpy(log_buffer_ + log_offset_, &log_record, 20);   // 公共部分
      log_offset_ += 20;
      memcpy(log_buffer_ + log_offset_, &(log_record->update_rid_), sizeof(RID)); // 特定内容
      log_offset_+=sizeof(RID);
      log_record->old_tuple_.SerializeTo(log_buffer_ + log_offset_);
      log_offset_+=log_record->old_tuple_.GetLength();
      log_record->new_tuple_.SerializeTo(log_buffer_ + log_offset_);
      log_offset_+=log_record->new_tuple_.GetLength();
      break;
    case LogRecordType::NEWPAGE:          // newpage
      // 检查log_buffer_是否足够
      need_length = 20 + sizeof(page_id_t) + sizeof(page_id_t); 
      trySwapBuffer(need_length);
      // 拷贝数据
      memcpy(log_buffer_ + log_offset_, &log_record, 20);   // 公共部分
      log_offset_ += 20;
      memcpy(log_buffer_ + log_offset_, &(log_record->prev_page_id_), sizeof(page_id_t)); // 特定内容
      log_offset_+=sizeof(page_id_t);
      memcpy(log_buffer_ + log_offset_, &(log_record->page_id_), sizeof(page_id_t));
      log_offset_+=sizeof(page_id_t);
      break;
    default:                              // transaction
      LOG_INFO("transaction,no need to append additional imformation.");
      break;
  }
  latch_.unlock();
  persistent_lsn_ = log_record->lsn_;
  return persistent_lsn_; 
}


/*********************************** functions add by cdz ***********************************/
/**
 * 刷新 log 到磁盘,三个时机:
 * 1.buffer pool manager(驱逐脏页)强制刷新;
 * 2.log_timeout 时间到; 
 * 3.log buffer 满;
 * 注: force 默认为 false
 */ 
void LogManager::FlushLog(bool force){
  if(force){                // 1.强制刷新(直接被调用,不会阻塞线程) => 这个情形是处于调用此函数的 线程 的上下文中
    if(!enable_logging) return;
    // 刷新,log是sequence write,直接追加即可,不必定位具体位置(详见 WriteLog )
    // 注:末尾的碎片置为0
    memset(log_buffer_+log_offset_ , 0, LOG_BUFFER_SIZE-log_offset_);  
    disk_manager_->WriteLog(log_buffer_,LOG_BUFFER_SIZE);
  }
  else{                     // 2/3. 时间到或log buffer满(需要条件变量,可能阻塞线程) => 这两个情形是作为后台线程处理
    timer_thread_ = new std::thread([&](){
      while (enable_logging){
        std::this_thread::sleep_for(log_timeout);
        SwapBuffer();
        need_flush_ = true; // 定时到,修改条件 (注: log buffer 满时修改条件见 AppendLogRecord函数)
        cv_.notify_one();
      }}
    );

    while(enable_logging){
      // 阻塞线程,等待时机
      std::unique_lock<std::mutex> ul(mtx_cv_);
      while(!need_flush_){
        cv_.wait(ul);
      }
      ul.unlock();

      // 刷新
      // 注:末尾的碎片置为0
      memset(log_buffer_+log_offset_ , 0, LOG_BUFFER_SIZE-log_offset_);
      disk_manager_->WriteLog(log_buffer_,LOG_BUFFER_SIZE);
      need_flush_ = false;
    }
  }
  
}


/**
 * 交换buffer,两种情况下调用:
 * 1.log_timeout 时间到;
 * 2.被强制刷新;
 */ 
void LogManager::SwapBuffer(){
    latch_.lock();
    char* buf = log_buffer_;
    log_buffer_ = flush_buffer_;
    flush_buffer_ = buf;
    log_offset_ = 0;
    latch_.unlock();
}

/**
 * 检查 log_buffer_ 剩余空间是否足够,并判断交换buffer:
 * 1.不够: 交换 log_buffer_ 与 flush_buffer_,并修改条件,通知后台的flush线程,然后返回true
 * 2.足够: 返回false
 * 线程安全:在调用 trySwapBuffer 前(见AppendLogRecord)已经上锁,这里不用考虑
 */ 
bool LogManager::trySwapBuffer(size_t need_length){
  if(log_offset_+need_length > LOG_BUFFER_SIZE ){   // 空间不足,交换
    char* buf = log_buffer_;
    log_buffer_ = flush_buffer_;
    flush_buffer_ = buf;
    log_offset_ = 0;
    need_flush_ = true;
    cv_.notify_one();
    LOG_INFO("log_buffer_ is full.");
    return true;
  }
  return false;
}


}  // namespace bustub
