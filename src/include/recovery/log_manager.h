//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.h
//
// Identification: src/include/recovery/log_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unistd.h>

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <future>              // NOLINT
#include <mutex>               // NOLINT

#include "recovery/log_record.h"
#include "storage/disk/disk_manager.h"
#include "common/logger.h"

namespace bustub {

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
class LogManager {
 public:
  explicit LogManager(DiskManager *disk_manager)
      : next_lsn_(0), persistent_lsn_(INVALID_LSN), disk_manager_(disk_manager) {
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }

  void RunFlushThread();
  void StopFlushThread();

  lsn_t AppendLogRecord(LogRecord *log_record);

  inline lsn_t GetNextLSN() { return next_lsn_; }
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }


  /*********************************** functions add by cdz ***********************************/
  void FlushLog(bool force=false);

 private:
  void SwapBuffer();

  bool trySwapBuffer(size_t need_length);


  // TODO(students): you may add your own member variables

  /** The atomic counter which records the next log sequence number. */
  std::atomic<lsn_t> next_lsn_;
  /** The log records before and including the persistent lsn have been written to disk. */
  std::atomic<lsn_t> persistent_lsn_;

  char *log_buffer_;
  char *flush_buffer_;
  size_t log_offset_;             // 当前日志的末尾(或者下次日志的开始)在log_buffer_中的偏移

  std::thread *flush_thread_ __attribute__((__unused__));

  std::mutex latch_;              // 用于保护共享变量 log_buffer_等 的修改(多个线程不能并发写日志)
  
  std::mutex mtx_cv_;             // 与条件变量配合使用    -- add by cdz
  std::condition_variable cv_;    // 刷新日志的条件变量
  bool need_flush_;               // 条件变量等待的条件(个人实现时合并了两个条件:时间到 和 log buffer满) -- add by cdz
  std::thread *timer_thread_ __attribute__((__unused__));   // 用于定时(待优化TODO)  -- add by cdz

  DiskManager *disk_manager_ __attribute__((__unused__));
};

}  // namespace bustub
