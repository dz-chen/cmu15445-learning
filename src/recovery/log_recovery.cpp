//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 * 专门反序列换的目的:
 *  1.deep copy;
 *  2.像 Tuple 类,内含data_ 字段,是动态分配内存的,若不专门反序列化,直接将内存表示强制转换为 LogRecord,data_数据将丢失
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) { 
  // record 头部固定20字节
  memcpy(log_record,data,20); 
  int offset = 20;
  if(log_record->GetSize()==0){
    LOG_DEBUG("log_record->size == 0, no more log in the buffer");
    return false;
  }

  // 特定内容
  switch (log_record->log_record_type_){
    case LogRecordType::INVALID:
      LOG_WARN("invalid log record type when DeserializeLogRecord");
      return false;
    case LogRecordType::INSERT:                               // insert
      memcpy(log_record+offset, data+offset, sizeof(RID));
      offset += sizeof(RID);
      log_record->insert_tuple_.DeserializeFrom(data+offset);
      break;
    case LogRecordType::MARKDELETE:                           // delete
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      memcpy(log_record+offset, data+offset, sizeof(RID));
      offset += sizeof(RID);
      log_record->delete_tuple_.DeserializeFrom(data+offset);
      break;
    case LogRecordType::UPDATE:                             // update
      memcpy(log_record+offset, data+offset, sizeof(RID));
      offset += sizeof(RID);
      log_record->old_tuple_.DeserializeFrom(data+offset);
      offset += sizeof(int32_t) + log_record->old_tuple_.GetLength(); // |size|data|
      log_record->new_tuple_.DeserializeFrom(data+offset);
      offset += sizeof(int32_t) + log_record->new_tuple_.GetLength();
      break;
    case LogRecordType::NEWPAGE:                          // newpage
      memcpy(log_record+offset, data+offset, 2*sizeof(page_id_t));
      offset += 2*sizeof(page_id_t);
      break;
    default:
      break;
  }
  assert(log_record->GetSize() == offset);
  return true;
}

/**
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 * 注意:
 *  Redo: 将已commit但尚未写入磁盘的txn 对应数据写入磁盘
 *  如何判断有数据尚未写入磁盘? => page头部的lsn 小于 log_record的lsn !!!
 *  如何判断是 active_txn_ ? => 根据LogRecordType,若某个事务一直都没有commit或者abort,则是active_txn_(俗称ATT)
 */
void LogRecovery::Redo() {
    /**
     * 每次读写log_file都是以 LOG_BUFFER_SIZE 为单位
     * 每个 log_buffer_ 末尾可能有碎片,不优雅,后续可修改 TODO
     */ 
    while(disk_manager_->ReadLog(log_buffer_,LOG_BUFFER_SIZE,file_offset_)){
      file_offset_ += LOG_BUFFER_SIZE;
      // 反序列化
      LogRecord log_record;
      buffer_offset_ = 0;
      while(DeserializeLogRecord(log_buffer_+buffer_offset_,&log_record)){
        buffer_offset_ += log_record.GetSize();
        lsn_mapping_[log_record.GetLSN()] = file_offset_ + buffer_offset_;
        active_txn_[log_record.GetTxnId()] = log_record.GetLSN();
        
        switch (log_record.log_record_type_){
          case LogRecordType::INVALID:
            LOG_WARN("invalid log record type when Redo log");
            continue;
          case LogRecordType::INSERT:                               // insert
            RID insert_rid = log_record.GetInsertRID();
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(insert_rid.GetPageId()));
            // 如果有数据尚未写入
            if(page->GetLSN()<log_record.GetLSN()){
              page->WLatch();
              page->InsertTuple(log_record.GetInsertTuple(),&insert_rid,nullptr,nullptr,nullptr);
              page->WUnlatch();
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
            break;
          case LogRecordType::MARKDELETE:                           // delete
            RID delete_rid = log_record.GetDeleteRID();
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_rid.GetPageId()));
            // 如果有数据尚未写入
            if(page->GetLSN()<log_record.GetLSN()){
              page->WLatch();
              page->MarkDelete(delete_rid,nullptr,nullptr,nullptr);
              page->WUnlatch();
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
            break;
          case LogRecordType::APPLYDELETE:
            RID delete_rid = log_record.GetDeleteRID();
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_rid.GetPageId()));
            // 如果有数据尚未写入
            if(page->GetLSN()<log_record.GetLSN()){
              page->WLatch();
              page->ApplyDelete(delete_rid,nullptr,nullptr);
              page->WUnlatch();
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
            break;
          case LogRecordType::ROLLBACKDELETE:
            RID delete_rid = log_record.GetDeleteRID();
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_rid.GetPageId()));
            // 如果有数据尚未写入
            if(page->GetLSN()<log_record.GetLSN()){
              page->WLatch();
              page->RollbackDelete(delete_rid,nullptr,nullptr);
              page->WUnlatch();
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
            break;
          case LogRecordType::UPDATE:                             // update
            RID update_rid = log_record.GetUpdateRID();
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(update_rid.GetPageId()));
            // 如果有数据尚未写入
            if(page->GetLSN()<log_record.GetLSN()){
              page->WLatch();
              page->UpdateTuple(log_record.GetUpdateTuple(),&log_record.GetOriginalTuple(),update_rid,nullptr,nullptr,nullptr);
              page->WUnlatch();
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
            break;
          case LogRecordType::BEGIN:                              // begin
            break;
          case LogRecordType::COMMIT:                             // commit
          case LogRecordType::ABORT:                              // abort
            // commit/abort了,说明不是活动事务
            active_txn_.erase(log_record.GetTxnId());
            break;
          case LogRecordType::NEWPAGE:                            // newpage
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.page_id_));
            // 如果有数据尚未写入
            if(page->GetLSN()<log_record.GetLSN()){
              page->WLatch();
              page->Init(log_record.page_id_,PAGE_SIZE,log_record.prev_page_id_,nullptr,nullptr);
              page->WUnlatch();
              if (log_record.prev_page_id_ != INVALID_PAGE_ID) { // 重要
                auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.prev_page_id_));
                if (prev_page->GetNextPageId() != log_record.page_id_) {
                  prev_page->SetNextPageId(log_record.page_id_);
                  buffer_pool_manager_->UnpinPage(log_record.prev_page_id_, true);
                } else {
                  buffer_pool_manager_->UnpinPage(log_record.prev_page_id_, false);
                }
              }
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
            break;
          default:
            break;
        }
      }
    }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 * 注意:
 *  Undo: 将尚未commit但已写入磁盘的txn对应数据删除
 *  恢复过程:先Redo,并维护active_txn_,lsn_mapping_ => 然后Undo
 */
void LogRecovery::Undo() {
  for(auto iter=active_txn_.begin(); iter!=active_txn_.end(); iter++){
    // txn_id_t txn_id = iter->first;
    lsn_t lsn = iter->second;           // 未提交事务的最新lsn
    while(lsn!=INVALID_LSN){            // 未提交事务的所有lsn的日志!!!
      int offset = lsn_mapping_[lsn];
      LogRecord log_record;
      // 这里每次读的多,但只反序列化第一个record
      disk_manager_->ReadLog(log_buffer_,LOG_BUFFER_SIZE,offset);
      DeserializeLogRecord(log_buffer_,&log_record);
      lsn = log_record.GetPrevLSN();

      switch (log_record.log_record_type_){
        case LogRecordType::INVALID:
          LOG_WARN("invalid log record type when undo log");
          continue;
        case LogRecordType::INSERT:                               // insert
          RID insert_rid = log_record.GetInsertRID();
          auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(insert_rid.GetPageId()));
          page->WLatch();
          page->ApplyDelete(insert_rid,nullptr,nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
          break;
        case LogRecordType::MARKDELETE:                           // delete
          RID delete_rid = log_record.GetDeleteRID();
          auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_rid.GetPageId()));
          page->WLatch();
          page->RollbackDelete(delete_rid,nullptr,nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
          break;
        case LogRecordType::APPLYDELETE:
          RID delete_rid = log_record.GetDeleteRID();
          auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_rid.GetPageId()));
          page->WLatch();
          page->InsertTuple(log_record.GetDeleteTuple(),&delete_rid,nullptr,nullptr,nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
          break;
        case LogRecordType::ROLLBACKDELETE:
          RID delete_rid = log_record.GetDeleteRID();
          auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_rid.GetPageId()));
          page->WLatch();
          page->MarkDelete(delete_rid,nullptr,nullptr,nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
          break;
        case LogRecordType::UPDATE:                             // update
          RID update_rid = log_record.GetUpdateRID();
          auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(update_rid.GetPageId()));
          page->WLatch();
          page->UpdateTuple(log_record.GetOriginalTuple(),&log_record.GetUpdateTuple(),update_rid,nullptr,nullptr,nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(),page->GetLSN()<log_record.GetLSN());
          break;
        case LogRecordType::BEGIN:                              // begin
        case LogRecordType::COMMIT:                             // commit
        case LogRecordType::ABORT:                              // abort
        case LogRecordType::NEWPAGE:                            // newpage
          break;
        default:
          break;
      }
  
    }

  }
}

}  // namespace bustub
