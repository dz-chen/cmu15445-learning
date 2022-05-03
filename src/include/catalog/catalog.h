#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;       // table object identifier
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;                     // table 的属性列
  std::string name_;                  // 表名
  std::unique_ptr<TableHeap> table_;  // TableHeap represents a physical table on disk, This is just a doubly-linked list of pages.
  table_oid_t oid_;                   // table 的 id
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;               // 用于索引的key的schema
  std::string name_;                // 索引名
  std::unique_ptr<Index> index_;    // Index内含 IndexMetadata
  index_oid_t index_oid_;           // 索引id
  std::string table_name_;          // 本索引属于哪个表
  const size_t key_size_;           // 用于索引的key的大小
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    names_[table_name] = next_table_oid_;
    // TODO:TableMetadata 中的 TableHeap成员如何解决 ? 如何知道 first_page_id_ ?
    tables_[next_table_oid_] = std::make_unique<TableMetadata>(schema,table_name,nullptr,next_table_oid_);
    next_table_oid_++;
    return tables_[next_table_oid_].get();
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    BUSTUB_ASSERT(names_.count(table_name) > 0, "input table name does not exist!");
    table_oid_t oid = names_[table_name];
    return tables_[oid].get();
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    BUSTUB_ASSERT(tables_.count(table_oid) > 0, "input table oid does not exist!");
    return tables_[table_oid].get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes,key中的各attr 在table schema中是第几个attr
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    BUSTUB_ASSERT(names_.count(table_name) > 0, "input table name does not exist!");
    // 添加 index_names_
    if(index_names_.count(table_name)==0){      // 如果该table暂时创建过索引
      std::unordered_map<std::string, index_oid_t> tmpMap;
      tmpMap[index_name] = next_index_oid_;
      index_names_[table_name] = tmpMap;
    }
    else{                                       // 在该表上建立过索引
      index_names_[table_name][index_name] = next_index_oid_;
    }

    // 添加 indexes_
    IndexMetadata* meta_data = new IndexMetadata(index_name,table_name,&schema,key_attrs);
    std::unique_ptr<Index> index(new Index(meta_data));     // TODO:Index是抽象函数,此处该如何解决 ?
    indexes_[next_index_oid_] = std::make_unique<IndexInfo>(key_schema,index_name,index,next_table_oid_,table_name,keysize);
    next_index_oid_++;
    return indexes_[next_index_oid_].get();
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    BUSTUB_ASSERT(names_.count(table_name) > 0, "input table name does not exist!");
    index_oid_t oid = index_names_[table_name][index_name];
    return indexes_[oid].get();
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    BUSTUB_ASSERT(indexes_.count(index_oid) > 0, "input index oid does not exist!");
    return indexes_[index_oid].get();
  }

  // 返回一个table的所有索引
  std::vector<IndexInfo*> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo*> res;
    for(auto it=index_names_[table_name].begin(); it!=index_names_[table_name].end(); it++){
      index_oid_t oid = it->second;
      IndexInfo* info = indexes_[oid].get();
      if(info->table_name_ == table_name) res.push_back(info);
    }
    return res;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};


  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers;  注意这里是两个嵌套的unordered_map! */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
