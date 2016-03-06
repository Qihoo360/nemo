#include "nemo.h"

#include <dirent.h>
#include <iostream>
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#include "nemo_list.h"
#include "nemo_zset.h"
#include "nemo_set.h"
#include "nemo_hash.h"
#include "port.h"
#include "util.h"
#include "xdebug.h"

namespace nemo {

Nemo::Nemo(const std::string &db_path, const Options &options)
    : db_path_(db_path),
    bgtask_flag_(true),
    bg_cv_(&mutex_bgtask_),
    save_flag_(false),
    dump_to_terminate_(false) {

   pthread_mutex_init(&(mutex_cursors_), NULL);
   pthread_mutex_init(&(mutex_dump_), NULL);

   if (db_path_[db_path_.length() - 1] != '/') {
     db_path_.append("/");
   }

   mkpath(db_path_.c_str(), 0755);
   mkpath((db_path_ + "kv").c_str(), 0755);
   mkpath((db_path_ + "hash").c_str(), 0755);
   mkpath((db_path_ + "list").c_str(), 0755);
   mkpath((db_path_ + "zset").c_str(), 0755);
   mkpath((db_path_ + "set").c_str(), 0755);

   cursors_store_.cur_size_ = 0;
   cursors_store_.max_size_ = 5000;
   cursors_store_.list_.clear();
   cursors_store_.map_.clear();

   // Open Options
   open_options_.create_if_missing = true;
   open_options_.write_buffer_size = options.write_buffer_size;
   if (!options.compression) {
     open_options_.compression = rocksdb::CompressionType::kNoCompression;
   }
   if (options.target_file_size_base > 0) {
     open_options_.target_file_size_base = (uint64_t)options.target_file_size_base;
   }
   if (options.target_file_size_multiplier > 0) {
     open_options_.target_file_size_multiplier = options.target_file_size_multiplier;
   }

   rocksdb::DBWithTTL *db_ttl;

   open_options_.meta_prefix = rocksdb::kMetaPrefix_KV;
   rocksdb::Status s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "kv", &db_ttl);
   if (!s.ok()) {
     log_err("open kv db %s error %s", db_path_.c_str(), s.ToString().c_str());
   }
   kv_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_HASH;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "hash", &db_ttl);
   if (!s.ok()) {
     log_err("open hash db %s error %s", db_path_.c_str(), s.ToString().c_str());
   }
   hash_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_LIST;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "list", &db_ttl);
   if (!s.ok()) {
     log_err("open list db %s error %s", db_path_.c_str(), s.ToString().c_str());
   }
   list_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_ZSET;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "zset", &db_ttl);
   if (!s.ok()) {
     log_err("open zset db %s error %s", db_path_.c_str(), s.ToString().c_str());
   }
   zset_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_SET;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "set", &db_ttl);
   if (!s.ok()) {
     log_err("open set db %s error %s", db_path_.c_str(), s.ToString().c_str());
   }
   set_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   // Add separator of Meta and data
   hash_db_->Put(rocksdb::WriteOptions(), "h", "");
   list_db_->Put(rocksdb::WriteOptions(), "l", "");
   zset_db_->Put(rocksdb::WriteOptions(), "y", "");
   zset_db_->Put(rocksdb::WriteOptions(), "z", "");
   set_db_->Put(rocksdb::WriteOptions(), "s", "");

   // Start BGThread
   s = StartBGThread();
   if (!s.ok()) {
     log_err("start bg thread error: %s", s.ToString().c_str());
   }

}


bool NemoMeta::Create(DBType type, MetaPtr &p_meta){
  switch (type) {
    case kHASH_DB:
      p_meta.reset(new HashMeta());
      break;
    case kLIST_DB:
      p_meta.reset(new ListMeta());
      break;
    case kSET_DB:
      p_meta.reset(new SetMeta());
      break;
    case kZSET_DB:
      p_meta.reset(new ZSetMeta());
    default:
      return false;
  }
  return true;
}

}   // namespace nemo
