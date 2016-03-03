#include "nemo.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <iostream>
#include <algorithm>
//#include "nemo_options.h"

#include "port.h"
#include "util.h"
#include "xdebug.h"


namespace nemo {

Nemo::Nemo(const std::string &db_path, const Options &options) :
    db_path_(db_path),
    bgtask_flag_(false),
    bg_cv_(&mutex_bgtask_),
    save_flag_(false),
    dump_to_terminate_(false)
{
   // pthread_mutex_init(&(mutex_kv_), NULL);
   // pthread_mutex_init(&(mutex_hash_), NULL);
   // pthread_mutex_init(&(mutex_list_), NULL);
   // pthread_mutex_init(&(mutex_zset_), NULL);
   // pthread_mutex_init(&(mutex_set_), NULL);
    pthread_mutex_init(&(mutex_cursors_), NULL);
    pthread_mutex_init(&(mutex_dump_), NULL);
    //pthread_mutex_init(&(mutex_bgtask_), NULL);

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
    } else {
        //log_info("open db success");
    }
    kv_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

    //rocksdb::DB* db;

    open_options_.meta_prefix = rocksdb::kMetaPrefix_HASH;
    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "hash", &db_ttl);
    if (!s.ok()) {
        log_err("open hash db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    hash_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);
    
    open_options_.meta_prefix = rocksdb::kMetaPrefix_LIST;
    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "list", &db_ttl);
    if (!s.ok()) {
        log_err("open list db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    list_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

    open_options_.meta_prefix = rocksdb::kMetaPrefix_ZSET;
    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "zset", &db_ttl);
    if (!s.ok()) {
        log_err("open zset db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
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

}
};

