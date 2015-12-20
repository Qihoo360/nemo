#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <iostream>
#include <algorithm>

#include "nemo.h"
#include "util.h"
#include "xdebug.h"


namespace nemo {

Nemo::Nemo(const std::string &db_path, const Options &options) :
    db_path_(db_path), save_flag_(false)
{
    pthread_mutex_init(&(mutex_kv_), NULL);
    pthread_mutex_init(&(mutex_hash_), NULL);
    pthread_mutex_init(&(mutex_list_), NULL);
    pthread_mutex_init(&(mutex_zset_), NULL);
    pthread_mutex_init(&(mutex_set_), NULL);
    pthread_mutex_init(&(mutex_cursors_), NULL);

    if (db_path_[db_path_.length() - 1] != '/') {
        db_path_.append("/");
    }

    mkpath(db_path_.c_str(), 0755);
    mkpath((db_path_ + "kv").c_str(), 0755);
    mkpath((db_path_ + "hash").c_str(), 0755);
    mkpath((db_path_ + "list").c_str(), 0755);
    mkpath((db_path_ + "zset").c_str(), 0755);
    mkpath((db_path_ + "set").c_str(), 0755);

    open_options_.create_if_missing = true;
    open_options_.write_buffer_size = options.write_buffer_size;
    if (options.target_file_size_base > 0) {
       open_options_.target_file_size_base = (uint64_t)options.target_file_size_base;
    }
    if (options.target_file_size_multiplier > 0) {
        open_options_.target_file_size_multiplier = options.target_file_size_multiplier;
    }
    
    rocksdb::DBWithTTL *db_ttl;
    rocksdb::Status s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "kv", &db_ttl, rocksdb::kMetaPrefix_KV);
    if (!s.ok()) {
        log_err("open kv db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    kv_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

    //rocksdb::DB* db;

    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "hash", &db_ttl, rocksdb::kMetaPrefix_HASH);
    if (!s.ok()) {
        log_err("open hash db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    hash_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);
    
    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "list", &db_ttl, rocksdb::kMetaPrefix_LIST);
    if (!s.ok()) {
        log_err("open list db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    list_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "zset", &db_ttl, rocksdb::kMetaPrefix_ZSET);
    if (!s.ok()) {
        log_err("open zset db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    zset_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

    s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "set", &db_ttl, rocksdb::kMetaPrefix_SET);
    if (!s.ok()) {
        log_err("open set db %s error %s", db_path_.c_str(), s.ToString().c_str());
    }
    set_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);
}
};

