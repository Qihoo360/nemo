#include "nemo.h"
#include "xdebug.h"

#include <iostream>

namespace nemo {

Nemo::Nemo(const std::string &db_path) :
    db_path_(db_path)
{
    pthread_mutex_init(&(mutex_kv_), NULL);
    pthread_mutex_init(&(mutex_hash_), NULL);
    pthread_mutex_init(&(mutex_list_), NULL);
    pthread_mutex_init(&(mutex_zset_), NULL);
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1500000000;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path_.append("kv"), &db);
    if (!s.ok()) {
        log_err("open kv db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    kv_db_ = std::unique_ptr<rocksdb::DB>(db);

    s = rocksdb::DB::Open(options, db_path_.append("szlh"), &db);
    if (!s.ok()) {
        log_err("open szlh db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    db_ = std::unique_ptr<rocksdb::DB>(db);
}
};

