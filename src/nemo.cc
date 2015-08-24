#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
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

    if (db_path_[db_path_.length() - 1] != '/') {
        db_path_.append("/");
    }
    if (opendir(db_path_.c_str()) == NULL) {
        mkdir(db_path_.c_str(), 0755);
    }
    if (opendir((db_path_ + "kv").c_str()) == NULL) {
        mkdir((db_path_ + "kv").c_str(), 0755);
    }
    if (opendir((db_path_ + "hash").c_str()) == NULL) {
        mkdir((db_path_ + "hash").c_str(), 0755);
    }
    if (opendir((db_path_ + "list").c_str()) == NULL) {
        mkdir((db_path_ + "list").c_str(), 0755);
    }
    if (opendir((db_path_ + "zset").c_str()) == NULL) {
        mkdir((db_path_ + "zset").c_str(), 0755);
    }

    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    // comment write_buffer_size
    //options.write_buffer_size = 1500000000;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path_ + "kv", &db);
    if (!s.ok()) {
        log_err("open kv db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    kv_db_ = std::unique_ptr<rocksdb::DB>(db);

    s = rocksdb::DB::Open(options, db_path_ + "hash", &db);
    if (!s.ok()) {
        log_err("open hash db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    hash_db_ = std::unique_ptr<rocksdb::DB>(db);
    
    s = rocksdb::DB::Open(options, db_path_ + "list", &db);
    if (!s.ok()) {
        log_err("open list db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    list_db_ = std::unique_ptr<rocksdb::DB>(db);

    s = rocksdb::DB::Open(options, db_path_ + "zset", &db);
    if (!s.ok()) {
        log_err("open zset db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        //log_info("open db success");
    }
    zset_db_ = std::unique_ptr<rocksdb::DB>(db);
}
};

