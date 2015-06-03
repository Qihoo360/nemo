#include "nemo.h"
#include "xdebug.h"

#include <iostream>

namespace nemo {

//using namespace nemo;

Nemo::Nemo(const std::string &db_path, rocksdb::Options options) :
    db_path_(db_path)
{
    pthread_mutex_init(&(writer_kv_.writer_mutex), NULL);
    pthread_mutex_init(&(writer_hash_.writer_mutex), NULL);
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path_, &db);
    if (!s.ok()) {
        log_err("open db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        log_info("open db success");
    }
    
    db_ = std::unique_ptr<rocksdb::DB>(db);
}
};

