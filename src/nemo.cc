#include "nemo.h"
#include "xdebug.h"

#include <iostream>

namespace nemo {

using namespace rocksdb;

Nemo::Nemo(const std::string &db_path, Options options) :
    db_path_(db_path)
{
    DB* db;
    Status s = DB::Open(options, db_path_, &db);
    if (!s.ok()) {
        log_err("open db %s error %s", db_path_.c_str(), s.ToString().c_str());
    } else {
        log_info("open db success");
    }
    
    db_ = std::unique_ptr<DB>(db);
}

Status Nemo::HSet(const std::string &key, const std::string &field, const std::string &val)
{
    Status s;
    s = db_->Put(rocksdb::WriteOptions(), key + field, val);
    log_info("val %s", val.c_str());
    return s;
}

Status Nemo::HGet(const std::string &key, const std::string &field, std::string *val)
{
    Status s;

    s = db_->Get(rocksdb::ReadOptions(), key + field, val);
    log_info("val %s", val->c_str());

    return s;
}

Status Nemo::HKeys(const std::string &key, std::vector<std::string> *fields)
{
    Status s;
    rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(key); it->Valid() && it->key().ToString() < key + '$'; it->Next()) {
        log_info("key %s val %s", it->key().ToString().c_str(), it->value().ToString().c_str());
    }
    assert(it->status().ok());
    delete it;
    return s;
}

};

