#include "nemo.h"
#include "nemo_hash.h"
#include "nemo_iterator.h"
#include "utilities/strings.h"
#include "xdebug.h"
using namespace nemo;

void Nemo::LockHash() {
    pthread_mutex_lock(&(writer_hash_.writer_mutex));
}

void Nemo::UnlockHash() {
    pthread_mutex_unlock(&(writer_hash_.writer_mutex));
}

rocksdb::Status Nemo::HSet(const std::string &key, const std::string &field, const std::string &val) {
    if(key.size() == 0 || field.size() == 0) {
        log_info("empty key or field!");
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    rocksdb::Status s;
    LockHash();
    writer_hash_.writebatch.Clear();
    int ret = hset_one(key, field, val);
    if(ret > 0){
        if(incr_hsize(key, ret) == -1) {
            UnlockHash();
            return rocksdb::Status::Corruption("incr_hsize error");
        }
    }
    s = db_->Write(rocksdb::WriteOptions(), &(writer_hash_.writebatch));
    UnlockHash();
    return s;
}

rocksdb::Status Nemo::HGet(const std::string &key, const std::string &field, std::string *val) {
    if(key.size() == 0 || field.size() == 0) {
        log_info("empty key or field!");
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    std::string dbkey = encode_hash_key(key, field);
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), dbkey, val);
    return s;
}

rocksdb::Status Nemo::HDel(const std::string &key, const std::string &field) {
    if(key.size() == 0 || field.size() == 0) {
        log_info("empty key or field!");
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    rocksdb::Status s;
    LockHash();
    writer_hash_.writebatch.Clear();
    int ret = hdel_one(key, field);
    if(ret > 0) {
        if(incr_hsize(key, -ret) == -1) {
            UnlockHash();
            return rocksdb::Status::Corruption("incr_hsize error");
        } 
        rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &(writer_hash_.writebatch));
        return s;
    }else if (ret == 0) {
        return rocksdb::Status::OK(); 
    }else {
        return rocksdb::Status::Corruption("hdel_one error");
    }
}

int64_t Nemo::HSize(const std::string &key) {
    if(key.size() == 0) {
        log_info("empty key");
        return -1;
    }
    std::string size_key = encode_hsize_key(key);
    std::string val;
    rocksdb::Status s;

    s = db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if(s.IsNotFound()){
        return 0;
    }else if(!s.ok()){
        return -1;
    }else{
        if(val.size() != sizeof(uint64_t)){
            return 0;
        }
        int64_t ret = *(int64_t *)val.data();
        return ret < 0? 0 : ret;
    }
}

int Nemo::hset_one(const std::string &key, const std::string &field, const std::string &val){
    int ret = 0;
    std::string dbval;
    rocksdb::Status s = HGet(key, field, &dbval);
    if(s.IsNotFound()) { // not found
        std::string hkey = encode_hash_key(key, field);
        writer_hash_.writebatch.Put(hkey, val);
        ret = 1;
    }else {
        if(dbval != val){
            std::string hkey = encode_hash_key(key, field);
            writer_hash_.writebatch.Put(hkey, val);
        }
        ret = 0;
    }
    return ret;
}

int Nemo::hdel_one(const std::string &key, const std::string &field) {
    int ret = 0;
    std::string dbval;
    rocksdb::Status s = HGet(key, field, &dbval);
    if(s.ok()) { 
        std::string hkey = encode_hash_key(key, field);
        writer_hash_.writebatch.Delete(hkey);
        ret = 1;
    }else if(s.IsNotFound()) {
        ret = 0;
    }else {
        ret = -1;
    }
    return ret;
}

int Nemo::incr_hsize(const std::string &key, int64_t incr){
    int64_t size = HSize(key);
    if(size == -1) {
        return -1;
    }
    size += incr;
    std::string size_key = encode_hsize_key(key);
    if(size == 0) {
        writer_hash_.writebatch.Delete(size_key);
    }else{
        writer_hash_.writebatch.Put(size_key, rocksdb::Slice((char *)&size, sizeof(int64_t)));
    }
    return 0;
}
