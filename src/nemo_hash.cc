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
        s = db_->Write(rocksdb::WriteOptions(), &(writer_hash_.writebatch));
        UnlockHash();
        return s;
    }else if (ret == 0) {
        UnlockHash();
        return rocksdb::Status::OK(); 
    }else {
        UnlockHash();
        return rocksdb::Status::Corruption("hdel_one error");
    }
}

bool Nemo::HExists(const std::string &key, const std::string &field) {
    rocksdb::Status s;
    std::string dbkey = encode_hash_key(key, field);
    std::string val;
    s = db_->Get(rocksdb::ReadOptions(), dbkey, &val);
    if(s.ok()) {
        return true;
    }else {
        return false;
    }
}

rocksdb::Status Nemo::HKeys(const std::string &key, std::vector<std::string> &keys) {
    if(key.size() == 0) {
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    std::string dbkey;
    std::string dbfield;
    std::string key_start = encode_hash_key(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while(it->Valid()) {
       if((it->key()).data()[0] != DataType::HASH){
           break;
       }
       decode_hash_key(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           keys.push_back(dbfield);
       }else {
           break;
       }
       it->Next();
    }
    return rocksdb::Status::OK();

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

rocksdb::Status Nemo::HGetall(const std::string &key, std::vector<Kv> &kvs) {
    if(key.size() == 0) {
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    std::string dbkey;
    std::string dbfield;
    std::string key_start = encode_hash_key(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while(it->Valid()) {
       if((it->key()).data()[0] != DataType::HASH){
           break;
       }
       decode_hash_key(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           kvs.push_back(Kv{dbkey, dbfield});
       }else {
           break;
       }
       it->Next();
    }
    return rocksdb::Status::OK();
}

int64_t Nemo::HLen(const std::string &key) {
    if(key.size() == 0) {
        return -1;
    }
    int64_t len = 0;
    std::string dbkey;
    std::string dbfield;
    std::string key_start = encode_hash_key(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while(it->Valid()) {
       if((it->key()).data()[0] != DataType::HASH){
           break;
       }
       decode_hash_key(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           len++;
       }else {
           break;
       }
       it->Next();
    }
    return len;
}

rocksdb::Status Nemo::HMSet(const std::string &key, const std::vector<Kv> &kvs) {
    if(key.size() == 0) {
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    rocksdb::Status s;
    std::vector<Kv>::const_iterator it;
    rocksdb::WriteBatch batch;
    for(it = kvs.begin(); it != kvs.end(); it++) {
        HSet(key, it->key, it->val);
    }
    return s;
}

rocksdb::Status Nemo::HMGet(const std::string &key, const std::vector<std::string> &keys, std::vector<Kvs> &kvss) {
    rocksdb::Status s;
    std::vector<std::string>::const_iterator it_key;
    for(it_key = keys.begin(); it_key != keys.end(); it_key++) {
        std::string en_key = encode_hash_key(key, *(it_key));
        std::string val("");
        s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
        kvss.push_back((Kvs){*(it_key), val, s});
    }
    return rocksdb::Status::OK();
}

HIterator* Nemo::HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit) {
    std::string key_start, key_end;
    key_start = encode_hash_key(key, start);
    if(end.empty()) {
        key_end = "";
    }else {
        key_end = encode_hash_key(key, end);
    }
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    if(it->Valid() && it->key() == key_start){
        it->Next();
    }
    return new HIterator(new Iterator(it, key_end, limit), key); 
}

int Nemo::hset_one(const std::string &key, const std::string &field, const std::string &val) {
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
