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
    rocksdb::Status s;
    LockHash();
    writer_kv_.writebatch.Clear();
    int ret = hset_one(key, field, val);
    if(ret >= 0){
        if(ret > 0){
            incr_hsize(key, ret);
        }
        s = db_->Write(rocksdb::WriteOptions(), &(writer_hash_.writebatch));
        return s;
    }
    UnlockHash();
    return rocksdb::Status::InvalidArgument("Invalid Argument");
}

rocksdb::Status Nemo::HGet(const std::string &key, const std::string &field, std::string *val){
    std::string dbkey = encode_hash_key(key, field);
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), dbkey, val);
    return s;
}

int64_t Nemo::HSize(const std::string &key) {
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
    if(key.size() == 0 || field.size() == 0){
        log_info("empty key or field!");
        return -1;
    }
    if(key.size() > NEMO_KEY_LEN_MAX ){
        log_info("key too long! %s", hexmem(key.data(), key.size()).c_str());
        return -1;
    }
    if(field.size() > NEMO_KEY_LEN_MAX){
        log_info("field too long! %s", hexmem(field.data(), field.size()).c_str());
        return -1;
    }
    int ret = 0;
    std::string dbval;
    rocksdb::Status s = HGet(key, field, &dbval);
    if(s.IsNotFound()){ // not found
        std::string hkey = encode_hash_key(key, field);
        writer_hash_.writebatch.Put(hkey, val);
        ret = 1;
    }else{
        if(dbval != val){
            std::string hkey = encode_hash_key(key, field);
            writer_hash_.writebatch.Put(hkey, val);
        }
        ret = 0;
    }
    return ret;
}

int Nemo::incr_hsize(const std::string &key, int64_t incr){
    int64_t size = HSize(key);
    size += incr;
    std::string size_key = encode_hsize_key(key);
    if(size == 0){
        writer_hash_.writebatch.Delete(size_key);
    }else{
        writer_hash_.writebatch.Put(size_key, rocksdb::Slice((char *)&size, sizeof(int64_t)));
    }
    return 0;
}
