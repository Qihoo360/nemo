#include "nemo.h"
#include "nemo_kv.h"
#include "nemo_iterator.h"
#include "utilities/strings.h"
#include "xdebug.h"
using namespace nemo;

rocksdb::Status Nemo::Set(const std::string &key, const std::string &val) {
    rocksdb::Status s;
    std::string buf = encode_kv_key(key);
    s = db_->Put(rocksdb::WriteOptions(), buf, val);
    return s;
}

rocksdb::Status Nemo::Get(const std::string &key, std::string *val) {
    rocksdb::Status s;
    std::string buf = encode_kv_key(key);
    s = db_->Get(rocksdb::ReadOptions(), buf, val);
    return s;
}

rocksdb::Status Nemo::Del(const std::string &key) {
    rocksdb::Status s;
    std::string en_key = encode_kv_key(key);
    s = db_->Delete(rocksdb::WriteOptions(), en_key);
    return s;
}

rocksdb::Status Nemo::MSet(const std::vector<KV> &kvs) {
    rocksdb::Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    for(it = kvs.begin(); it != kvs.end(); it++) {
        batch.Put(encode_kv_key(it->key), it->val); 
    }
    s = db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;
}

rocksdb::Status Nemo::MDel(const std::vector<std::string> &keys) {
    rocksdb::Status s;
    std::vector<std::string>::const_iterator it;
    rocksdb::WriteBatch batch;
    for(it = keys.begin(); it != keys.end(); it++) {
       batch.Delete(encode_kv_key(*it)); 
    }
    s = db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;

}

rocksdb::Status Nemo::MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss) {
    rocksdb::Status s;
    std::vector<std::string>::const_iterator it_key;
    for(it_key = keys.begin(); it_key != keys.end(); it_key++) {
        std::string en_key = encode_kv_key(*it_key);
        std::string val("");
        s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
        kvss.push_back((KVS){*(it_key), val, s});
    }
    return rocksdb::Status::OK();
}

rocksdb::Status Nemo::Incrby(const std::string &key, int64_t by, std::string &new_val) {
    rocksdb::Status s;
    std::string en_key = encode_kv_key(key);
    std::string val;
    s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
    if(s.IsNotFound()) {
        new_val = int64_to_str(by);        
    } else if(s.ok()) {
        new_val = int64_to_str((str_to_int64(val) + by));
    } else {
        return rocksdb::Status::Corruption("Get error");
    }
    s = db_->Put(rocksdb::WriteOptions(), en_key, new_val);
    return s;
}

rocksdb::Status Nemo::GetSet(const std::string &key, const std::string &new_val, std::string *old_val) {
    rocksdb::Status s;
    std::string en_key = encode_kv_key(key);
    std::string val;
    *old_val = "";
    MutexLock l(&mutex_kv_);
    s = db_->Get(rocksdb::ReadOptions(), en_key, old_val);
    if(!s.ok() && !s.IsNotFound()) {
        return rocksdb::Status::Corruption("Get error");
    }else {
        s = db_->Put(rocksdb::WriteOptions(), en_key, new_val);
        return s;
    }
}

KIterator* Nemo::scan(const std::string &start, const std::string &end, uint64_t limit) {
    std::string key_start, key_end;
    key_start = encode_kv_key(start);
    if(end.empty()){
        key_end = "";
    }else{
        key_end = encode_kv_key(end);
    }
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    if(it->Valid() && it->key() == key_start){
        it->Next();
    }
    return new KIterator(new Iterator(it, key_end, limit)); 
}
