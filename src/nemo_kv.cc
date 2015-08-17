#include "nemo.h"
#include "nemo_kv.h"
#include "nemo_iterator.h"
#include "utilities/util.h"
#include "xdebug.h"
using namespace nemo;

Status Nemo::Set(const std::string &key, const std::string &val) {
    Status s;
    std::string buf = EncodeKvKey(key);
    s = db_->Put(rocksdb::WriteOptions(), buf, val);
    return s;
}

Status Nemo::Get(const std::string &key, std::string *val) {
    Status s;
    std::string buf = EncodeKvKey(key);
    s = db_->Get(rocksdb::ReadOptions(), buf, val);
    return s;
}

Status Nemo::Del(const std::string &key) {
    Status s;
    std::string en_key = EncodeKvKey(key);
    s = db_->Delete(rocksdb::WriteOptions(), en_key);
    return s;
}

Status Nemo::MSet(const std::vector<KV> &kvs) {
    Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = kvs.begin(); it != kvs.end(); it++) {
        batch.Put(EncodeKvKey(it->key), it->val); 
    }
    s = db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;
}

Status Nemo::MDel(const std::vector<std::string> &keys) {
    Status s;
    std::vector<std::string>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = keys.begin(); it != keys.end(); it++) {
       batch.Delete(EncodeKvKey(*it)); 
    }
    s = db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;

}

Status Nemo::MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss) {
    Status s;
    std::vector<std::string>::const_iterator it_key;
    for (it_key = keys.begin(); it_key != keys.end(); it_key++) {
        std::string en_key = EncodeKvKey(*it_key);
        std::string val("");
        s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
        kvss.push_back((KVS){*(it_key), val, s});
    }
    return Status::OK();
}

Status Nemo::Incrby(const std::string &key, int64_t by, std::string &new_val) {
    Status s;
    std::string en_key = EncodeKvKey(key);
    std::string val;
    s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);        
    } else if (s.ok()) {
        int64_t ival;
        StrToInt64(val.data(), val.size(), &ival); 
        new_val = std::to_string(ival + by);
    } else {
        return Status::Corruption("Get error");
    }
    s = db_->Put(rocksdb::WriteOptions(), en_key, new_val);
    return s;
}

Status Nemo::Decrby(const std::string &key, int64_t by, std::string &new_val) {
    Status s;
    std::string en_key = EncodeKvKey(key);
    std::string val;
    s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);        
    } else if (s.ok()) {
        int64_t ival;
        StrToInt64(val.data(), val.size(), &ival); 
        new_val = std::to_string(ival - by);
    } else {
        return Status::Corruption("Get error");
    }
    s = db_->Put(rocksdb::WriteOptions(), en_key, new_val);
    return s;
}

Status Nemo::GetSet(const std::string &key, const std::string &new_val, std::string *old_val) {
    Status s;
    std::string en_key = EncodeKvKey(key);
    std::string val;
    *old_val = "";
    MutexLock l(&mutex_kv_);
    s = db_->Get(rocksdb::ReadOptions(), en_key, old_val);
    if (!s.ok() && !s.IsNotFound()) {
        return Status::Corruption("Get error");
    }else {
        s = db_->Put(rocksdb::WriteOptions(), en_key, new_val);
        return s;
    }
}

KIterator* Nemo::Scan(const std::string &start, const std::string &end, uint64_t limit) {
    std::string key_start, key_end;
    key_start = EncodeKvKey(start);
    if (end.empty()) {
        key_end = "";
    } else {
        key_end = EncodeKvKey(end);
    }
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    if (it->Valid() && it->key() == key_start) {
        it->Next();
    }
    return new KIterator(new Iterator(it, key_end, limit)); 
}
