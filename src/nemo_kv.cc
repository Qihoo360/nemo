#include "nemo.h"
#include "nemo_iterator.h"
#include "utilities/util.h"
#include "xdebug.h"
using namespace nemo;

Status Nemo::Set(const std::string &key, const std::string &val) {
    Status s;
    s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
    return s;
}

Status Nemo::Get(const std::string &key, std::string *val) {
    Status s;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, val);
    return s;
}

Status Nemo::Del(const std::string &key) {
    Status s;
    s = kv_db_->Delete(rocksdb::WriteOptions(), key);
    return s;
}

Status Nemo::MSet(const std::vector<KV> &kvs) {
    Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = kvs.begin(); it != kvs.end(); it++) {
        batch.Put(it->key, it->val); 
    }
    s = kv_db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;
}

Status Nemo::MDel(const std::vector<std::string> &keys, int64_t* count) {
    *count = 0;
    Status s;
    std::string val;
    std::vector<std::string>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = keys.begin(); it != keys.end(); it++) {
        s = kv_db_->Get(rocksdb::ReadOptions(), *it, &val);
        if (s.ok()) {
            (*count)++;
            batch.Delete(*it); 
        }
    }
    s = kv_db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;

}

Status Nemo::MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss) {
    Status s;
    std::vector<std::string>::const_iterator it_key;
    for (it_key = keys.begin(); it_key != keys.end(); it_key++) {
        std::string val("");
        s = kv_db_->Get(rocksdb::ReadOptions(), *it_key, &val);
        kvss.push_back((KVS){*(it_key), val, s});
    }
    return Status::OK();
}

Status Nemo::Incrby(const std::string &key, int64_t by, std::string &new_val) {
    Status s;
    std::string val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);        
    } else if (s.ok()) {
        int64_t ival;
        StrToInt64(val.data(), val.size(), &ival); 
        new_val = std::to_string(ival + by);
    } else {
        return Status::Corruption("Get error");
    }
    s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    return s;
}

Status Nemo::Decrby(const std::string &key, int64_t by, std::string &new_val) {
    Status s;
    std::string val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);        
    } else if (s.ok()) {
        int64_t ival;
        StrToInt64(val.data(), val.size(), &ival); 
        new_val = std::to_string(ival - by);
    } else {
        return Status::Corruption("Get error");
    }
    s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    return s;
}

Status Nemo::GetSet(const std::string &key, const std::string &new_val, std::string *old_val) {
    Status s;
    std::string val;
    *old_val = "";
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, old_val);
    if (!s.ok() && !s.IsNotFound()) {
        return Status::Corruption("Get error");
    }else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
        return s;
    }
}

KIterator* Nemo::Scan(const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot) {
    std::string key_end;
    if (end.empty()) {
        key_end = "";
    } else {
        key_end = end;
    }
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    if (use_snapshot) {
        iterate_options.snapshot = kv_db_->GetSnapshot();
    }
    iterate_options.fill_cache = false;
    it = kv_db_->NewIterator(iterate_options);
    it->Seek(start);
    return new KIterator(new Iterator(it, key_end, limit, iterate_options)); 
}
