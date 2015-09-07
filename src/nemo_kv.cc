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
        if (!StrToInt64(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not a integer");
        } 
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
        if (!StrToInt64(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not a integer");
        } 
        new_val = std::to_string(ival - by);
    } else {
        return Status::Corruption("Get error");
    }
    s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    return s;
}

Status Nemo::Incrbyfloat(const std::string &key, double by, std::string &new_val) {
    Status s;
    std::string val;
    std::string res;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);        
    } else if (s.ok()) {
        double ival;
        if (!StrToDouble(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not a float");
        } 
        res = std::to_string(ival + by);
    } else {
        return Status::Corruption("Get error");
    }
    size_t pos = res.find_last_not_of("0", res.size());
    pos = pos == std::string::npos ? pos : pos+1;
    new_val = res.substr(0, pos); 
    if (new_val[new_val.size()-1] == '.') {
        new_val = new_val.substr(0, new_val.size()-1);
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

Status Nemo::Append(const std::string &key, const std::string &value, int64_t *new_len) {
    Status s;
    *new_len = 0;
    std::string old_val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &old_val);
    if (s.ok()) {
        std::string new_val = old_val.append(value);
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val); 
        *new_len = new_val.length();
    } else if (s.IsNotFound()) {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, value); 
        *new_len = value.length();
    }
    return s;
}

Status Nemo::Setnx(const std::string &key, const std::string &value, int64_t *ret) {
    *ret = 0;
    std::string val;
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, value);
        *ret = 1;
    }
    return s;
}

Status Nemo::MSetnx(const std::vector<KV> &kvs, int64_t *ret) {
    Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    std::string val;
    *ret = 1;
    for (it = kvs.begin(); it != kvs.end(); it++) {
        s = kv_db_->Get(rocksdb::ReadOptions(), it->key, &val);
        if (s.ok()) {
            *ret = 0;
            break;
        }
        batch.Put(it->key, it->val); 
    }
    if (*ret == 1) {
        s = kv_db_->Write(rocksdb::WriteOptions(), &(batch));
    }
    return s;
}

Status Nemo::Getrange(const std::string key, int64_t start, int64_t end, std::string &substr) {
    substr = "";
    std::string val;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        int64_t size = val.length();
        int64_t start_t = start >= 0 ? start : size + start;
        int64_t end_t = end >= 0 ? end : size + end;
        if (start_t > end_t || start_t > size -1 || end_t < 0) {
            return Status::OK();
        }
        if (start_t < 0) {
            start_t  = 0;
        }
        if (end_t >= size) {
            end_t = size - 1;
        }
        substr = val.substr(start_t, end_t-start_t+1);
    }
    return s;
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
