#include <climits>
#include <ctime>

#include "nemo.h"
#include "nemo_hash.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

Status Nemo::HSet(const std::string &key, const std::string &field, const std::string &val) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    MutexLock l(&mutex_hash_);
    rocksdb::WriteBatch writebatch;

    int ret = DoHSet(key, field, val, writebatch);
    if (ret > 0) {
        if (IncrHLen(key, ret, writebatch) == -1) {
            return Status::Corruption("incrhlen error");
        }
    }
    s = hash_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(writebatch));
    return s;
}

Status Nemo::HSetNoLock(const std::string &key, const std::string &field, const std::string &val) {
    Status s;
    rocksdb::WriteBatch writebatch;
    int ret = DoHSet(key, field, val, writebatch);
    if (ret > 0) {
        if (IncrHLen(key, ret, writebatch) == -1) {
            return Status::Corruption("incrhlen error");
        }
    }
    s = hash_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(writebatch));
    return s;
}

Status Nemo::HGet(const std::string &key, const std::string &field, std::string *val) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    std::string dbkey = EncodeHashKey(key, field);
    Status s = hash_db_->Get(rocksdb::ReadOptions(), dbkey, val);
    return s;
}

Status Nemo::HDel(const std::string &key, const std::string &field) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    MutexLock l(&mutex_hash_);
    rocksdb::WriteBatch writebatch;
    int ret = DoHDel(key, field, writebatch);
    if (ret > 0) {
        if (IncrHLen(key, -ret, writebatch) == -1) {
            return Status::Corruption("incrlen error");
        }
        s = hash_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(writebatch));
        return s;
    } else if (ret == 0) {
        return Status::NotFound(); 
    } else {
        return Status::Corruption("DoHDel error");
    }
}

Status Nemo::HDelKey(const std::string &key) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;
    std::string size_key = EncodeHsizeKey(key);

    s = hash_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (!s.ok()) {
        return s;
    }

    int64_t len = *(int64_t *)val.data();
    if (len <= 0) {
      return Status::NotFound("");
    }

    len = 0;
    MutexLock l(&mutex_hash_);
    s = hash_db_->PutWithKeyVersion(rocksdb::WriteOptions(), size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));

    return s;
}

Status Nemo::HExpire(const std::string &key, const int32_t seconds, int64_t *res) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    std::string size_key = EncodeHsizeKey(key);
    s = hash_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        return Status::NotFound("");
      }

      if (seconds > 0) {
        MutexLock l(&mutex_hash_);
        s = hash_db_->PutWithKeyTTL(rocksdb::WriteOptions(), size_key, val, seconds);
      } else { 
        s = HDelKey(key);
      }
      *res = 1;
    }
    return s;
}

Status Nemo::HTTL(const std::string &key, int64_t *res) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    std::string size_key = EncodeHsizeKey(key);
    s = hash_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = -2;
    } else if (s.ok()) {
        int32_t ttl;
        s = hash_db_->GetKeyTTL(rocksdb::ReadOptions(), size_key, &ttl);
        *res = ttl;
    }
    return s;
}

bool Nemo::HExists(const std::string &key, const std::string &field) {
    Status s;
    std::string dbkey = EncodeHashKey(key, field);
    std::string val;
    s = hash_db_->Get(rocksdb::ReadOptions(), dbkey, &val);
    if (s.ok()) {
        return true;
    } else {
        return false;
    }
}

Status Nemo::HPersist(const std::string &key, int64_t *res) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    *res = 0;
    std::string size_key = EncodeHsizeKey(key);
    s = hash_db_->Get(rocksdb::ReadOptions(), size_key, &val);

    if (s.ok()) {
        int32_t ttl;
        s = hash_db_->GetKeyTTL(rocksdb::ReadOptions(), size_key, &ttl);
        if (ttl >= 0) {
            MutexLock l(&mutex_hash_);
            s = hash_db_->Put(rocksdb::WriteOptions(), size_key, val);
            *res = 1;
        }
    }
    return s;
}

Status Nemo::HExpireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    std::string size_key = EncodeHsizeKey(key);
    s = hash_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        return Status::NotFound("");
      }

      std::time_t cur = std::time(0);
      if (timestamp <= cur) {
        s = HDelKey(key);
      } else {
        MutexLock l(&mutex_hash_);
        s = hash_db_->PutWithExpiredTime(rocksdb::WriteOptions(), size_key, val, timestamp);
      }
      *res = 1;
    }
    return s;
}

Status Nemo::HKeys(const std::string &key, std::vector<std::string> &fields) {
    std::string dbkey;
    std::string dbfield;
    std::string key_start = EncodeHashKey(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.snapshot = hash_db_->GetSnapshot();
    iterate_options.fill_cache = false;
    it = hash_db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while (it->Valid()) {
       if ((it->key()).data()[0] != DataType::kHash) {
           break;
       }
       DecodeHashKey(it->key(), &dbkey, &dbfield);
       if (dbkey == key) {
           fields.push_back(dbfield);
       } else {
           break;
       }
       it->Next();
    }
    hash_db_->ReleaseSnapshot(iterate_options.snapshot);
    delete it;
    return Status::OK();
}

int64_t Nemo::HLen(const std::string &key) {
    std::string size_key = EncodeHsizeKey(key);
    std::string val;
    Status s;

    s = hash_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        return 0;
    } else if(!s.ok()) {
        return -1;
    } else {
        if (val.size() != sizeof(uint64_t)) {
            return 0;
        }
        int64_t ret = *(int64_t *)val.data();
        return ret < 0? 0 : ret;
    }
}

Status Nemo::HGetall(const std::string &key, std::vector<FV> &fvs) {
    std::string dbkey;
    std::string dbfield;
    std::string key_start = EncodeHashKey(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.snapshot = hash_db_->GetSnapshot();
    iterate_options.fill_cache = false;
    it = hash_db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while (it->Valid()) {
       if ((it->key()).data()[0] != DataType::kHash) {
           break;
       }
       DecodeHashKey(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           fvs.push_back(FV{dbfield, it->value().ToString()});
       } else {
           break;
       }
       it->Next();
    }
    hash_db_->ReleaseSnapshot(iterate_options.snapshot);
    delete it;
    return Status::OK();
}

//int64_t Nemo::HLen(const std::string &key) {
//    int64_t len = 0;
//    std::string dbkey;
//    std::string dbfield;
//    std::string key_start = EncodeHashKey(key, "");
//    rocksdb::Iterator *it;
//    rocksdb::ReadOptions iterate_options;
//    iterate_options.fill_cache = false;
//    it = hash_db_->NewIterator(iterate_options);
//    it->Seek(key_start);
//    while (it->Valid()) {
//       if((it->key()).data()[0] != DataType::kHash) { 
//           break;
//       }
//       DecodeHashKey(it->key(), &dbkey, &dbfield);
//       if (dbkey == key) {
//           len++;
//       } else {
//           break;
//       }
//       it->Next();
//    }
//    return len;
//}

Status Nemo::HMSet(const std::string &key, const std::vector<FV> &fvs) {
    if (key.size() == 0 || key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }
    Status s;
    std::vector<FV>::const_iterator it;
    for (it = fvs.begin(); it != fvs.end(); it++) {
        HSet(key, it->field, it->val);
    }
    return s;
}

Status Nemo::HMGet(const std::string &key, const std::vector<std::string> &fields, std::vector<FVS> &fvss) {
    Status s;
    std::vector<std::string>::const_iterator it_key;
    for (it_key = fields.begin(); it_key != fields.end(); it_key++) {
        std::string en_key = EncodeHashKey(key, *(it_key));
        std::string val("");
        s = hash_db_->Get(rocksdb::ReadOptions(), en_key, &val);
        fvss.push_back((FVS){*(it_key), val, s});
    }
    return Status::OK();
}

HIterator* Nemo::HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot) {
    std::string key_start, key_end;
    key_start = EncodeHashKey(key, start);
    if (end.empty()) {
        key_end = "";
    } else {
        key_end = EncodeHashKey(key, end);
    }
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    if (use_snapshot) {
        iterate_options.snapshot = hash_db_->GetSnapshot();
    }
    iterate_options.fill_cache = false;
    it = hash_db_->NewIterator(iterate_options);
    it->Seek(key_start);
    return new HIterator(new Iterator(it, key_end, limit, iterate_options), key); 
}

Status Nemo::HSetnx(const std::string &key, const std::string &field, const std::string &val) {
    Status s;
    std::string str_val;
    MutexLock l(&mutex_hash_);
    s = HGet(key, field, &str_val);
    if (s.IsNotFound()) {
        rocksdb::WriteBatch writebatch;
        int ret = DoHSet(key, field, val, writebatch);
        if (ret > 0) {
            if (IncrHLen(key, ret, writebatch) == -1) {
                return Status::Corruption("incrhlen error");
            }
        }
        s = hash_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(writebatch));
        return s;
    } else if(s.ok()) {
        return Status::Corruption("Already Exist");
    } else {
        return Status::Corruption("HGet Error");
    }
}

int64_t Nemo::HStrlen(const std::string &key, const std::string &field) {
    Status s;
    std::string val;
    s = HGet(key, field, &val);
    if (s.ok()) {
        return val.length();
    } else if (s.IsNotFound()) {
        return 0;
    } else {
        return -1;
    }
}

Status Nemo::HVals(const std::string &key, std::vector<std::string> &vals) {
    std::string dbkey;
    std::string dbfield;
    std::string key_start = EncodeHashKey(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.snapshot = hash_db_->GetSnapshot();
    iterate_options.fill_cache = false;
    it = hash_db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while (it->Valid()) {
       if ((it->key()).data()[0] != DataType::kHash) {
           break;
       }
       DecodeHashKey(it->key(), &dbkey, &dbfield);
       if (dbkey == key) {
           vals.push_back(it->value().ToString());
       } else {
           break;
       }
       it->Next();
    }
    hash_db_->ReleaseSnapshot(iterate_options.snapshot);
    delete it;
    return Status::OK();
}

Status Nemo::HIncrby(const std::string &key, const std::string &field, int64_t by, std::string &new_val) {
    Status s;
    std::string val;
    MutexLock l(&mutex_hash_);
    s = HGet(key, field, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);
    } else if (s.ok()) {
        int64_t ival;
        if (!StrToInt64(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not integer");
        } 
        if ((by >= 0 && LLONG_MAX - by < ival) || (by < 0 && LLONG_MIN - by > ival)) {
            return Status::InvalidArgument("Overflow");
        }
        new_val = std::to_string((ival + by));
    } else {
        return Status::Corruption("HIncrby error");
    }
    s = HSetNoLock(key, field, new_val);
    return s;
}

Status Nemo::HIncrbyfloat(const std::string &key, const std::string &field, double by, std::string &new_val) {
    Status s;
    std::string val;
    std::string res;
    MutexLock l(&mutex_hash_);
    s = HGet(key, field, &val);
    if (s.IsNotFound()) {
        res = std::to_string(by);
    } else if (s.ok()) {
        double dval;
        if (!StrToDouble(val.data(), val.size(), &dval)) {
            return Status::Corruption("value is not float");
        }
        
        dval += by;
        if (isnan(dval) || isinf(dval)) {
            return Status::InvalidArgument("Overflow");
        }
        res  = std::to_string(dval);
    } else {
        return Status::Corruption("HIncrbyfloat error");
    }
    size_t pos = res.find_last_not_of("0", res.size());
    pos = pos == std::string::npos ? pos : pos+1;
    new_val = res.substr(0, pos); 
    if (new_val[new_val.size()-1] == '.') {
        new_val = new_val.substr(0, new_val.size()-1);
    }
    s = HSetNoLock(key, field, new_val);
    return s;
}


int Nemo::DoHSet(const std::string &key, const std::string &field, const std::string &val, rocksdb::WriteBatch &writebatch) {
    int ret = 0;
    std::string dbval;
    Status s = HGet(key, field, &dbval);
    if (s.IsNotFound()) { // not found
        std::string hkey = EncodeHashKey(key, field);
        writebatch.Put(hkey, val);
        ret = 1;
    } else {
        if(dbval != val){
            std::string hkey = EncodeHashKey(key, field);
            writebatch.Put(hkey, val);
        }
        ret = 0;
    }
    return ret;
}

int Nemo::DoHDel(const std::string &key, const std::string &field, rocksdb::WriteBatch &writebatch) {
    int ret = 0;
    std::string dbval;
    Status s = HGet(key, field, &dbval);
    if (s.ok()) { 
        std::string hkey = EncodeHashKey(key, field);
        writebatch.Delete(hkey);
        ret = 1;
    } else if(s.IsNotFound()) {
        ret = 0;
    } else {
        ret = -1;
    }
    return ret;
}

int Nemo::IncrHLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch) {
    int64_t len = HLen(key);
    if (len == -1) {
        return -1;
    }
    len += incr;
    std::string size_key = EncodeHsizeKey(key);
    writebatch.Put(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));

   // if (len == 0) {
   //     //writebatch.Delete(size_key);
   //     writebatch.Merge(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
   // } else {
   //     writebatch.Put(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
   // }
    return 0;
}
