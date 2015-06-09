#include "nemo.h"
#include "nemo_hash.h"
#include "nemo_iterator.h"
#include "utilities/strings.h"
#include "xdebug.h"
using namespace nemo;

rocksdb::Status Nemo::HSet(const std::string &key, const std::string &field, const std::string &val) {
    rocksdb::Status s;
    MutexLock l(&mutex_hash_);
    rocksdb::WriteBatch writebatch;
    int ret = DoHSet(key, field, val, writebatch);
    if(ret > 0){
        if(IncrHLen(key, ret, writebatch) == -1) {
            return rocksdb::Status::Corruption("incrhlen error");
        }
    }
    s = db_->Write(rocksdb::WriteOptions(), &(writebatch));
    return s;
}

rocksdb::Status Nemo::HGet(const std::string &key, const std::string &field, std::string *val) {
    std::string dbkey = encode_hash_key(key, field);
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), dbkey, val);
    return s;
}

rocksdb::Status Nemo::HDel(const std::string &key, const std::string &field) {
    rocksdb::Status s;
    MutexLock l(&mutex_hash_);
    rocksdb::WriteBatch writebatch;
    int ret = DoHDel(key, field, writebatch);
    if(ret > 0) {
        if(IncrHLen(key, -ret, writebatch) == -1) {
            return rocksdb::Status::Corruption("incrlen error");
        } 
        s = db_->Write(rocksdb::WriteOptions(), &(writebatch));
        return s;
    }else if (ret == 0) {
        return rocksdb::Status::OK(); 
    }else {
        return rocksdb::Status::Corruption("DoHDel error");
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

rocksdb::Status Nemo::HKeys(const std::string &key, std::vector<std::string> &fields) {
    std::string dbkey;
    std::string dbfield;
    std::string key_start = encode_hash_key(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while(it->Valid()) {
       if((it->key()).data()[0] != DataType::kHash){
           break;
       }
       decode_hash_key(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           fields.push_back(dbfield);
       }else {
           break;
       }
       it->Next();
    }
    return rocksdb::Status::OK();

}

int64_t Nemo::HLen(const std::string &key) {
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

rocksdb::Status Nemo::HGetall(const std::string &key, std::vector<FV> &fvs) {
    std::string dbkey;
    std::string dbfield;
    std::string key_start = encode_hash_key(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while(it->Valid()) {
       if((it->key()).data()[0] != DataType::kHash){
           break;
       }
       decode_hash_key(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           fvs.push_back(FV{dbfield, it->value().ToString()});
       }else {
           break;
       }
       it->Next();
    }
    return rocksdb::Status::OK();
}

//int64_t Nemo::HLen(const std::string &key) {
//    int64_t len = 0;
//    std::string dbkey;
//    std::string dbfield;
//    std::string key_start = encode_hash_key(key, "");
//    rocksdb::Iterator *it;
//    rocksdb::ReadOptions iterate_options;
//    iterate_options.fill_cache = false;
//    it = db_->NewIterator(iterate_options);
//    it->Seek(key_start);
//    while(it->Valid()) {
//       if((it->key()).data()[0] != DataType::kHash){
//           break;
//       }
//       decode_hash_key(it->key(), &dbkey, &dbfield);
//       if(dbkey == key) {
//           len++;
//       }else {
//           break;
//       }
//       it->Next();
//    }
//    return len;
//}

rocksdb::Status Nemo::HMSet(const std::string &key, const std::vector<FV> &fvs) {
    if(key.size() == 0) {
        return rocksdb::Status::InvalidArgument("Invalid Argument");
    }
    rocksdb::Status s;
    std::vector<FV>::const_iterator it;
    for(it = fvs.begin(); it != fvs.end(); it++) {
        HSet(key, it->field, it->val);
    }
    return s;
}

rocksdb::Status Nemo::HMGet(const std::string &key, const std::vector<std::string> &fields, std::vector<FVS> &fvss) {
    rocksdb::Status s;
    std::vector<std::string>::const_iterator it_key;
    for(it_key = fields.begin(); it_key != fields.end(); it_key++) {
        std::string en_key = encode_hash_key(key, *(it_key));
        std::string val("");
        s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
        fvss.push_back((FVS){*(it_key), val, s});
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

rocksdb::Status Nemo::HSetnx(const std::string &key, const std::string &field, const std::string &val) {
    rocksdb::Status s;
    std::string str_val;
    MutexLock l(&mutex_hash_);
    s = HGet(key, field, &str_val);
    if(s.IsNotFound()) {
        rocksdb::WriteBatch writebatch;
        int ret = DoHSet(key, field, val, writebatch);
        if(ret > 0){
            if(IncrHLen(key, ret, writebatch) == -1) {
                return rocksdb::Status::Corruption("incrhlen error");
            }
        }
        s = db_->Write(rocksdb::WriteOptions(), &(writebatch));
        return s;
    } else if(s.ok()) {
        return rocksdb::Status::Corruption("Already Exist");
    } else {
        return rocksdb::Status::Corruption("HGet Error");
    }
}

int64_t Nemo::HStrlen(const std::string &key, const std::string &field) {
    rocksdb::Status s;
    std::string val;
    s = HGet(key, field, &val);
    if(s.ok()) {
        return val.length();
    } else if(s.IsNotFound()) {
        return 0;
    } else {
        return -1;
    }
}

rocksdb::Status Nemo::HVals(const std::string &key, std::vector<std::string> &vals) {
    std::string dbkey;
    std::string dbfield;
    std::string key_start = encode_hash_key(key, "");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(key_start);
    while(it->Valid()) {
       if((it->key()).data()[0] != DataType::kHash){
           break;
       }
       decode_hash_key(it->key(), &dbkey, &dbfield);
       if(dbkey == key) {
           vals.push_back(it->value().ToString());
       }else {
           break;
       }
       it->Next();
    }
    return rocksdb::Status::OK();
}

rocksdb::Status Nemo::HIncrby(const std::string &key, const std::string &field, int64_t by, std::string &new_val) {
    rocksdb::Status s;
    std::string val;
    s = HGet(key, field, &val);
    if(s.IsNotFound()) {
        new_val = int64_to_str(by);
    } else if(s.ok()) {
        new_val = int64_to_str((str_to_int64(val) + by));
    } else {
        return rocksdb::Status::Corruption("HGet error");
    }
    s = HSet(key, field, new_val);
    return s;
}

int Nemo::DoHSet(const std::string &key, const std::string &field, const std::string &val, rocksdb::WriteBatch &writebatch) {
    int ret = 0;
    std::string dbval;
    rocksdb::Status s = HGet(key, field, &dbval);
    if(s.IsNotFound()) { // not found
        std::string hkey = encode_hash_key(key, field);
        writebatch.Put(hkey, val);
        ret = 1;
    }else {
        if(dbval != val){
            std::string hkey = encode_hash_key(key, field);
            writebatch.Put(hkey, val);
        }
        ret = 0;
    }
    return ret;
}

int Nemo::DoHDel(const std::string &key, const std::string &field, rocksdb::WriteBatch &writebatch) {
    int ret = 0;
    std::string dbval;
    rocksdb::Status s = HGet(key, field, &dbval);
    if(s.ok()) { 
        std::string hkey = encode_hash_key(key, field);
        writebatch.Delete(hkey);
        ret = 1;
    }else if(s.IsNotFound()) {
        ret = 0;
    }else {
        ret = -1;
    }
    return ret;
}

int Nemo::IncrHLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch){
    int64_t len = HLen(key);
    if(len == -1) {
        return -1;
    }
    len += incr;
    std::string size_key = encode_hsize_key(key);
    if(len == 0) {
        writebatch.Delete(size_key);
    }else{
        writebatch.Put(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
    }
    return 0;
}
