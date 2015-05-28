#include "nemo.h"
#include "nemo_kv.h"
#include "nemo_iterator.h"
#include "utilities/strings.h"
#include "xdebug.h"
using namespace nemo;

void Nemo::LockKv() {
    pthread_mutex_lock(&(writer_kv_.writer_mutex));
}

void Nemo::UnlockKv() {
    pthread_mutex_unlock(&(writer_kv_.writer_mutex));
}

rocksdb::Status Nemo::Set(const std::string &key, const std::string &val) {
    rocksdb::Status s;
    std::string buf = encode_kv_key(key);
    LockKv();
    s = db_->Put(rocksdb::WriteOptions(), buf, val);
    UnlockKv();
    log_info("Set return %s", s.ToString().c_str());
    return s;
}

rocksdb::Status Nemo::Get(const std::string &key, std::string *val) {
    rocksdb::Status s;
    std::string buf = encode_kv_key(key);
    s = db_->Get(rocksdb::ReadOptions(), buf, val);
    log_info("Get return %s", s.ToString().c_str());
    return s;
}

rocksdb::Status Nemo::Delete(const std::string &key) {
    rocksdb::Status s;
    std::string en_key = encode_kv_key(key);
    LockKv();
    s = db_->Delete(rocksdb::WriteOptions(), en_key);
    UnlockKv();
    log_info("Delete return %s", s.ToString().c_str());
    return s;
}

rocksdb::Status Nemo::MultiSet(const std::vector<Kv> kvs) {
    rocksdb::Status s;
    std::vector<Kv>::const_iterator it;
    writer_kv_.writebatch.Clear();
    for(it = kvs.begin(); it != kvs.end(); it++) {
       writer_kv_.writebatch.Put(encode_kv_key(it->key), it->val); 
    }
    LockKv();
    s = db_->Write(rocksdb::WriteOptions(), &(writer_kv_.writebatch));
    UnlockKv();
    return s;
}

rocksdb::Status Nemo::MultiDel(const std::vector<std::string>& keys) {
    rocksdb::Status s;
    std::vector<std::string>::const_iterator it;
    writer_kv_.writebatch.Clear();
    for(it = keys.begin(); it != keys.end(); it++) {
       writer_kv_.writebatch.Delete(encode_kv_key(*it)); 
    }
    LockKv();
    s = db_->Write(rocksdb::WriteOptions(), &(writer_kv_.writebatch));
    UnlockKv();
    return s;

}

rocksdb::Status Nemo::MultiGet(const std::vector<std::string>& keys, std::vector<Kvs> &kvss) {
    rocksdb::Status s;
    std::vector<std::string>::const_iterator it_key;
    for(it_key = keys.begin(); it_key != keys.end(); it_key++) {
        std::string en_key = encode_kv_key(*it_key);
        std::string val;
        s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
        kvss.push_back((Kvs){*(it_key), val, s});
    }
    return rocksdb::Status::OK();
}

rocksdb::Status Nemo::Incr(const std::string key, int64_t by, std::string &new_val) {
    rocksdb::Status s;
    std::string en_key = encode_kv_key(key);
    std::string val;
    LockKv();
    s = db_->Get(rocksdb::ReadOptions(), en_key, &val);
    if(s.IsNotFound()) {
        new_val = int64_to_str(by);        
    }
    else if(s.ok()) {
        new_val = int64_to_str((str_to_int64(val) + by));
    }
    s = db_->Put(rocksdb::WriteOptions(), en_key, new_val);
    UnlockKv();
    return s;
}

KIterator* Nemo::scan(const std::string &start, const std::string &end, uint64_t limit) {
    std::string key_start, key_end;
    key_start = encode_kv_key(start);
    if(end.empty()){
        key_end = "";
    }else{
        key_end = encode_kv_key(end);
    }
    //dump(key_start.data(), key_start.size(), "scan.start");
    //dump(key_end.data(), key_end.size(), "scan.end");
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = db_->NewIterator(iterate_options);
    it->Seek(start);
    if(it->Valid() && it->key() == start){
        it->Next();
    }
    return new KIterator(new Iterator(it, end, limit)); 
}
