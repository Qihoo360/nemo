#include "nemo_kv.h"
#include "nemo_iterator.h"
#include "xdebug.h"

rocksdb::Status nemo::Nemo::Set(const std::string &key, const std::string &val) {
    rocksdb::Status s;
    std::string buf = encode_kv_key(key);
    s = db_->Put(rocksdb::WriteOptions(), buf, val);
    log_info("Set return  %s", s.ToString().c_str());
    return s;
}

rocksdb::Status nemo::Nemo::Get(const std::string &key, std::string *val) {
    rocksdb::Status s;
    std::string buf = encode_kv_key(key);
    s = db_->Get(rocksdb::ReadOptions(), buf, val);
    log_info("Get return %s", s.ToString().c_str());
    return s;
}

nemo::KIterator* nemo::Nemo::scan(const rocksdb::Slice &start, const rocksdb::Slice &end, uint64_t limit) {
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
    it->Seek(start.ToString());
    if(it->Valid() && it->key() == start){
        it->Next();
    }
    return new KIterator(new Iterator(it, end.ToString(), limit)); 
}
