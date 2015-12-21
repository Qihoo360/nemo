#include <ctime>
#include <climits>
#include <list>
#include <climits>

#include "nemo.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

Status Nemo::Set(const std::string &key, const std::string &val, const int32_t ttl) {
    Status s;
    if (ttl <= 0) {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
    } else {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, val, ttl);
    }
    return s;
}

Status Nemo::Get(const std::string &key, std::string *val) {
    Status s;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, val);
    return s;
}

Status Nemo::KDel(const std::string &key, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    *res = 0;
    if (s.ok()) {
        s = kv_db_->Delete(rocksdb::WriteOptions(), key);
        *res = 1;
    }
    return s;
}


Status Nemo::MSet(const std::vector<KV> &kvs) {
    Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = kvs.begin(); it != kvs.end(); it++) {
        batch.Put(it->key, it->val); 
    }
    s = kv_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(batch), 0);
    return s;
}

Status Nemo::KMDel(const std::vector<std::string> &keys, int64_t* count) {
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
    s = kv_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(batch));
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

Status Nemo::Incrby(const std::string &key, const int64_t by, std::string &new_val) {
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
        if ((by >= 0 && LLONG_MAX - by < ival) || (by < 0 && LLONG_MIN - by > ival)) {
            return Status::InvalidArgument("Overflow");
        }
        new_val = std::to_string(ival + by);
    } else {
        return Status::Corruption("Get error");
    }
    int64_t ttl;
    s = KTTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Decrby(const std::string &key, const int64_t by, std::string &new_val) {
    Status s;
    std::string val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(-by);        
    } else if (s.ok()) {
        int64_t ival;
        if (!StrToInt64(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not a integer");
        }
        if ((by >= 0 && LLONG_MIN + by > ival) || (by < 0 && LLONG_MAX + by < ival)) {
            return Status::InvalidArgument("Overflow");
        }
        new_val = std::to_string(ival - by);
    } else {
        return Status::Corruption("Get error");
    }
    int64_t ttl;
    s = KTTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Incrbyfloat(const std::string &key, const double by, std::string &new_val) {
    Status s;
    std::string val;
    std::string res;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        res = std::to_string(by);        
    } else if (s.ok()) {
        double dval;
        if (!StrToDouble(val.data(), val.size(), &dval)) {
            return Status::Corruption("value is not a float");
        } 

        dval += by;
        if (isnan(dval) || isinf(dval)) {
            return Status::InvalidArgument("Overflow");
        }
        res = std::to_string(dval);
    } else {
        return Status::Corruption("Get error");
    }
    size_t pos = res.find_last_not_of("0", res.size());
    pos = pos == std::string::npos ? pos : pos+1;
    new_val = res.substr(0, pos); 
    if (new_val[new_val.size()-1] == '.') {
        new_val = new_val.substr(0, new_val.size()-1);
    }
    int64_t ttl;
    s = KTTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
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
    std::string new_val;
    if (s.ok()) {
        new_val = old_val.append(value);
    } else if (s.IsNotFound()) {
        new_val = value;
    } else {
        return s;
    }

    int64_t ttl;
    s = KTTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Setnx(const std::string &key, const std::string &value, int64_t *ret, const int32_t ttl) {
    *ret = 0;
    std::string val;
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        if (ttl <= 0) {
            s = kv_db_->Put(rocksdb::WriteOptions(), key, value);
        } else {
            s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, value, ttl);
        }
        *ret = 1;
    }
    return s;
}

Status Nemo::Setxx(const std::string &key, const std::string &value, int64_t *ret, const int32_t ttl) {
    *ret = 0;
    std::string val;
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        if (ttl <= 0) {
            s = kv_db_->Put(rocksdb::WriteOptions(), key, value);
        } else {
            s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, value, ttl);
        }
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
        s = kv_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(batch), 0);
    }
    return s;
}

Status Nemo::Getrange(const std::string key, const int64_t start, const int64_t end, std::string &substr) {
    substr = "";
    std::string val;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        int64_t size = val.length();
        int64_t start_t = start >= 0 ? start : size + start;
        int64_t end_t = end >= 0 ? end : size + end;
        if (start_t > size - 1 || (start_t != 0 && start_t > end_t) || (start_t != 0 && end_t < 0)) {
            return Status::OK();
        }
        if (start_t < 0) {
            start_t  = 0;
        }
        if (end_t >= size) {
            end_t = size - 1;
        }
        if (start_t == 0 && end_t < 0) {
            end_t = 0;
        }
        substr = val.substr(start_t, end_t-start_t+1);
    }
    return s;
}

Status Nemo::Setrange(const std::string key, const int64_t offset, const std::string &value, int64_t *len) {
    std::string val;
    std::string new_val;
    if (offset < 0) {
        return Status::Corruption("offset < 0");
    }
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        if (val.length() + offset > (1<<29)) {
            return Status::Corruption("too big");
        }
        if ((size_t)offset > val.length()) {
            val.resize(offset);
            new_val = val.append(value);
        } else {
            std::string head = val.substr(0, offset);
            std::string tail;
            if (offset + value.length() - 1 < val.length() -1 ) {
                tail = val.substr(offset+value.length());
            }
            new_val = head + value + tail;
        }
        *len = new_val.length();
    } else if (s.IsNotFound()) {
        std::string tmp(offset, '\0');
        new_val = tmp.append(value);
        *len = new_val.length();
    }
    int64_t ttl;
    s = KTTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Strlen(const std::string &key, int64_t *len) {
    Status s;
    std::string val;
    s = Get(key, &val);
    if (s.ok()) {
        *len = val.length();
    } else if (s.IsNotFound()) {
        *len = 0;
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

Status Nemo::GetStartKey(int64_t cursor, std::string* start_key) {
    //std::string scan_keys_store_pre = "Scan_Keys_Store_";
    std::string scan_keys_store_pre = std::string(100, '\0');;
    std::string store_key = scan_keys_store_pre + std::to_string(cursor + LLONG_MAX/2);
    Status s = Get(store_key, start_key);
    int64_t res;
    if (s.ok()) {
        KDel(store_key, &res);
    }
    return s;
}

int64_t Nemo::StoreAndGetCursor(int64_t cursor, const std::string& next_key) {
    MutexLock l(&mutex_cursors_);
    //std::string scan_keys_store_pre = "Scan_Keys_Store_", store_key, cursor_str;
    std::string scan_keys_store_pre = std::string(100, '\0'), store_key, cursor_str;
    nemo::KIterator* kiter = Scan(scan_keys_store_pre, "", -1);
    std::list<int64_t> cursor_ls;
    bool isExsit = false;
    while (kiter->Next() && kiter->Key().substr(0, scan_keys_store_pre.size()) == scan_keys_store_pre) {
        cursor_ls.push_back(std::stoll(kiter->Key().substr(scan_keys_store_pre.size())) - LLONG_MAX/2);
    }
    delete kiter;
    if (cursor_ls.size() == 0) {
        store_key = scan_keys_store_pre + std::to_string(cursor + LLONG_MAX/2);
        Set(store_key, next_key, 3600);//set 10 days to live
        return cursor;
    }
    std::list<int64_t>::iterator iter_ls = cursor_ls.begin();
    while (iter_ls != cursor_ls.end() && *iter_ls != cursor) {
        iter_ls++;
    }
    if (iter_ls == cursor_ls.end()) {
        store_key = scan_keys_store_pre + std::to_string(cursor + LLONG_MAX/2);
        Set(store_key, next_key, 3600);
        return cursor;
    }
    //cursor_ls.sort();
    //iter_ls = cursor_ls.begin();
    while (iter_ls != cursor_ls.end()) {
        if (cursor < *iter_ls) {
            break;
        } else if (cursor == *iter_ls) {
            cursor++;
            iter_ls++;
        } else {
            iter_ls++;
        }
    }
    store_key = scan_keys_store_pre + std::to_string(cursor + LLONG_MAX/2);
    Set(store_key, next_key, 3600);
    return cursor;
}

bool Nemo::ScanKeysWithTTL(std::unique_ptr<rocksdb::DBWithTTL>& db, std::string& start_key, const std::string& pattern, std::vector<std::string>& keys, int64_t* count, std::string* next_key) {
    assert(*count > 0);
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    bool is_over = true;
    //std::string scan_keys_store_pre = "Scan_Keys_Store_";
    std::string scan_keys_store_pre = std::string(100, '\0');;
    int64_t ttl;
    Status s;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    std::string key;
    it->Seek(start_key);
    while (it->Valid() && (*count) > 0) {
        key = it->key().ToString();
        s = TTL(key, &ttl);
        if (!s.ok() || ttl == 0 || ttl == -2) {
            continue;
        }
        if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0) && key.substr(0, scan_keys_store_pre.size()) != scan_keys_store_pre) {
            keys.push_back(key);
            (*count)--;
        }
        it->Next();
    }
    if (it->Valid()) {//the scan is over in the kv_db_
        is_over = false;
        *next_key = it->key().ToString();
    } else { // the kv_db is over, but the scan is over or not
        *next_key = "";
    }
    delete it;
    return is_over;
}


bool Nemo::ScanKeys(std::unique_ptr<rocksdb::DBWithTTL>& db, const char kType, std::string& start_key, const std::string& pattern, std::vector<std::string>& keys, int64_t* count, std::string *next_key) {
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    bool is_over = true;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    std::string start_key_real;
    start_key_real.assign(1, kType);
    start_key_real.append(start_key.data(), start_key.size());
    it->Seek(start_key_real);
    std::string key;
    while (it->Valid() && *count) {
        key = it->key().ToString();
        if (key.size() == 0 || key.at(0) != kType) {
        break;
        }
        if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
            key.erase(key.begin());//删除第一个标记字母
            keys.push_back(key);
            (*count)--;
        }
        it->Next();
    }
    if (it->Valid() && it->key().ToString().at(0) == kType) {
        is_over = false;
        *next_key = it->key().ToString();
        next_key->erase(next_key->begin());
    } else {
        *next_key = "";
    }
    delete it;

    return is_over;
}

Status Nemo::SeekCursor(int64_t cursor, std::string* start_key) {//the sequence is the same with Scan function
    uint64_t offset, offset_acc = 0;
    Status s;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    rocksdb::Iterator* iter;
    //std::string scan_keys_store_pre = "Scan_Keys_Store_";
    std::string scan_keys_store_pre = std::string(100, '\0');;

    int32_t db_index = 0, index;
    char kType;
    while (cursor >= offset_acc && db_index < 5) {
        switch (db_index) {
            case 0: s = ScanKeyNumWithTTL(kv_db_, offset);
                    iter = kv_db_->NewIterator(iterate_options);
                    iter->Seek(std::string(99, '\0') + char(0x1));
                    break;
            case 1: s = ScanKeyNum(hash_db_, DataType::kHSize, offset);
                    iter = hash_db_->NewIterator(iterate_options);
                    iter->SeekToFirst();
                    break;
            case 2: s = ScanKeyNum(zset_db_, DataType::kZSize, offset);
                    iter = zset_db_->NewIterator(iterate_options);
                    iter->SeekToFirst();
                    break;
            case 3: s = ScanKeyNum(set_db_, DataType::kSSize, offset);
                    iter = set_db_->NewIterator(iterate_options);
                    iter->SeekToFirst();
                    break;
            case 4: s = ScanKeyNum(list_db_, DataType::kLMeta, offset);
                    iter = list_db_->NewIterator(iterate_options);
                    iter->SeekToFirst();
                    break;
        }
        if (!s.ok()) {
            delete iter;
            break;
        }
        if (offset+offset_acc < cursor+1) {
            offset_acc += offset;
        } else {
            //iter->SeekToFirst();
            //iter->Skip(cursor-offset_acc);
            index = cursor-offset_acc;
            while (index > 0) {
                iter->Next();
                index--;
            }
            *start_key = iter->key().ToString();
            if ((*start_key).substr(0, scan_keys_store_pre.size()) != scan_keys_store_pre) {
                delete iter;
                break;
            }
            iter->Seek(scan_keys_store_pre.substr(0, scan_keys_store_pre.size()-1) + static_cast<char>(scan_keys_store_pre.back() + 1));
            if (iter->Valid()) {
                *start_key = iter->key().ToString();
                delete iter;
                break;
            }
        }
        delete iter;
        db_index++;
    }

    if (db_index >= 5 || !s.ok())//cursor too large or scankeys error all set the start_key to the last element
    {
        iter = list_db_->NewIterator(iterate_options);
        iter->SeekToFirst();
        //iter->Skip(cursor);//skip to last position
        while (iter->Valid()) {
            *start_key = iter->key().ToString();
            iter->Next();
        }
        delete iter;
    }
    switch (db_index) {
        case 0: *start_key = std::string("k") + (*start_key); break;
        case 1: start_key->erase(start_key->begin()); *start_key = std::string("h") + *start_key; break;
        case 2: start_key->erase(start_key->begin()); *start_key = std::string("z") + *start_key; break;
        case 3: start_key->erase(start_key->begin()); *start_key = std::string("s") + *start_key; break;
        case 4: start_key->erase(start_key->begin()); *start_key = std::string("l") + *start_key; break;
        default: start_key->erase(start_key->begin()); *start_key = std::string("l") + *start_key; 
    }
    return Status::OK();
}

Status Nemo::Scan(int64_t cursor, std::string& pattern, int64_t count, std::vector<std::string>& keys, int64_t* cursor_ret_ptr) {//the sequence is kv, hash, zset, set, list
    std::string scan_store_key = "Scan_Key_Store";
    std::string start_key = "k";
    std::string next_key;
    std::string field;
    Status s;
    bool is_over;
    int64_t count_origin = count;

    *cursor_ret_ptr = 0;
    keys.clear();
    if (count < 0) {
        //return Status::Corruption("Count is less than 0");
        start_key = "k";
    }
    if (cursor < 0) {
        return Status::OK();
    } else if (cursor > 0) {
        s = GetStartKey(cursor, &start_key);
    }
    if (s.IsNotFound()) {
        //SeekCursor(cursor, &start_key);
        start_key = "k";
    }

    char key_type = start_key.at(0);
    start_key.erase(start_key.begin());
    switch (key_type) {
        case 'k':
            is_over = ScanKeysWithTTL(kv_db_, start_key, pattern, keys, &count, &next_key);
            if (count == 0 && is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("h"));
                break;
            } else if (count == 0 && !is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("k")+next_key);
                break;
            }
            start_key = next_key;
        case 'h':
            is_over = ScanKeys(hash_db_, DataType::kHSize, start_key, pattern, keys, &count, &next_key);
            if (count == 0 && is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("z"));
                break;
            } else if (count == 0 && !is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("h")+next_key);
                break;
            }
            start_key= next_key;
        case 'z':
            is_over = ScanKeys(zset_db_, DataType::kZSize, start_key, pattern, keys, &count, &next_key);
            if (count == 0 && is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("s"));
                break;
            } else if (count == 0 && !is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("z")+next_key);
                break;
            }
            start_key = next_key;
        case 's':
            is_over = ScanKeys(set_db_, DataType::kSSize, start_key, pattern, keys, &count, &next_key);
            if (count == 0 && is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("l"));
                break;
            } else if (count == 0 && !is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("s")+next_key);
                break;
            }
            start_key = next_key;
        case 'l':
            is_over = ScanKeys(list_db_, DataType::kLMeta, start_key, pattern, keys, &count, &next_key);
            start_key = next_key;
            if (!is_over) {
                *cursor_ret_ptr = StoreAndGetCursor(cursor+count_origin, std::string("l")+next_key);
            } else {
                *cursor_ret_ptr = 0;
            }
    }
    return Status::OK();
}

Status Nemo::KExpire(const std::string &key, const int32_t seconds, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
        if (seconds > 0) {
            s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, val, seconds);
        } else { 
            s = kv_db_->Delete(rocksdb::WriteOptions(), key);
        }
        *res = 1;
    }
    return s;
}

Status Nemo::KTTL(const std::string &key, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        *res = -2;
    } else if (s.ok()) {
        int32_t ttl;
        s = kv_db_->GetKeyTTL(rocksdb::ReadOptions(), key, &ttl);
        *res = ttl;
    }
    return s;
}

Status Nemo::KPersist(const std::string &key, int64_t *res) {
    Status s;
    std::string val;

    *res = 0;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        int32_t ttl;
        s = kv_db_->GetKeyTTL(rocksdb::ReadOptions(), key, &ttl);
        if (s.ok() && ttl >= 0) {
            s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
            *res = 1;
        }
    }
    return s;
}

Status Nemo::KExpireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
        std::time_t cur = std::time(0);
        if (timestamp <= cur) {
            s = kv_db_->Delete(rocksdb::WriteOptions(), key);
        } else {
            s = kv_db_->PutWithExpiredTime(rocksdb::WriteOptions(), key, val, timestamp);
        }
        *res = 1;
    }
    return s;
}

// we don't check timestamp here
Status Nemo::SetWithExpireAt(const std::string &key, const std::string &val, const int32_t timestamp) {
    //std::time_t cur = std::time(0);
    Status s;
    if (timestamp <= 0) {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
    } else {
        s = kv_db_->PutWithExpiredTime(rocksdb::WriteOptions(), key, val, timestamp);
    }
    return s;
}

Status Nemo::GetSnapshot(Snapshots &snapshots) {
    const rocksdb::Snapshot* psnap;

    psnap = kv_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = hash_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = zset_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = set_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = list_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    return Status::OK();
}

Status Nemo::ScanKeysWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, Snapshot *snapshot, const std::string pattern, std::vector<std::string>& keys) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = snapshot;
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    it->Seek(std::string(99, '\0') + char(0x1));
    for (; it->Valid(); it->Next()) {
      std::string key = it->key().ToString();
      if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
          keys.push_back(key);
      }
  //     printf ("ScanDB key=(%s) value=(%s) val_size=%u\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
   //           it->value().ToString().size());
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

Status Nemo::ScanKeys(std::unique_ptr<rocksdb::DBWithTTL> &db, Snapshot *snapshot, const char kType, const std::string &pattern, std::vector<std::string>& keys) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = snapshot;
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    std::string key_start = "a";
    key_start[0] = kType;
    it->Seek(key_start);

    for (; it->Valid(); it->Next()) {
      if (kType != it->key().ToString().at(0)) {
        break;
      }
      std::string key = it->key().ToString().substr(1);
      if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
          keys.push_back(key);
      }
//       printf ("ScanDB key=(%s) value=(%s) val_size=%u\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
 //             it->value().ToString().size());
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

// String APIs

Status Nemo::Keys(const std::string &pattern, std::vector<std::string>& keys) {
    Status s;
    std::vector<const rocksdb::Snapshot*> snapshots;

    s = GetSnapshot(snapshots);
    if (!s.ok()) return s;

    s = ScanKeysWithTTL(kv_db_, snapshots[0], pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(hash_db_, snapshots[1], DataType::kHSize, pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(zset_db_, snapshots[2], DataType::kZSize, pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(set_db_, snapshots[3], DataType::kSSize, pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(list_db_, snapshots[4], DataType::kLMeta, pattern, keys);
    if (!s.ok()) return s;

}

// Note: return Status::OK()
Status Nemo::MDel(const std::vector<std::string> &keys, int64_t* count) {
    *count = 0;
    Status s;
    std::string val;
    std::vector<std::string>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = keys.begin(); it != keys.end(); it++) {
        int64_t res;
        s = Del(*it, &res);
        //s = kv_db_->Get(rocksdb::ReadOptions(), *it, &val);
        if (s.ok()) {
            (*count) += res;
        } else if (!s.IsNotFound()) {
          return s;
        }
    }

    return Status::OK();
 //   if (*count > 0) {
 //     return Status::OK();
 //   } else {
 //     return s;
 //   }
}

// Note: return only Status::OK(), not Status::NotFound()
Status Nemo::Del(const std::string &key, int64_t *count) {
    int ok_cnt = 0;
    int64_t del_cnt = 0;
    Status s;
    
    s = KDel(key, count);
    if (s.ok()) { ok_cnt++; del_cnt += *count; }
    else if (!s.IsNotFound()) { return s; }

    s = HDelKey(key, count);
    if (s.ok()) { ok_cnt++; del_cnt += *count; }
    else if (!s.IsNotFound()) { return s; }

    s = ZDelKey(key, count);
    if (s.ok()) { ok_cnt++; del_cnt += *count; }
    else if (!s.IsNotFound()) { return s; }

    s = SDelKey(key, count);
    if (s.ok()) { ok_cnt++; del_cnt += *count; }
    else if (!s.IsNotFound()) { return s; }

    s = LDelKey(key, count);
    if (s.ok()) { ok_cnt++; del_cnt += *count; }
    else if (!s.IsNotFound()) { return s; }

    if (ok_cnt) {
      if (del_cnt > 0) {
        *count = 1;
      } else {
        *count = 0;
      }
    }
    return Status::OK();
}

Status Nemo::Expire(const std::string &key, const int32_t seconds, int64_t *res) {
    int cnt = 0;
    Status s;
    
    s = KExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = HExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = ZExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = SExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = LExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    if (cnt) {
      *res = 1;
      return Status::OK();
    } else {
      return Status::NotFound("");
    }
}

Status Nemo::TTL(const std::string &key, int64_t *res) {
    Status s;
    
    s = KTTL(key, res);
    if (s.ok()) return s;

    s = HTTL(key, res);
    if (s.ok()) return s;

    s = ZTTL(key, res);
    if (s.ok()) return s;

    s = STTL(key, res);
    if (s.ok()) return s;

    s = LTTL(key, res);
    if (s.ok()) return s;

    return s; 
}

Status Nemo::Persist(const std::string &key, int64_t *res) {
    int ok_cnt = 0;
    int res_total = 0;
    Status s;
    
    s = KPersist(key, res);
    if (s.ok()) { ok_cnt++; res_total += *res; }
    else if (!s.IsNotFound()) { return s; }

    s = HPersist(key, res);
    if (s.ok()) { ok_cnt++; res_total += *res; }
    else if (!s.IsNotFound()) { return s; }

    s = ZPersist(key, res);
    if (s.ok()) { ok_cnt++; res_total += *res; }
    else if (!s.IsNotFound()) { return s; }

    s = SPersist(key, res);
    if (s.ok()) { ok_cnt++; res_total += *res; }
    else if (!s.IsNotFound()) { return s; }

    s = LPersist(key, res);
    if (s.ok()) { ok_cnt++; res_total += *res; }
    else if (!s.IsNotFound()) { return s; }

    if (ok_cnt) {
      if (res_total > 0) {
        *res = 1;
      } else {
        *res = 0;
      }
      return Status::OK();
    } else {
      return Status::NotFound("");
    }
}

Status Nemo::Expireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    int cnt = 0;
    Status s;
    
    s = KExpireat(key, timestamp, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = HExpireat(key, timestamp, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = ZExpireat(key, timestamp, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = SExpireat(key, timestamp, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = LExpireat(key, timestamp, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    if (cnt) {
      *res = 1;
      return Status::OK();
    } else {
      return Status::NotFound("");
    }
}
