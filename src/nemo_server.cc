#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <string>

#include "nemo.h"
#include "nemo_const.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

const std::string DEFAULT_BG_PATH = "dump";

Status Nemo::SaveDBWithTTL(const std::string &db_path, const std::string &key_type, const char meta_prefix, std::unique_ptr<rocksdb::DBWithTTL> &src_db, const rocksdb::Snapshot *snapshot) {
    //printf ("db_path=%s\n", db_path.c_str());
    
    rocksdb::DBWithTTL *dst_db;

    rocksdb::Options option(open_options_);
    option.meta_prefix = meta_prefix;

    rocksdb::Status s = rocksdb::DBWithTTL::Open(option, db_path, &dst_db);
    if (!s.ok()) {
        log_err("save db %s, open error %s", db_path.c_str(), s.ToString().c_str());
        return s;
    }

    //printf ("\nSaveDBWithTTL seqnumber=%d\n", snapshot->GetSequenceNumber());
    int64_t ttl;
    rocksdb::ReadOptions iterate_options;
    iterate_options.snapshot = snapshot;
    iterate_options.fill_cache = false;
    
    rocksdb::Iterator* it = src_db->NewIterator(iterate_options);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string raw_key(it->key().data(), it->key().size());

        if (key_type == KV_DB) {
            s = KTTL(it->key().ToString(), &ttl);
        } else if (key_type == HASH_DB) {
            if (raw_key[0] == meta_prefix) {
                s = HTTL(raw_key.substr(1), &ttl);
                //printf ("0.1 DB:%s meta_prefix=%c raw_key(%s) ttl return %s, ttl is %ld\n", key_type.c_str(), meta_prefix, raw_key.c_str(), s.ToString().c_str(), ttl);
            } else {
                int32_t len = *((uint8_t *)(raw_key.data() + 1));
                std::string key(raw_key.data() + 2, len);
                s = HTTL(key, &ttl);
                //printf ("0.2 DB:%s meta_prefix=%c raw_key(%s) key(%s) ttl return %s, ttl is %ld\n", key_type.c_str(), meta_prefix, raw_key.c_str(), key.c_str(), s.ToString().c_str(), ttl);
            }
        } else if (key_type == LIST_DB) {
            if (raw_key[0] == meta_prefix) {
                s = LTTL(raw_key.substr(1), &ttl);
            } else {
                int32_t len = *((uint8_t *)(raw_key.data() + 1));
                std::string key(raw_key.data() + 2, len);
                s = LTTL(key, &ttl);
            }
        } else if (key_type == ZSET_DB) {
            if (raw_key[0] == meta_prefix) {
                s = ZTTL(raw_key.substr(1), &ttl);
            } else {
                int32_t len = *((uint8_t *)(raw_key.data() + 1));
                std::string key(raw_key.data() + 2, len);
                s = ZTTL(key, &ttl);
            }
        } else if (key_type == SET_DB) {
            if (raw_key[0] == meta_prefix) {
                s = STTL(raw_key.substr(1), &ttl);
            } else {
                int32_t len = *((uint8_t *)(raw_key.data() + 1));
                std::string key(raw_key.data() + 2, len);
                s = STTL(key, &ttl);
            }
        }

        if (s.ok()) {
            if (ttl == -1) {
                s = dst_db->Put(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString());
            } else if (ttl > 0) {
                s = dst_db->PutWithKeyTTL(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString(), ttl);
            }
        }
    }
    delete it;
    //src_db->ReleaseSnapshot(iterate_options.snapshot);
    delete dst_db;

    return Status::OK();
}

Status Nemo::SaveDB(const std::string &db_path, std::unique_ptr<rocksdb::DB> &src_db, const rocksdb::Snapshot *snapshot) {
    if (opendir(db_path.c_str()) == NULL) {
        mkdir(db_path.c_str(), 0755);
    }

    //printf ("db_path=%s\n", db_path.c_str());
    
    rocksdb::DB* dst_db;
    rocksdb::Status s = rocksdb::DB::Open(open_options_, db_path, &dst_db);
    if (!s.ok()) {
        log_err("save db %s, open error %s", db_path.c_str(), s.ToString().c_str());
        return s;
    }

    //printf ("\nSaveDB seqnumber=%d\n", snapshot->GetSequenceNumber());
    rocksdb::ReadOptions iterate_options;
    iterate_options.snapshot = snapshot;
    iterate_options.fill_cache = false;
    
    rocksdb::Iterator* it = src_db->NewIterator(iterate_options);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
       // printf ("SaveDB key=(%s) value=(%s) val_size=%u\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
        //        it->value().ToString().size());

        s = dst_db->Put(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString());
    }
    delete it;
    src_db->ReleaseSnapshot(iterate_options.snapshot);
    delete dst_db;

    return Status::OK();
}

//Status Nemo::BGSaveReleaseSnapshot(Snapshots &snapshots) {
//
//    // Note the order which is decided by GetSnapshot
//    kv_db_->ReleaseSnapshot(snapshots[0]);
//    hash_db_->ReleaseSnapshot(snapshots[1]);
//    zset_db_->ReleaseSnapshot(snapshots[2]);
//    set_db_->ReleaseSnapshot(snapshots[3]);
//    list_db_->ReleaseSnapshot(snapshots[4]);
//
//    return Status::OK();
//}

Status Nemo::BGSaveGetSpecifySnapshot(const std::string key_type, Snapshot * &snapshot) {

    if (key_type == KV_DB) {
      snapshot = kv_db_->GetSnapshot();
      if (snapshot == nullptr) {
        return Status::Corruption("GetSnapshot failed");
      }
    } else if (key_type == HASH_DB) {
      snapshot = hash_db_->GetSnapshot();
      if (snapshot == nullptr) {
        return Status::Corruption("GetSnapshot failed");
      }
    } else if (key_type == LIST_DB) {
      snapshot = list_db_->GetSnapshot();
      if (snapshot == nullptr) {
        return Status::Corruption("GetSnapshot failed");
      }
    } else if (key_type == ZSET_DB) {
      snapshot = zset_db_->GetSnapshot();
      if (snapshot == nullptr) {
        return Status::Corruption("GetSnapshot failed");
      }
    } else if (key_type == SET_DB) {
      snapshot = set_db_->GetSnapshot();
      if (snapshot == nullptr) {
        return Status::Corruption("GetSnapshot failed");
      }
    } else {
        return Status::InvalidArgument("");
    }
    return Status::OK();
}

Status Nemo::BGSaveGetSnapshot(Snapshots &snapshots) {
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

struct SaveArgs {
  void *p;
  const std::string key_type;
  Snapshot *snapshot;

  SaveArgs(void *_p, const std::string &_key_type, Snapshot* _snapshot)
      : p(_p), key_type(_key_type), snapshot(_snapshot) {};
};


void* call_BGSaveSpecify(void *arg) {

    Nemo* p = (Nemo*)(((SaveArgs*)arg)->p);
    Snapshot* snapshot = ((SaveArgs*)arg)->snapshot;
    std::string key_type = ((SaveArgs*)arg)->key_type;
    Status s = p->BGSaveSpecify(key_type, snapshot);
    return nullptr;
}

Status Nemo::BGSaveSpecify(const std::string key_type, Snapshot* snapshot) {
//void* Nemo::BGSaveSpecify(void *arg) {
    Status s;

    if (key_type == KV_DB) {
      s = SaveDBWithTTL(dump_path_ + KV_DB, key_type, '\0', kv_db_, snapshot);
    } else if (key_type == HASH_DB) {
      s = SaveDBWithTTL(dump_path_ + HASH_DB, key_type, DataType::kHSize, hash_db_, snapshot);
    } else if (key_type == ZSET_DB) {
      s = SaveDBWithTTL(dump_path_ + ZSET_DB, key_type, DataType::kZSize, zset_db_, snapshot);
    } else if (key_type == SET_DB) {
      s = SaveDBWithTTL(dump_path_ + SET_DB, key_type, DataType::kSSize, set_db_, snapshot);
    } else if (key_type == LIST_DB) {
      s = SaveDBWithTTL(dump_path_ + LIST_DB, key_type, DataType::kLMeta, list_db_, snapshot);
    } else {
      s = Status::InvalidArgument("");
    }

    {
        MutexLock l(&mutex_dump_);
        dump_pthread_ts_.erase(key_type);
    }
    return s;
}


Status Nemo::BGSave(Snapshots &snapshots, const std::string &db_path) {
    std::string path = db_path;
    if (path.empty()) {
        path = DEFAULT_BG_PATH;
    }

    if (path[path.length() - 1] != '/') {
        path.append("/");
    }
    if (opendir(path.c_str()) == NULL) {
        mkpath(path.c_str(), 0755);
    }

    {
        MutexLock l(&mutex_dump_);
        if (dump_pthread_ts_.size() != 0 || dump_snapshots_.size() != 0) {
            return Status::Corruption("DB dumping is performing.");
        }
        dump_path_ = path;
    }

    Status s;

    pthread_t tid[5];
    SaveArgs *arg_kv = new SaveArgs(this, KV_DB, snapshots[0]);
    if (pthread_create(&tid[0], NULL, &call_BGSaveSpecify, arg_kv) != 0) {
        return Status::Corruption("pthead_create failed.");
    }

    SaveArgs *arg_hash = new SaveArgs(this, HASH_DB, snapshots[1]);
    if (pthread_create(&tid[1], NULL, &call_BGSaveSpecify, arg_hash) != 0) {
        return Status::Corruption("pthead_create failed.");
    }

    SaveArgs *arg_zset = new SaveArgs(this, ZSET_DB, snapshots[2]);
    if (pthread_create(&tid[2], NULL, &call_BGSaveSpecify, arg_zset) != 0) {
        return Status::Corruption("pthead_create failed.");
    }

    SaveArgs *arg_set = new SaveArgs(this, SET_DB, snapshots[3]);
    if (pthread_create(&tid[3], NULL, &call_BGSaveSpecify, arg_set) != 0) {
        return Status::Corruption("pthead_create failed.");
    }

    SaveArgs *arg_list = new SaveArgs(this, LIST_DB, snapshots[4]);
    if (pthread_create(&tid[4], NULL, &call_BGSaveSpecify, arg_list) != 0) {
        return Status::Corruption("pthead_create failed.");
    }

    {
        MutexLock l(&mutex_dump_);
        dump_pthread_ts_[KV_DB] = tid[0];
        dump_pthread_ts_[HASH_DB] = tid[1];
        dump_pthread_ts_[ZSET_DB] = tid[2];
        dump_pthread_ts_[SET_DB] = tid[3];
        dump_pthread_ts_[LIST_DB] = tid[4];
        dump_snapshots_ = snapshots;
    }

    int ret;
    void *retval;
    for (int i = 0; i < 5; i++) {
      if ((ret = pthread_join(tid[i], &retval)) != 0) {
          std::string msg = std::to_string(ret);
          return Status::Corruption("pthead_join failed with " + msg);
      }
    }

    {
        MutexLock l(&mutex_dump_);
        kv_db_->ReleaseSnapshot(dump_snapshots_[0]);
        hash_db_->ReleaseSnapshot(dump_snapshots_[1]);
        zset_db_->ReleaseSnapshot(dump_snapshots_[2]);
        set_db_->ReleaseSnapshot(dump_snapshots_[3]);
        list_db_->ReleaseSnapshot(dump_snapshots_[4]);
        dump_pthread_ts_.clear();
        dump_snapshots_.clear();
    }

    delete arg_kv;
    delete arg_hash;
    delete arg_zset;
    delete arg_set;
    delete arg_list;

    return Status::OK();
}

Status Nemo::BGSaveOff() {
    MutexLock l(&mutex_dump_);
    if (dump_snapshots_.size() == 0 && dump_pthread_ts_.size() == 0) {
        return Status::Corruption("there is no dumping threads");
    }
    for (std::map<std::string, pthread_t>::iterator iter = dump_pthread_ts_.begin(); iter != dump_pthread_ts_.end(); iter++) {
        if (iter->second != 0) {
            pthread_cancel(iter->second);
        }
    }
    kv_db_->ReleaseSnapshot(dump_snapshots_[0]);
    hash_db_->ReleaseSnapshot(dump_snapshots_[1]);
    zset_db_->ReleaseSnapshot(dump_snapshots_[2]);
    set_db_->ReleaseSnapshot(dump_snapshots_[3]);
    list_db_->ReleaseSnapshot(dump_snapshots_[4]);
    dump_pthread_ts_.clear();
    dump_snapshots_.clear();
    std::string dump_path_failed;
    if (dump_path_[dump_path_.length()-1] == '/') {
        dump_path_failed = dump_path_.substr(0, dump_path_.length()-1) + "_FAILED";
    } else {
        dump_path_failed = dump_path_ + "_FAILED";
    }
    rename(dump_path_.c_str(), dump_path_failed.c_str());
    return Status::OK();
}

#if 0
Status Nemo::BGSave(Snapshots &snapshots, const std::string &db_path) {
    if (save_flag_) {
        return Status::Corruption("Already saving");
    }

    // maybe need lock
    save_flag_ = true;

    std::string path = db_path;
    if (path.empty()) {
        path = DEFAULT_BG_PATH;
    }
    
    if (path[path.length() - 1] != '/') {
        path.append("/");
    }
    if (opendir(path.c_str()) == NULL) {
        mkpath(path.c_str(), 0755);
    }

    Status s;
    s = SaveDBWithTTL(path + KV_DB, kv_db_, snapshots[0]);
    if (!s.ok()) return s;
    
    s = SaveDB(path + HASH_DB, hash_db_, snapshots[1]);
    if (!s.ok()) return s;

    s = SaveDB(path + ZSET_DB, zset_db_, snapshots[2]);
    if (!s.ok()) return s;

    s = SaveDB(path + SET_DB, set_db_, snapshots[3]);
    if (!s.ok()) return s;

    s = SaveDB(path + LIST_DB, list_db_, snapshots[4]);
    if (!s.ok()) return s;
    
    save_flag_ = false;

    //BGSaveReleaseSnapshot(snapshots);
    
    return Status::OK();
}
#endif

Status Nemo::ScanKeyNumWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, uint64_t &num) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = db->GetSnapshot();
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    num = 0;
    it->Seek(std::string(99, '\0')+ char(0x1));
    for (; it->Valid(); it->Next()) {
        num++;
       //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
       //       it->value().ToString().size(), num);
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

Status Nemo::ScanKeyNum(std::unique_ptr<rocksdb::DBWithTTL> &db, const char kType, uint64_t &num) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = db->GetSnapshot();
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);
    std::string key_start = "a";
    key_start[0] = kType;
    it->Seek(key_start);

    num = 0;
    for (; it->Valid(); it->Next()) {
      if (kType != it->key().ToString().at(0)) {
        break;
      }
      num++;
       //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
       //       it->value().ToString().size(), num);
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

Status Nemo::GetSpecifyKeyNum(const std::string type, uint64_t &num) {
    if (type == KV_DB) {
      ScanKeyNumWithTTL(kv_db_, num);
    } else if (type == HASH_DB) {
      ScanKeyNum(hash_db_, DataType::kHSize, num);
    } else if (type == LIST_DB) {
      ScanKeyNum(list_db_,  DataType::kLMeta, num);
    } else if (type == ZSET_DB) {
      ScanKeyNum(zset_db_, DataType::kZSize, num);
    } else if (type == SET_DB) {
      ScanKeyNum(set_db_, DataType::kSSize, num);
    } else {
      return Status::InvalidArgument("");
    }

    return Status::OK();
}

Status Nemo::GetKeyNum(std::vector<uint64_t>& nums) {
    uint64_t num;

    ScanKeyNumWithTTL(kv_db_, num);
    nums.push_back(num);

    ScanKeyNum(hash_db_, DataType::kHSize, num);
    nums.push_back(num);

    ScanKeyNum(list_db_,  DataType::kLMeta, num);
    nums.push_back(num);

    ScanKeyNum(zset_db_, DataType::kZSize, num);
    nums.push_back(num);

    ScanKeyNum(set_db_, DataType::kSSize, num);
    nums.push_back(num);

    return Status::OK();
}

Status Nemo::Compact(){
    Status s;
    s = kv_db_ -> CompactRange(NULL,NULL);
    if (!s.ok()) return s;
    s = hash_db_ -> CompactRange(NULL,NULL);
    if (!s.ok()) return s;
    s = zset_db_ -> CompactRange(NULL,NULL);
    if (!s.ok()) return s;
    s = set_db_ -> CompactRange(NULL,NULL);
    if (!s.ok()) return s;
    s = list_db_ -> CompactRange(NULL,NULL);
    if (!s.ok()) return s;
    return Status::OK();
}
