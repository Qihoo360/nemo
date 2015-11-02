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

//Status Nemo::DebugObject(const std::string &type, const std::string &key, std::string *reslut) {
//    Status s;
//    if (type == "kv") {
//        std::string val;
//        s = src_db->Get(rocksdb::ReadOptions(), key, val);
//        *result = "type:kv  valuelength:" + val.length();
//    } else if (type == "hash") {
//}

Status Nemo::SaveDBWithTTL(const std::string &db_path, std::unique_ptr<rocksdb::DBWithTTL> &src_db, const rocksdb::Snapshot *snapshot) {
    if (opendir(db_path.c_str()) == NULL) {
        mkdir(db_path.c_str(), 0755);
    }

    //printf ("db_path=%s\n", db_path.c_str());
    rocksdb::Options options;
    options.create_if_missing = true;
    //options.write_buffer_size = (1 << 27);
    
    rocksdb::DBWithTTL *dst_db;
    rocksdb::Status s = rocksdb::DBWithTTL::Open(options, db_path, &dst_db);
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
        TTL(it->key().ToString(), &ttl);
        //printf ("SaveDBWithTTL key=(%s) value=(%s) val_size=%u, ttl=%ld\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
         //       it->value().ToString().size(), ttl);

        if (ttl <= 0) {
            s = dst_db->Put(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString());
        } else {
            s = dst_db->PutWithKeyTTL(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString(), ttl);
        }
    }
    delete it;
    src_db->ReleaseSnapshot(iterate_options.snapshot);
    delete dst_db;

    return Status::OK();
}

Status Nemo::SaveDB(const std::string &db_path, std::unique_ptr<rocksdb::DB> &src_db, const rocksdb::Snapshot *snapshot) {
    if (opendir(db_path.c_str()) == NULL) {
        mkdir(db_path.c_str(), 0755);
    }

    //printf ("db_path=%s\n", db_path.c_str());
    rocksdb::Options options;
    options.create_if_missing = true;
    //options.write_buffer_size = (1 << 27);
    
    rocksdb::DB* dst_db;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &dst_db);
    if (!s.ok()) {
        log_err("save db %s, open error %s", db_path.c_str(), s.ToString().c_str());
        return s;
    }

    //printf ("\nSaveDB seqnumber=%d\n", snapshot->GetSequenceNumber());
    int64_t ttl;
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
    s = SaveDBWithTTL(path + "kv", kv_db_, snapshots[0]);
    if (!s.ok()) return s;
    
    s = SaveDB(path + "hash", hash_db_, snapshots[1]);
    if (!s.ok()) return s;

    s = SaveDB(path + "zset", zset_db_, snapshots[2]);
    if (!s.ok()) return s;

    s = SaveDB(path + "set", set_db_, snapshots[3]);
    if (!s.ok()) return s;

    s = SaveDB(path + "list", list_db_, snapshots[4]);
    if (!s.ok()) return s;
    
    save_flag_ = false;

    //BGSaveReleaseSnapshot(snapshots);
    
    return Status::OK();
}

Status Nemo::ScanKeyNumWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, uint64_t &num) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = db->GetSnapshot();
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    num = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        num++;
       //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
       //       it->value().ToString().size(), num);
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

Status Nemo::ScanKeyNum(std::unique_ptr<rocksdb::DB> &db, const char kType, uint64_t &num) {
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
    if (type == "kv") {
      ScanKeyNumWithTTL(kv_db_, num);
    } else if (type == "hash") {
      ScanKeyNum(hash_db_, DataType::kHSize, num);
    } else if (type == "list") {
      ScanKeyNum(list_db_,  DataType::kLMeta, num);
    } else if (type == "zset") {
      ScanKeyNum(zset_db_, DataType::kZSize, num);
    } else if (type == "set") {
      ScanKeyNum(set_db_, DataType::kSSize, num);
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
