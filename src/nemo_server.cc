#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>

#include "nemo.h"
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

// not used
//Status Nemo::BGSaveReleaseSnapshot(const std::vector<rocksdb::Snapshot *> snapshots) {
//
//    // Note the order which is decided by GetSnapshot
//    kv_db_->ReleaseSnapshot(snapshots[0]);
//    hash_db_->ReleaseSnapshot(snapshots[1]);
//    zset_db_->ReleaseSnapshot(snapshots[2]);
//    set_db_->ReleaseSnapshot(snapshots[3]);
//    list_db_->ReleaseSnapshot(snapshots[4]);
//
//    return Stauts::OK();
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
